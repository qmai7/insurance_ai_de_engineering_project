# Serving APIs — fraud-prediction-api + model-server

Two services, one hop apart. `fraud-prediction-api` is the public entry point:
it takes an ID, finds the features, and returns a decision. `model-server`
holds the model in memory and answers with a probability.

Both live in `api-serving-ns`, alongside the Redis they depend on.

---

## 1. Why two services and not one

The obvious alternative is one FastAPI app that loads the model itself. It was
rejected for three reasons, all of which show up as work you would otherwise
have to do twice:

- **They scale on different things.** The API is I/O-bound — a Redis read and
  an HTTP call — so it wants many cheap replicas. The model-server is
  CPU-bound and holds a model plus scikit-learn and pandas in resident memory,
  so it wants few expensive ones. Merged, one of the two is always sized wrong,
  and the KEDA request-rate autoscaler in §1 would be scaling an sklearn
  process by request count.
- **A/B testing needs two models, not two APIs.** §13 compares a champion
  against a challenger. If the model lives inside the API, splitting traffic
  between models means splitting traffic between whole API deployments — and
  then the comparison includes every difference between those deployments, not
  just the model. Separating them means the split happens on the one hop where
  the *only* difference is which model answered.
- **Their dependency stacks are disjoint.** The API needs `feast` and `gcsfs`
  and imports no ML library. The model-server needs `mlflow` and
  `scikit-learn` and never touches Feast. One image would ship each stack to a
  workload that never imports it, and every API bugfix would rebuild and
  redeploy an sklearn layer.

The cost is one network hop and one more image in CI. That hop is what §12's
Tempo traces are drawn across, so it is not purely overhead.

### The request path

```
        POST /predict {claim_id, customer_id}
                    │
                    ▼
    ┌───────────────────────────────────┐
    │ fraud-prediction-api              │
    │   1. pydantic validation          │
    │   2. Feast online read ──────────────►  Redis (api-serving-ns)
    │   3. build the feature row        │
    │   4. POST to model-server ────────┐
    │   6. threshold -> is_fraud        ││
    │   7. fire drift-api (async, §2)   ││
    └───────────────────────────────────┘│
                    ▲                    │
                    │                    ▼
                    │   ┌──────────────────────────────────┐
                    │   │ Service: model-server            │
                    │   │   VirtualService weights         │
                    │   ├──────────────┬───────────────────┤
                    └───┤ champion     │ challenger        │
   probability +        │ @production  │ @challenger       │
   x-model-role/version │              │ (§13, off by      │
                        │              │  default)         │
                        └──────────────┴───────────────────┘
                                 │
                                 ▼
                    GCS: the promoted MLflow artifact
```

Steps 2 and 4 are the only two network calls on the hot path, and both are
awaited. Step 7 is not — see §5.

---

## 2. fraud-prediction-api

### The contract

`POST /predict` takes two IDs and nothing else:

```json
{ "claim_id": "claim-0001", "customer_id": "cust-0042" }
```

This is the whole point of the service, and the rubric's name for it — "the
data-pulling API" — is the requirement: **the caller does not send features.**
A claims system knows a claim ID; it does not know
`f_customer_payment_failure_rate_90d`. If the caller had to supply features,
the feature store would be pointless and every caller would be free to compute
a feature slightly differently from how training computed it.

The response says what was decided and who decided it:

```json
{
  "claim_id": "claim-0001",
  "customer_id": "cust-0042",
  "fraud_probability": 0.87,
  "is_fraud": true,
  "threshold": 0.5,
  "model_name": "fraud-detector",
  "model_role": "champion",
  "model_version": "3",
  "request_id": "0f9c..."
}
```

`fraud_probability` and `is_fraud` are both returned deliberately.
`is_fraud` is `probability >= threshold`, and `threshold` is echoed so the
caller can see the operating point it was judged against — which is a review
capacity, not a property of the model (see [`docs/ml.md`](ml.md)). A caller
that disagrees with the threshold can re-derive its own decision from the
probability without a redeployment.

### Validation

Pydantic models in
[`fraud_prediction_api/models.py`](../fraud_prediction_api/models.py), which is
what makes a malformed request a 422 rather than something the handler has to
defend against:

| Field | Constraint | Why |
|---|---|---|
| `claim_id`, `customer_id` | `min_length=1` | an empty string is a valid `str` and a guaranteed Redis miss; rejecting it early turns a 404 into a 422, which tells the caller the truth |
| `fraud_probability` | `ge=0.0, le=1.0` | on the **response** model, so a model-server returning a nonsense score fails here rather than reaching a caller who trusts it |
| `threshold` | `ge=0.0, le=1.0` | a misconfigured `FRAUD_THRESHOLD` becomes a startup-visible error, not a service that silently flags everything |

The probability bound is checked twice on purpose — once by
`ModelServerClient` when it reads the response, and once by the pydantic
response model. The client check produces a 502 (the upstream is wrong); the
pydantic check is the backstop for any other path that constructs a response.

### Status codes

Each one distinguishes a different party's fault, which is what makes the
metrics in §12 readable — a rise in 404s and a rise in 502s are different
incidents:

| Code | Means | Cause |
|---|---|---|
| 422 | the caller sent something invalid | pydantic |
| 404 | the IDs are fine, the features are not there | `FeatureLookupError` — not yet materialized, or the Redis TTL expired |
| 502 | the model-server failed or answered nonsense | `InferenceError` |
| 503 | this pod is not ready | Feast store failed to initialise at startup |

The 404 is worth dwelling on. A missing feature is **not** an error to paper
over with a default value: imputing a zero for
`f_customer_total_claims_90d` produces a confident, wrong prediction that
nothing downstream can distinguish from a real one.
`FeastOnlineFeatureRepository` therefore compares what came back against
`config.MODEL_FEATURES` and raises if anything is missing or `None`, naming the
missing features in the message.

### Health checks

Two endpoints, and the split matters more than it looks:

- **`/healthz`** (liveness) — the process is up. It deliberately reports
  nothing about the Feast store or the model. A liveness probe that failed on a
  broken dependency would restart a pod that restarting cannot fix, and you
  would get a crash loop instead of a diagnosis.
- **`/readyz`** (readiness) — the Feast store is initialised. A pod failing
  this is removed from the Service's endpoints, so it stops receiving traffic
  without being killed. It stays up, and its logs stay readable.

Startup is written to make that distinction real. The Feast registry lives in
GCS, so constructing the store is a network call — and the lifespan tolerates
its failure rather than raising:

```python
app.state.features = None
try:
    app.state.features = FeastOnlineFeatureRepository(FeatureStore(...))
except Exception:
    logger.exception("Feast store init failed; API will stay not-ready")
```

A transient GCS problem therefore produces a pod that is up, not-ready, and
says why in its logs — rather than a `CrashLoopBackOff` whose only signal is
the restart count. Every handler reads the store through one `_features()`
helper that raises 503 when it is `None`, so a failed startup can never surface
as an `AttributeError` 500.

### Async

Fully async, and the two blocking things are handled differently because they
are different problems:

- **`httpx.AsyncClient`** for the model-server call — natively async, created
  once in the lifespan and closed on shutdown. One client, not one per
  request, so the connection pool is reused; a per-request client re-does the
  TCP and TLS handshake on every prediction.
- **`asyncio.to_thread`** for the Feast read. `store.get_online_features()` is
  synchronous — Feast's Redis client blocks — and calling it directly from a
  coroutine would stall the event loop for the whole round trip, blocking
  *every* concurrent request rather than just this one. That is the
  single-worker failure mode that makes async services perform worse than sync
  ones under load.

---

## 3. model-server

### What it does at startup

1. Resolve the registry alias (`production` or `challenger`) to a `gs://` path.
2. Download and deserialise the artifact from that path.
3. Keep it in memory, and record which version it is.

Step 1 goes through `ModelRegistryService.aliased_model_location()` — the same
class the training pipeline uses (§7), so the registry stays the single source
of truth for where a model lives. Promotion moves the alias; the location
follows. Nothing in the chart names a GCS path.

Step 2 uses `mlflow.sklearn.load_model`, not `mlflow.pyfunc`. Pyfunc's
`predict` returns a classifier's default output, which is a hard 0/1 label —
and this service has to return a probability, because the API's threshold and
every drift metric in §12 operate on the score, not the label.

**Loading happens once, at startup, not per request.** A GCS download plus an
sklearn deserialise is hundreds of milliseconds and would dominate the latency
SLA §9 load-tests against. The consequence is deliberate and worth stating
plainly: *moving the registry alias does not change what a running pod serves.*
A pod is pinned to the version it started with, which is exactly what makes the
traffic weight the only variable during a §13 ramp. Picking up a new version is
a rollout.

### The wire protocol

KServe's v1 shape, kept even though KServe is not deployed:

```
POST /v1/models/fraud-detector:predict
  {"instances": [[<one value per config.MODEL_FEATURES entry>]]}
  -> {"predictions": [0.87]}
     x-model-role: champion
     x-model-version: 3
```

It is a published contract that Triton, MLServer and TorchServe all speak, so a
real inference server can be dropped in behind the same Service without
touching the caller. Inventing a bespoke payload would have bought nothing and
closed that door.

Column order is positional, which is the risk in this design: transpose two
values and you get a plausible, wrong prediction with no error. Both sides
therefore build the row from one constant, `ml.config.MODEL_FEATURES` — the
same list the training matrix is built from.

`PredictRequest` validates row width against that list, so a wrong-width row is
a 422 naming the expected width instead of a pandas reshape error surfacing
from inside the `ColumnTransformer` as a 500. Caller error stays caller error.

### Not re-deriving the dtypes

The model's first stage is a `ColumnTransformer` that selects columns *by name*,
so a bare array will not do — the frame has to carry the same column names and
dtypes training used. `_predict_sync` calls
`ModelBuilder.to_matrix`, the same coercion the training pipeline runs, rather
than reimplementing it:

```python
frame = pd.DataFrame(instances, columns=config.MODEL_FEATURES)
matrix = ModelBuilder.to_matrix(frame)
scores = model.predict_proba(matrix)[:, 1]
```

A second copy of those rules here is precisely how train/serve skew starts. The
rules are not obvious either — whole-number features are cast to `float64` so
the MLflow signature can represent a missing value, and booleans go through
nullable `boolean` so a missing flag stays missing instead of becoming `False`.
Reimplementing that from memory and getting one cast wrong produces a model
that scores fine and is quietly wrong.

### Attribution

`/v1/models/{name}` reports which alias, role and version this pod loaded, and
every prediction response carries `x-model-role` and `x-model-version`.

This exists because of §13. Under a traffic split both roles answer on the same
Service name, so the API cannot know from its own configuration which model
scored a given claim — only the response can tell it. `ModelServerClient` reads
those headers into a `Prediction`, and the API echoes them, so it is impossible
to log a probability without the version that produced it. Every A/B proxy
metric groups by these two fields.

The client falls back to its configured version when the headers are absent, so
substituting a third-party inference server degrades attribution rather than
breaking predictions.

---

## 4. Layering

Three layers, and the rule is that each one only knows about the one below it
(§15):

| Layer | Where | Knows about |
|---|---|---|
| Request | `main.py` — routes, status codes, request IDs | pydantic models and the two services below |
| Business logic | thresholding, missing-feature policy, drift fan-out | nothing about HTTP or Redis wire formats |
| Data access / model client | `FeastOnlineFeatureRepository`, `ModelServerClient` | Feast and HTTP respectively |

Two named patterns are doing the work:

**Repository** — `FeastOnlineFeatureRepository` wraps Feast, and
`MlflowRegistryModelLoader` wraps MLflow + GCS behind the `ModelLoader`
protocol. This is what makes the test suite possible without a cluster:
`tests/test_model_server.py` substitutes a `StubLoader` returning a fake
estimator and covers the entire HTTP surface and every scoring path with no
MLflow, no GCS and no fitted model. The seam is not decorative — it is the
difference between a test job that runs in seconds and one that needs
credentials.

**Adapter** — `ModelServerClient` adapts an HTTP inference protocol to a
Python call returning a `Prediction`. The business logic asks for a
probability; it does not know that answering involves JSON, a positional row,
or two response headers.

---

## 5. Deployment

### Charts

[`charts/fraud-prediction-api/`](../charts/fraud-prediction-api/) and
[`charts/model-server/`](../charts/model-server/), each its own Argo CD
Application in `charts/argocd-apps/`, both syncing into `api-serving-ns`.

Separate Applications despite the shared namespace because a §13 ramp is a
change to the model-server chart's weights, and keeping it its own Application
means each ramp step is one small, legible diff in the Argo CD UI.

The model-server chart renders its Deployment from a `range` over the two
roles, so champion and challenger come from one definition. A copy-pasted
second Deployment is how the two drift apart in probe settings or resource
requests — and then an A/B comparison is measuring the difference between two
deployments rather than between two models.

### Identity

Both services get a Workload Identity KSA created by
[`charts/bootstrap-secrets.sh`](../charts/bootstrap-secrets.sh) —
`fraud-prediction-api` reads the Feast registry from GCS, `model-server`
downloads the model artifact. Two KSAs rather than one shared identity: they
are different workloads with different reasons to reach the bucket, so a future
least-privilege split is a config change rather than a redeployment. Neither
holds a key.

The KSAs are created by the script, not the charts, because the annotation
needs a GCP service-account email that only Terraform state knows — baking one
project's email into a committed chart would make the repo non-portable.

### Timeouts

Three of them, and they have to be ordered or the outermost one defeats the
others:

| Where | Value | |
|---|---|---|
| VirtualService `perTryTimeout` | 2s | one attempt against one pod |
| VirtualService `timeout` | 5s | the whole route, retries included |
| API `MODEL_SERVER_TIMEOUT_SECONDS` | 6s | the httpx client |

The client's timeout has to outlast the mesh's total budget. Set it to 2s and
the API gives up during the retry that would have succeeded — the retry is
still in flight, the client has already returned a 502, and the mesh's
resilience is invisible. See [`docs/service_mesh.md`](service_mesh.md).

### Rollout and rollback

Deployed with `helm upgrade --atomic`, so a release whose pods never pass
`/readyz` is rolled back automatically instead of leaving a half-migrated
Deployment. `--atomic` is what makes the readiness probe load-bearing: it is
the signal `--atomic` waits on, which is another reason readiness reflects
dependencies while liveness does not.

For the model-server specifically, `--atomic` catches the most likely bad
release directly: an image whose scikit-learn cannot deserialise the promoted
artifact fails to load, fails `/readyz`, and never enters the Service's
endpoints — so the rollout is reverted before a single request reaches it.

---

## 6. Testing

[`tests/test_model_server.py`](../tests/test_model_server.py) and
[`tests/test_fraud_prediction_api.py`](../tests/test_fraud_prediction_api.py),
run per-push by the two workflows in §7. Test design follows §9:

- **Equivalence partitioning** on the request contract: a row of exactly
  `len(MODEL_FEATURES)` values is the valid class; anything else is not.
- **Boundary value analysis** on row width — parameterised at
  `len(MODEL_FEATURES) ± 1`, the two cases either side of the boundary, plus
  the empty-instances case. Same idea on the probability bound: `1.2` is
  rejected as outside `[0, 1]`.
- **Failure-mode tests over happy-path tests.** The cases that earn their place
  are the ones asserting a *specific* wrong answer does not happen: a failed
  startup is a 503 and not an `AttributeError` 500; a wrong-width row is a 422
  and not a 500; an estimator that raises becomes a `ScoringError` and not an
  unhandled exception; missing headers degrade attribution instead of breaking
  the response.

The `ModelLoader` seam is what keeps these tests cheap — no MLflow, no GCS, no
fitted model, so the CI test job needs no credentials and finishes in seconds.

Not yet done, and stated rather than implied: coverage is not measured against
§9's >90% bar, and neither mutation testing (`mutmut`), property-based
idempotency testing (`hypothesis`), nor the `locust` load test exists. §9 is a
separate rubric item and has not been built.

---

## 7. CI/CD

One workflow per image, following the four-job shape and the reasoning in
[`docs/cicd.md`](cicd.md):

| Workflow | Triggers on | Builds | Bumps |
|---|---|---|---|
| [fraud-prediction-api](../.github/workflows/fraud-prediction-api.yml) | `fraud_prediction_api/**`, its test, its dockerfile | `fraud-prediction-api` | `charts/fraud-prediction-api/values.yaml` |
| [model-server](../.github/workflows/model-server.yml) | `model_server/**`, its test, its dockerfile | `model-server` | `charts/model-server/values.yaml` |

Two details specific to these two:

**Neither triggers on its own `charts/` directory.** `update-manifest` commits
to `values.yaml` in exactly that directory, so listing it would make each
workflow trigger itself — the same loop-prevention reason `training-pipeline`
spells out its `ml/` paths instead of using `ml/**`.

For the model-server that exclusion does a second job: a §13 weight change is a
`values.yaml` edit with no code change, so there is no image to build. Argo CD
reconciles it directly. Triggering CI would produce an identical image under a
new tag and roll both Deployments mid-ramp — restarting the very pods whose
behaviour is being measured.

**`pytest-asyncio` is installed explicitly.** Both suites have
`@pytest.mark.asyncio` tests, and without the plugin pytest skips them
silently — the job passes while testing nothing.

---

## 8. Trying it

There is no gateway yet, so both services are `ClusterIP` and reaching either
UI means a port-forward. FastAPI's Swagger is at `/docs` on both (neither app
disables it).

```bash
kubectl port-forward -n api-serving-ns svc/fraud-prediction-api 8080:8080
# http://localhost:8080/docs
```

What currently blocks a working `/predict`, in the order it has to be fixed:

1. **The running pod is pre-refactor code.** `fraud-prediction-api:0.1.0` in
   Artifact Registry still has `KSERVE_PREDICT_URL` pointing at
   `fraud-detector-predictor.kserve-ns` — a Service that never came up. `/docs`
   renders and `/readyz` is green, but a prediction cannot succeed.
2. **No `model-server` image exists.** Pushing to `model_server/**` builds it
   (§7); nothing else does.
3. **`charts/model-server` has never been installed**, and there are no Argo CD
   Applications registered, so it needs a manual `helm install` for now.

Running locally is useful for the request/response contract and nothing else:
the tolerant startup means `/docs` renders, but `/predict` returns 503, because
a laptop has neither the GCS-hosted Feast registry nor Redis.

`kubectl port-forward svc/model-server` works for hitting the model directly,
but note that it **bypasses the mesh** — see
[`docs/service_mesh.md`](service_mesh.md#verifying-the-split-is-real) before
drawing conclusions about the traffic split from it.

## 9. What is not built yet

Stated plainly rather than left to be discovered:

- **`drift-api` (§2)** does not exist. Step 7 of the request path — the async
  fan-out after each prediction — is described here as the design, not as
  running code.
- **The gateway (§10)** is not deployed: no basic auth, no rate limit, no
  domain, no HTTPS. Both services are `ClusterIP` and reachable only in-cluster.
- **KEDA autoscaling (§1)** is not configured. Both run at fixed
  `replicaCount: 1`.
- **Prometheus metrics (§12)** are not exposed by either service. Request
  IDs are generated and echoed, which is the groundwork for Tempo trace
  propagation, but no tracing is wired up.
- **The champion/challenger split (§13)** is implemented and rendered, but
  `challenger.enabled` defaults to `false` — there is nothing aliased
  `@challenger` in the registry yet.
