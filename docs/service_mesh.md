# Service mesh — Managed Cloud Service Mesh + champion/challenger

The mesh is what makes §13's A/B test and §14's service-to-service auth
configuration rather than code. This doc covers why a mesh at all, what is
actually installed, and how the champion/challenger split works.

---

## 1. Why a service mesh

A mesh puts a proxy (Envoy) next to every pod and routes all traffic between
pods through those proxies. Nothing in the application changes; the proxy is
injected as an extra container. What that buys is a place to put concerns that
would otherwise have to live *inside* every service:

- **Traffic routing that is not DNS.** Kubernetes gives one load-balancing
  policy: a Service picks uniformly among its ready endpoints. "Send 10% of
  requests to this version" is not expressible. Without a mesh, the split has
  to move into the caller — the API would need to know both model versions
  exist, hold the percentage in its own config, and be redeployed to change it.
  With a mesh the caller has one host and no opinion, and the percentage is a
  routing rule.
- **Identity and encryption between services, without touching them.** The
  proxies each hold a certificate identifying their workload and negotiate mTLS
  on the application's behalf. So "the model is only callable by the API, over
  an authenticated connection" is a policy, not TLS code in two FastAPI apps
  and two certificates to rotate.
- **Resilience policy in one place.** Retries, timeouts and ejecting a failing
  pod are the same logic in every service, reimplemented slightly differently
  each time. In a mesh they are attributes of a route.
- **Uniform telemetry.** Every proxy reports the same request metrics for every
  hop, so per-version latency and error rates exist without either service
  being instrumented.

The alternative for the traffic-splitting requirement specifically would be a
second Service per model version and the weighting logic in the API — which
means the A/B percentage lives in application config, every change is a
redeployment, and there is still no mTLS.

**This project uses Managed Cloud Service Mesh**, GKE's managed Istio: Google
runs the control plane, the cluster gets the injected sidecars and the Istio
CRDs. Everything below — `DestinationRule`, `VirtualService`,
`PeerAuthentication` — is the Istio API, so a self-installed Istio (istioctl or
the upstream Helm charts) is a viable option and the manifests in
`charts/model-server/` would work against it unchanged. The difference is who
operates istiod, not what you write.

---

## 2. What Terraform provisions

[`terraform/modules/mesh/`](../terraform/modules/mesh/), three resources plus
the APIs, in a required order:

1. **APIs** — `gkehub`, `meshconfig`, `meshca`, `meshtelemetry`,
   `trafficdirector`. `meshca` is the one whose absence is confusing: without
   it the sidecars start normally and every mTLS handshake fails, because
   nothing is issuing workload certificates.
2. **`google_gke_hub_membership`** — registers the cluster in the project's
   fleet. A fleet membership is the object mesh features attach to; nothing
   mesh-related can be enabled on a cluster that is not a member.
3. **`google_gke_hub_feature`** + **`google_gke_hub_feature_membership`** — the
   `servicemesh` feature, set to `MANAGEMENT_AUTOMATIC` for that membership.
   Google then picks and upgrades the control-plane revision. The alternative,
   `MANAGEMENT_MANUAL`, means pinning a revision and canary-upgrading the
   control plane by hand — work with no payoff on a cluster that is rebuilt
   every session and never outlives a revision.

Two decisions worth their own note.

**It is in Terraform, not a `gcloud` command.** `gcloud container fleet mesh
enable` would work and would be shorter. But §11's rule is that
`terraform destroy` leaves zero orphaned billing, and a hand-enabled mesh
survives the destroy with the fleet still registered — exactly the orphan the
rule exists to prevent. In state, the membership goes away with the cluster.

**It is behind `enable_service_mesh`.** Sidecar injection adds an Envoy
container to every pod in a labelled namespace, and Autopilot bills per pod's
resource requests. Sessions working on the batch or streaming steps (§18 steps
2–4) need no mesh and should not pay for one. Turning it off is safe:
`charts/model-server` gates its mesh resources behind `mesh.enabled`, so the
chart still renders into a mesh-less cluster instead of sync-failing on missing
CRDs.

### State of play on this cluster

The mesh described above is **already enabled, and was not created by this
Terraform module.** What is live right now:

| | |
|---|---|
| Fleet membership | `insurance-gke` — exists, **not in Terraform state** |
| Control plane | managed; `istio-system` has no pods, which is the expected shape |
| CRDs | 15 `*.istio.io` CRDs installed |
| `api-serving-ns` | labelled `istio-injection: enabled` (the legacy style) |
| Injected revision | `asm-managed-rapid` — the rapid channel |

Two consequences to know before touching Terraform:

- `module.mesh` takes `existing_fleet_membership_id` for exactly this case. Left
  null against an already-registered cluster, `terraform apply` attempts a
  *second* membership for the same cluster. The clean fix is to import the real
  one, after which the variable goes back to null:

  ```bash
  terraform import 'module.mesh.google_gke_hub_membership.cluster[0]' \
    projects/aide-playground/locations/global/memberships/insurance-gke
  ```

  Until that import happens, `terraform destroy` leaves the fleet registration
  behind — the one live exception to §11's zero-orphans rule, surfaced as the
  `mesh_membership_is_managed_here` output rather than left to be discovered on
  a billing page.

- **Do not "upgrade" the namespace label.** The legacy `istio-injection=enabled`
  is working here and resolves to `asm-managed-rapid`. Relabelling to
  `istio.io/rev=asm-managed` — the default channel — would not match the live
  control plane, and the failure mode is silent: pods come up with no sidecar,
  no error anywhere, and the traffic split just stops happening. If a namespace
  does need labelling, read the value off an already-injected pod first:

  ```bash
  kubectl get pod -n api-serving-ns <pod> -o jsonpath='{.metadata.labels.istio\.io/rev}'
  ```

### Why the label is not Terraform's job

Sidecar injection is a **namespace label**, so applying it from Terraform would
mean putting the kubernetes provider in this state — and then `terraform
destroy` depends on a reachable API server that the same destroy is tearing
down. The module emits the command as the `mesh_namespace_label_command` output
instead of running it.

Only `api-serving-ns` is labelled. `data-ns` deliberately is not: Kafka, Flink
and Spark speak protocols Envoy has no reason to be in the middle of, and the
Airflow materialize job writes to Redis in `api-serving-ns` from *outside* the
mesh — which is why the mTLS policy in §5 is workload-scoped and not
namespace-wide.

---

## 3. DestinationRule vs VirtualService

The division of labour, because it is the thing most often gotten backwards:

| | Answers | Contains |
|---|---|---|
| **DestinationRule** | *what the destinations are* | subsets (named groups of endpoints, by pod label), TLS mode, load balancing, outlier detection |
| **VirtualService** | *how a request is routed* | match conditions, weights across subsets, timeouts, retries |

The subset **names** are the contract between them. A VirtualService weight
pointing at a subset no DestinationRule defines is a 503 at request time, not a
validation error at apply time — and it is the single most common failure when
standing this up.

Both are in [`charts/model-server/templates/`](../charts/model-server/templates/).

### The precondition: one Service, both roles

Everything rests on a detail in
[`service.yaml`](../charts/model-server/templates/service.yaml) — the selector
omits the `version` label:

```yaml
selector:
  app.kubernetes.io/name: model-server   # NOT version: champion
```

So champion and challenger pods are endpoints of the *same* Service, and the
API has exactly one host to call. Which pods actually receive a request is then
a mesh decision. Two Services would have moved that choice into the API's
configuration, where changing the split means a redeployment — and there would
be nothing left for the mesh to decide.

The port must also be **named `http`**. Istio infers a port's protocol from the
name; an unnamed port is treated as opaque TCP and every L7 feature the split
depends on — weighting, retries, per-route timeouts — silently stops applying.
Nothing errors; the traffic just stops being split.

### DestinationRule

```yaml
spec:
  host: model-server.api-serving-ns.svc.cluster.local
  trafficPolicy:
    tls:
      mode: ISTIO_MUTUAL
    loadBalancer:
      simple: ROUND_ROBIN
    outlierDetection:
      consecutive5xxErrors: 3
      interval: 10s
      baseEjectionTime: 30s
  subsets:
    - name: champion
      labels: { version: champion }
    - name: challenger
      labels: { version: challenger }
```

- `ISTIO_MUTUAL` — the sidecar presents its mesh-issued identity certificate
  and the destination verifies it. §14's service-to-service auth.
- `ROUND_ROBIN` is *within* a subset — which of a role's replicas answers. The
  weights across subsets are the VirtualService's job.
- `outlierDetection` ejects a pod that returns three consecutive 5xx from the
  load-balancing pool for 30s. On a canary this is the safety net that matters:
  a challenger that starts failing is pulled without waiting for anyone to
  notice a dashboard.

**Both subsets are defined unconditionally**, not gated on
`challenger.enabled`. If the challenger subset vanished from the file whenever
the challenger was scaled down, the VirtualService would briefly reference an
undefined subset during the ramp-down and return 503s. An empty subset is
harmless — Istio resolves it to no endpoints, and a route with weight 0 never
selects it.

### VirtualService

```yaml
spec:
  hosts:
    - model-server.api-serving-ns.svc.cluster.local
  http:
    - name: predict
      timeout: 5s
      retries:
        attempts: 2
        perTryTimeout: 2s
        retryOn: connect-failure,refused-stream,503
      route:
        - destination: { host: ..., subset: champion }
          weight: 90
        - destination: { host: ..., subset: challenger }
          weight: 10
```

No gateway is attached: the host list is the in-cluster Service name only. The
model-server is never reachable from outside — the only public entry point is
`fraud-prediction-api` behind the §10 gateway.

**Retries.** The three conditions are the ones safe to retry:
`connect-failure` and `refused-stream` never reached the application, and `503`
is what an ejected or not-yet-ready pod returns. Scoring is a pure function of
the request — the §9 idempotency property — so a retry cannot double-apply
anything.

The important part is that **a retry re-runs route selection.** A request that
lands on a failing challenger can be answered by the champion on the retry.
That is the difference between a bad canary degrading and a bad canary being an
outage.

**The weights must sum to 100** or Istio rejects the resource. The chart fails
the render rather than letting that reach the cluster:

```
Error: champion.weight + challenger.weight must equal 100, got 100 + 10 = 110
```

A `helm template` error during a ramp step, rather than an Argo CD sync failure
discovered afterwards — with the previous VirtualService still live and no
obvious sign that the step did not take.

**No sticky routing.** There is deliberately no header or cookie match pinning
a caller to one model. Claims arrive independently and there is no user session
to keep consistent, so per-request weighting gives an unbiased sample for the
proxy metrics below. Sticky routing would instead correlate each model's sample
with whatever distinguishes its callers — which is precisely the bias the A/B
test is trying to avoid.

---

## 4. Running a champion/challenger test

### The model side

Both roles run the same image. What differs is one environment variable:

| | `MODEL_ROLE` | `MODEL_ALIAS` |
|---|---|---|
| champion | `champion` | `production` |
| challenger | `challenger` | `challenger` |

`MODEL_ALIAS` is what a pod resolves against the MLflow registry at startup, so
**which** model is under test is a registry decision, not a chart edit:

```bash
python -m ml.promote --version 7 --alias challenger
```

`MODEL_ROLE` is a separate knob from the alias so a rollback drill can run two
versions of the *same* alias against each other. It is also the label every
metric groups by.

Because a pod loads its model once at startup, it is pinned to that version —
which is what makes the traffic weight the only variable during a ramp. Moving
the `challenger` alias while a test is running changes nothing until the pods
restart.

`challenger.enabled` is `false` by default. With nothing aliased
`@challenger`, the pods would start, fail `/readyz` and sit out of the
Service's endpoints forever; turning it on is the deliberate first step of a
run.

### The ramp

§13's staged ramp is one pair of values, applied four times:

| Step | `champion.weight` | `challenger.weight` |
|---|---|---|
| 1 | 90 | 10 |
| 2 | 75 | 25 |
| 3 | 50 | 50 |
| 4 | 0 | 100 |

Each step is a commit to
[`charts/model-server/values.yaml`](../charts/model-server/values.yaml) that
Argo CD reconciles. **Nothing is rebuilt and no pod restarts** — Envoy reloads
the route weights in place. That is what makes staging the ramp cheap enough to
actually do in four steps instead of one, and it makes a rollback a single
`git revert`.

Step 4 is not the end. Serving 100% from the `challenger` alias is a traffic
state, not a promotion; the challenger becomes the champion by moving the
`production` alias to that version and returning the weights to 100/0.

### Verifying the split is real

A configured weight is not evidence of a working split — the weights can be
perfect while no routing happens at all. Two ways to check, and one trap.

**The trap: `kubectl port-forward` bypasses the mesh.** Port-forwarding
`svc/model-server` resolves the Service to a single pod and forwards straight
to that pod IP. It never touches the Service VIP and there is no caller sidecar
in the path, so no `VirtualService` routing is applied. You will see one role
100% of the time and conclude the split is broken. Traffic has to *originate
inside a meshed pod*.

**From the CLI** — read the attribution headers on 40 requests:

```bash
kubectl exec -n api-serving-ns deploy/fraud-prediction-api -c api -- sh -c '
  for i in $(seq 1 40); do
    curl -s -D - -o /dev/null http://model-server:8080/v1/models/fraud-detector:predict \
      -H "content-type: application/json" -d "{\"instances\":[[...]]}" | grep -i x-model-role
  done' | sort | uniq -c
```

**From a UI** — the API's own Swagger docs, which is the more convenient route
and works because the split's outcome is echoed on the prediction response:

```bash
kubectl port-forward -n api-serving-ns svc/fraud-prediction-api 8080:8080
# then open http://localhost:8080/docs and POST /predict repeatedly
```

Watch `model_role` in the response body flip between `champion` and
`challenger`. Port-forwarding the *API* is fine — the mesh hop being tested is
the API→model-server call, which happens inside the cluster from a pod that
does have a sidecar. Only port-forwarding the model-server directly would skip
it.

Beyond that, the **Cloud Service Mesh dashboard in the GCP console** shows
topology and per-service golden signals once traffic is flowing, since the
managed control plane streams telemetry without anything being installed.
Kiali is not deployed, and the Grafana panels in §12 do not exist yet.

If everything lands on one role, the usual causes in order: you port-forwarded
the model-server (above); the other role's pods are not Ready (a subset with no
ready endpoints gets no traffic regardless of weight); the Service port is not
named `http`; or the namespace has no injection label and there are no
sidecars.

---

## 5. mTLS (§14)

Two halves, and one alone is not enough.

The DestinationRule's `ISTIO_MUTUAL` configures the **caller** to use mTLS. On
its own, a caller with no sidecar — or one configured to skip it — would still
be accepted, because nothing is requiring it at the destination.

[`peerauthentication.yaml`](../charts/model-server/templates/peerauthentication.yaml)
supplies the other half:

```yaml
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: model-server
  mtls:
    mode: STRICT
```

STRICT makes the model-server reject plaintext outright, so "every request to
the model is authenticated" is enforced at the destination rather than assumed
of every client.

**Workload-scoped, not namespace-wide**, and that is a deliberate scoping
decision. `api-serving-ns` also holds Redis, and a namespace-wide STRICT policy
would break any client without a sidecar — the Feast materialize job reaching
Redis from `data-ns` being the obvious one. Enforcing per workload keeps the
blast radius to the workload actually being secured.

Two consequences that follow from STRICT:

- **`fraud-prediction-api` must have a sidecar too.** It is the client side of
  this handshake, so without one every prediction fails at the transport layer
  rather than in the request. That is why its Deployment carries
  `sidecar.istio.io/inject: "true"`, driven by `mesh.enabled` in its values —
  the two settings have to be turned on together.
- **Health probes still work.** The kubelet has no sidecar and its probes
  would fail STRICT, so Istio rewrites HTTP probes through the sidecar's
  port 15020. This is on by default; it is worth knowing because "readiness
  broke the moment mTLS went STRICT" is otherwise an unexplained symptom.

---

## 6. Judging the test without ground truth

The constraint that shapes all of §13: **there is no label at request time.**
Whether a claim was fraudulent is known weeks later, if ever. So the challenger
cannot be judged on accuracy during the ramp, and every metric below is a proxy
for "is this model behaving like a model that works".

| Metric | What it is | What a bad reading means |
|---|---|---|
| **Prediction-distribution drift** (PSI/KS between the two roles' score distributions) | the same claims population scored by both models | a large divergence means the challenger disagrees *systematically*, not on edge cases — most often a feature-pipeline mismatch rather than a better model |
| **Disagreement rate** | share of claims where the two roles land on opposite sides of the threshold | quantifies the operational impact: this is how many claims would be routed differently |
| **Fraud-flag rate over time**, per role | share of claims flagged | the challenger flagging 3× as many is a review-capacity problem regardless of whether it is right |
| **Latency (p50/p95/p99)**, per role | from the Envoy sidecar metrics | a slower challenger eats the §9 SLA budget |
| **Error rate**, per role | 5xx from the sidecar | the plainest disqualifier; also what `outlierDetection` acts on automatically |

The first three come from the application: they are computed from
`model_role` and `model_version` on the prediction responses, which is why the
model-server sets those headers and the API echoes them. The last two come from
the mesh for free, and are reported per version *because* the DestinationRule
subsets exist — Envoy labels its metrics with the subset it routed to.

The comparison is only valid because both roles score the same claims
population, which is what per-request weighting guarantees and sticky routing
would have broken.

---

## 7. What is not built yet

- **The Grafana A/B dashboard (§13)** does not exist. The metrics above are the
  design; the panels are not built, and neither is the Prometheus scrape of the
  Envoy sidecar metrics (§12).
- **No challenger has been run.** The split is implemented, renders, and the
  weight guard is verified, but `challenger.enabled` defaults to `false` and
  nothing is aliased `@challenger` in the registry. The ramp has not been
  executed end-to-end against a real second model.
- **Nothing in `charts/model-server/` has been applied to the live mesh yet.**
  The mesh itself is up and injecting (see §2), and `fraud-prediction-api`
  already runs with a sidecar — but the `DestinationRule`, `VirtualService` and
  `PeerAuthentication` have only been rendered and linted, and no
  `model-server` image exists in Artifact Registry. The mTLS handshake and the
  observed split are untested in practice.
- **The mesh is not under Terraform's control** (§2): the fleet membership was
  created by hand and needs importing.
- **Nothing is under Argo CD.** `argocd-ns` has zero `Application`s
  registered, so the "each ramp step is a commit Argo CD reconciles" loop is
  the design, not yet the mechanism — right now a weight change needs a manual
  `helm upgrade`.
- **`data-ns` is outside the mesh**, so §14's mTLS covers the serving hop only,
  not Airflow/Spark/Kafka traffic.
