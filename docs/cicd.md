# CI/CD — GitHub Actions + Argo CD

GitHub Actions builds and pushes images; Argo CD is
what actually gets a new image running in the cluster.


## 1.GitHub Actions pipelines

Five workflows total, one per component — each triggers
independently on the paths it owns, so touching one component never rebuilds
another:

| Workflow | Triggers on | Builds | Bumps |
|---|---|---|---|
| **Airflow pipelines** | `dags/insurance_batch_pipeline.py`, `jobs/**`, `docker_image/dockerfile.airflow` | `airflow-spark` image | `charts/airflow/values.yaml` |
| Materialize Pipeline | `dags/feature_store_materialize.py`, `feature_store/**` | same `airflow-spark` image (Feast runs inside the Airflow image) | `charts/airflow/values.yaml` |
| **Training Pipeline** | `ml/config.py`, `ml/gate.py`, `ml/promote.py`, `ml/repositories.py`, `ml/services.py`, `ml/submit.py`, `ml/train.py`, `ml/training-job.yaml`, `docker_image/dockerfile.training` | `training` image | `ml/pipeline.py`'s `TRAINING_IMAGE`, then recompiles `ml/pipeline.yaml` |
| fraud-prediction-api | §10, not yet built | — | — |
| drift-api | §10, not yet built | — | — |

Two workflows are implemented so far —
[`.github/workflows/airflow-pipelines.yml`](../.github/workflows/airflow-pipelines.yml)
and
[`.github/workflows/training-pipeline.yml`](../.github/workflows/training-pipeline.yml).
Materialize Pipeline follows the identical shape as Airflow pipelines (same
image, different trigger paths). Training Pipeline's `update-manifest` job
differs from the other two: its deployment target isn't a standing Kubernetes
resource Argo CD reconciles — a Kubeflow run is a one-shot API call, not a
Deployment — so that job installs `kfp==2.17.0`, bumps `TRAINING_IMAGE` with
`sed`, recompiles `ml/pipeline.yaml`, and commits both. Nobody automatically
submits the recompiled pipeline; that's still a manual `ml/submit.py --wait`
or the drift DAG in §12.

Training Pipeline's trigger list spells out each `ml/` file explicitly rather
than using `ml/**`, for the same loop-prevention reason `airflow-pipelines`
excludes `charts/airflow/values.yaml`: `ml/pipeline.py` and `ml/pipeline.yaml`
are exactly what `update-manifest` commits to, and GitHub Actions can't
combine `paths:` and `paths-ignore:` on the same trigger, so the safe list is
everything in `ml/` *except* those two.

### Job structure

![GithubAction_Airflow_pipeline](/assets/GithubAction_Airflow_pipeline.png)

- **`lint`** — `ruff check jobs/ dags/`. Scoped to a narrow starter rule set
  (`E4,E7,E9,F` in `pyproject.toml`) rather than ruff's full default: this
  codebase was never linted before, and blocking CI on ~44 pre-existing style
  issues unrelated to CI/CD wasn't the point of standing this up. Broaden the
  rule set incrementally, later.
- **`test`** — `pytest jobs/ dags/`. No unit tests exist yet (§9 is a
  separate, not-yet-built rubric item), so this currently just confirms "no
  tests collected" (exit code 5, treated as success) rather than faking a
  placeholder test.
- **`build-and-push`** — authenticates via Workload Identity Federation
  (`google-github-actions/auth`, no static key), builds the image tagged with
  the short commit SHA (commit identifier), pushes to Artifact Registry.
  **but the cluster is still running the old img tag**. GKE has no idea the new image exists in the Artifact Registry. 
  The only way to get GKE to pull and run new image is to change the tag in Git, which is what **`update-manifest`** does. 
- **`update-manifest`** —  bumps the image tag in the relevant chart's
  `values.yaml`, commits as `github-actions[bot]`, pushes. ArgoCD constantly watches and diff what's declared in `charts/airflow/values.yaml` vs what's actually running in the cluster, and reconciles any drift. This is the exact
  commit Argo CD reacts to — CI's job ends here.


**The loop end to end**: a push to `dags/` or `ml/` triggers its workflow →
image is built and pushed to Artifact Registry → the workflow commits an
updated image tag → Argo CD's `application-controller` notices the git change
on its next poll → it renders the affected chart and applies the diff → the
new pod pulls the image from Artifact Registry using the node service
account's `artifactregistry.reader` grant (§8, no `imagePullSecret` needed).

## 2.Argo CD

### Why Argo CD?

- **It's the missing link CI can't be.** A GitHub Actions workflow can build
  an image and commit a new tag into `charts/airflow/values.yaml`, but
  something still has to turn that commit into a running pod. Argo CD is that
  something — it notices the commit and runs the `helm upgrade` on your
  behalf.
- **Self-healing against manual drift.** Debugging this project involves a lot
  of `kubectl describe` / `kubectl edit` / manual scaling. Argo CD diffs live
  cluster state against `charts/` continuously and flags (or reverts) anything
  that drifted, so git stays the actual source of truth instead of slowly
  going stale.

  Example: ArgoCD notices a drift in **Airflow** 

![argocd_outofsync](/assets/argocd_outofsync.png)

- **App-of-apps matches the repo layout that already exists.** `charts/` is
  already one subfolder per service (`postgres/`, `airflow/`, `mlflow/`,
  `redis/`, the `kubeflow/` kustomize overlay). A single root `Application`
  pointing at `charts/` can auto-discover each of those as its own child
  `Application`, so nothing about the existing folder structure has to change.


### Key components

**No cluster credentials in CI.** GitHub Actions only ever needs Artifact
  Registry push rights (via Workload Identity Federation — no static key sits
  in a GitHub secret). It never holds a kubeconfig. Argo CD runs *inside* the
  cluster and pulls from git, so the one credential that could do the most
  damage (cluster-admin) never leaves the cluster.

**Argo CD control plane** (`argocd-ns`) — four pieces working together:
`repo-server` renders each Helm chart / kustomize overlay in `charts/` into
plain manifests, `application-controller` diffs those manifests against live
cluster state and applies the difference, `server` is the API/UI on top of
both, and `redis` caches the rendered manifests so every diff doesn't mean
re-running `helm template` from scratch. SSO (Dex) and sync notifications are
disabled — neither has a purpose on a single-admin, no-alerting cluster.

**App-of-apps** — one root `Application` points at `charts/argocd-apps/`, a
folder of one `Application` manifest per service (`postgres`, `clickhouse`,
`mlflow`, `redis`, `airflow`, later `kubeflow`). Each of those in turn points
at the actual chart under `charts/<service>/`, or, for `airflow`, layers this
repo's `values.yaml` on top of the upstream chart via a multi-source
`Application`. Adding a new service is adding one manifest to
`charts/argocd-apps/`, not registering anything by hand.

ArgoCD dashboard
![argocd_dashboard](/assets/argocd_dashboard.png)

We can click on any of the child to see the live resource tree(Deployment, Service, etc..), diff against git. Example of **MLflow**:

![argocd_mlflow](/assets/argocd_mlflow.png)