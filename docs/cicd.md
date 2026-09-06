# CI/CD — GitHub Actions + Argo CD

GitHub Actions builds and pushes images; Argo CD is
what actually gets a new image running in the cluster.

## Why Argo CD

- **No cluster credentials in CI.** GitHub Actions only ever needs Artifact
  Registry push rights (via Workload Identity Federation — no static key sits
  in a GitHub secret). It never holds a kubeconfig. Argo CD runs *inside* the
  cluster and pulls from git, so the one credential that could do the most
  damage (cluster-admin) never leaves the cluster.

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
- **App-of-apps matches the repo layout that already exists.** `charts/` is
  already one subfolder per service (`postgres/`, `airflow/`, `mlflow/`,
  `redis/`, the `kubeflow/` kustomize overlay). A single root `Application`
  pointing at `charts/` can auto-discover each of those as its own child
  `Application`, so nothing about the existing folder structure has to change.

## Key components

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


**GitHub Actions pipelines** — one workflow per component. See below for how
many, and the job structure each one follows.

**The loop end to end**: a push to `dags/` or `ml/` triggers its workflow →
image is built and pushed to Artifact Registry → the workflow commits an
updated image tag → Argo CD's `application-controller` notices the git change
on its next poll → it renders the affected chart and applies the diff → the
new pod pulls the image from Artifact Registry using the node service
account's `artifactregistry.reader` grant (§8, no `imagePullSecret` needed).

## GitHub Actions pipelines

Five workflows total, one per component (`CLAUDE.md` §3 and §8) — each triggers
independently on the paths it owns, so touching one component never rebuilds
another:

| Workflow | Triggers on | Builds | Bumps |
|---|---|---|---|
| **Airflow pipelines** | `dags/insurance_batch_pipeline.py`, `jobs/**`, `docker_image/dockerfile.airflow` | `airflow-spark` image | `charts/airflow/values.yaml` |
| Materialize Pipeline | `dags/feature_store_materialize.py`, `feature_store/**` | same `airflow-spark` image (Feast runs inside the Airflow image) | `charts/airflow/values.yaml` |
| Training Pipeline | `ml/**` | `training` image | `ml/pipeline.py`'s `TRAINING_IMAGE`, then recompiles `ml/pipeline.yaml` |
| fraud-prediction-api | §10, not yet built | — | — |
| drift-api | §10, not yet built | — | — |

Only **Airflow pipelines** is implemented so far —
[`.github/workflows/airflow-pipelines.yml`](../.github/workflows/airflow-pipelines.yml).
Materialize Pipeline follows the identical shape (same image, different
trigger paths); Training Pipeline differs because its deployment target isn't
a standing Kubernetes resource Argo CD reconciles — a Kubeflow run is a
one-shot API call, not a Deployment — so that workflow's job ends at "image
pushed, `pipeline.yaml` recompiled and committed," and someone (or the drift
DAG in §12) still has to submit the run.

### Job structure

Four jobs, chained sequentially rather than run in parallel — GitHub's UI
renders one node per job, but it visually merges jobs that have no `needs`
between them into a single box once they converge on the same downstream
job. A straight chain is what actually shows as four separate, connected
boxes in the Actions graph:

```text
lint ──► test ──► build-and-push ──► update-manifest
```

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
  the short commit SHA, pushes to Artifact Registry.
- **`update-manifest`** — bumps the image tag in the relevant chart's
  `values.yaml`, commits as `github-actions[bot]`, pushes. This is the exact
  commit Argo CD reacts to — CI's job ends here, it never touches the cluster.

### Why the trigger paths matter

Each workflow explicitly excludes the file it commits to. `airflow-pipelines`
triggers on `dags/**`/`jobs/**`/the Dockerfile, but never on
`charts/airflow/values.yaml` — since `update-manifest` commits to that exact
path, including it in the trigger would be an infinite loop: commit → workflow
→ commit → workflow.

### Workload Identity Federation

`terraform/modules/ci` provisions a WIF pool + provider trusting
`token.actions.githubusercontent.com`, scoped with an `attribute_condition` so
only workflow runs from this exact repo can mint a matching token — a
workflow in some other repo in the same GitHub org can't impersonate this
identity. That identity (`insurance-github-ci@...`) is granted
`roles/artifactregistry.writer` on the one Artifact Registry repository only,
not the project. No JSON key is ever created, downloaded, or stored as a
GitHub secret; the provider name and service-account email are safe to commit
in plaintext in the workflow file, since neither is usable without also
controlling a run in this repo.
