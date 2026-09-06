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


**GitHub Actions pipelines** — one workflow per component (Materialize
Pipeline, Training Pipeline, Airflow pipelines, and later
fraud-prediction-api/drift-api in §10): lint, unit test, build the image, push
it to Artifact Registry, then commit the new tag into the relevant chart's
`values.yaml` (or, for the Training Pipeline, into `ml/pipeline.py` before
recompiling `ml/pipeline.yaml`). CI's responsibility ends at that commit — it
never touches the cluster directly.

**The loop end to end**: a push to `dags/` or `ml/` triggers its workflow →
image is built and pushed to Artifact Registry → the workflow commits an
updated image tag → Argo CD's `application-controller` notices the git change
on its next poll → it renders the affected chart and applies the diff → the
new pod pulls the image from Artifact Registry using the node service
account's `artifactregistry.reader` grant (§8, no `imagePullSecret` needed).
