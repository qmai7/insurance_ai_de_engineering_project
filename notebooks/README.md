# Notebooks

| Notebook | What it is |
|---|---|
| [`01_fraud_model_baseline.ipynb`](01_fraud_model_baseline.ipynb) | CLAUDE.md §4 — the fraud model built by hand before any pipeline exists. Feast offline retrieval → label join → temporal split → train → evaluate → `.joblib`. |

Design decisions and results are written up in [`../docs/ml.md`](../docs/ml.md).

## Its own virtualenv, deliberately

The notebook does **not** run in the project's main environment, and this is the
same conflict that forced two container images (see
[`../docker_image/dockerfile.feast`](../docker_image/dockerfile.feast)):

`feast[redis,gcp]` resolves to numpy 2.x, and PySpark 3.5.1 does not work with
numpy 2.x. The root `pyproject.toml` env has PySpark in it because every batch job
needs it. Installing Feast there would silently break every Spark job's pandas
conversion — including the export that produces the very Parquet this notebook
reads.

So the dependency sets stay apart:

| Environment | Contains | Used by |
|---|---|---|
| `.venv` (root `pyproject.toml`) | PySpark, Delta, clickhouse-connect | batch jobs, local Spark runs |
| `.venv-ml` | Feast SDK, scikit-learn, MLflow, KFP SDK, Jupyter | this notebook, and `ml/` locally |

```bash
python3 -m venv .venv-ml
.venv-ml/bin/pip install \
  "feast[redis,gcp]==0.65.0" "gcsfs>=2024.6.0" "mlflow==3.15.2" \
  scikit-learn joblib matplotlib jupyter ipykernel nbconvert jupytext kfp
```

`mlflow` and `kfp` are here for the `ml/` package rather than the notebook: `kfp`
compiles [`../ml/pipeline.py`](../ml/pipeline.py), and `mlflow` lets
`python -m ml.train` run against a port-forwarded tracking server. Both are
pinned to the versions in [`../docker_image/dockerfile.mlflow`](../docker_image/dockerfile.mlflow) and
[`../docker_image/dockerfile.training`](../docker_image/dockerfile.training) — an MLflow client and server
share a registry schema.

The Feast version is pinned to match [`../docker_image/dockerfile.feast`](../docker_image/dockerfile.feast).
It has to: the registry in GCS is written by that image and read here, and the
entity-key serialization version is part of the on-disk format.

## Running it

Reads happen over `gs://`, so application-default credentials must be present:

```bash
gcloud auth application-default login     # once
.venv-ml/bin/jupyter lab notebooks/       # interactive
```

Or headless, which is how the committed outputs were produced:

```bash
cd notebooks && ../.venv-ml/bin/jupyter nbconvert \
  --to notebook --execute --inplace 01_fraud_model_baseline.ipynb
```

**The cluster does not need to be running.** The notebook reads the Feast registry
and the offline Parquet from GCS, and never touches Redis — online serving is §1's
concern, and a training job reading the online store would be reading features
that have already expired. What it *does* need is that the Materialize Pipeline
has run at least once against current Gold, since that is what writes the offline
Parquet.

## Outputs are committed

The `.ipynb` is stored with its executed outputs. The notebook is a graded
deliverable, so the numbers and plots need to be readable without a GCP project, a
populated bucket, and a 500 MB virtualenv. `models/fraud_model.joblib` and its
sidecar `models/fraud_model.metadata.json` are committed for the same reason.
