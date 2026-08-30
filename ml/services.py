"""
Business logic for the training path — the five key classes of CLAUDE.md §16.

    TrainingDataService   assemble a labelled, point-in-time-correct dataset
    SplitService          divide it into train and validation by time
    ModelBuilder          construct the estimator, preprocessing included
    EvaluationService     score it the way the business will read it
    ModelRegistryService  record the run and register the model

None of them touches a filesystem, a bucket or a tracking server directly:
data arrives through the repositories in `repositories.py`, and the only
service that talks to MLflow is the last one. That boundary is what lets the
first four be unit-tested with in-memory fixtures (§9), and what lets §5's
Kubeflow components reuse them as components rather than reimplementing them.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from . import config
from .repositories import (
    ClaimSpineRepository,
    DataVersionRepository,
    FeatureRepository,
    LabelRepository,
)


@dataclass(frozen=True)
class TrainingDataset:
    """A training frame plus the provenance needed to reproduce it."""

    frame: pd.DataFrame
    data_version: int
    feature_refs: list[str]

    @property
    def rows(self) -> int:
        return len(self.frame)


@dataclass(frozen=True)
class DataSplit:
    train: pd.DataFrame
    validation: pd.DataFrame
    cutoff: pd.Timestamp

    def describe(self) -> dict:
        return {
            "cutoff": str(self.cutoff.date()),
            "train_rows": len(self.train),
            "train_positives": int(self.train[config.LABEL].sum()),
            "train_fraud_rate": float(self.train[config.LABEL].mean()),
            "val_rows": len(self.validation),
            "val_positives": int(self.validation[config.LABEL].sum()),
            "val_fraud_rate": float(self.validation[config.LABEL].mean()),
        }


class TrainingDataService:
    """
    Turns "which claims, labelled how" into a model-ready frame.

    Three steps, in this order for a reason: the spine says which claims exist,
    the labels say what happened, and only then does Feast supply what was known
    at the time. Retrieving features first and filtering later would ask the
    feature store for rows no label exists for.
    """

    def __init__(
        self,
        spine_repository: ClaimSpineRepository,
        label_repository: LabelRepository,
        feature_repository: FeatureRepository,
        version_repository: DataVersionRepository,
    ):
        self._spine = spine_repository
        self._labels = label_repository
        self._features = feature_repository
        self._versions = version_repository

    def build(self, feature_refs: list[str] | None = None) -> TrainingDataset:
        feature_refs = feature_refs or config.FEATURE_REFS

        entity_df = self._entity_dataframe()
        frame = self._features.get_historical_features(entity_df, feature_refs)
        self._validate(frame, entity_df, feature_refs)

        return TrainingDataset(
            frame=frame.sort_values(config.EVENT_TIMESTAMP).reset_index(drop=True),
            data_version=self._versions.current_version(),
            feature_refs=feature_refs,
        )

    def _entity_dataframe(self) -> pd.DataFrame:
        spine = self._spine.load()
        labels = self._labels.load()
        entity_df = spine.merge(labels, on="claim_id", how="inner")

        if len(entity_df) != len(spine):
            raise ValueError(
                f"{len(spine) - len(entity_df)} of {len(spine)} claims have no label; "
                "the label table and the claims export are out of sync"
            )
        return entity_df

    @staticmethod
    def _validate(frame: pd.DataFrame, entity_df: pd.DataFrame, feature_refs: list[str]) -> None:
        """
        Two assertions on the point-in-time join, both of which have caught a real
        bug rather than a hypothetical one.

        Row count first: Feast drops an entity entirely when no feature row
        qualifies at its timestamp, so a point-in-time miss shows up as *fewer
        rows*, not as nulls. A single-snapshot feature source once reduced this
        join to zero rows while every command in the pipeline reported success.

        Then per-column null rate: a column that is null everywhere means a view
        resolved but never matched — usually a TTL that excludes the whole
        dataset. Partial nulls are fine and expected (risk_segment is absent for
        pre-schema-change customers), so only an entirely empty column fails.
        """
        if len(frame) != len(entity_df):
            raise ValueError(
                f"point-in-time join returned {len(frame)} rows for "
                f"{len(entity_df)} entities — features are missing at those timestamps"
            )

        feature_columns = [ref.split(":")[1] for ref in feature_refs]
        empty = [c for c in feature_columns if frame[c].isna().all()]
        if empty:
            raise ValueError(f"feature(s) never matched any entity: {empty}")


class SplitService:
    """
    Temporal train/validation split.

    A quantile of the event timestamp rather than a fixed date, so the split
    survives the dataset window moving — the generator anchors its window to
    "now", so a hardcoded cutoff would drift into producing an empty validation
    set without failing.
    """

    def __init__(self, quantile: float = config.SPLIT_QUANTILE):
        if not 0 < quantile < 1:
            raise ValueError(f"split quantile must be in (0, 1), got {quantile}")
        self._quantile = quantile

    def split(self, dataset: TrainingDataset) -> DataSplit:
        frame = dataset.frame
        cutoff = frame[config.EVENT_TIMESTAMP].quantile(self._quantile)

        train = frame[frame[config.EVENT_TIMESTAMP] < cutoff]
        validation = frame[frame[config.EVENT_TIMESTAMP] >= cutoff]

        for name, part in (("train", train), ("validation", validation)):
            if part.empty:
                raise ValueError(f"{name} split is empty at cutoff {cutoff}")
            if part[config.LABEL].nunique() < 2:
                # A split with one class cannot train and cannot be scored.
                # Loud here beats a confusing sklearn error three calls later.
                raise ValueError(f"{name} split contains only one class at cutoff {cutoff}")

        return DataSplit(train=train, validation=validation, cutoff=cutoff)


class ModelBuilder:
    """
    Builds the estimator, with preprocessing inside it.

    The pipeline is the artifact. The prediction API reads raw feature values from
    Redis and must not be expected to reproduce an imputation strategy or a
    one-hot column order — if preprocessing lived outside the estimator, any drift
    between the two implementations would be train/serve skew that no test catches.
    """

    def __init__(self, random_state: int = config.RANDOM_STATE):
        self._random_state = random_state

    @staticmethod
    def to_matrix(frame: pd.DataFrame) -> pd.DataFrame:
        """Make dtypes explicit before the pipeline sees them."""
        matrix = frame[config.MODEL_FEATURES].copy()
        for column in config.NUMERIC:
            # float64 even for the whole-number features (age, claim_day_of_week,
            # the 90-day claim count). Not cosmetic: MLflow infers the model
            # signature from these dtypes, and an integer column in a signature
            # cannot represent a missing value — so a prediction request whose
            # Redis lookup missed one of them would be rejected by schema
            # enforcement rather than scored. Doubles carry NaN, so the served
            # model degrades instead of erroring.
            matrix[column] = matrix[column].astype("float64")
        for column in config.BINARY:
            # Feast returns booleans as object; nullable boolean -> float keeps a
            # missing value missing instead of coercing it to False.
            matrix[column] = matrix[column].astype("boolean").astype("float64")
        for column in config.CATEGORICAL:
            matrix[column] = matrix[column].astype(object)
        return matrix

    def preprocessor(self):
        from sklearn.compose import ColumnTransformer
        from sklearn.impute import SimpleImputer
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder, StandardScaler

        return ColumnTransformer(
            transformers=[
                (
                    "numeric",
                    Pipeline([
                        # Median, not mean: claim amounts are heavy-tailed.
                        ("impute", SimpleImputer(strategy="median")),
                        ("scale", StandardScaler()),
                    ]),
                    config.NUMERIC,
                ),
                ("binary", SimpleImputer(strategy="most_frequent"), config.BINARY),
                (
                    "categorical",
                    Pipeline([
                        # "unknown" as its own category rather than a filled-in
                        # guess: a null risk_segment means the column did not
                        # exist when that customer signed up, which is signal.
                        ("impute", SimpleImputer(strategy="constant", fill_value="unknown")),
                        # handle_unknown="ignore" so a category first seen in
                        # production cannot raise at prediction time.
                        ("encode", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                    ]),
                    config.CATEGORICAL,
                ),
            ],
            remainder="drop",
        )

    def build(self):
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline

        return Pipeline([
            ("prep", self.preprocessor()),
            # class_weight="balanced" reweights the loss by inverse class
            # frequency. Without it, at a ~6% base rate the model converges to
            # predicting "legit" for everything — ~94% accuracy, zero fraud found.
            ("clf", LogisticRegression(
                max_iter=2000,
                class_weight="balanced",
                random_state=self._random_state,
            )),
        ])

    def params(self) -> dict:
        """What to log as MLflow params: the shape of the model, not its weights."""
        return {
            "estimator": "LogisticRegression",
            "class_weight": "balanced",
            "max_iter": 2000,
            "random_state": self._random_state,
            "n_numeric": len(config.NUMERIC),
            "n_binary": len(config.BINARY),
            "n_categorical": len(config.CATEGORICAL),
            "excluded_from_model": ",".join(config.EXCLUDED_FROM_MODEL),
            "split_quantile": config.SPLIT_QUANTILE,
        }


@dataclass
class Evaluation:
    pr_auc: float
    roc_auc: float
    baseline_pr_auc: float
    operating_point: dict
    budget_curve: list[dict] = field(default_factory=list)

    def metrics(self) -> dict:
        """Flattened for MLflow, which takes scalar metrics only."""
        flat = {
            "pr_auc": self.pr_auc,
            "roc_auc": self.roc_auc,
            "baseline_pr_auc": self.baseline_pr_auc,
            "precision_at_budget": self.operating_point["precision"],
            "recall_at_budget": self.operating_point["recall"],
            "lift_at_budget": self.operating_point["lift_vs_random"],
            "fraud_caught_at_budget": self.operating_point["fraud_caught"],
        }
        for point in self.budget_curve:
            budget = int(point["budget"] * 100)
            flat[f"precision_at_{budget}pct"] = point["precision"]
            flat[f"recall_at_{budget}pct"] = point["recall"]
        return flat


class EvaluationService:
    """
    Scores a fitted model the way the business will read it.

    Accuracy is not among the metrics, and its absence is deliberate: a model that
    labels every claim "legit" scores ~89% on this validation set and catches no
    fraud, so accuracy is a number a do-nothing model wins.

    Ranking quality (PR-AUC) plus precision and recall at a *review budget* is the
    honest pair. The budget is a staffing capacity, so the numbers hold even if the
    model's probabilities are poorly calibrated — only the ordering has to be right.
    """

    def __init__(
        self,
        review_budget: float = config.REVIEW_BUDGET,
        budget_curve: tuple[float, ...] = config.BUDGET_CURVE,
    ):
        self._review_budget = review_budget
        self._budget_curve = budget_curve

    def evaluate(self, model, X_val: pd.DataFrame, y_val: np.ndarray) -> Evaluation:
        from sklearn.metrics import average_precision_score, roc_auc_score

        scores = model.predict_proba(X_val)[:, 1]
        baseline = float(np.mean(y_val))

        return Evaluation(
            pr_auc=float(average_precision_score(y_val, scores)),
            roc_auc=float(roc_auc_score(y_val, scores)),
            baseline_pr_auc=baseline,
            operating_point=self._at_budget(scores, y_val, baseline, self._review_budget),
            budget_curve=[
                self._at_budget(scores, y_val, baseline, budget)
                for budget in self._budget_curve
            ],
        )

    @staticmethod
    def _at_budget(scores: np.ndarray, y_val: np.ndarray, baseline: float, budget: float) -> dict:
        from sklearn.metrics import confusion_matrix

        threshold = float(np.quantile(scores, 1 - budget))
        flagged = scores >= threshold
        _, false_pos, false_neg, true_pos = confusion_matrix(
            y_val, flagged, labels=[0, 1]
        ).ravel()

        precision = float(true_pos / max(true_pos + false_pos, 1))
        return {
            "budget": budget,
            "threshold": threshold,
            "reviewed": int(flagged.sum()),
            "precision": precision,
            "recall": float(true_pos / max(true_pos + false_neg, 1)),
            "fraud_caught": int(true_pos),
            "fraud_missed": int(false_neg),
            "lift_vs_random": float(precision / baseline) if baseline else 0.0,
        }


class ModelRegistryService:
    """
    The only service that talks to MLflow.

    Two responsibilities kept apart on purpose:

      log_run       always. Every training run is recorded, successful or not
                    interesting, so runs are comparable over time.
      promote       never automatically. CLAUDE.md leaves model promotion as a
                    manual gate for now, so registering a version and *pointing
                    production at it* are different operations. `ml/promote.py`
                    is the manual step; an automated registry watcher is the
                    documented stretch goal.

    Promotion moves an alias rather than a stage, because MLflow 3 removed
    stages. An alias also moves atomically between versions, which is exactly the
    operation §13's champion/challenger ramp needs.
    """

    def __init__(
        self,
        tracking_uri: str = config.TRACKING_URI,
        experiment: str = config.EXPERIMENT,
        registered_model: str = config.REGISTERED_MODEL,
    ):
        import mlflow

        self._mlflow = mlflow
        self._registered_model = registered_model
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment)

    def log_run(
        self,
        model,
        dataset: TrainingDataset,
        split: DataSplit,
        evaluation: Evaluation,
        params: dict,
        input_example: pd.DataFrame,
        extra_tags: dict | None = None,
    ) -> dict:
        from mlflow.models import infer_signature

        with self._mlflow.start_run() as run:
            self._mlflow.log_params(params)
            self._mlflow.log_params({f"split_{k}": v for k, v in split.describe().items()})
            self._mlflow.log_metrics(evaluation.metrics())

            # The tag that makes a model traceable to its data (§7). Delta
            # versions every write to the Gold table, so this integer names the
            # exact snapshot — re-readable with `versionAsOf`.
            self._mlflow.set_tags({
                "data_version": dataset.data_version,
                "data_rows": dataset.rows,
                "feature_refs": ",".join(dataset.feature_refs),
                "label_source": config.LABEL_PATH,
                "split": f"temporal@{config.SPLIT_QUANTILE}",
                **(extra_tags or {}),
            })

            signature = infer_signature(
                input_example, model.predict_proba(input_example)[:, 1]
            )
            info = self._mlflow.sklearn.log_model(
                sk_model=model,
                name="model",
                signature=signature,
                input_example=input_example,
                registered_model_name=self._registered_model,
                # MLflow 3 serialises sklearn models with skops rather than
                # pickle, and skops refuses to load a type it has not been told
                # is safe. A ColumnTransformer stores numpy dtypes, so saving
                # fails without this.
                #
                # Declaring the type is the right fix rather than switching back
                # to `serialization_format="pickle"`: MLflow refuses to *load* a
                # pickled model unless MLFLOW_ALLOW_PICKLE_DESERIALIZATION is set
                # in the reader's environment, which would push a security opt-out
                # onto every consumer of this model — including KServe in §8.
                #
                # numpy.dtype is a type descriptor, not executable state, and the
                # list is persisted into the MLmodel flavor config, so readers
                # need no configuration of their own.
                skops_trusted_types=["numpy.dtype"],
            )

            return {
                "run_id": run.info.run_id,
                "experiment_id": run.info.experiment_id,
                "model_uri": info.model_uri,
                "version": self._latest_version(),
            }

    def promote(self, version: str, alias: str = config.PRODUCTION_ALIAS) -> None:
        """Point an alias at a version. The manual gate, called explicitly."""
        self._client().set_registered_model_alias(self._registered_model, alias, version)

    def aliased_model_location(self, alias: str = config.PRODUCTION_ALIAS) -> str | None:
        """
        The real `gs://` path of the aliased version — what KServe fetches (§7).

        Two indirections, neither obvious:

        1. A ModelVersion's `source` in MLflow 3 is `models:/<logged-model-id>`,
           not a storage path. Handing that to KServe would give it nothing to
           download.
        2. The id has to be parsed out of `source`, because the registry response
           leaves `ModelVersion.model_id` unset — reading that attribute returns
           None even though the id is right there in the URI.

        Resolving it here rather than in the serving config keeps the registry the
        single source of truth for where a model lives: promotion moves the alias,
        and the location follows.
        """
        version = self.current_alias_version(alias)
        if version is None:
            return None
        client = self._client()
        source = client.get_model_version(self._registered_model, version).source
        return client.get_logged_model(source.removeprefix("models:/")).artifact_location

    def current_alias_version(self, alias: str = config.PRODUCTION_ALIAS) -> str | None:
        try:
            return self._client().get_model_version_by_alias(
                self._registered_model, alias
            ).version
        except Exception:
            # No alias set yet is the normal state before the first promotion,
            # not an error worth propagating.
            return None

    def _client(self):
        from mlflow import MlflowClient

        return MlflowClient()

    def _latest_version(self) -> str | None:
        versions = self._client().search_model_versions(
            f"name='{self._registered_model}'", order_by=["version_number DESC"], max_results=1
        )
        return versions[0].version if versions else None
