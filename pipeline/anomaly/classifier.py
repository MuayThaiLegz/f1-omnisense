"""
F1 Severity Classifier — Supervised stage on ensemble pseudo-labels.

Ported from OmniSense DataSense AnomalyClassifier (core/ViaAnomalyDetect.py),
adapted for F1 telemetry: multi-class severity, small sequential race data,
per-system classifiers, temporal feature engineering.

The severity output feeds preventative maintenance scheduling:
  critical → alert_and_remediate
  high     → alert
  medium   → log_and_monitor
  low      → log
  normal   → none
"""

import logging
import pickle
import re
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from lightgbm import LGBMClassifier
    LGBM_AVAILABLE = True
except ImportError:
    LGBM_AVAILABLE = False

from sklearn.utils.class_weight import compute_sample_weight

logger = logging.getLogger(__name__)

SEVERITY_CLASSES = ["normal", "low", "medium", "high", "critical"]
SEVERITY_TO_INT = {s: i for i, s in enumerate(SEVERITY_CLASSES)}
INT_TO_SEVERITY = {i: s for i, s in enumerate(SEVERITY_CLASSES)}

MAINTENANCE_ACTION_MAP = {
    "critical": "alert_and_remediate",
    "high": "alert",
    "medium": "log_and_monitor",
    "low": "log",
    "normal": "none",
}

# Minimum samples needed to attempt classifier training
MIN_TRAIN_SAMPLES = 6
# Minimum number of distinct severity classes to train
MIN_CLASSES = 2


# ─── Feature Engineering ─────────────────────────────────────────────

class F1FeatureEngineer:
    """F1-specific temporal feature engineering for race-sequential telemetry."""

    def add_temporal_features(self, df: pd.DataFrame, feature_cols: list) -> tuple:
        """
        Add race-over-race deltas and rolling stats.

        Returns (enriched_df, list_of_new_col_names).
        """
        new_cols = []
        for col in feature_cols:
            if col not in df.columns:
                continue

            # Race-over-race delta (degradation signal)
            delta_col = f"{col}_delta"
            df[delta_col] = df[col].diff().fillna(0)
            new_cols.append(delta_col)

            # 3-race rolling mean and std (trend smoothing)
            roll_mean_col = f"{col}_roll3_mean"
            roll_std_col = f"{col}_roll3_std"
            df[roll_mean_col] = df[col].rolling(3, min_periods=1).mean()
            df[roll_std_col] = df[col].rolling(3, min_periods=1).std().fillna(0)
            new_cols.append(roll_mean_col)
            new_cols.append(roll_std_col)

        return df, new_cols

    def add_season_context(self, df: pd.DataFrame) -> tuple:
        """
        Add season-position features for component wear accumulation.

        Returns (enriched_df, list_of_new_col_names).
        """
        n = len(df)
        df["race_index"] = np.arange(n)
        df["season_pct"] = df["race_index"] / max(n - 1, 1)
        df["is_second_half"] = (df["season_pct"] >= 0.5).astype(int)
        return df, ["race_index", "season_pct", "is_second_half"]

    def engineer(self, df: pd.DataFrame, feature_cols: list) -> tuple:
        """
        Full pipeline.  Returns (enriched_df, all_feature_cols).

        all_feature_cols includes originals + new temporal + season cols.
        """
        df = df.copy()
        df, temporal_cols = self.add_temporal_features(df, feature_cols)
        df, season_cols = self.add_season_context(df)
        all_features = list(feature_cols) + temporal_cols + season_cols
        return df, all_features


# ─── Classifier ──────────────────────────────────────────────────────

# Ported from OmniSense AnomalyClassifier (ViaAnomalyDetect.py:753-762)
_LABEL_LEAK_PATTERN = re.compile(
    r"(anomaly|error|ensemble|score|level|cluster|voted|weighted|"
    r"enhanced|dynamic|severity|distance|reliability|voting)",
    re.IGNORECASE,
)
_IDENTIFIER_PATTERN = re.compile(
    r"^(id|_id|index|idx|race|driver|samples|bio_samples)$",
    re.IGNORECASE,
)


def _remove_label_leakage(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only numeric columns that won't leak label info."""
    numeric = df.select_dtypes(include=[np.number])
    safe = [
        c for c in numeric.columns
        if not _LABEL_LEAK_PATTERN.search(c) and not _IDENTIFIER_PATTERN.match(c)
    ]
    return numeric[safe]


def _sanitize_feature_names(df: pd.DataFrame) -> pd.DataFrame:
    """Replace chars LightGBM can't handle.  Ported from OmniSense (ViaAnomalyDetect.py:775)."""
    sanitized = [re.sub(r"[^a-zA-Z0-9_]", "_", str(c)) for c in df.columns]
    seen: dict = {}
    unique = []
    for c in sanitized:
        if c in seen:
            seen[c] += 1
            unique.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            unique.append(c)
    out = df.copy()
    out.columns = unique
    return out


class F1SeverityClassifier:
    """
    Multi-class severity classifier for F1 telemetry.

    Trains LightGBM on ensemble pseudo-labels with confidence weighting.
    Adapted from OmniSense for small data (~18 races per driver-system).
    """

    def _compute_confidence_weights(self, df: pd.DataFrame) -> tuple:
        """
        Confidence weighting via Anomaly_Score_STD.
        Ported from OmniSense (ViaAnomalyDetect.py:866-875).

        Returns (mask, weights).  mask is boolean array of kept samples,
        weights is confidence array for those samples.
        """
        if "Anomaly_Score_STD" not in df.columns:
            return np.ones(len(df), dtype=bool), None

        confidence = 1 - df["Anomaly_Score_STD"].rank(pct=True)

        # Relaxed threshold for small data (OmniSense uses 0.2)
        mask = confidence > 0.1

        # If filtering removes too many, keep all
        if mask.sum() < MIN_TRAIN_SAMPLES:
            return np.ones(len(df), dtype=bool), confidence.values

        return mask.values, confidence[mask].values

    def train(self, df: pd.DataFrame, label_col: str = "Anomaly_Level") -> dict:
        """
        Train LightGBM multi-class on ensemble pseudo-labels.

        Returns model_bundle dict, or None if training is not possible.
        """
        if not LGBM_AVAILABLE:
            logger.warning("LightGBM not installed — classifier disabled")
            return None

        if label_col not in df.columns:
            logger.warning(f"Label column '{label_col}' not found")
            return None

        # Encode labels
        y_all = df[label_col].map(SEVERITY_TO_INT)
        if y_all.isna().any():
            y_all = y_all.fillna(SEVERITY_TO_INT["normal"])
        y_all = y_all.astype(int).values

        n_classes = len(np.unique(y_all))
        if n_classes < MIN_CLASSES:
            logger.info(f"Only {n_classes} severity class(es) — skipping classifier")
            return None

        if len(df) < MIN_TRAIN_SAMPLES:
            logger.info(f"Only {len(df)} samples — skipping classifier")
            return None

        # Remove leakage, sanitize
        x_clean = _remove_label_leakage(df)
        if x_clean.empty:
            logger.warning("No features left after leakage removal")
            return None

        original_features = list(x_clean.columns)
        x_safe = _sanitize_feature_names(x_clean)

        # Confidence weighting (ported from OmniSense)
        conf_mask, conf_weights = self._compute_confidence_weights(df)
        x_train = x_safe.loc[conf_mask].copy() if not conf_mask.all() else x_safe.copy()
        y_train = y_all[conf_mask] if not conf_mask.all() else y_all

        # Handle NaN/Inf
        x_train = x_train.replace([np.inf, -np.inf], 0).fillna(0)

        # Class balance weights
        class_weights = compute_sample_weight("balanced", y_train)

        # Combine: confidence * class_balance (element-wise)
        if conf_weights is not None and len(conf_weights) == len(class_weights):
            combined_weights = conf_weights * class_weights
        else:
            combined_weights = class_weights

        # LightGBM — tuned for small F1 data
        model = LGBMClassifier(
            n_estimators=50,
            num_leaves=7,
            max_depth=3,
            min_child_samples=3,
            subsample=0.8,
            colsample_bytree=0.8,
            learning_rate=0.1,
            reg_alpha=1.0,
            reg_lambda=2.0,
            objective="multiclass",
            num_class=len(SEVERITY_CLASSES),
            random_state=42,
            n_jobs=-1,
            verbose=-1,
        )

        model.fit(x_train, y_train, sample_weight=combined_weights)

        # LOO-CV for diagnostics (small data — fast)
        cv_acc = self._loo_cv_accuracy(x_train, y_train, combined_weights)

        # Class distribution
        unique, counts = np.unique(y_train, return_counts=True)
        class_dist = {INT_TO_SEVERITY.get(int(u), "?"): int(c) for u, c in zip(unique, counts)}

        bundle = {
            "model": model,
            "feature_names": list(x_safe.columns),
            "original_feature_names": original_features,
            "label_map": SEVERITY_TO_INT,
            "cv_accuracy": cv_acc,
            "class_distribution": class_dist,
            "n_samples": len(y_train),
        }

        logger.info(
            f"  Classifier trained: {len(y_train)} samples, "
            f"{n_classes} classes, LOO-CV={cv_acc:.2%}, dist={class_dist}"
        )
        return bundle

    def _loo_cv_accuracy(self, X: pd.DataFrame, y: np.ndarray, weights: np.ndarray) -> float:
        """Leave-one-out cross-validation accuracy for small datasets."""
        if len(y) < MIN_TRAIN_SAMPLES:
            return 0.0

        correct = 0
        for i in range(len(y)):
            idx = np.concatenate([np.arange(0, i), np.arange(i + 1, len(y))])
            X_tr = X.iloc[idx]
            y_tr = y[idx]
            w_tr = weights[idx]

            n_classes = len(np.unique(y_tr))
            if n_classes < MIN_CLASSES:
                continue

            m = LGBMClassifier(
                n_estimators=30, num_leaves=5, max_depth=3,
                min_child_samples=2, reg_alpha=1.0, reg_lambda=2.0,
                objective="multiclass", num_class=len(SEVERITY_CLASSES),
                random_state=42, verbose=-1, n_jobs=1,
            )
            m.fit(X_tr, y_tr, sample_weight=w_tr)
            pred = m.predict(X.iloc[[i]])
            if pred[0] == y[i]:
                correct += 1

        return correct / len(y) if len(y) > 0 else 0.0

    def predict(self, df: pd.DataFrame, model_bundle: dict) -> pd.DataFrame:
        """
        Predict severity + probabilities.

        Adds columns: classifier_severity, classifier_confidence,
        classifier_prob_{normal,low,medium,high,critical}
        """
        if model_bundle is None:
            return self._fallback(df)

        model = model_bundle["model"]
        train_features = model_bundle["original_feature_names"]

        # Remove leakage, align features
        x_clean = _remove_label_leakage(df)

        # Add missing cols, drop extra
        for col in train_features:
            if col not in x_clean.columns:
                x_clean[col] = 0
        x_clean = x_clean[train_features]
        x_safe = _sanitize_feature_names(x_clean)
        x_safe = x_safe.replace([np.inf, -np.inf], 0).fillna(0)

        preds = model.predict(x_safe)
        proba = model.predict_proba(x_safe)

        # Map LightGBM's class indices to our severity labels.
        # model.classes_ contains the actual class ints that were in training data,
        # which may be a subset of [0,1,2,3,4] if some severities were absent.
        trained_classes = model.classes_  # e.g. [0, 1, 3, 4] if "medium" was absent

        out = df.copy()
        out["classifier_severity"] = [INT_TO_SEVERITY.get(int(p), "normal") for p in preds]
        out["classifier_confidence"] = proba.max(axis=1)

        for sev in SEVERITY_CLASSES:
            sev_int = SEVERITY_TO_INT[sev]
            if sev_int in trained_classes:
                col_idx = list(trained_classes).index(sev_int)
                out[f"classifier_prob_{sev}"] = proba[:, col_idx]
            else:
                out[f"classifier_prob_{sev}"] = 0.0

        return out

    def _fallback(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pass through ensemble labels when classifier can't train."""
        out = df.copy()
        out["classifier_severity"] = out.get("Anomaly_Level", "normal")
        out["classifier_confidence"] = 0.0
        for sev in SEVERITY_CLASSES:
            out[f"classifier_prob_{sev}"] = 0.0
        return out


# ─── Pipeline Orchestrator ───────────────────────────────────────────

class ClassifierPipeline:
    """
    Orchestrator: feature-engineer → train per-system → predict → persist.
    Called from run_f1_anomaly after the ensemble step.
    """

    def __init__(self, output_dir: Path = None):
        self.output_dir = output_dir or (
            Path(__file__).resolve().parents[1] / "output" / "classifiers"
        )
        self.engineer = F1FeatureEngineer()
        self.classifier = F1SeverityClassifier()

    def _model_path(self, driver_code: str, system: str) -> Path:
        safe_system = re.sub(r"[^a-zA-Z0-9]", "_", system)
        return self.output_dir / f"{driver_code}_{safe_system}.pkl"

    def save_model(self, model_bundle: dict, driver_code: str, system: str) -> Path:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        path = self._model_path(driver_code, system)
        with open(path, "wb") as f:
            pickle.dump(model_bundle, f)
        return path

    def load_model(self, driver_code: str, system: str):
        path = self._model_path(driver_code, system)
        if not path.exists():
            return None
        with open(path, "rb") as f:
            return pickle.load(f)

    def train_and_predict_system(
        self,
        merged_df: pd.DataFrame,
        system: str,
        feature_cols: list,
        ensemble_result_df: pd.DataFrame,
        driver_code: str,
    ) -> pd.DataFrame:
        """
        Full classifier pipeline for one driver + one system.

        1. Feature-engineer merged telemetry
        2. Combine with ensemble results (labels + STD for confidence)
        3. Train classifier on pseudo-labels
        4. Predict (smoothed probabilities)
        5. Persist model
        """
        # 1. Feature engineering on raw telemetry
        enriched, all_feat_cols = self.engineer.engineer(merged_df, feature_cols)

        # 2. Combine: engineered features + ensemble label columns
        ensemble_cols = [c for c in ensemble_result_df.columns
                         if c in ("Anomaly_Level", "Anomaly_Score_STD", "Anomaly_Score_Mean")]
        combined = enriched[all_feat_cols].copy()
        for col in ensemble_cols:
            combined[col] = ensemble_result_df[col].values

        # 3. Train
        model_bundle = self.classifier.train(combined, label_col="Anomaly_Level")

        # 4. Predict
        result = self.classifier.predict(combined, model_bundle)

        # 5. Persist
        if model_bundle is not None:
            path = self.save_model(model_bundle, driver_code, system)
            logger.info(f"  Model saved: {path.name}")

        return result

    @staticmethod
    def enrich_health_entry(entry: dict, classifier_row) -> dict:
        """
        Add classifier fields to an existing per-race per-system JSON entry.
        Non-destructive — only adds new keys.
        """
        severity = classifier_row.get("classifier_severity", entry.get("level", "normal"))
        confidence = float(classifier_row.get("classifier_confidence", 0))

        entry["classifier_severity"] = severity
        entry["classifier_confidence"] = round(confidence, 4)
        entry["severity_probabilities"] = {
            sev: round(float(classifier_row.get(f"classifier_prob_{sev}", 0)), 4)
            for sev in SEVERITY_CLASSES
        }
        entry["maintenance_action"] = MAINTENANCE_ACTION_MAP.get(severity, "none")
        return entry
