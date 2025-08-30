"""
udfs.py (modular)
-----------------
Factory functions that return Pandas UDFs for model-based anomaly inference.
These UDFs are designed to run on Spark executors and use a broadcasted
model dictionary keyed by trading symbol (e.g., "BTC/USDT").

Changes in this revision
- Symbol normalization now reuses model_utils.normalise_symbol_key to ensure
  the same logic as model loading (handles BTCUSDT -> BTC/USDT etc.).
"""
from __future__ import annotations
from typing import Dict, Any
import logging

import numpy as np
import pandas as pd

from pyspark.sql import types as T
from pyspark.sql.functions import pandas_udf

# Reuse the same normaliser as the loader
from model_utils import normalise_symbol_key

logger = logging.getLogger(__name__)


# -------------------------
# Helpers
# -------------------------

def _get_model(models: Dict[str, Any], sym: str):
    """Pick a model for symbol with a couple of fallbacks."""
    if not models:
        return None
    key = normalise_symbol_key(sym)
    if key in models:
        return models[key]
    # Try common alternate forms
    alt = key.replace("/", "")
    if alt in models:
        return models[alt]
    # Fallback to a default model if present
    return models.get("_default_", None)


# -------------------------
# Pandas UDF factories
# -------------------------

def create_isolation_forest_udf(bc_models):
    """Return a Pandas UDF that predicts anomaly (1) or normal (0)."""
    logger.info("Creating Isolation Forest predict UDF (broadcast id: %s)", id(bc_models))

    @pandas_udf(T.IntegerType())
    def predict_udf(symbol: pd.Series, z: pd.Series, pct: pd.Series, gap: pd.Series) -> pd.Series:
        try:
            models = bc_models.value if bc_models is not None else None
        except Exception:
            models = None

        # Prepare features; fill NaNs so sklearn doesn't crash
        z_ = pd.to_numeric(z, errors="coerce").fillna(0.0).to_numpy()
        pct_ = pd.to_numeric(pct, errors="coerce").fillna(0.0).to_numpy()
        gap_ = pd.to_numeric(gap, errors="coerce").fillna(0.0).to_numpy()
        X = np.column_stack([z_, pct_, gap_])

        out = np.zeros(len(z_), dtype=np.int32)
        if models is None or len(models) == 0:
            return pd.Series(out)

        # Batch by symbol to avoid model switches per row
        syms = symbol.fillna("").astype(str).tolist()
        idx_by_sym: Dict[str, list[int]] = {}
        for i, s in enumerate(syms):
            idx_by_sym.setdefault(normalise_symbol_key(s), []).append(i)

        for sym_key, idxs in idx_by_sym.items():
            model = _get_model(models, sym_key)
            if model is None:
                continue
            Xi = X[idxs]
            try:
                # sklearn IsolationForest: predict -> 1 (normal) or -1 (anomaly)
                pred = model.predict(Xi)
                out[idxs] = (pred == -1).astype(np.int32)
            except Exception:
                # Be safe: treat as normal
                out[idxs] = 0

        return pd.Series(out)

    return predict_udf


def create_anomaly_score_udf(bc_models):
    """Return a Pandas UDF that yields an anomaly score (float, higher=worse)."""
    logger.info("Creating Isolation Forest score UDF (broadcast id: %s)", id(bc_models))

    @pandas_udf(T.DoubleType())
    def score_udf(symbol: pd.Series, z: pd.Series, pct: pd.Series, gap: pd.Series) -> pd.Series:
        try:
            models = bc_models.value if bc_models is not None else None
        except Exception:
            models = None

        z_ = pd.to_numeric(z, errors="coerce").fillna(0.0).to_numpy()
        pct_ = pd.to_numeric(pct, errors="coerce").fillna(0.0).to_numpy()
        gap_ = pd.to_numeric(gap, errors="coerce").fillna(0.0).to_numpy()
        X = np.column_stack([z_, pct_, gap_])

        out = np.zeros(len(z_), dtype=np.float64)
        if models is None or len(models) == 0:
            return pd.Series(out)

        syms = symbol.fillna("").astype(str).tolist()
        idx_by_sym: Dict[str, list[int]] = {}
        for i, s in enumerate(syms):
            idx_by_sym.setdefault(normalise_symbol_key(s), []).append(i)

        for sym_key, idxs in idx_by_sym.items():
            model = _get_model(models, sym_key)
            if model is None:
                continue
            Xi = X[idxs]
            try:
                if hasattr(model, "score_samples"):
                    # Lower (more negative) means more anomalous in IF; flip the sign
                    scores = -model.score_samples(Xi)
                elif hasattr(model, "decision_function"):
                    scores = -model.decision_function(Xi)
                else:
                    scores = np.zeros(len(idxs), dtype=np.float64)
                out[idxs] = scores
            except Exception:
                # default 0.0
                out[idxs] = 0.0

        return pd.Series(out)

    return score_udf
