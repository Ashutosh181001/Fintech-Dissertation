"""
model_utils.py (modular)
------------------------
Model loading and broadcasting utilities for the Spark streaming pipeline.

Goals
- Load per-symbol IsolationForest models from a directory (local) or GCS (optional)
- Provide a safe broadcast wrapper for executors
- Offer a tiny dummy model fallback for dev/testing when real models are absent

Conventions
- Filenames may be any of the following forms (case-insensitive):
  BTCUSDT.pkl, BTC_USDT.pkl, BTC-USDT.pkl, BTC-USD.pkl, BTC-USD.joblib, _default_.pkl
- Key used at inference time is uppercased and normalised to use "/" (e.g. BTC/USDT)

Notes
- GCS loading is optional and requires `gcsfs` or `google-cloud-storage` to be installed.
  If neither is available, the loader will log a warning and skip GCS.
"""
from __future__ import annotations
from typing import Dict, Iterable, Optional
import os
import io
import re
import glob
import logging

# Optional deps
try:  # joblib preferred for sklearn objects
    import joblib  # type: ignore
except Exception:  # pragma: no cover
    joblib = None

import pickle

# Optional: create dummy models if sklearn not available
try:
    from sklearn.ensemble import IsolationForest  # type: ignore
    _SKLEARN_AVAILABLE = True
except Exception:  # pragma: no cover
    IsolationForest = None  # type: ignore
    _SKLEARN_AVAILABLE = False

logger = logging.getLogger(__name__)


# -------------------------
# Key normalisation
# -------------------------

def normalise_symbol_key(sym: str) -> str:
    if not sym:
        return ""
    s = sym.upper().strip()
    # Common normalisations
    s = s.replace("-", "/")
    if "/" not in s and len(s) > 3:
        # Attempt to insert slash for pairs like BTCUSDT -> BTC/USDT
        # Heuristic: split roughly in half if ends with USDT, USD, USDC, BTC, ETH, BNB
        for quote in ("USDT", "USD", "USDC", "BTC", "ETH", "BNB"):
            if s.endswith(quote) and len(s) > len(quote):
                base = s[: -len(quote)]
                return f"{base}/{quote}"
    return s


# -------------------------
# Loaders
# -------------------------

def _load_model_bytes(buf: bytes):
    """Try joblib first, then pickle."""
    if joblib is not None:
        try:
            return joblib.load(io.BytesIO(buf))
        except Exception:
            pass
    # Fallback: pickle
    return pickle.load(io.BytesIO(buf))


def _load_model_file(path: str):
    with open(path, "rb") as f:
        data = f.read()
    return _load_model_bytes(data)


def _list_local_model_files(root: str) -> Iterable[str]:
    patterns = [
        os.path.join(root, "*.pkl"),
        os.path.join(root, "*.pickle"),
        os.path.join(root, "*.joblib"),
    ]
    files = []
    for p in patterns:
        files.extend(glob.glob(p))
    return files


def _list_gcs_model_files(gcs_path: str) -> Iterable[str]:  # pragma: no cover
    """Return list of fully-qualified gs:// paths. Requires gcsfs or google-cloud-storage."""
    try:
        import gcsfs  # type: ignore
        fs = gcsfs.GCSFileSystem()
        # Expand wildcard if user passed gs://bucket/path/*.pkl
        if gcs_path.endswith("*"):
            return [f"gs://{p}" for p in fs.glob(gcs_path.replace("gs://", ""))]
        # Otherwise list directory
        if gcs_path.endswith("/"):
            base = gcs_path.replace("gs://", "")
        else:
            base = gcs_path.replace("gs://", "") + "/"
        return [f"gs://{p}" for p in fs.glob(base + "*.p*")]
    except Exception:
        # Try google-cloud-storage as a fallback
        try:
            from google.cloud import storage  # type: ignore
            if not gcs_path.startswith("gs://"):
                raise ValueError("gcs_path must start with gs://")
            _, rest = gcs_path.split("gs://", 1)
            parts = rest.split("/", 1)
            bucket_name = parts[0]
            prefix = parts[1] if len(parts) > 1 else ""
            if prefix and not prefix.endswith("/"):
                prefix = prefix + "/"
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blobs = list(client.list_blobs(bucket, prefix=prefix))
            return [f"gs://{bucket_name}/{b.name}" for b in blobs if b.name.lower().endswith((".pkl", ".pickle", ".joblib"))]
        except Exception as e:
            logger.warning("GCS listing unavailable: %s", e)
            return []


def _read_gcs_file(path: str) -> Optional[bytes]:  # pragma: no cover
    try:
        import gcsfs  # type: ignore
        fs = gcsfs.GCSFileSystem()
        with fs.open(path, "rb") as f:
            return f.read()
    except Exception:
        try:
            from google.cloud import storage  # type: ignore
            if not path.startswith("gs://"):
                raise ValueError("gcs path must start with gs://")
            _, rest = path.split("gs://", 1)
            bucket_name, blob_name = rest.split("/", 1)
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            return blob.download_as_bytes()
        except Exception as e:
            logger.warning("GCS read unavailable for %s: %s", path, e)
            return None


# Public API
# -----------

def load_models_from_path(models_path: str, symbols: Iterable[str]) -> Dict[str, object]:
    """Load models for the requested symbols from a local dir or GCS path.

    Parameters
    ----------
    models_path : str
        Local directory (e.g., ./models) or GCS prefix (gs://bucket/path)
    symbols : Iterable[str]
        Symbol universe to look for. Also tries a `_default_` model.
    """
    models: Dict[str, object] = {}
    if not models_path:
        logger.warning("No models_path provided; returning empty model set.")
        return models

    if models_path.startswith("gs://"):
        logger.info("Loading models from GCS: %s", models_path)
        files = list(_list_gcs_model_files(models_path))
        for fpath in files:
            buf = _read_gcs_file(fpath)
            if buf is None:
                continue
            try:
                model = _load_model_bytes(buf)
            except Exception as e:
                logger.warning("Failed to load model %s: %s", fpath, e)
                continue
            key = _infer_key_from_filename(os.path.basename(fpath))
            models[key] = model
    else:
        logger.info("Loading models from local dir: %s", models_path)
        files = _list_local_model_files(models_path)
        for fpath in files:
            try:
                model = _load_model_file(fpath)
            except Exception as e:
                logger.warning("Failed to load model %s: %s", fpath, e)
                continue
            key = _infer_key_from_filename(os.path.basename(fpath))
            models[key] = model

    # Filter/normalise keys to requested symbols
    requested = {normalise_symbol_key(s) for s in symbols}
    out: Dict[str, object] = {}
    for k, v in models.items():
        nk = normalise_symbol_key(k)
        if nk in requested or k == "_default_":
            out[nk if k != "_default_" else k] = v

    logger.info("Loaded %d model(s) for requested symbols (plus any default).", len(out))
    return out


def _infer_key_from_filename(name: str) -> str:
    n = name.lower()
    if n.startswith("_default_"):
        return "_default_"
    base = os.path.splitext(name)[0]
    return normalise_symbol_key(base)


# Broadcasting
# ------------

def broadcast_models(spark, models: Dict[str, object]):
    if not models:
        logger.warning("Broadcasting empty model dict.")
    return spark.sparkContext.broadcast(models)


# Dummy models for development
# ----------------------------

class _ThresholdDummyIF:
    """A tiny stub mimicking IsolationForest predict/score APIs.

    - predict(X): returns 1 for normal, -1 for anomaly
    - decision_function(X): higher = more normal, so we return negative abs(z)
      to emulate anomalies at large |z| and |pct|.
    """
    def __init__(self, z_thr: float = 3.5, pct_thr: float = 5.0):
        self.z_thr = z_thr
        self.pct_thr = pct_thr

    def predict(self, X):
        import numpy as np
        z = np.asarray(X)[:, 0]
        pct = np.asarray(X)[:, 1]
        mask = (np.abs(z) > self.z_thr) | (np.abs(pct) > self.pct_thr)
        out = np.ones(len(z), dtype=int)
        out[mask] = -1
        return out

    def decision_function(self, X):
        import numpy as np
        z = np.asarray(X)[:, 0]
        pct = np.asarray(X)[:, 1]
        # lower means more anomalous in IF; return negative magnitude
        return -np.maximum(np.abs(z) / max(self.z_thr, 1e-9), np.abs(pct) / max(self.pct_thr, 1e-9))


def create_dummy_models(symbols: Iterable[str], z_thr: float = 3.5, pct_thr: float = 5.0) -> Dict[str, object]:
    logger.warning("Creating dummy IsolationForest-like models for symbols: %s", ", ".join(symbols))
    return {normalise_symbol_key(s): _ThresholdDummyIF(z_thr=z_thr, pct_thr=pct_thr) for s in symbols}


def ensure_broadcast_models(
    spark,
    models_path: str,
    symbols: Iterable[str],
    allow_dummy: bool = True,
) -> Optional[object]:
    """Load models and broadcast them. Optionally create dummy models if none found.

    Returns
    -------
    Broadcast or None
    """
    models = load_models_from_path(models_path, symbols)
    if not models and allow_dummy:
        models = create_dummy_models(symbols)
    if not models:
        logger.warning("No models available to broadcast.")
        return None
    return broadcast_models(spark, models)
