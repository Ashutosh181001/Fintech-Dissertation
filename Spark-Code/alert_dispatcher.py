"""
alert_dispatcher.py (config-only)
---------------------------------
Sidecar that polls recent anomalies from your sinks (Local/GCS Parquet or BigQuery)
and forwards alert payloads via `alerts.AlertManager`.

Config-only behavior:
- Reads everything from config.py (no environment variables).
- You may add an optional section in config.py:

DISPATCHER = {
    "lookback_minutes": 5,
    "poll_interval_sec": 20,
    # Optional: explicit state file path; if absent we auto-place it next to the sink
    # "state_path": "./output/anomaly_alert_state.json",
}

If DISPATCHER is missing, sensible defaults above are used.
"""
from __future__ import annotations
from typing import List, Dict, Any, Optional
import os
import time
import json
import glob
import logging
from datetime import datetime, timedelta

import pandas as pd  # type: ignore

import config
from alerts import AlertManager

logger = logging.getLogger("alert-dispatcher")


# -------------------------
# Config accessors (with defaults)
# -------------------------
_DISPATCH = getattr(config, "DISPATCHER", {}) or {}
_LOOKBACK_MIN = int(_DISPATCH.get("lookback_minutes", 5))
_POLL_SEC = int(_DISPATCH.get("poll_interval_sec", 20))


def _default_state_path() -> str:
    mode = (config.OUTPUT_MODE or "local").lower()
    if mode == "local":
        base = config.LOCAL_OUTPUT_DIR
        return os.path.join(base, "anomaly_alert_state.json")
    # Otherwise temp path in GCS case; for BQ we still keep a small local file
    return os.path.join("/tmp", "crypto", "anomaly_alert_state.json")


_STATE_PATH = _DISPATCH.get("state_path") or _default_state_path()


# -------------------------
# State persistence
# -------------------------

def _load_state(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {"last_ts_iso": None}


def _save_state(path: str, state: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(state, f)
    except Exception as e:
        logger.warning("Failed to persist state %s: %s", path, e)


# -------------------------
# Fetchers
# -------------------------

def _read_parquet_local(base_dir: str, since_ts: datetime) -> pd.DataFrame:
    anom_dir = os.path.join(base_dir, "anomalies")
    pattern = os.path.join(anom_dir, "**", "*.parquet")
    files = glob.glob(pattern, recursive=True)
    if not files:
        logger.info("No parquet files yet under %s", anom_dir)
        return pd.DataFrame()

    # Only read recent files (last 24 hours of files)
    recent_files = []
    cutoff_time = time.time() - (24 * 3600)  # 24 hours ago
    for f in files:
        if os.path.getmtime(f) > cutoff_time:
            recent_files.append(f)

    if not recent_files:
        return pd.DataFrame()

    # Read with filters
    try:
        df = pd.read_parquet(
            recent_files,
            filters=[('timestamp', '>=', since_ts.isoformat())]
        )
        return df
    except Exception as e:
        logger.warning("Parquet read failed at %s: %s", anom_dir, e)
        return pd.DataFrame()


def _read_parquet_gcs(bucket: str) -> pd.DataFrame:  # pragma: no cover
    # Reads all parquet parts under gs://<bucket>/anomalies/**.parquet
    try:
        import fsspec  # noqa: F401
    except Exception:
        pass  # pandas/pyarrow can often handle gs:// via GCSFS if installed
    path = f"gs://{bucket}/anomalies/**/*.parquet"
    try:
        return pd.read_parquet(path)
    except Exception as e:
        logger.warning("GCS parquet read failed: %s", e)
        return pd.DataFrame()


def _read_bigquery(project: str, dataset: str, since_ts: datetime) -> pd.DataFrame:  # pragma: no cover
    try:
        from google.cloud import bigquery  # type: ignore
    except Exception as e:
        logger.error("google-cloud-bigquery not available: %s", e)
        return pd.DataFrame()

    client = bigquery.Client(project=project)
    sql = f"""
        SELECT symbol, timestamp, price, z_score, price_change_pct, volume_spike,
               anomaly_type, anomaly_score
        FROM `{project}.{dataset}.anomalies`
        WHERE timestamp >= @since
        ORDER BY timestamp ASC
        LIMIT 5000
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("since", "TIMESTAMP", since_ts.isoformat())]
    )
    try:
        return client.query(sql, job_config=job_config).result().to_dataframe(create_bqstorage_client=True)
    except Exception as e:
        logger.warning("BigQuery query failed: %s", e)
        return pd.DataFrame()


# -------------------------
# Core helpers
# -------------------------

def _now_local() -> datetime:
    # Use naive local time consistently (we already write local time in parquet)
    return datetime.now()


def _select_since(df: pd.DataFrame, since_ts: datetime) -> pd.DataFrame:
    if df.empty:
        return df
    if "timestamp" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        try:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False)
        except Exception:
            pass
    return df[df["timestamp"] >= since_ts]


def _make_payloads(df: pd.DataFrame) -> List[Dict[str, Any]]:
    cols = [
        "symbol", "timestamp", "price", "z_score", "price_change_pct",
        "volume_spike", "anomaly_type", "anomaly_score",
    ]
    avail = [c for c in cols if c in df.columns]
    out = []
    if not avail:
        return out
    df2 = df[avail].copy()
    df2.sort_values("timestamp", inplace=True)
    for _, row in df2.iterrows():
        out.append({
            "symbol": row.get("symbol"),
            "timestamp": row.get("timestamp").isoformat() if pd.notnull(row.get("timestamp")) else None,
            "price": _safe_float(row.get("price")),
            "z_score": _safe_float(row.get("z_score")),
            "price_change_pct": _safe_float(row.get("price_change_pct")),
            "volume_spike": _safe_float(row.get("volume_spike")),
            "anomaly_type": row.get("anomaly_type"),
            "anomaly_score": _safe_float(row.get("anomaly_score")),
            "source": "dispatcher",
        })
    return out


def _safe_float(x):
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


# -------------------------
# One-shot and loop
# -------------------------

def run_once(am: AlertManager, state: Dict[str, Any]) -> Dict[str, Any]:
    mode = config.OUTPUT_MODE.lower()

    # Determine starting point
    if state.get("last_ts_iso"):
        try:
            since = datetime.fromisoformat(state["last_ts_iso"])  # local naive
        except Exception:
            since = _now_local() - timedelta(minutes=_LOOKBACK_MIN)
        since = max(since, _now_local() - timedelta(minutes=_LOOKBACK_MIN))
    else:
        since = _now_local() - timedelta(minutes=_LOOKBACK_MIN)

    if mode == "local":
        df = _read_parquet_local(config.LOCAL_OUTPUT_DIR, since)
    elif mode == "gcs":  # pragma: no cover
        df = _read_parquet_gcs(config.GCS_BUCKET)
    elif mode == "bigquery":  # pragma: no cover
        df = _read_bigquery(config.GCP_PROJECT, config.BQ_DATASET, since)
    else:
        logger.error("Unsupported OUTPUT_MODE for dispatcher: %s", mode)
        return state

    df = _select_since(df, since)
    if df.empty:
        logger.info("No anomalies found since %s", since.isoformat())
        return state

    payloads = _make_payloads(df)
    logger.info("Found %d anomalies to consider for alerts.", len(payloads))

    last_ts = state.get("last_ts_iso")
    for p in payloads:
        ok = am.send_alert(p)
        if ok and p.get("timestamp"):
            last_ts = p["timestamp"]

    if last_ts:
        state["last_ts_iso"] = last_ts
    return state


def run_loop() -> None:
    # Logging setup
    try:
        logging.basicConfig(
            level=getattr(logging, config.LOGGING_CONFIG["level"]),
            format=config.LOGGING_CONFIG["format"],
            datefmt=config.LOGGING_CONFIG["date_format"],
        )
    except Exception:
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    logger.info("Starting alert dispatcher (mode=%s)", config.OUTPUT_MODE)

    state = _load_state(_STATE_PATH)
    am = AlertManager()

    try:
        while True:
            state = run_once(am, state)
            _save_state(_STATE_PATH, state)
            time.sleep(_POLL_SEC)
    except KeyboardInterrupt:
        logger.info("Interrupt received; exiting.")
    except Exception as e:
        logger.exception("Dispatcher error: %s", e)


if __name__ == "__main__":
    run_loop()
