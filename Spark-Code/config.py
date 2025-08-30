"""
config.py — single source of truth (no env reads)
Edit values here; the app won't require shell exports.

TIMEZONE CONVENTION:
- All internal processing uses UTC
- All stored timestamps are UTC (Parquet, BigQuery)
- Local timezone conversion happens only at display layer
- Kafka timestamps are milliseconds since epoch (UTC)
"""
from typing import List

# =====================
# App / runtime
# =====================
APP_NAME = "CryptoDetectModular"
OUTPUT_MODE = "local"              # local | gcs | bigquery
CONSOLE_PREVIEW = True             # show anomalies in console sink
SPARK_SQL_TZ = "UTC"               # Use UTC for all internal processing

# =====================
# Symbols
# =====================
SUPPORTED_SYMBOLS: List[str] = [
    "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "ADA/USDT"
]

# =====================
# Kafka (producer + ingest)
# =====================
KAFKA_CONFIG = {
    "broker": "localhost:9092",
    "topic": "crypto_trades",        # keep underscore to avoid dot issues
    "acks": 1,                        # 0 | 1 | -1 (or 'all' where supported)
    "linger_ms": 50,
    "batch_size": 32768,
    "num_partitions": 3,       # More partitions allows more throughput. Change it if we feel incoming data is slow
    "compression_type": "gzip",      # use 'gzip' unless python-snappy is installed
    # Ingest pacing (StreamIngest)
    "max_offsets_per_trigger": "1000",
}

# =====================
# Windows / triggers
# =====================
WINDOWS = {
    "duration": "60 seconds",
    "slide": "20 seconds",
}
PROCESSING_TRIGGER = "20 seconds"
WATERMARK_DELAY = "30 seconds"

# =====================
# Detection thresholds
# =====================
# Replace single threshold with per-symbol
ANOMALY_CONFIG = {
    "BTC/USDT": {
        "z_score_threshold": 3.5,
        "volume_spike_ratio": 3.0,
        "price_change_pct": 3.0,  # BTC is less volatile
    },
    "ETH/USDT": {
        "z_score_threshold": 3.8,
        "volume_spike_ratio": 3.5,
        "price_change_pct": 4.0,  # ETH slightly more volatile
    },
    "SOL/USDT": {
        "z_score_threshold": 4.0,
        "volume_spike_ratio": 4.0,
        "price_change_pct": 6.0,  # SOL more volatile
    },
    # Add more as needed
    "DEFAULT": {  # Fallback for unlisted symbols
        "z_score_threshold": 3.5,
        "volume_spike_ratio": 3.0,
        "price_change_pct": 5.0,
    }
}
# =====================
# Storage
# =====================
LOCAL_OUTPUT_DIR = "./output"
MODELS_PATH = "./models"

# GCP / BigQuery (used only if OUTPUT_MODE == 'bigquery')
GCP_PROJECT = "your-project-id"
BQ_DATASET = "crypto_anomalies"
GCS_BUCKET = "crypto-anomaly-detection"

# =====================
# Writer behavior
# =====================
# No offset needed when using UTC consistently
TS_OFFSET_HOURS = 0
STREAM_SINGLE_FILE = True      # one parquet file per micro-batch (local/GCS)

# =====================
# Alerts + Dispatcher
# =====================
ALERTS = {
    "enable": False,
    "methods": ["telegram"],
    "telegram_token": "",
    "telegram_chat_id": "",
    "slack_webhook_url": "",
    "cooldown_seconds": 60,
}
# Alert dispatcher settings (used by alert_dispatcher.py)
DISPATCHER = {
    "lookback_minutes": 5,                 # how far back to scan for fresh anomalies
    "poll_interval_sec": 20,               # how often to poll sinks
    # Optional: where to persist last processed timestamp (for local mode)
    # default is f"{LOCAL_OUTPUT_DIR}/anomaly_alert_state.json" if not set
    # "state_path": f"{LOCAL_OUTPUT_DIR}/anomaly_alert_state.json",
}

# =====================
# Logging
# =====================
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "date_format": "%Y-%m-%d %H:%M:%S",
}

# =====================
# Synthetic data injection (producer testing)
# =====================
INJECTION = {
    "enable": False,
    "probability": 0.01,
    "magnitude_pct": 2.0,
}