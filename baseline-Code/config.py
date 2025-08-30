"""
Centralized configuration for Multi-Symbol Real-Time Anomaly Detection Pipeline
"""

import os
from typing import Dict, Any

# ===========================
# SUPPORTED SYMBOLS
# ===========================
SUPPORTED_SYMBOLS = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT"]

# ===========================
# KAFKA CONFIGURATION
# ===========================
KAFKA_CONFIG = {
    "topic": "crypto_trades",  # Single topic for all symbols
    "broker": "localhost:9092",
    "consumer_group": "anomaly_detector",
    "auto_offset_reset": "latest",
}

# ===========================
# DATA CONFIGURATION
# ===========================
DATA_CONFIG = {
    "symbols": SUPPORTED_SYMBOLS,  # List of supported symbols
    "rolling_window": 1000,  # Number of trades for rolling statistics (per symbol)
    "min_history": 50,  # Minimum trades needed for meaningful statistics
}

# ===========================
# ANOMALY DETECTION THRESHOLDS
# ===========================
ANOMALY_CONFIG = {
    "z_score_threshold": 3.5,  # Standard deviations for z-score anomaly
    "contamination": 0.01,  # Expected anomaly rate for Isolation Forest
    "n_estimators": 100,  # Number of trees in Isolation Forest
    "random_state": 42,
    "cooldown_seconds": 60,  # Cooldown period between anomaly alerts per symbol
}

# ===========================
# MODEL PATHS
# ===========================
MODEL_PATHS = {
    "base_model": "model_isoforest.pkl",
    "tuned_model": "model_isoforest_best.pkl",
    # Per-symbol models will be saved as: model_isoforest_BTC_USDT.pkl, etc.
}

# ===========================
# STORAGE CONFIGURATION
# ===========================
STORAGE_CONFIG = {
    "use_database": True,  # Use SQLite if True, CSV if False
    "database_path": "trading_anomalies.db",
    "trades_csv": "trades.csv",
    "anomalies_csv": "anomalies.csv",
}

# ===========================
# ALERT CONFIGURATION
# ===========================
ALERT_CONFIG = {
    "enabled": True,
    "channel": "telegram",  # Options: "telegram" or "slack"

    # Telegram settings
    "telegram_token": os.getenv("TELEGRAM_TOKEN", ""),
    "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),

    # Slack settings (alternative)
    "slack_webhook_url": os.getenv("SLACK_WEBHOOK_URL", ""),

    # Alert filtering
    "alert_on_zscore": True,
    "alert_on_isoforest": False,  # Only alert on filtered (z-score + isoforest)
    "alert_on_filtered": True,

    # Per-symbol alert settings
    "symbol_specific_thresholds": {
        "BTC/USDT": {"z_score_threshold": 3.5},
        "ETH/USDT": {"z_score_threshold": 3.5},
        "BNB/USDT": {"z_score_threshold": 4.0},  # Less volatile
        "SOL/USDT": {"z_score_threshold": 3.0},  # More volatile
        "XRP/USDT": {"z_score_threshold": 3.5},
    }
}

# ===========================
# FEATURE ENGINEERING
# ===========================
FEATURE_CONFIG = {
    "features_for_model": ["z_score", "price_change_pct", "time_gap_sec"],
    "vwap_window": 100,  # Window for VWAP calculation
    "velocity_window": 20,  # Window for price velocity
    "volume_spike_window": 50,  # Window for volume spike detection
}

# ===========================
# PIPELINE CONFIGURATION
# ===========================
PIPELINE_CONFIG = {
    "retrain_interval": 3000,  # Retrain model every N trades (per symbol)
    "status_interval": 100,  # Print status every N trades (per symbol)
    "sleep_between_trades": False,  # Add delay for debugging
    "sleep_duration": 0.1,  # Seconds to sleep if enabled
}

# ===========================
# DASHBOARD CONFIGURATION
# ===========================
DASHBOARD_CONFIG = {
    "refresh_interval": 5,  # Seconds between dashboard refreshes
    "max_display_trades": 1000,  # Maximum trades to display per symbol
    "chart_height": 600,  # Pixel height for charts
    "default_symbol": "BTC/USDT",  # Default symbol to display
}

# ===========================
# ACADEMIC EVALUATION
# ===========================
EVALUATION_CONFIG = {
    "enable_synthetic_injection": False,  # Enable for dissertation evaluation
    "synthetic_anomaly_rate": 0.05,  # 5% injection rate (applies to all symbols)
    "evaluation_window_hours": 24,
    "minimum_samples": 100,
}

# ===========================
# LOGGING CONFIGURATION
# ===========================
LOGGING_CONFIG = {
    "level": "INFO",  # Options: DEBUG, INFO, WARNING, ERROR
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "date_format": "%Y-%m-%d %H:%M:%S",
}


# ===========================
# HELPER FUNCTIONS
# ===========================
def get_config() -> Dict[str, Any]:
    """Get complete configuration dictionary"""
    return {
        "kafka": KAFKA_CONFIG,
        "data": DATA_CONFIG,
        "anomaly": ANOMALY_CONFIG,
        "models": MODEL_PATHS,
        "storage": STORAGE_CONFIG,
        "alerts": ALERT_CONFIG,
        "features": FEATURE_CONFIG,
        "pipeline": PIPELINE_CONFIG,
        "dashboard": DASHBOARD_CONFIG,
        "evaluation": EVALUATION_CONFIG,
        "logging": LOGGING_CONFIG,
        "benchmark": BENCHMARK_CONFIG,
        "symbols": SUPPORTED_SYMBOLS,
    }
# ===========================
# BENCHMARK / METRICS CONFIG
# ===========================
BENCHMARK_CONFIG = {
    # File outputs
    "csv_path": os.getenv("BENCHMARK_CSV", "performance_log.csv"),
    "json_path": os.getenv("BENCHMARK_JSON", "benchmark_summary.json"),

    # Time-based flush interval for BenchmarkLogger (seconds)
    "report_interval_sec": int(os.getenv("BENCHMARK_REPORT_SEC", "10")),

    # Run metadata for reproducibility (stamped into every row)
    "env_mode": os.getenv("ENV_MODE", "Local"),
    "region": os.getenv("REGION", ""),
    "replica": os.getenv("POD_NAME", ""),  # will be set automatically in K8s
    "run_metadata_path": os.getenv("RUN_METADATA_PATH", "run_metadata.json"),
}


def get_symbol_config(symbol: str) -> Dict[str, Any]:
    """Get configuration specific to a symbol"""
    config = get_config()

    # Add any symbol-specific overrides
    if symbol in ALERT_CONFIG.get("symbol_specific_thresholds", {}):
        symbol_thresholds = ALERT_CONFIG["symbol_specific_thresholds"][symbol]
        config["anomaly"].update(symbol_thresholds)

    return config


def validate_config() -> bool:
    """Validate configuration settings"""
    issues = []

    # Check alert configuration
    if ALERT_CONFIG["enabled"]:
        if ALERT_CONFIG["channel"] == "telegram":
            if not ALERT_CONFIG["telegram_token"] or not ALERT_CONFIG["telegram_chat_id"]:
                issues.append("Telegram credentials not configured")
        elif ALERT_CONFIG["channel"] == "slack":
            if not ALERT_CONFIG["slack_webhook_url"]:
                issues.append("Slack webhook URL not configured")

    # Check thresholds
    if ANOMALY_CONFIG["z_score_threshold"] <= 0:
        issues.append("Z-score threshold must be positive")

    if not (0 < ANOMALY_CONFIG["contamination"] < 1):
        issues.append("Contamination must be between 0 and 1")

    # Check cooldown
    if ANOMALY_CONFIG["cooldown_seconds"] < 0:
        issues.append("Cooldown period must be non-negative")

    # Check symbols
    if not SUPPORTED_SYMBOLS:
        issues.append("No symbols configured")

    # Print validation results
    if issues:
        print("⚠️ Configuration issues detected:")
        for issue in issues:
            print(f"  - {issue}")
        return False

    print("✅ Configuration validated successfully")
    print(f"📊 Configured for {len(SUPPORTED_SYMBOLS)} symbols: {', '.join(SUPPORTED_SYMBOLS)}")
    print(f"⏱️ Anomaly cooldown: {ANOMALY_CONFIG['cooldown_seconds']} seconds")
    return True


# Run validation on import
if __name__ == "__main__":
    validate_config()
    print("\n📋 Current Configuration:")
    import json

    print(json.dumps(get_config(), indent=2, default=str))