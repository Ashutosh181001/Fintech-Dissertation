"""
Centralized configuration for Multi-Symbol Real-Time Anomaly Detection Pipeline
Master's Dissertation: "Real-Time Anomaly Detection in Crypto Streaming Data"
Supports: BTC/USDT, ETH/USDT, BNB/USDT, SOL/USDT, XRP/USDT
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
    "topic": os.getenv("KAFKA_TOPIC_TRADES", "crypto_trades"),
    "broker": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    "consumer_group": os.getenv("KAFKA_CONSUMER_GROUP", "anomaly-detector"),
    "auto_offset_reset": "latest",

    # Add these for Confluent Cloud authentication
    "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
    "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
    "sasl_plain_username": os.getenv("KAFKA_USERNAME", "A3CABVMHM4URBAUI"),
    "sasl_plain_password": os.getenv("KAFKA_PASSWORD", "cfltHyeFe85HN/fZaAcTw62saY7xCAr2WndwyHnddfnXFhm8pPVZhq5aInVlRfGg"),
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
    "use_database": True,
    "database_path": os.getenv("DATABASE_PATH", "trading_anomalies.db"),
    "trades_csv": os.getenv("TRADES_CSV", "trades.csv"),
    "anomalies_csv": os.getenv("ANOMALIES_CSV", "anomalies.csv"),
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

    # Per-symbol alert settings (optional future enhancement)
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
    "retrain_interval": 1000,  # Retrain model every N trades (per symbol)
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
    "csv_path": "/data/output/performance_log.csv",  # Instead of "performance_log.csv"
    "json_path": "/data/output/benchmark_summary.json",  # Instead of "benchmark_summary.json"
    "report_interval_sec": 10,
    "env_mode": "GKE",
    "region": "",
    "replica": "",
    "run_metadata_path": "/data/output/run_metadata.json"  # Also move this
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