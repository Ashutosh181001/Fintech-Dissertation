"""
Detection module for Spark streaming anomaly pipeline.
- Computes windowed features (via spark_features)
- Applies rule-based anomalies with per-symbol thresholds (z-score, volume, price change)
- Optionally applies Isolation Forest via vectorized Pandas UDFs
- Implements filtered detection (both z-score AND IF must trigger for highest confidence)
- Returns an enriched DataFrame; writing is handled elsewhere.
"""
from typing import Optional, Dict, Any
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

import config
from spark_features import compute_window_features

# Optional model utilities (Isolation Forest)
try:
    from model_utils import load_models_from_path, broadcast_models
    from udfs import create_isolation_forest_udf, create_anomaly_score_udf
    MODELS_AVAILABLE = True
except Exception:  # pragma: no cover
    MODELS_AVAILABLE = False

logger = logging.getLogger(__name__)


class DetectionEngine:
    """Encapsulates anomaly detection logic for streaming trades with per-symbol thresholds."""

    def __init__(
        self,
        spark,
        models_path: Optional[str] = None,
    ) -> None:
        self.spark = spark
        self.models_path = models_path or getattr(config, "MODELS_PATH", "./models")

        # Model loading/broadcast (optional)
        self.bc_models = None
        self._init_models_if_available()

        logger.info(
            "DetectionEngine initialized | per-symbol thresholds | models=%s",
            "enabled" if self.bc_models is not None else "disabled",
        )

    # -------------------------
    # Internal helpers
    # -------------------------
    def _init_models_if_available(self) -> None:
        if not MODELS_AVAILABLE:
            logger.warning("Model utilities not available; running rules-only detection.")
            return
        try:
            symbols = getattr(config, "SUPPORTED_SYMBOLS", None)
            models = load_models_from_path(self.models_path, symbols)
            if not models:
                logger.warning("No Isolation Forest models found at %s; rules-only detection.", self.models_path)
                return
            self.bc_models = broadcast_models(self.spark, models)
            logger.info("Broadcasted %d Isolation Forest models.", len(models))
        except Exception as e:  # pragma: no cover
            logger.exception("Failed to load/broadcast models: %s", e)
            self.bc_models = None

    def _get_symbol_config(self, symbol: str) -> Dict[str, float]:
        """Get configuration for a specific symbol, with fallback to DEFAULT"""
        if hasattr(config, "ANOMALY_CONFIG") and isinstance(config.ANOMALY_CONFIG, dict):
            # Per-symbol config
            if symbol in config.ANOMALY_CONFIG:
                return config.ANOMALY_CONFIG[symbol]
            # Default config
            if "DEFAULT" in config.ANOMALY_CONFIG:
                return config.ANOMALY_CONFIG["DEFAULT"]
        # Hard defaults
        return {
            "z_score_threshold": 3.5,
            "volume_spike_ratio": 3.0,
            "price_change_pct": 5.0,
        }

    def _add_rule_anomalies(self, df: DataFrame) -> DataFrame:
        """
        Apply rule-based anomaly flags (z-score, volume spike, price change) with per-symbol thresholds.

        IMPORTANT CHANGE (fixes pickling error):
        - UDFs do NOT capture `self` or `spark`.
        - We build small lookup dicts on the driver, broadcast them, and reference only those in UDFs.
        """
        logger.debug("Adding rule-based anomaly flags with per-symbol thresholds...")

        # --- Build driver-side defaults from config ---
        default_cfg = self._get_symbol_config("DEFAULT")
        default_z = float(default_cfg.get("z_score_threshold", 3.5))
        default_vol = float(default_cfg.get("volume_spike_ratio", 3.0))
        default_pct = float(default_cfg.get("price_change_pct", 5.0))

        # --- Build per-symbol maps (driver-only) ---
        z_map: Dict[str, float] = {}
        vol_map: Dict[str, float] = {}
        pct_map: Dict[str, float] = {}

        if hasattr(config, "ANOMALY_CONFIG") and isinstance(config.ANOMALY_CONFIG, dict):
            for sym, sym_cfg in config.ANOMALY_CONFIG.items():
                if sym == "DEFAULT" or not isinstance(sym_cfg, dict):
                    continue
                if "z_score_threshold" in sym_cfg:
                    z_map[str(sym)] = float(sym_cfg["z_score_threshold"])
                if "volume_spike_ratio" in sym_cfg:
                    vol_map[str(sym)] = float(sym_cfg["volume_spike_ratio"])
                if "price_change_pct" in sym_cfg:
                    pct_map[str(sym)] = float(sym_cfg["price_change_pct"])

        # --- Broadcast the small lookup dicts (safe to use inside UDFs) ---
        sc = self.spark.sparkContext
        bc_z = sc.broadcast(z_map)
        bc_vol = sc.broadcast(vol_map)
        bc_pct = sc.broadcast(pct_map)

        # --- Define pure Python functions that only reference broadcast/plain data ---
        def _get_z_threshold(symbol: Any, _m=bc_z, _d=default_z):
            if symbol is None:
                return _d
            return float(_m.value.get(symbol, _d))

        def _get_volume_threshold(symbol: Any, _m=bc_vol, _d=default_vol):
            if symbol is None:
                return _d
            return float(_m.value.get(symbol, _d))

        def _get_price_change_threshold(symbol: Any, _m=bc_pct, _d=default_pct):
            if symbol is None:
                return _d
            return float(_m.value.get(symbol, _d))

        # Wrap as UDFs (no capture of self/spark)
        get_z_threshold = F.udf(_get_z_threshold, T.DoubleType())
        get_volume_threshold = F.udf(_get_volume_threshold, T.DoubleType())
        get_price_change_threshold = F.udf(_get_price_change_threshold, T.DoubleType())

        # (Optional) keep SQL function names if other parts expect them
        self.spark.udf.register("get_z_threshold", _get_z_threshold, T.DoubleType())
        self.spark.udf.register("get_volume_threshold", _get_volume_threshold, T.DoubleType())
        self.spark.udf.register("get_price_change_threshold", _get_price_change_threshold, T.DoubleType())

        # Add threshold columns based on symbol
        df = df.withColumn("z_threshold", get_z_threshold(F.col("symbol")))
        df = df.withColumn("volume_threshold", get_volume_threshold(F.col("symbol")))
        df = df.withColumn("price_change_threshold", get_price_change_threshold(F.col("symbol")))

        # Apply per-symbol thresholds for anomaly detection
        df = df.withColumn(
            "is_z_anomaly",
            F.when(F.abs(F.col("z_score")) > F.col("z_threshold"), F.lit(True)).otherwise(F.lit(False)),
        )

        df = df.withColumn(
            "is_volume_anomaly",
            F.when(F.col("volume_spike") > F.col("volume_threshold"), F.lit(True)).otherwise(F.lit(False)),
        )

        df = df.withColumn(
            "is_price_change_anomaly",
            F.when(F.abs(F.col("price_change_pct")) > F.col("price_change_threshold"), F.lit(True)).otherwise(F.lit(False)),
        )

        # Log threshold info for debugging
        logger.debug("Per-symbol thresholds applied for z-score, volume spike, and price change")

        return df

    def _add_if_anomalies(self, df: DataFrame) -> DataFrame:
        """Apply Isolation Forest prediction/score if models are available. Always ensure columns exist."""
        if not self.bc_models:
            logger.debug("Isolation Forest disabled (no broadcast models). Adding default columns.")
            return (
                df.withColumn("is_if_anomaly", F.lit(False))
                  .withColumn("if_anomaly_score", F.lit(0.0))
            )

        logger.debug("Adding Isolation Forest predictions via Pandas UDFs ...")
        try:
            if_predict_udf = create_isolation_forest_udf(self.bc_models)
            if_score_udf = create_anomaly_score_udf(self.bc_models)

            df = df.withColumn(
                "is_if_anomaly_int",
                if_predict_udf(
                    F.col("symbol"),
                    F.col("z_score"),
                    F.col("price_change_pct"),
                    F.col("time_gap_sec"),
                ),
            ).withColumn("is_if_anomaly", (F.col("is_if_anomaly_int") == F.lit(1))).drop("is_if_anomaly_int")

            df = df.withColumn(
                "if_anomaly_score",
                if_score_udf(
                    F.col("symbol"),
                    F.col("z_score"),
                    F.col("price_change_pct"),
                    F.col("time_gap_sec"),
                ),
            )
        except Exception as e:  # pragma: no cover
            logger.exception("Isolation Forest UDFs failed: %s", e)
            df = df.withColumn("is_if_anomaly", F.lit(False))
            df = df.withColumn("if_anomaly_score", F.lit(0.0))

        return df

    def _add_filtered_anomaly(self, df: DataFrame) -> DataFrame:
        """Add filtered anomaly detection - both z-score AND Isolation Forest must trigger."""
        logger.debug("Adding filtered anomaly detection (z-score AND Isolation Forest)...")

        # Filtered anomaly: BOTH z-score AND Isolation Forest must agree
        df = df.withColumn(
            "is_filtered_anomaly",
            F.col("is_z_anomaly") & F.col("is_if_anomaly"),
        )

        # Also create a high-confidence score when both models agree
        df = df.withColumn(
            "filtered_confidence",
            F.when(
                F.col("is_filtered_anomaly"),
                (F.abs(F.col("z_score")) / F.col("z_threshold")) * F.col("if_anomaly_score"),
            ).otherwise(F.lit(0.0)),
        )

        logger.info("Filtered anomaly detection added (requires both z-score and IF agreement)")

        return df

    # -------------------------
    # Public API
    # -------------------------
    def apply(
        self,
        trades_df: DataFrame,
        window_duration: str = "60 seconds",
        window_slide: str = "10 seconds",
    ) -> DataFrame:
        """
        Compute features and attach anomaly signals to the streaming trades.
        Includes per-symbol thresholds and filtered detection.
        """
        logger.info(
            "Building window features: duration=%s, slide=%s | columns=%s",
            window_duration,
            window_slide,
            ", ".join([c for c in trades_df.columns]),
        )

        # Window features (rolling stats, z-score, pct change, volume metrics, etc.)
        df = compute_window_features(trades_df, window_duration=window_duration, window_slide=window_slide)

        # Rule-based signals with per-symbol thresholds
        df = self._add_rule_anomalies(df)

        # Isolation Forest (optional, with column guarantees)
        df = self._add_if_anomalies(df)

        # Filtered anomaly detection (both z-score AND IF must trigger)
        df = self._add_filtered_anomaly(df)

        # Combined OR-flag for downstream writers/dashboards
        # Now includes filtered anomaly as a detection type
        df = df.withColumn(
            "is_anomaly",
            F.coalesce(F.col("is_filtered_anomaly"), F.lit(False))  # Filtered has priority
            | F.coalesce(F.col("is_z_anomaly"), F.lit(False))
            | F.coalesce(F.col("is_volume_anomaly"), F.lit(False))
            | F.coalesce(F.col("is_price_change_anomaly"), F.lit(False))
            | F.coalesce(F.col("is_if_anomaly"), F.lit(False)),
        )

        # CRITICAL: Resolve a single anomaly_type for storage/alerts
        # Priority order: filtered > z > if > pct > volume
        df = df.withColumn(
            "anomaly_type",
            F.when(F.col("is_filtered_anomaly"), F.lit("filtered"))  # Highest priority
             .when(F.col("is_z_anomaly"), F.lit("z"))
             .when(F.col("is_if_anomaly"), F.lit("if"))
             .when(F.col("is_price_change_anomaly"), F.lit("pct"))
             .when(F.col("is_volume_anomaly"), F.lit("volume"))
             .otherwise(F.lit("none")),
        )

        # CRITICAL: Score preference aligned with type
        # For filtered anomalies, use the combined confidence score
        df = df.withColumn(
            "anomaly_score",
            F.when(F.col("anomaly_type") == "filtered", F.col("filtered_confidence"))
             .when(F.col("anomaly_type") == "z", F.abs(F.col("z_score")))
             .when(F.col("anomaly_type") == "if", F.col("if_anomaly_score"))
             .when(F.col("anomaly_type") == "pct", F.abs(F.col("price_change_pct")))
             .when(F.col("anomaly_type") == "volume", F.col("volume_spike"))
             .otherwise(F.lit(0.0)),
        )

        # Detection timestamp
        df = df.withColumn("detection_time", F.current_timestamp())

        # Add detection quality indicator
        df = df.withColumn(
            "detection_quality",
            F.when(F.col("is_filtered_anomaly"), F.lit("high"))  # Both models agree
             .when(F.col("is_z_anomaly") | F.col("is_if_anomaly"), F.lit("medium"))  # One model
             .when(F.col("is_anomaly"), F.lit("low"))  # Other detections
             .otherwise(F.lit("none"))
        )

        # Drop intermediate threshold columns to keep output clean
        df = df.drop("z_threshold", "volume_threshold", "price_change_threshold")

        logger.info("Detection DAG built successfully with filtered detection. Output columns: %s", ", ".join(df.columns))
        return df