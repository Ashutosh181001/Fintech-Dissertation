"""
spark_features.py (refactor)
----------------------------
Feature engineering utilities for the modular Spark streaming pipeline.

Responsibilities
- Compute windowed features from parsed trades (per symbol)
- Provide helpers to shape DataFrames for downstream sinks (trades/anomalies)
- Keep side-effect free and easily testable

Input expectations
- Columns: symbol (str), price (double), quantity (double), event_time (timestamp)
  - If `event_time` is missing but `timestamp` exists (timestamp), we coalesce to it.

Output (from `compute_window_features`)
- Aggregated by window + symbol with:
  last_price, first_price, avg_price, std_price, trade_count, window_volume,
  max_quantity, avg_quantity, price_change_pct, volume_spike, z_score,
  time_range_sec, mean_interarrival_sec (aliased to time_gap_sec),
  latest_event_time, hour, window_start, window_end

Notes
- We deliberately avoid joining back to the raw row-level trades to keep state small.
- All downstream writers should use `prepare_trades_output` / `prepare_anomalies_output`.
"""
from typing import Optional
import logging

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import TimestampType

logger = logging.getLogger(__name__)


# -------------------------
# Core Feature Engineering
# -------------------------

def compute_window_features(
    trades_df: DataFrame,
    *,
    window_duration: str = "60 seconds",
    window_slide: str = "10 seconds",
) -> DataFrame:
    """
    Compute per-symbol windowed features for anomaly detection.

    Parameters
    ----------
    trades_df : DataFrame
        Parsed trades with columns: symbol, price, quantity, event_time (or timestamp)
    window_duration : str
        Size of the sliding window (e.g., "60 seconds").
    window_slide : str
        Slide interval (e.g., "10 seconds").

    Returns
    -------
    DataFrame
        Aggregated features per (window, symbol) with derived metrics and
        partition columns for downstream sinks.
    """
    logger.info(
        "Computing windowed features | duration=%s, slide=%s | cols=%s",
        window_duration,
        window_slide,
        ", ".join(trades_df.columns),
    )

    # Be resilient to missing event_time
    df = trades_df
    if "event_time" not in df.columns and "timestamp" in df.columns:
        logger.warning("event_time missing; using timestamp column as event_time")
        df = df.withColumn("event_time", F.col("timestamp").cast(TimestampType()))

    # Group by sliding window and symbol
    grouped = (
        df.groupBy(F.window("event_time", window_duration, window_slide).alias("w"), F.col("symbol"))
        .agg(
            F.first("price", ignorenulls=True).alias("first_price"),
            F.last("price", ignorenulls=True).alias("last_price"),
            F.avg("price").alias("avg_price"),
            F.stddev_samp("price").alias("std_price"),
            F.sum("quantity").alias("window_volume"),
            F.avg("quantity").alias("avg_quantity"),
            F.max("quantity").alias("max_quantity"),
            F.count(F.lit(1)).alias("trade_count"),
            F.min("event_time").alias("min_event_time"),
            F.max("event_time").alias("max_event_time"),
        )
    )

    # Derived metrics
    eps = F.lit(1e-9)

    features = (
        grouped
        .withColumn("price_change_pct", F.when(F.col("first_price") == 0, F.lit(0.0))
                    .otherwise(((F.col("last_price") - F.col("first_price")) / (F.col("first_price") + eps)) * 100.0))
        .withColumn("volume_spike", F.col("max_quantity") / (F.col("avg_quantity") + eps))
        .withColumn("z_score", F.when((F.col("std_price").isNull()) | (F.col("std_price") == 0), F.lit(0.0))
                    .otherwise((F.col("last_price") - F.col("avg_price")) / F.col("std_price")))
        .withColumn("time_range_sec", F.unix_timestamp("max_event_time") - F.unix_timestamp("min_event_time"))
        .withColumn("mean_interarrival_sec", F.when(F.col("trade_count") > 0,
                                                    F.col("time_range_sec") / F.col("trade_count")).otherwise(F.lit(None)))
        .withColumn("time_gap_sec", F.col("mean_interarrival_sec"))
        .withColumn("latest_event_time", F.col("max_event_time"))
        .withColumn("window_start", F.col("w.start"))
        .withColumn("window_end", F.col("w.end"))
        .withColumn("hour", F.date_format(F.col("latest_event_time"), "yyyy-MM-dd-HH"))
        .drop("w")
    )

    logger.info("Windowed features ready. Columns: %s", ", ".join(features.columns))
    return features


# -------------------------
# Output Shaping Helpers
# -------------------------

def prepare_trades_output(df: DataFrame) -> DataFrame:
    """
    Select and rename columns for the `trades` sink.

    Expected input columns (from features + detection):
      symbol, latest_event_time, last_price, window_volume, z_score,
      price_change_pct, volume_spike, hour
    """
    logger.debug("Preparing trades output frame ...")

    cols = [
        F.col("symbol").cast("string"),
        F.col("latest_event_time").alias("timestamp"),
        F.col("last_price").cast("double").alias("price"),
        F.col("window_volume").cast("double").alias("window_volume"),
        F.col("z_score").cast("double").alias("z_score"),
        F.col("price_change_pct").cast("double").alias("price_change_pct"),
        F.col("volume_spike").cast("double").alias("volume_spike"),
        F.col("hour").alias("hour"),
        F.col("window_start"),
        F.col("window_end"),
        F.col("trade_count"),
    ]

    return df.select(*cols)


def prepare_anomalies_output(df: DataFrame) -> DataFrame:
    """
    Select and rename columns for the `anomalies` sink.

    Requires an `is_anomaly` boolean column and (optionally) `anomaly_type`, `anomaly_score`.
    """
    logger.debug("Preparing anomalies output frame ...")

    base = df.filter(F.col("is_anomaly") == True)

    cols = [
        F.col("symbol").cast("string"),
        F.col("latest_event_time").alias("timestamp"),
        F.col("last_price").cast("double").alias("price"),
        F.col("z_score").cast("double").alias("z_score"),
        F.col("price_change_pct").cast("double").alias("price_change_pct"),
        F.col("volume_spike").cast("double").alias("volume_spike"),
        F.col("time_gap_sec").cast("double").alias("time_gap_sec"),
        F.coalesce(F.col("anomaly_type"), F.lit("unknown")).alias("anomaly_type"),
        F.coalesce(F.col("anomaly_score"), F.lit(0.0)).cast("double").alias("anomaly_score"),
        F.col("hour").alias("hour"),
        F.col("window_start"),
        F.col("window_end"),
    ]

    # Include IF score if present
    if "if_anomaly_score" in df.columns:
        cols.append(F.col("if_anomaly_score").cast("double").alias("if_anomaly_score"))

    return base.select(*cols)
