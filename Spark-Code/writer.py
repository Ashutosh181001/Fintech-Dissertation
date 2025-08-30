"""
writer.py — config-only
- Reads all toggles from config.py (no env lookups).
- Stores all timestamps in UTC (no offset needed).
- Shows UK time in console for human readability.
- Writes one file per micro-batch for local/GCS when STREAM_SINGLE_FILE=True.
- Co-located checkpoints under each sink folder with cleanup management.
"""
from typing import Optional
import os
import time
import logging

from pyspark.sql import DataFrame, functions as F

import config
from spark_features import prepare_trades_output, prepare_anomalies_output

logger = logging.getLogger(__name__)


class StreamWriter:
    def __init__(
        self,
        spark,
        output_mode: Optional[str] = None,
        local_output_dir: Optional[str] = None,
        gcs_bucket: Optional[str] = None,
        gcp_project: Optional[str] = None,
        bq_dataset: Optional[str] = None,
        processing_trigger: Optional[str] = None,
    ) -> None:
        self.spark = spark
        self.output_mode = (output_mode or config.OUTPUT_MODE).lower()
        self.local_output_dir = local_output_dir or config.LOCAL_OUTPUT_DIR
        self.gcs_bucket = gcs_bucket or config.GCS_BUCKET
        self.gcp_project = gcp_project or config.GCP_PROJECT
        self.bq_dataset = bq_dataset or config.BQ_DATASET
        self.processing_trigger = processing_trigger or config.PROCESSING_TRIGGER

        # Config-only controls
        self.offset_hours = int(getattr(config, "TS_OFFSET_HOURS", 0))  # Should be 0 now
        self.single_file = bool(getattr(config, "STREAM_SINGLE_FILE", True))

        # Checkpoint cleanup settings
        self.checkpoint_cleanup = True  # Enable old checkpoint cleanup
        self.checkpoint_retention_hours = 24  # Keep checkpoints for 24 hours

        if self.output_mode == "local":
            os.makedirs(os.path.join(self.local_output_dir, "trades"), exist_ok=True)
            os.makedirs(os.path.join(self.local_output_dir, "anomalies"), exist_ok=True)

        logger.info(
            "StreamWriter | mode=%s | UTC storage | trigger=%s | single_file=%s | checkpoint_cleanup=%s",
            self.output_mode,
            self.processing_trigger,
            self.single_file,
            self.checkpoint_cleanup,
        )

    # ---- helpers ----
    def _shift(self, colname: str):
        """Legacy method - now returns column as-is since we use UTC everywhere"""
        if self.offset_hours != 0:
            logger.warning("TS_OFFSET_HOURS is non-zero (%d). This is deprecated - use UTC!", self.offset_hours)
            return F.col(colname) + F.expr(f"INTERVAL {self.offset_hours} HOURS")
        return F.col(colname)

    def _apply_offset_trades(self, df: DataFrame) -> DataFrame:
        """No offset needed - timestamps already in UTC"""
        # Just ensure hour column is consistent
        if "hour" in df.columns:
            df = df.withColumn("hour", F.date_format(F.col("timestamp"), "yyyy-MM-dd-HH"))
        return df

    def _apply_offset_anoms(self, df: DataFrame) -> DataFrame:
        """No offset needed - timestamps already in UTC"""
        # Just ensure hour column is consistent
        if "hour" in df.columns:
            df = df.withColumn("hour", F.date_format(F.col("timestamp"), "yyyy-MM-dd-HH"))
        return df

    def _get_checkpoint_location(self, sink_name: str) -> str:
        """Generate checkpoint location with cleanup settings"""
        if self.output_mode == "local":
            base = f"{self.local_output_dir}/{sink_name}/_checkpoints"
        elif self.output_mode == "gcs":
            base = f"gs://{self.gcs_bucket}/{sink_name}/_checkpoints"
        else:  # bigquery
            base = f"gs://{self.gcs_bucket}/bq_checkpoints/{sink_name}"

        # Add timestamp to checkpoint path for rotation
        if self.checkpoint_cleanup and self.output_mode == "local":
            # Use date-based subdirectory for easier cleanup
            date_suffix = time.strftime("%Y%m%d")
            return f"{base}/{date_suffix}"

        return base

    # ---- sinks ----
    def write_trades_stream(self, df: DataFrame):
        self._require_cols(df, ["latest_event_time", "last_price", "window_volume", "symbol"])  # minimal
        out_df = prepare_trades_output(df)
        out_df = self._apply_offset_trades(out_df)
        if self.single_file and self.output_mode in ("local", "gcs"):
            out_df = out_df.repartition(1)

        checkpoint_location = self._get_checkpoint_location("trades")

        if self.output_mode == "local":
            output_path = f"{self.local_output_dir}/trades"
            return (
                out_df.writeStream
                .outputMode("append")
                .format("parquet")
                .option("path", output_path)
                .option("checkpointLocation", checkpoint_location)
                .trigger(processingTime=self.processing_trigger)
                .queryName("trades")
            )
        if self.output_mode == "gcs":
            output_path = f"gs://{self.gcs_bucket}/trades"
            return (
                out_df.writeStream
                .outputMode("append")
                .format("parquet")
                .option("path", output_path)
                .option("checkpointLocation", checkpoint_location)
                .trigger(processingTime=self.processing_trigger)
                .queryName("trades")
            )
        if self.output_mode == "bigquery":
            table = f"{self.gcp_project}.{self.bq_dataset}.trades"
            return (
                out_df.writeStream
                .outputMode("append")
                .format("bigquery")
                .option("table", table)
                .option("temporaryGcsBucket", self.gcs_bucket)
                .option("checkpointLocation", checkpoint_location)
                .option("createDisposition", "CREATE_IF_NEEDED")
                .option("partitionField", "timestamp")
                .option("partitionType", "DAY")
                .option("clusteredFields", "symbol")
                .trigger(processingTime=self.processing_trigger)
                .queryName("trades")
            )
        raise ValueError(f"Unsupported output mode: {self.output_mode}")

    def write_anomalies_stream(self, df: DataFrame):
        self._require_cols(df, ["latest_event_time", "last_price", "symbol", "is_anomaly"])  # minimal
        out_df = prepare_anomalies_output(df)
        out_df = self._apply_offset_anoms(out_df)
        if self.single_file and self.output_mode in ("local", "gcs"):
            out_df = out_df.repartition(1)

        checkpoint_location = self._get_checkpoint_location("anomalies")

        if self.output_mode == "local":
            output_path = f"{self.local_output_dir}/anomalies"
            return (
                out_df.writeStream
                .outputMode("append")
                .format("parquet")
                .option("path", output_path)
                .option("checkpointLocation", checkpoint_location)
                .trigger(processingTime=self.processing_trigger)
                .queryName("anomalies")
            )
        if self.output_mode == "gcs":
            output_path = f"gs://{self.gcs_bucket}/anomalies"
            return (
                out_df.writeStream
                .outputMode("append")
                .format("parquet")
                .option("path", output_path)
                .option("checkpointLocation", checkpoint_location)
                .trigger(processingTime=self.processing_trigger)
                .queryName("anomalies")
            )
        if self.output_mode == "bigquery":
            table = f"{self.gcp_project}.{self.bq_dataset}.anomalies"
            return (
                out_df.writeStream
                .outputMode("append")
                .format("bigquery")
                .option("table", table)
                .option("temporaryGcsBucket", self.gcs_bucket)
                .option("checkpointLocation", checkpoint_location)
                .option("createDisposition", "CREATE_IF_NEEDED")
                .option("partitionField", "timestamp")
                .option("partitionType", "DAY")
                .option("clusteredFields", "symbol,anomaly_type")
                .trigger(processingTime=self.processing_trigger)
                .queryName("anomalies")
            )
        raise ValueError(f"Unsupported output mode: {self.output_mode}")

    def write_console_stream(self, df: DataFrame):
        """
        Console output with both UTC and UK local time for human readability.
        Storage remains UTC, but we show UK time for operators monitoring the console.
        """
        # Add UK local time for display (conversion from UTC)
        df_display = df.withColumn(
            "uk_time",
            F.from_utc_timestamp(F.col("latest_event_time"), "Europe/London")
        )

        cols = [
            "symbol",
            F.col("uk_time").alias("time_uk"),
            F.col("latest_event_time").alias("time_utc"),
            F.col("last_price").alias("price"),
            F.round("z_score", 2).alias("z_score"),
            "anomaly_type",
            F.round("anomaly_score", 2).alias("score"),
        ]

        console_df = df_display.filter(F.col("is_anomaly") == True).select(*cols)

        return (
            console_df.writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", False)
            .trigger(processingTime="10 seconds")
            .queryName("console")
        )

    # ---- utils ----
    def _require_cols(self, df: DataFrame, cols: list) -> None:
        missing = [c for c in cols if c not in df.columns]
        if missing:
            logger.warning("Expected columns missing from input df: %s", ", ".join(missing))