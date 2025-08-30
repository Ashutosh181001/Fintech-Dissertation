"""
stream_ingest.py (config-only)
------------------------------
Kafka -> Spark Structured Streaming ingestion for Binance trade records.
Reads **only** from config.py (no env lookups).
Parses JSON, casts types, derives event_time, applies watermarking, and
returns a tidy DataFrame ready for feature engineering.

Expected input message (from kafka_producer):
{
  "symbol": "BTC/USDT",
  "timestamp": 1719938475123,  # ms since epoch (trade time)
  "price": 67234.1,
  "quantity": 0.0012,
  "trade_id": 123456789,
  "is_buyer_maker": false,
  "injected": false
}
"""
from __future__ import annotations
from typing import Optional
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, BooleanType, TimestampType,
)

import config

logger = logging.getLogger(__name__)


class StreamIngest:
    """Encapsulates Kafka reading, JSON parsing, and watermarking."""

    def __init__(
        self,
        spark: SparkSession,
        topic: Optional[str] = None,
        bootstrap_servers: Optional[str] = None,
        starting_offsets: str = "latest",
        fail_on_data_loss: str = "false",
        max_offsets_per_trigger: Optional[str] = None,
        watermark: str = None,
    ) -> None:
        self.spark = spark

        # Read strictly from config by default (no env reads)
        self.topic = topic or config.KAFKA_CONFIG["topic"]
        self.bootstrap_servers = bootstrap_servers or config.KAFKA_CONFIG["broker"]
        self.starting_offsets = starting_offsets
        self.fail_on_data_loss = fail_on_data_loss
        self.max_offsets_per_trigger = (
            max_offsets_per_trigger if max_offsets_per_trigger is not None
            else config.KAFKA_CONFIG.get("max_offsets_per_trigger", "1000")
        )
        self.watermark = watermark or config.WATERMARK_DELAY

        logger.info(
            "StreamIngest | brokers=%s | topic=%s | offsets=%s | watermark=%s | maxOffsetsPerTrigger=%s",
            self.bootstrap_servers,
            self.topic,
            self.starting_offsets,
            self.watermark,
            self.max_offsets_per_trigger,
        )

        # Schema for the incoming Kafka value (JSON)
        self.trade_schema = StructType([
            StructField("symbol", StringType(), False),
            StructField("timestamp", LongType(), False),  # ms since epoch
            StructField("price", DoubleType(), False),
            StructField("quantity", DoubleType(), False),
            StructField("trade_id", LongType(), True),
            StructField("is_buyer_maker", BooleanType(), True),
            StructField("injected", BooleanType(), True),
        ])

    # -------------------------
    # Public API
    # -------------------------
    def read_kafka_stream(self) -> DataFrame:
        """Create a streaming DataFrame from Kafka (raw key/value bytes)."""
        logger.info("Creating Kafka source for topic '%s'", self.topic)
        raw = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", self.starting_offsets)
            .option("failOnDataLoss", self.fail_on_data_loss)
            .option("maxOffsetsPerTrigger", self.max_offsets_per_trigger)
            .load()
        )
        logger.debug("Kafka source columns: %s", ", ".join(raw.columns))
        return raw

    def parse_trades(self, kafka_df: DataFrame) -> DataFrame:
        """Parse JSON in the Kafka `value` column and cast fields."""
        logger.info("Parsing Kafka JSON messages -> typed trade rows")

        # Convert binary value to string, then to structured columns.
        json_df = kafka_df.select(F.col("value").cast("string").alias("json"))
        parsed = json_df.select(F.from_json(F.col("json"), self.trade_schema).alias("r")).select("r.*")

        # Convert ms -> timestamp and derive event_time (same value; kept separately for clarity)
        parsed = parsed.withColumn("event_time", (F.col("timestamp") / 1000).cast(TimestampType()))
        parsed = parsed.withColumn("timestamp", (F.col("timestamp") / 1000).cast(TimestampType()))

        # Default columns for robustness
        if "is_buyer_maker" not in parsed.columns:
            parsed = parsed.withColumn("is_buyer_maker", F.lit(False))
        if "injected" not in parsed.columns:
            parsed = parsed.withColumn("injected", F.lit(False))

        # Watermark on event_time to bound state
        parsed = parsed.withWatermark("event_time", self.watermark)

        logger.info("Parsed stream ready. Columns: %s", ", ".join(parsed.columns))
        return parsed

    def source(self) -> DataFrame:
        """Convenience: read and parse in one call."""
        return self.parse_trades(self.read_kafka_stream())
