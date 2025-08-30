# main_detect_spark.py (config-only)
# Orchestrates: StreamIngest → DetectionEngine → StreamWriter

import time
import logging
from typing import List

from pyspark.sql import SparkSession

import config
from stream_ingest import StreamIngest
from detection import DetectionEngine
from writer import StreamWriter


def _init_logging():
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


def _build_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", config.SPARK_SQL_TZ)  # Now UTC
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )
    if config.OUTPUT_MODE == "bigquery":
        builder = builder.config("temporaryGcsBucket", config.GCS_BUCKET)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run():
    _init_logging()
    logger = logging.getLogger("orchestrator")

    spark = _build_spark(config.APP_NAME)

    try:
        tz_effective = spark.conf.get("spark.sql.session.timeZone")
    except Exception:
        tz_effective = "(unknown)"

    logger.info("=" * 60)
    logger.info("🚀 Starting Spark job: %s", config.APP_NAME)
    logger.info("🕒 Spark SQL timezone: %s (all storage in UTC)", tz_effective)
    logger.info("📡 Kafka topic: %s", config.KAFKA_CONFIG["topic"])
    logger.info("💾 Output mode: %s", config.OUTPUT_MODE)
    if config.OUTPUT_MODE == "local":
        logger.info("📁 Local dir: %s", config.LOCAL_OUTPUT_DIR)
    logger.info("⏱️ Window: %s / %s", config.WINDOWS["duration"], config.WINDOWS["slide"])
    logger.info("=" * 60)

    # Build stages
    ingest = StreamIngest(
        spark,
        watermark=config.WATERMARK_DELAY,
        starting_offsets="latest",
        max_offsets_per_trigger=config.KAFKA_CONFIG.get("max_offsets_per_trigger", "1000"),
    )
    detector = DetectionEngine(
        spark,
        models_path=config.MODELS_PATH,
    )
    writer = StreamWriter(spark)

    # Assemble DAG
    trades_stream = ingest.source()
    enriched = detector.apply(
        trades_stream,
        window_duration=config.WINDOWS["duration"],
        window_slide=config.WINDOWS["slide"],
    )

    # Start sinks
    queries: List = []
    try:
        q_trades = writer.write_trades_stream(enriched).start()
        queries.append(q_trades)
        logger.info("✅ Trades sink started: %s", q_trades.name)
    except Exception as e:
        logger.exception("Failed to start trades sink: %s", e)

    try:
        q_anoms = writer.write_anomalies_stream(enriched).start()
        queries.append(q_anoms)
        logger.info("✅ Anomalies sink started: %s", q_anoms.name)
    except Exception as e:
        logger.exception("Failed to start anomalies sink: %s", e)

    if config.CONSOLE_PREVIEW:
        try:
            q_console = writer.write_console_stream(enriched).start()
            queries.append(q_console)
            logger.info("🖨️ Console preview started: %s (showing UK time for operators)", q_console.name)
        except Exception as e:
            logger.warning("Console sink not started: %s", e)

    if not queries:
        logger.error("No sinks could be started. Exiting.")
        return

    # Monitor
    try:
        while any(q.isActive for q in queries):
            time.sleep(10)
            for q in list(queries):
                if q.lastProgress:
                    lp = q.lastProgress
                    name = q.name or "query"
                    in_rate = lp.get("inputRowsPerSecond", 0)
                    proc_rate = lp.get("processedRowsPerSecond", 0)
                    batch_id = lp.get("batchId", 0)
                    logger.info("📊 %s — batch=%s, in=%.2f r/s, proc=%.2f r/s",
                                name, batch_id, in_rate, proc_rate)
                if not q.isActive:
                    logger.info("Stream finished: %s", q.name)
    except KeyboardInterrupt:
        logger.info("🛑 Interrupt received; stopping queries...")
    finally:
        for q in queries:
            try:
                q.stop()
            except Exception:
                pass
        try:
            spark.stop()
        except Exception:
            pass
        logger.info("✅ Shutdown complete.")


if __name__ == "__main__":
    run()