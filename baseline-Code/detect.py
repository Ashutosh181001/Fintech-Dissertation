"""
Main Detection Module
Real-time anomaly detection pipeline for multiple crypto pairs
Supports: BTC/USDT, ETH/USDT, BNB/USDT, SOL/USDT, XRP/USDT
"""

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import joblib
import os
import logging
from kafka import KafkaConsumer
from sklearn.ensemble import IsolationForest

# Local modules
import config
from features import compute_features
from db_writer import DataWriter
from alerts import AlertManager
from benchmark_logger import BenchmarkLogger

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.LOGGING_CONFIG["level"]),
    format=config.LOGGING_CONFIG["format"],
    datefmt=config.LOGGING_CONFIG["date_format"]
)
logger = logging.getLogger(__name__)

# Supported symbols (kept as before)
SUPPORTED_SYMBOLS = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT"]


class MultiSymbolAnomalyDetector:
    """Multi-symbol anomaly detection pipeline with cooldown + rich benchmarking"""

    def __init__(self):
        # Load configuration
        self.cfg = config.get_config()

        # Benchmark logger (new API)
        bm = self.cfg.get("benchmark", {})
        run_meta = {
            "env_mode": bm.get("env_mode", "Local"),
            "region": bm.get("region", ""),
            "replica": bm.get("replica", ""),
            "partitions": 0,  # set once we know consumer assignment
        }
        self.benchmark = BenchmarkLogger(
            csv_path=bm.get("csv_path", "performance_log.csv"),
            json_path=bm.get("json_path", "benchmark_summary.json"),
            report_interval_sec=int(bm.get("report_interval_sec", 10)),
            run_metadata=run_meta,
        )
        # Persist run metadata for reproducibility
        try:
            with open(bm.get("run_metadata_path", "run_metadata.json"), "w") as f:
                json.dump({"run_id": self.benchmark.run_metadata["run_id"], **run_meta}, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to write run_metadata.json: {e}")

        # Initialize components
        self.data_writer = DataWriter(
            # db_writer instrumentation calls this on every successful write
            metrics_hook=lambda write_ms, nbytes: self.benchmark.log_io(write_ms, nbytes)
        )
        self.alert_manager = AlertManager()

        # Initialize models and buffers for each symbol
        self.models = {}
        self.is_model_fitted = {}
        self.history_dict = {}
        self.buffer_for_training = {}
        self.last_anomaly_time = {}  # For cooldown tracking

        # Initialize counters per symbol
        self.trade_counter = {symbol: 0 for symbol in SUPPORTED_SYMBOLS}
        self.anomaly_counter = {symbol: 0 for symbol in SUPPORTED_SYMBOLS}

        # Global counters
        self.total_trades = 0
        self.last_status_time = time.time()

        # Cooldown period in seconds (configurable)
        self.anomaly_cooldown = config.ANOMALY_CONFIG.get("cooldown_seconds", 60)

        # Initialize models for each symbol
        self._init_models()

        # Load historical data for each symbol
        self._load_historical_data()

        # Initialize Kafka consumer
        self.consumer = self._init_kafka_consumer()

    # ---------------------------
    # Initialization helpers
    # ---------------------------
    def _init_models(self):
        """Initialize models for each symbol"""
        base_model = self._load_model()

        for symbol in SUPPORTED_SYMBOLS:
            if base_model is not None:
                self.models[symbol] = IsolationForest(
                    n_estimators=config.ANOMALY_CONFIG["n_estimators"],
                    contamination=config.ANOMALY_CONFIG["contamination"],
                    random_state=config.ANOMALY_CONFIG["random_state"]
                )
                # model will be (re)trained per symbol
                self.is_model_fitted[symbol] = False
                logger.info(f"{symbol} model queued for training")
            else:
                self.models[symbol] = IsolationForest(
                    n_estimators=config.ANOMALY_CONFIG["n_estimators"],
                    contamination=config.ANOMALY_CONFIG["contamination"],
                    random_state=config.ANOMALY_CONFIG["random_state"]
                )
                self.is_model_fitted[symbol] = False

            self.history_dict[symbol] = pd.DataFrame(columns=['timestamp', 'price', 'quantity', 'is_buyer_maker'])
            self.buffer_for_training[symbol] = []

    def _load_model(self):
        """Attempt to load a pre-trained base model if available"""
        try:
            path = config.MODEL_PATHS["base_model"]
            if os.path.exists(path):
                model = joblib.load(path)
                logger.info(f"Loaded base model from {path}")
                return model
        except Exception as e:
            logger.warning(f"Base model load failed: {e}")
        return None

    def _load_historical_data(self):
        """Load historical trades for warm start for each symbol"""
        for symbol in SUPPORTED_SYMBOLS:
            try:
                recent_trades = self.data_writer.get_recent_trades(symbol=symbol, minutes=60)
                if not recent_trades.empty:
                    window = config.DATA_CONFIG["rolling_window"]
                    self.history_dict[symbol] = recent_trades[
                        ['timestamp', 'price', 'quantity', 'is_buyer_maker']
                    ].tail(window).copy()
                    logger.info(f"Loaded {len(self.history_dict[symbol])} historical trades for {symbol}")
            except Exception as e:
                logger.error(f"Failed to load historical data for {symbol}: {e}")

    def _init_kafka_consumer(self):
        """Initialize Kafka consumer"""
        try:
            consumer = KafkaConsumer(
                config.KAFKA_CONFIG["topic"],
                bootstrap_servers=config.KAFKA_CONFIG["broker"],
                auto_offset_reset=config.KAFKA_CONFIG["auto_offset_reset"],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id=config.KAFKA_CONFIG["consumer_group"]
            )
            logger.info("Kafka consumer initialized successfully")
            return consumer
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise

    # ---------------------------
    # Utility: cooldown + lag
    # ---------------------------
    def check_cooldown(self, symbol: str, anomaly_type: str) -> bool:
        """Check if cooldown period has passed for this anomaly type"""
        if symbol not in self.last_anomaly_time:
            return True
        if anomaly_type not in self.last_anomaly_time[symbol]:
            return True

        last_time = self.last_anomaly_time[symbol][anomaly_type]
        current_time = time.time()
        return (current_time - last_time) >= self.anomaly_cooldown

    def update_cooldown(self, symbol: str, anomaly_type: str):
        """Update the last anomaly time for cooldown tracking"""
        if symbol not in self.last_anomaly_time:
            self.last_anomaly_time[symbol] = {}
        self.last_anomaly_time[symbol][anomaly_type] = time.time()

    def _sample_consumer_lag(self):
        """Compute simple group lag snapshot (min/avg/max) and log it."""
        try:
            parts = list(self.consumer.assignment())
            if not parts:
                return
            end = self.consumer.end_offsets(parts)
            lags = []
            for tp in parts:
                end_off = end.get(tp, 0)
                committed = self.consumer.committed(tp) or 0
                lags.append(max(end_off - committed, 0))
            if lags:
                self.benchmark.log_lag(min(lags), sum(lags) / len(lags), max(lags))
                # also stamp partition count
                self.benchmark.set_partitions(len(parts))
        except Exception as e:
            logger.debug(f"lag sample failed: {e}")

    # ---------------------------
    # Detection + training
    # ---------------------------
    def detect_anomalies(self, features: dict, symbol: str) -> list:
        """
        Detect anomalies using z-score and Isolation Forest for a specific symbol
        Returns list of detected anomaly types
        """
        detected = []

        # Z-score detection
        if abs(features.get('z_score', 0)) > config.ANOMALY_CONFIG["z_score_threshold"]:
            detected.append("z_score")

        # Isolation Forest detection
        if (self.is_model_fitted[symbol] and
            len(self.buffer_for_training[symbol]) >= config.DATA_CONFIG["min_history"]):
            try:
                model_feats = [
                    features.get('z_score', 0),
                    features.get('price_change_pct', 0),
                    features.get('time_gap_sec', 0)
                ]
                pred = self.models[symbol].predict([model_feats])[0]
                if pred == -1:
                    tag = "filtered_isoforest" if "z_score" in detected else "isoforest"
                    detected.append(tag)
            except Exception as e:
                logger.error(f"Model prediction failed for {symbol}: {e}")

        return detected

    def update_model(self, symbol: str):
        """Retrain model for a specific symbol (timed for refit metrics)"""
        min_samples = 100
        if len(self.buffer_for_training[symbol]) < min_samples:
            return

        try:
            data = np.array(
                self.buffer_for_training[symbol][-config.DATA_CONFIG["rolling_window"]:]
                if len(self.buffer_for_training[symbol]) > config.DATA_CONFIG["rolling_window"]
                else self.buffer_for_training[symbol]
            )

            t0 = time.perf_counter()
            self.models[symbol].fit(data)
            fit_ms = (time.perf_counter() - t0) * 1000.0
            self.benchmark.log_refit(fit_ms, symbol=symbol)
            self.is_model_fitted[symbol] = True

            # Save model with symbol suffix
            model_path = config.MODEL_PATHS["base_model"].replace('.pkl', f'_{symbol.replace("/", "_")}.pkl')
            joblib.dump(self.models[symbol], model_path)

            logger.info(f"{symbol} model trained with {len(data)} samples and saved")
        except Exception as e:
            logger.error(f"Model retraining failed for {symbol}: {e}")

    # ---------------------------
    # Status + main loop
    # ---------------------------
    def print_status(self):
        """Print periodic status update (unchanged)"""
        now = time.time()
        if now - self.last_status_time >= 30:
            logger.info("=" * 60)
            logger.info("MULTI-SYMBOL DETECTOR STATUS")

            for symbol in SUPPORTED_SYMBOLS:
                trades = self.trade_counter[symbol]
                anomalies = self.anomaly_counter[symbol]
                fitted = self.is_model_fitted[symbol]

                if trades > 0:
                    rate = (anomalies / trades * 100)
                    logger.info(
                        f"├─ {symbol}: {trades:,} trades, {anomalies} anomalies ({rate:.2f}%), "
                        f"Model: {'✓' if fitted else '⏳'}"
                    )

            logger.info(f"└─ TOTAL: {self.total_trades:,} trades")
            logger.info("=" * 60)

            self.last_status_time = now

    def run(self):
        """Main detection loop with E2E latency + lag + I/O metrics"""
        logger.info("Starting multi-symbol anomaly detection pipeline...")
        logger.info(f"Monitoring: {', '.join(SUPPORTED_SYMBOLS)}")
        logger.info(f"Anomaly cooldown: {self.anomaly_cooldown} seconds")

        for message in self.consumer:
            try:
                trade = message.value

                # Extract symbol (producer embeds it in each message)
                symbol = trade.get('symbol', 'BTC/USDT')  # default to BTC if missing

                # Skip if unsupported symbol
                if symbol not in SUPPORTED_SYMBOLS:
                    logger.warning(f"Unsupported symbol: {symbol}")
                    continue

                # Bench: start processing timer, capture original event timestamp in ms
                payload_ts_ms = float(trade.get("timestamp") or 0.0)
                proc_start = time.perf_counter()

                # Compute features for this symbol
                features, self.history_dict[symbol] = compute_features(
                    trade,
                    self.history_dict[symbol],
                    config.DATA_CONFIG["rolling_window"]
                )

                # Add symbol to features for storage
                features['symbol'] = symbol

                # Persist trade (db_writer calls our metrics hook internally)
                trade_id = self.data_writer.log_trade(features)
                self.trade_counter[symbol] += 1
                self.total_trades += 1

                # Bench: end processing timing
                proc_end = time.perf_counter()

                # Print trade info periodically
                if self.trade_counter[symbol] % config.PIPELINE_CONFIG["status_interval"] == 0:
                    logger.info(
                        f"[{symbol}] {features['timestamp']} | "
                        f"Price: ${features['price']:,.2f} | "
                        f"Z: {features.get('z_score', 0):.2f} | "
                        f"Δ%: {features.get('price_change_pct', 0):.2f}"
                    )

                # Not enough history? continue
                if len(self.history_dict[symbol]) < config.DATA_CONFIG["min_history"]:
                    # still record processing time
                    self.benchmark.log_trade(
                        symbol,
                        processing_time_sec=(proc_end - proc_start),
                        detection_time_sec=0.0
                    )
                    # sample lag + maybe flush
                    self._sample_consumer_lag()
                    self.benchmark.maybe_flush()
                    continue

                # Update training buffer for this symbol
                self.buffer_for_training[symbol].append([
                    features.get('z_score', 0),
                    features.get('price_change_pct', 0),
                    features.get('time_gap_sec', 0)
                ])

                # Bench: detection timing
                det_start = time.perf_counter()
                anomalies = self.detect_anomalies(features, symbol)
                det_end = time.perf_counter()

                # E2E latency = decision time (now) - original trade time (payload)
                now_ms = time.time() * 1000.0
                e2e_ms = max(now_ms - payload_ts_ms, 0.0)
                self.benchmark.log_e2e_ms(symbol, e2e_ms)
                self.benchmark.mark_first_decision()

                # Record per-iteration processing/detection times
                self.benchmark.log_trade(
                    symbol,
                    processing_time_sec=(proc_end - proc_start),
                    detection_time_sec=(det_end - det_start)
                )

                # Process detected anomalies with cooldown
                if anomalies:
                    for anomaly_type in anomalies:
                        if self.check_cooldown(symbol, anomaly_type):
                            # Log anomaly
                            self.data_writer.log_anomaly(features, anomaly_type, trade_id)
                            self.anomaly_counter[symbol] += 1
                            self.benchmark.log_anomaly(symbol)

                            # Send alert
                            self.alert_manager.send_alert(features, anomaly_type)
                            self.benchmark.inc_alerts(1)

                            # Update cooldown
                            self.update_cooldown(symbol, anomaly_type)

                            logger.warning(
                                f"[{symbol}] {anomaly_type.upper()} anomaly detected! "
                                f"Price: ${features['price']:,.2f}"
                            )
                        else:
                            time_left = self.anomaly_cooldown - (
                                time.time() - self.last_anomaly_time[symbol][anomaly_type]
                            )
                            logger.debug(
                                f"[{symbol}] {anomaly_type} anomaly in cooldown "
                                f"({time_left:.0f}s remaining)"
                            )

                # Retrain model periodically for each symbol
                if not self.is_model_fitted[symbol] and len(self.buffer_for_training[symbol]) >= 100:
                    logger.info(f"[{symbol}] Sufficient data for initial model training")
                    self.update_model(symbol)
                elif self.trade_counter[symbol] % config.PIPELINE_CONFIG["retrain_interval"] == 0:
                    self.update_model(symbol)

                # Optional sleep
                if config.PIPELINE_CONFIG["sleep_between_trades"]:
                    time.sleep(config.PIPELINE_CONFIG["sleep_duration"])

                # Sample consumer lag and flush metrics on interval
                self._sample_consumer_lag()
                self.benchmark.maybe_flush()

            except Exception as e:
                logger.error(f"Error processing trade: {e}")
                self.benchmark.inc_exceptions(1)
                continue

    def shutdown(self):
        """Clean shutdown"""
        try:
            self.consumer.close()
            # Final flush for metrics
            self.benchmark.shutdown()
            logger.info("Detector shutdown complete")

            # Print final statistics (unchanged)
            logger.info("=" * 60)
            logger.info("FINAL STATISTICS")

            for symbol in SUPPORTED_SYMBOLS:
                trades = self.trade_counter[symbol]
                anomalies = self.anomaly_counter[symbol]

                if trades > 0:
                    rate = (anomalies / trades * 100)
                    logger.info(f"├─ {symbol}: {trades:,} trades, {anomalies} anomalies ({rate:.2f}%)")

            logger.info(f"└─ TOTAL: {self.total_trades:,} trades")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Error during shutdown: {e}")


if __name__ == "__main__":
    detector = MultiSymbolAnomalyDetector()
    try:
        detector.run()
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        detector.shutdown()
