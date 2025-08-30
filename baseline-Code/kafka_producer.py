"""
Multi‑symbol Binance producer
=============================

This module maintains a live stream of cryptocurrency trades from
Binance’s public WebSocket API and forwards them into a Kafka topic.
Unlike a naïve implementation which opens a separate WebSocket
connection per trading pair, this version uses Binance’s combined
stream endpoint to multiplex all desired trading pairs over a single
connection. This dramatically reduces connection overhead and makes
reconnection logic much simpler.

The producer keeps per‑symbol and global statistics, optionally
injects synthetic anomalies for evaluation purposes and exposes
periodic status logging. Messages are flushed regularly to Kafka to
ensure they are persisted even when the producer process is long
running.

The module assumes a companion `config.py` exists in the import path
providing two dictionaries: `KAFKA_CONFIG` describing at least a
`broker` and `topic`, and `EVALUATION_CONFIG` with
`enable_synthetic_injection` (bool) and `synthetic_anomaly_rate` (0–1).

Example usage::

    import asyncio
    from multi_symbol_producer import MultiSymbolBinanceProducer

    async def main():
        producer = MultiSymbolBinanceProducer()
        await producer.run()

    if __name__ == '__main__':
        asyncio.run(main())

"""

import asyncio
import json
import time
from datetime import datetime
from typing import Dict, Optional

import logging

import numpy as np
import websockets
from kafka import KafkaProducer

import config

# Base endpoint for combined streams; note the trailing slash. When
# connecting to individual streams the path would be ``/ws/``, but
# combined streams live under ``/stream``.
BINANCE_WS_BASE = "wss://stream.binance.com:9443/"

# Symbol mapping provides a consistent, human friendly representation
# across the system. Keys must be upper‑case as returned by Binance.
SYMBOL_MAP: Dict[str, str] = {
    "BTCUSDT": "BTC/USDT",
    "ETHUSDT": "ETH/USDT",
    "BNBUSDT": "BNB/USDT",
    "SOLUSDT": "SOL/USDT",
    "XRPUSDT": "XRP/USDT",
}


class MultiSymbolBinanceProducer:
    """Stream multiple Binance pairs to Kafka.

    This producer opens a single combined WebSocket connection to
    Binance for all configured trading pairs, parses trade messages,
    optionally injects synthetic anomalies, then serialises and
    publishes the resulting records to Kafka.

    It maintains per‑symbol statistics (counts and anomaly counts) and
    logs a summary approximately every 30 seconds. Reconnection logic
    employs exponential backoff with a reasonable maximum delay.

    """

    # List of lower‑case trading pairs to subscribe to. When adding
    # pairs here ensure they are also represented in ``SYMBOL_MAP``.
    SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt"]

    def __init__(self) -> None:
        # Load configuration from the imported module. These keys must
        # exist on the provided dictionaries.
        self.kafka_config = config.KAFKA_CONFIG
        self.evaluation_config = config.EVALUATION_CONFIG

        # Initialise a Kafka producer. Serialise all outgoing values
        # as JSON encoded UTF‑8.
        self.producer: KafkaProducer = self._init_kafka_producer()

        # Maintain trade and anomaly counters per formatted symbol.
        self.total_trades: Dict[str, int] = {
            formatted: 0 for formatted in SYMBOL_MAP.values()
        }
        self.synthetic_anomalies: Dict[str, int] = {
            formatted: 0 for formatted in SYMBOL_MAP.values()
        }

        # Statistics and timing for status reporting.
        self.last_status_time: float = time.time()

    def _init_kafka_producer(self) -> KafkaProducer:
        """Initialise and return a configured Kafka producer.

        The producer is configured with sensible retry and timeout
        values and uses JSON serialisation for message values.

        """
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_config["broker"],
                value_serializer=lambda v: json.dumps(v).encode("utf‑8"),
                retries=5,
                retry_backoff_ms=100,
                request_timeout_ms=30000,
                api_version=(0, 10, 1),
            )
            logging.getLogger(__name__).info("✅ Kafka producer initialised successfully")
            return producer
        except Exception as exc:
            logging.getLogger(__name__).error(f"Failed to initialise Kafka producer: {exc}")
            raise

    def generate_synthetic_anomaly(self, original_price: float) -> Dict[str, float]:
        """Produce a synthetic anomaly for academic evaluation.

        Anomaly types are selected randomly based on a defined list
        specifying multipliers and corresponding labels. The returned
        dictionary contains the modified price and metadata about the
        anomaly.

        Args:
            original_price: The unmodified trade price.

        Returns:
            A dict with keys ``modified_price``, ``type`` and
            ``multiplier``.
        """
        anomaly_types = [
            {"multiplier": 1.05, "type": "price_spike_5pct"},
            {"multiplier": 0.95, "type": "price_drop_5pct"},
            {"multiplier": 1.08, "type": "price_spike_8pct"},
            {"multiplier": 0.92, "type": "price_drop_8pct"},
        ]
        anomaly = np.random.choice(anomaly_types)  # type: ignore[arg-type]
        modified_price = original_price * anomaly["multiplier"]
        return {
            "modified_price": modified_price,
            "type": anomaly["type"],
            "multiplier": anomaly["multiplier"],
        }

    async def _connect(self, max_retries: int = 5):
        """Establish a combined WebSocket connection to Binance.

        The combined endpoint allows multiplexing multiple streams over
        one connection. This method attempts to connect repeatedly
        using exponential backoff and logs each attempt. If a connection
        cannot be made after the specified retries it returns ``None``.

        Args:
            max_retries: Maximum number of connection attempts before
                giving up. A value of 0 disables retry logic.

        Returns:
            A connected ``WebSocketClientProtocol`` or ``None`` if
            unsuccessful.
        """
        attempt = 0
        # Build the combined stream path: e.g. btcusdt@trade/ethusdt@trade
        stream_path = "/".join(f"{sym}@trade" for sym in self.SYMBOLS)
        url = f"{BINANCE_WS_BASE}stream?streams={stream_path}"
        logger = logging.getLogger(__name__)
        while max_retries == 0 or attempt < max_retries:
            attempt += 1
            try:
                logger.info(
                    f"Connecting to combined Binance stream (attempt {attempt}/{max_retries or '∞'})..."
                )
                websocket = await websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10,
                )
                logger.info("✅ Connected to Binance combined WebSocket")
                return websocket
            except Exception as exc:
                logger.error(f"Connection attempt {attempt} failed: {exc}")
                if max_retries and attempt >= max_retries:
                    logger.error(
                        f"Giving up after {max_retries} unsuccessful connection attempts"
                    )
                    return None
                # Exponential backoff capped at 30 seconds
                wait_time = min(2 ** attempt, 30)
                await asyncio.sleep(wait_time)

        return None

    async def _process_stream(self, websocket) -> None:
        """Consume and process messages from the combined stream.

        This method runs a loop receiving trade messages, extracting
        symbol and trade details, injecting anomalies if configured
        and forwarding the resulting record to Kafka. It maintains
        per‑symbol statistics and prints periodic status updates. When
        the connection is closed it returns so the caller can attempt
        reconnection.

        Args:
            websocket: An established websocket to Binance.
        """
        logger = logging.getLogger(__name__)
        while True:
            try:
                # Attempt to receive a message with a timeout. If the
                # connection closes, ``recv`` will raise a
                # ``websockets.exceptions.ConnectionClosed`` error,
                # which we catch below. We avoid checking ``websocket.closed``
                # directly to maintain compatibility with newer
                # versions of the websockets library where this
                # attribute has been removed.
                msg = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                payload = json.loads(msg)
                # Combined streams wrap the actual event in a wrapper:
                # {"stream": "btcusdt@trade", "data": {...}}
                stream_name = payload.get("stream")
                data = payload.get("data")
                if not stream_name or not data:
                    continue  # Skip malformed messages
                
                symbol_lower = stream_name.split("@")[0]
                symbol_upper = symbol_lower.upper()
                symbol_formatted = SYMBOL_MAP.get(symbol_upper, symbol_upper)

                original_price = float(data["p"])
                current_price = original_price
                injected = False
                anomaly_info = None

                # Inject synthetic anomalies with configured probability
                if self.evaluation_config.get("enable_synthetic_injection", False):
                    rate = self.evaluation_config.get("synthetic_anomaly_rate", 0.0)
                    if np.random.random() < rate:
                        anomaly_info = self.generate_synthetic_anomaly(original_price)
                        current_price = anomaly_info["modified_price"]
                        injected = True
                        self.synthetic_anomalies[symbol_formatted] += 1
                        logger.warning(
                            f"🚨 Synthetic anomaly in {symbol_formatted}: {anomaly_info['type']} - "
                            f"${original_price:,.2f} → ${current_price:,.2f}"
                        )

                # Construct the trade record. Use the trade time (T) as
                # the timestamp field; expose it as a simple integer in
                # milliseconds since epoch for easier downstream
                # processing. Additional fields from the raw event can
                # be added here if needed by consumers.
                trade_record = {
                    "symbol": symbol_formatted,
                    "timestamp": data["T"],  # trade time in ms
                    "price": current_price,
                    "quantity": float(data["q"]),
                    "trade_id": data["t"],
                    "is_buyer_maker": data["m"],
                    "injected": injected,
                }
                # Send the record to Kafka. We do not specify a key so
                # partitioning is left to Kafka’s default strategy. In
                # practice you might use the symbol as the key.
                try:
                    self.producer.send(self.kafka_config["topic"], value=trade_record)
                    # Increment trade counters
                    self.total_trades[symbol_formatted] += 1
                    total_symbol_trades = self.total_trades[symbol_formatted]
                    # Periodically log the first few trades and then
                    # every 100th trade for that symbol.
                    if total_symbol_trades <= 5 or total_symbol_trades % 100 == 0:
                        logger.info(
                            f"✓ {symbol_formatted} trade #{total_symbol_trades}: "
                            f"${current_price:,.2f} - {trade_record['quantity']:.8f}"
                        )
                except Exception as kafka_error:
                    logger.error(f"Failed to send {symbol_formatted} trade to Kafka: {kafka_error}")

                # Flush periodically to ensure data reaches Kafka. A
                # small flush interval reduces latency at the cost of
                # throughput; here we flush every 100 total trades.
                total_all = sum(self.total_trades.values())
                if total_all % 100 == 0:
                    try:
                        self.producer.flush(5.0)
                    except Exception as flush_error:
                        logger.warning(f"Kafka flush failed: {flush_error}")

                # Print a status update every 30 seconds regardless of
                # message volume.
                current_time = time.time()
                if current_time - self.last_status_time >= 30:
                    self._log_status()
                    self.last_status_time = current_time

            except asyncio.TimeoutError:
                # The connection is still alive but no data has been
                # received within the timeout. Send a ping to keep the
                # connection alive; if it fails, the next iteration will
                # drop out due to the connection being closed.
                logger.debug("WebSocket timeout – sending ping")
                try:
                    await websocket.ping()
                except Exception:
                    logger.warning("WebSocket ping failed; connection may be closed")
                    break
            except websockets.exceptions.ConnectionClosed:
                # The connection was closed gracefully or due to an error.
                logger.error("WebSocket connection closed")
                break
            except json.JSONDecodeError as json_err:
                logger.error(f"JSON decode error: {json_err}")
                continue
            except Exception as exc:
                logger.error(f"Unexpected error processing trade: {exc}")
                continue

    def _log_status(self) -> None:
        """Output a formatted status report to the log.

        Shows the total trades and anomalies per symbol and the
        aggregate totals. Called periodically by the processing loop.
        """
        logger = logging.getLogger(__name__)
        logger.info("=" * 60)
        logger.info("📊 MULTI‑SYMBOL PRODUCER STATUS")
        total_all = sum(self.total_trades.values())
        for symbol in SYMBOL_MAP.values():
            trades = self.total_trades[symbol]
            anomalies = self.synthetic_anomalies[symbol]
            if trades > 0:
                rate = (
                    (anomalies / trades) * 100.0
                    if self.evaluation_config.get("enable_synthetic_injection", False)
                    else 0
                )
                logger.info(
                    f"├─ {symbol}: {trades:,} trades, {anomalies} anomalies ({rate:.1f}%)"
                )
            else:
                logger.info(f"├─ {symbol}: No trades yet")
        logger.info(f"└─ TOTAL: {total_all:,} trades")
        logger.info("=" * 60)

    async def run(self) -> None:
        """Main entry point for the producer.

        Establishes a combined WebSocket connection, spawns a task to
        process incoming messages and automatically reconnects on
        failure. This loop runs indefinitely until cancelled.
        """
        logger = logging.getLogger(__name__)
        logger.info("=" * 60)
        logger.info("STARTING MULTI‑SYMBOL KAFKA PRODUCER")
        logger.info(f"Topic: {self.kafka_config['topic']}")
        logger.info(f"Broker: {self.kafka_config['broker']}")
        logger.info(f"Symbols: {', '.join(SYMBOL_MAP.values())}")
        if self.evaluation_config.get("enable_synthetic_injection", False):
            rate = self.evaluation_config.get("synthetic_anomaly_rate", 0.0) * 100.0
            logger.info(f"Synthetic injection: ENABLED ({rate:.1f}%)")
        else:
            logger.info("Synthetic injection: DISABLED")
        logger.info("=" * 60)
        while True:
            websocket = await self._connect(max_retries=5)
            if not websocket:
                # Sleep briefly before retrying to connect
                await asyncio.sleep(10)
                continue
            try:
                await self._process_stream(websocket)
            except KeyboardInterrupt:
                logger.info("Shutdown requested")
                break
            except Exception as exc:
                logger.error(f"Error in processing loop: {exc}")
            finally:
                # Ensure the WebSocket is closed before looping again
                try:
                    await websocket.close()
                except Exception:
                    pass
                # Short pause before reconnecting
                await asyncio.sleep(5)

    def shutdown(self) -> None:
        """Flush and close the Kafka producer, logging final stats."""
        logger = logging.getLogger(__name__)
        try:
            self.producer.flush()
            self.producer.close()
            logger.info("=" * 60)
            logger.info("PRODUCER SHUTDOWN COMPLETE")
            total_all = sum(self.total_trades.values())
            logger.info(f"Total trades across all symbols: {total_all:,}")
            for symbol in SYMBOL_MAP.values():
                trades = self.total_trades[symbol]
                anomalies = self.synthetic_anomalies[symbol]
                if trades > 0:
                    if self.evaluation_config.get("enable_synthetic_injection", False):
                        rate = (anomalies / trades) * 100.0
                        logger.info(
                            f"├─ {symbol}: {trades:,} trades, {anomalies} anomalies ({rate:.2f}%)"
                        )
                    else:
                        logger.info(f"├─ {symbol}: {trades:,} trades")
            logger.info("=" * 60)
        except Exception as exc:
            logger.error(f"Error during shutdown: {exc}")


async def main() -> None:
    """Convenience entry point for running the producer standalone."""
    producer = MultiSymbolBinanceProducer()
    try:
        await producer.run()
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("\n🛑 Received interrupt signal")
    except Exception as exc:
        logging.getLogger(__name__).error(f"Producer crashed: {exc}")
    finally:
        producer.shutdown()


if __name__ == "__main__":
    import logging as _logging
    try:
        level = getattr(_logging, config.LOGGING_CONFIG["level"])
        fmt = config.LOGGING_CONFIG["format"]
        datefmt = config.LOGGING_CONFIG["date_format"]
    except Exception:
        level = _logging.INFO
        fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        datefmt = "%Y-%m-%d %H:%M:%S"
    _logging.basicConfig(level=level, format=fmt, datefmt=datefmt)
    asyncio.run(main())