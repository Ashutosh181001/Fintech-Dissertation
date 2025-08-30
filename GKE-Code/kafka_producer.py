"""
Multi‑symbol Binance producer (Confluent Cloud–ready)
====================================================

This module streams multiple Binance pairs over a single combined
WebSocket and publishes trades to a Kafka topic **on Confluent Cloud**
using SASL/SSL via the official `confluent_kafka` client (librdkafka).

It assumes a companion `config.py` providing `KAFKA_CONFIG` with keys:
- broker (bootstrap.servers)
- topic
- security_protocol (e.g., "SASL_SSL")
- sasl_mechanism (e.g., "PLAIN")
- sasl_plain_username (API key)
- sasl_plain_password (API secret)

Requirements (add to your image/requirements.txt):
    confluent-kafka
    websockets
    numpy
    certifi

On GKE Autopilot this file reads all sensitive values from env via
`config.py` (loaded from a ConfigMap + Secret as you’ve set up).
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime
from typing import Dict, Optional

import logging

import numpy as np
import websockets
import certifi
from confluent_kafka import Producer as CKProducer

import config

# Base endpoint for combined streams; note the trailing slash.
BINANCE_WS_BASE = "wss://stream.binance.com:9443/"

# Symbol mapping to a consistent, human‑friendly format.
SYMBOL_MAP: Dict[str, str] = {
    "BTCUSDT": "BTC/USDT",
    "ETHUSDT": "ETH/USDT",
    "BNBUSDT": "BNB/USDT",
    "SOLUSDT": "SOL/USDT",
    "XRPUSDT": "XRP/USDT",
}


class CKProducerAdapter:
    """Thin adapter so the rest of the code can call send/flush/close
    much like kafka-python. We JSON‑encode the payload and produce to a
    topic; `flush()` and `close()` map to librdkafka flush calls.
    """

    def __init__(self, conf: Dict[str, object], default_topic: str):
        self._p = CKProducer(conf)
        self._topic = default_topic
        self._log = logging.getLogger(__name__)

    def send(self, topic: str, value) -> None:
        # Drive delivery callbacks and queue events
        self._p.poll(0)
        try:
            self._p.produce(topic, json.dumps(value).encode("utf-8"))
        except BufferError as exc:
            # queue full; try to drain and retry once
            self._log.warning(f"Producer queue full; draining: {exc}")
            self._p.poll(0.5)
            self._p.produce(topic, json.dumps(value).encode("utf-8"))

    def flush(self, timeout: float = 5.0) -> None:
        self._p.flush(timeout)

    def close(self) -> None:
        self._p.flush(5.0)


class MultiSymbolBinanceProducer:
    """Stream multiple Binance pairs to Kafka (Confluent Cloud)."""

    # Lower‑case trading pairs to subscribe to.
    SYMBOLS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt"]

    def __init__(self) -> None:
        self.kafka_config = config.KAFKA_CONFIG
        self.evaluation_config = getattr(config, "EVALUATION_CONFIG", {})

        # Confluent/SSL‑aware producer
        self.producer = self._init_kafka_producer()

        # Counters
        self.total_trades: Dict[str, int] = {fmt: 0 for fmt in SYMBOL_MAP.values()}
        self.synthetic_anomalies: Dict[str, int] = {fmt: 0 for fmt in SYMBOL_MAP.values()}
        self.last_status_time: float = time.time()

    def _init_kafka_producer(self) -> CKProducerAdapter:
        """Initialise a confluent_kafka Producer wrapped by an adapter."""
        conf: Dict[str, object] = {
            "bootstrap.servers": self.kafka_config["broker"],
            "client.id": "crypto-producer",
            # Essential for Confluent Cloud
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": self.kafka_config["sasl_plain_username"],
            "sasl.password": self.kafka_config["sasl_plain_password"],
            "ssl.ca.location": certifi.where(),
            # pragmatic timeouts for cloud brokers
            "socket.timeout.ms": 30000,
            "request.timeout.ms": 30000,
            "metadata.max.age.ms": 900000,
            "retries": 5,
            "client.dns.lookup": "use_all_dns_ips",
            # stronger durability by default
            "acks": "all",
            # modest compression to save egress (optional)
            "compression.type": "lz4",
        }

        try:
            logging.getLogger(__name__).info("Using confluent_kafka Producer (SASL/SSL ready)")
            return CKProducerAdapter(conf, self.kafka_config["topic"])
        except Exception as exc:
            logging.getLogger(__name__).error(f"Failed to init Producer: {exc}")
            raise

    def generate_synthetic_anomaly(self, original_price: float) -> Dict[str, float]:
        """Return a synthetic anomaly record for evaluation."""
        anomaly_types = [
            {"multiplier": 1.05, "type": "price_spike_5pct"},
            {"multiplier": 0.95, "type": "price_drop_5pct"},
            {"multiplier": 1.08, "type": "price_spike_8pct"},
            {"multiplier": 0.92, "type": "price_drop_8pct"},
        ]
        anomaly = np.random.choice(anomaly_types)  # type: ignore[arg-type]
        modified_price = original_price * anomaly["multiplier"]
        return {"modified_price": modified_price, "type": anomaly["type"], "multiplier": anomaly["multiplier"]}

    async def _connect(self, max_retries: int = 5):
        """Establish a combined WebSocket connection to Binance."""
        attempt = 0
        stream_path = "/".join(f"{sym}@trade" for sym in self.SYMBOLS)
        url = f"{BINANCE_WS_BASE}stream?streams={stream_path}"
        logger = logging.getLogger(__name__)
        while max_retries == 0 or attempt < max_retries:
            attempt += 1
            try:
                logger.info(f"Connecting to Binance (attempt {attempt}/{max_retries or '∞'})…")
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
                    logger.error(f"Giving up after {max_retries} attempts")
                    return None
                await asyncio.sleep(min(2 ** attempt, 30))
        return None

    async def _process_stream(self, websocket) -> None:
        """Consume trade messages and publish to Kafka."""
        logger = logging.getLogger(__name__)
        while True:
            try:
                msg = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                payload = json.loads(msg)
                stream_name = payload.get("stream")
                data = payload.get("data")
                if not stream_name or not data:
                    continue

                symbol_lower = stream_name.split("@")[0]
                symbol_upper = symbol_lower.upper()
                symbol_formatted = SYMBOL_MAP.get(symbol_upper, symbol_upper)

                original_price = float(data["p"])  # trade price
                current_price = original_price
                injected = False
                anomaly_info = None

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

                trade_record = {
                    "symbol": symbol_formatted,
                    "timestamp": data["T"],  # ms epoch
                    "price": current_price,
                    "quantity": float(data["q"]),
                    "trade_id": data["t"],
                    "is_buyer_maker": data["m"],
                    "injected": injected,
                }

                try:
                    self.producer.send(self.kafka_config["topic"], value=trade_record)
                    self.total_trades[symbol_formatted] += 1
                    total_symbol_trades = self.total_trades[symbol_formatted]
                    if total_symbol_trades <= 5 or total_symbol_trades % 100 == 0:
                        logger.info(
                            f"✓ {symbol_formatted} trade #{total_symbol_trades}: "
                            f"${current_price:,.2f} - {trade_record['quantity']:.8f}"
                        )
                except Exception as kafka_error:
                    logger.error(f"Failed to send {symbol_formatted} trade to Kafka: {kafka_error}")

                total_all = sum(self.total_trades.values())
                if total_all % 100 == 0:
                    try:
                        self.producer.flush(5.0)
                    except Exception as flush_error:
                        logger.warning(f"Kafka flush failed: {flush_error}")

                current_time = time.time()
                if current_time - self.last_status_time >= 30:
                    self._log_status()
                    self.last_status_time = current_time

            except asyncio.TimeoutError:
                logger.debug("WebSocket timeout – sending ping")
                try:
                    await websocket.ping()
                except Exception:
                    logger.warning("WebSocket ping failed; connection may be closed")
                    break
            except websockets.exceptions.ConnectionClosed:
                logger.error("WebSocket connection closed")
                break
            except json.JSONDecodeError as json_err:
                logger.error(f"JSON decode error: {json_err}")
                continue
            except Exception as exc:
                logger.error(f"Unexpected error processing trade: {exc}")
                continue

    def _log_status(self) -> None:
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
                logger.info(f"├─ {symbol}: {trades:,} trades, {anomalies} anomalies ({rate:.1f}%)")
            else:
                logger.info(f"├─ {symbol}: No trades yet")
        logger.info(f"└─ TOTAL: {total_all:,} trades")
        logger.info("=" * 60)

    async def run(self) -> None:
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
                try:
                    await websocket.close()
                except Exception:
                    pass
                await asyncio.sleep(5)

    def shutdown(self) -> None:
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
                        logger.info(f"├─ {symbol}: {trades:,} trades, {anomalies} anomalies ({rate:.2f}%)")
                    else:
                        logger.info(f"├─ {symbol}: {trades:,} trades")
            logger.info("=" * 60)
        except Exception as exc:
            logger.error(f"Error during shutdown: {exc}")


async def main() -> None:
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
