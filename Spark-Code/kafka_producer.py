"""
kafka_producer.py (config-only)
--------------------------------
Binance WebSocket → Kafka producer that reads **only** from config.py
(no environment variables required).

• Symbols: config.SUPPORTED_SYMBOLS
• Kafka broker/topic & producer tuning: config.KAFKA_CONFIG
• JSON schema matches stream_ingest.py
• Snappy → gzip fallback if python-snappy not installed
"""
from __future__ import annotations
from typing import List, Dict, Any
import json
import time
import random
import logging
import signal

from kafka import KafkaProducer  # type: ignore
try:
    from kafka.admin import KafkaAdminClient, NewTopic  # type: ignore
    _ADMIN_AVAILABLE = True
except Exception:
    _ADMIN_AVAILABLE = False

try:
    import websocket  # from websocket-client
except Exception as e:  # pragma: no cover
    raise SystemExit(
        "websocket-client is required. Install with: pip install websocket-client\n"
        f"Original error: {e}"
    )

import config

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=getattr(logging, config.LOGGING_CONFIG["level"]),
    format=config.LOGGING_CONFIG["format"],
    datefmt=config.LOGGING_CONFIG["date_format"],
)
logger = logging.getLogger("kafka-producer")


# -------------------------
# Symbol / URL helpers
# -------------------------

def _to_stream_symbol(sym: str) -> str:
    """Convert "BTC/USDT" → "btcusdt" for Binance stream path."""
    s = sym.replace("-", "/").upper().strip()
    if "/" in s:
        base, quote = s.split("/", 1)
        return (base + quote).lower()
    return s.lower()


def _combined_stream_url(symbols: List[str]) -> str:
    streams = "/".join([f"{_to_stream_symbol(s)}@trade" for s in symbols])
    return f"wss://stream.binance.com:9443/stream?streams={streams}"


# -------------------------
# Kafka helpers
# -------------------------

def _build_producer() -> KafkaProducer:
    cfg = config.KAFKA_CONFIG

    # Ensure valid acks (int 0/1/-1 or 'all')
    acks_cfg = cfg.get("acks", 1)
    acks: int | str
    if isinstance(acks_cfg, int):
        acks = acks_cfg
    else:
        try:
            acks = int(acks_cfg)
        except Exception:
            acks = acks_cfg if str(acks_cfg).lower() == "all" else 1

    # Compression fallback
    codec = str(cfg.get("compression_type", "snappy")).lower()
    if codec == "snappy":
        try:
            import snappy  # type: ignore  # noqa: F401
        except Exception:
            codec = "gzip"
            logger.warning("snappy not available; falling back to gzip")

    producer = KafkaProducer(
        bootstrap_servers=cfg["broker"],
        acks=acks,
        linger_ms=int(cfg.get("linger_ms", 50)),
        batch_size=int(cfg.get("batch_size", 32768)),
        compression_type=codec,
        value_serializer=lambda d: json.dumps(d, separators=(",", ":")).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
    )
    return producer


def _ensure_topic(topic: str, partitions: int = 1, rf: int = 1) -> None:
    if not _ADMIN_AVAILABLE:
        return
    try:
        admin = KafkaAdminClient(bootstrap_servers=config.KAFKA_CONFIG["broker"], client_id="producer-preflight")
        topics = admin.list_topics()
        if topic not in topics:
            admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=rf)])
            logger.info("Created Kafka topic: %s", topic)
        admin.close()
    except Exception as e:
        logger.warning("Topic preflight skipped/failed: %s", e)


# -------------------------
# Injection (optional synthetic anomalies)
# -------------------------

def maybe_inject(payload: Dict[str, Any]) -> Dict[str, Any]:
    inj = config.INJECTION
    if not inj.get("enable", False):
        payload["injected"] = False
        return payload
    if random.random() > float(inj.get("probability", 0.0)):
        payload["injected"] = False
        return payload
    mag = float(inj.get("magnitude_pct", 2.0)) / 100.0
    direction = -1.0 if random.random() < 0.5 else 1.0
    payload["price"] = max(0.0, float(payload["price"]) * (1.0 + direction * mag))
    payload["injected"] = True
    return payload


# -------------------------
# WebSocket handlers
# -------------------------
class _WS:
    def __init__(self, symbols: List[str], producer: KafkaProducer, topic: str) -> None:
        self.symbols = symbols
        self.producer = producer
        self.topic = topic
        self.count = 0
        self.count_by_symbol: Dict[str, int] = {s: 0 for s in symbols}
        self.start_ts = time.time()

    def on_open(self, ws):
        logger.info("WebSocket opened: %s", _combined_stream_url(self.symbols))

    def on_close(self, ws, *args):
        logger.warning("WebSocket closed: %s", args)

    def on_error(self, ws, error):
        logger.exception("WebSocket error: %s", error)

    def on_message(self, ws, message: str):
        try:
            obj = json.loads(message)
            d = obj.get("data", {})
            sym = d.get("s", "")  # e.g., BTCUSDT

            # Normalize to BASE/QUOTE (simple split by common quotes)
            if sym and "/" not in sym:
                u = sym.upper()
                for quote in ("USDT", "USD", "USDC", "BTC", "ETH", "BNB"):
                    if u.endswith(quote) and len(u) > len(quote):
                        sym_norm = f"{u[:-len(quote)]}/{quote}"
                        break
                else:
                    sym_norm = sym
            else:
                sym_norm = sym

            payload = {
                "symbol": sym_norm,
                "timestamp": int(d.get("T", 0)),  # ms since epoch
                "price": float(d.get("p", 0.0)),
                "quantity": float(d.get("q", 0.0)),
                "trade_id": int(d.get("t", 0)),
                "is_buyer_maker": bool(d.get("m", False)),
                "injected": False,
            }
            payload = maybe_inject(payload)
            self.producer.send(self.topic, key=sym_norm, value=payload)

            self.count += 1
            if sym_norm in self.count_by_symbol:
                self.count_by_symbol[sym_norm] += 1
            if self.count % 200 == 0:
                elapsed = max(1e-6, time.time() - self.start_ts)
                rps = self.count / elapsed
                top = ", ".join([f"{k}:{v}" for k, v in self.count_by_symbol.items() if v > 0])
                logger.info("produced=%d (~%.1f msg/s) | %s", self.count, rps, top)
        except Exception as e:
            logger.exception("Failed to handle message: %s", e)


# -------------------------
# Main
# -------------------------

def main() -> None:
    symbols = config.SUPPORTED_SYMBOLS
    broker = config.KAFKA_CONFIG["broker"]
    topic = config.KAFKA_CONFIG["topic"]

    _ensure_topic(topic, partitions=config.KAFKA_CONFIG.get("num_partitions", 3), rf=1)
    producer = _build_producer()
    logger.info("Kafka producer ready | broker=%s | topic=%s | symbols=%s", broker, topic, ",".join(symbols))

    url = _combined_stream_url(symbols)

    ws_handlers = _WS(symbols, producer, topic)
    ws_app = websocket.WebSocketApp(
        url,
        on_open=ws_handlers.on_open,
        on_message=ws_handlers.on_message,
        on_error=ws_handlers.on_error,
        on_close=ws_handlers.on_close,
    )

    stop = False

    def _sigint(*_):
        nonlocal stop
        stop = True
        try:
            ws_app.close()
        except Exception:
            pass
        try:
            producer.flush(5)
            producer.close()
        except Exception:
            pass
        logger.info("Shutdown complete.")

    signal.signal(signal.SIGINT, _sigint)
    signal.signal(signal.SIGTERM, _sigint)

    backoff = 1.0
    while not stop:
        try:
            ws_app.run_forever(ping_interval=20, ping_timeout=10)
            backoff = 1.0  # reset if clean exit
        except Exception as e:
            logger.warning("WebSocket reconnect in %.1fs (error: %s)", backoff, e)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


if __name__ == "__main__":
    main()
