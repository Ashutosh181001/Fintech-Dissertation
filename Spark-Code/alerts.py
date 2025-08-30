"""
alerts.py (modular)
-------------------
Lightweight alerting utility used by the sidecar dispatcher or other jobs.
Supports Telegram and Slack based on settings in `config.ALERTS`.

Design
- Stateless senders with an in-process cooldown + dedupe (best-effort)
- Plain-text messages (avoid Markdown escaping issues)
- Fail-soft: network errors are logged and do not crash the caller

Usage
-----
    from alerts import AlertManager
    am = AlertManager()
    am.send_alert({
        "symbol": "BTC/USDT",
        "timestamp": "2025-08-10T18:22:30Z",
        "price": 67123.5,
        "z_score": 4.1,
        "price_change_pct": -2.3,
        "volume_spike": 3.7,
        "anomaly_type": "z",
        "anomaly_score": 4.1,
        "source": "streaming",
    })
"""
from __future__ import annotations
from typing import Dict, Any, Optional
import logging
import time
import hashlib
import requests  # type: ignore
import config
import json
import os
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)


def _coerce_str(x: Any) -> str:
    if x is None:
        return ""
    return str(x)


def _payload_key(p: Dict[str, Any]) -> str:
    """Stable key for dedupe/cooldown: symbol|timestamp|type|score-rounded."""
    parts = [
        _coerce_str(p.get("symbol")),
        _coerce_str(p.get("timestamp")),
        _coerce_str(p.get("anomaly_type")),
        _coerce_str(round(float(p.get("anomaly_score", 0.0)), 3)),
    ]
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _format_message(p: Dict[str, Any]) -> str:
    sym = p.get("symbol", "?")
    ts = p.get("timestamp", "?")
    typ = p.get("anomaly_type", "unknown")
    score = p.get("anomaly_score", 0)
    price = p.get("price", None)
    z = p.get("z_score", None)
    pct = p.get("price_change_pct", None)
    vol = p.get("volume_spike", None)
    src = p.get("source", "pipeline")

    lines = [
        f"ANOMALY: {sym}",
        f"type={typ} score={score}",
        f"time={ts}",
    ]
    if price is not None:
        lines.append(f"price={price}")
    if z is not None:
        lines.append(f"z={z}")
    if pct is not None:
        lines.append(f"pct={pct}")
    if vol is not None:
        lines.append(f"vol_spike={vol}")
    lines.append(f"source={src}")

    return " | ".join(lines)


class AlertManager:
    """Multi-channel alert dispatcher (Telegram, Slack).

    Best used by a polling/streaming dispatcher that decides *what* to alert.
    This class focuses on *how* to alert and best-effort stability (cooldown/dedupe).
    """

    def __init__(
            self,
            *,
            enable: Optional[bool] = None,
            methods: Optional[list[str]] = None,
            telegram_token: Optional[str] = None,
            telegram_chat_id: Optional[str] = None,
            slack_webhook_url: Optional[str] = None,
            cooldown_seconds: Optional[int] = None,
            state_path: Optional[str] = None,  # Add this parameter
    ) -> None:
        aconf = config.ALERTS
        self.enable = aconf["enable"] if enable is None else enable
        self.methods = methods or aconf.get("methods", ["telegram"])  # ["telegram", "slack"]
        self.telegram_token = telegram_token or aconf.get("telegram_token", "")
        self.telegram_chat_id = telegram_chat_id or aconf.get("telegram_chat_id", "")
        self.slack_webhook_url = slack_webhook_url or aconf.get("slack_webhook_url", "")
        self.cooldown_seconds = cooldown_seconds or int(aconf.get("cooldown_seconds", 60))

        # Add persistent state path
        self.state_path = state_path or os.path.join(config.LOCAL_OUTPUT_DIR, "alert_cooldowns.json")

        # Load existing cooldowns from disk
        self._last_sent_at = self._load_cooldowns()


        logger.info(
            "AlertManager configured | enable=%s | methods=%s | cooldown=%ss",
            self.enable,
            ",".join(self.methods),
            self.cooldown_seconds,
        )

    # -------------------------
    # Public API
    # -------------------------
    def send_alert(self, payload: Dict[str, Any]) -> bool:
        """Send an alert to all configured channels (best-effort).

        Returns True if at least one channel returns success.
        Applies in-process cooldown keyed by (symbol, ts, type, score-3dp).
        """
        if not self.enable:
            logger.debug("Alerts disabled; dropping: %s", payload)
            return False

        key = _payload_key(payload)
        now = time.time()
        last = self._last_sent_at.get(key, 0.0)
        if now - last < self.cooldown_seconds:
            logger.info("Cooling down duplicate alert (%.1fs left): %s", self.cooldown_seconds - (now - last), key)
            return False

        text = _format_message(payload)
        ok_any = False

        for m in self.methods:
            m = (m or "").strip().lower()
            try:
                if m == "telegram":
                    ok = self._send_telegram(text)
                elif m == "slack":
                    ok = self._send_slack(text)
                else:
                    logger.warning("Unknown alert method: %s", m)
                    ok = False
            except Exception as e:  # pragma: no cover
                logger.exception("Alert method '%s' failed: %s", m, e)
                ok = False
            ok_any = ok_any or ok

        if ok_any:
            self._last_sent_at[key] = now
            self._save_cooldowns()
        return ok_any

    # -------------------------
    # Senders
    # -------------------------
    def _send_telegram(self, text: str) -> bool:
        if not self.telegram_token or not self.telegram_chat_id:
            logger.warning("Telegram alert requested but token/chat_id missing. Skipping.")
            return False
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        try:
            resp = requests.post(url, data={"chat_id": self.telegram_chat_id, "text": text}, timeout=10)
            if resp.status_code // 100 == 2:
                logger.info("Telegram alert delivered.")
                return True
            logger.warning("Telegram alert failed: %s %s", resp.status_code, resp.text)
            return False
        except Exception as e:
            logger.exception("Telegram request error: %s", e)
            return False

    def _send_slack(self, text: str) -> bool:
        if not self.slack_webhook_url:
            logger.warning("Slack alert requested but webhook URL missing. Skipping.")
            return False
        try:
            resp = requests.post(self.slack_webhook_url, json={"text": text}, timeout=10)
            if resp.status_code // 100 == 2:
                logger.info("Slack alert delivered.")
                return True
            logger.warning("Slack alert failed: %s %s", resp.status_code, resp.text)
            return False
        except Exception as e:
            logger.exception("Slack request error: %s", e)
            return False

    def _load_cooldowns(self) -> Dict[str, float]:
        """Load cooldown state from disk"""
        if not self.state_path or not os.path.exists(self.state_path):
            return {}
        try:
            with open(self.state_path, 'r') as f:
                data = json.load(f)
                # Clean old entries (> 24 hours)
                cutoff = time.time() - 86400
                return {k: v for k, v in data.items() if v > cutoff}
        except Exception as e:
            logger.warning("Failed to load cooldowns: %s", e)
            return {}

    def _save_cooldowns(self) -> None:
        """Persist cooldown state to disk"""
        if not self.state_path:
            return
        try:
            os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
            with open(self.state_path, 'w') as f:
                json.dump(self._last_sent_at, f)
        except Exception as e:
            logger.warning("Failed to save cooldowns: %s", e)