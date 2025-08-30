"""
Alert System Module
Sends anomaly alerts via Telegram or Slack
"""

import json
import time
import requests
from typing import Dict
import logging
import config

logger = logging.getLogger(__name__)


class AlertManager:
    """Manages alert notifications through configured channels"""

    def __init__(self):
        self.enabled = config.ALERT_CONFIG["enabled"]
        self.channel = config.ALERT_CONFIG["channel"]

        # Telegram config
        self.telegram_token = config.ALERT_CONFIG["telegram_token"]
        self.telegram_chat_id = config.ALERT_CONFIG["telegram_chat_id"]

        # Slack config
        self.slack_webhook_url = config.ALERT_CONFIG["slack_webhook_url"]

        # Alert filtering
        self.alert_on_zscore = config.ALERT_CONFIG["alert_on_zscore"]
        self.alert_on_isoforest = config.ALERT_CONFIG["alert_on_isoforest"]
        self.alert_on_filtered = config.ALERT_CONFIG["alert_on_filtered"]

        self._validate_config()

    def _validate_config(self):
        """Validate alert configuration"""
        if not self.enabled:
            logger.info("Alerts are disabled")
            return

        if self.channel == "telegram":
            if not self.telegram_token or not self.telegram_chat_id:
                logger.warning("Telegram credentials not configured - disabling alerts")
                self.enabled = False
        elif self.channel == "slack":
            if not self.slack_webhook_url:
                logger.warning("Slack webhook not configured - disabling alerts")
                self.enabled = False
        else:
            logger.warning(f"Unknown alert channel: {self.channel}")
            self.enabled = False

    def send_alert(self, features: Dict, method: str):
        """
        Send alert based on detection method and configuration

        Parameters:
        -----------
        features: Dict
            Feature dictionary with anomaly details
        method: str
            Detection method ('z_score', 'isoforest', 'filtered_isoforest')
        """
        if not self.enabled:
            return

        # Check if we should alert for this method
        if method == "z_score" and not self.alert_on_zscore:
            return
        if method == "isoforest" and not self.alert_on_isoforest:
            return
        if method == "filtered_isoforest" and not self.alert_on_filtered:
            return

        # Format message
        message = self._format_message(features, method)

        # Send to configured channel
        if self.channel == "telegram":
            self._send_telegram(message)
        elif self.channel == "slack":
            self._send_slack(message, features, method)

    def _format_message(self, features: Dict, method: str) -> str:
        """Format alert message"""
        emoji_map = {
            'z_score': '📊',
            'isoforest': '🌳',
            'filtered_isoforest': '🎯'
        }

        emoji = emoji_map.get(method, '🚨')
        symbol = features.get('symbol', 'BTC/USDT')

        message = f"{emoji} **ANOMALY DETECTED**\n\n"
        message += f"**Type:** {method.replace('_', ' ').title()}\n"
        message += f"**Symbol:** {symbol}\n"
        message += f"**Time:** {features.get('timestamp', 'N/A')}\n"
        message += f"**Price:** ${features.get('price', 0):,.2f}\n"
        message += f"**Z-Score:** {features.get('z_score', 0):.2f}\n"

        if features.get('price_change_pct'):
            message += f"**Price Change:** {features['price_change_pct']:.2f}%\n"

        if features.get('volume_spike', 1) > 2:
            message += f"**Volume Spike:** {features['volume_spike']:.1f}x normal\n"

        if features.get('vwap_deviation'):
            message += f"**VWAP Deviation:** {features['vwap_deviation']:.2f}%\n"

        return message

    def _send_telegram(self, message: str):
        """Send Telegram notification"""
        try:
            # Convert markdown for Telegram
            telegram_message = message.replace('**', '*')

            response = requests.post(
                f"https://api.telegram.org/bot{self.telegram_token}/sendMessage",
                data={
                    'chat_id': self.telegram_chat_id,
                    'text': telegram_message,
                    'parse_mode': 'Markdown'
                },
                timeout=10
            )

            if response.status_code == 200:
                logger.debug("Telegram alert sent successfully")
            else:
                logger.error(f"Failed to send Telegram alert: {response.text}")

        except Exception as e:
            logger.error(f"Error sending Telegram alert: {e}")

    def _send_slack(self, message: str, features: Dict, method: str):
        """Send Slack notification"""
        try:
            # Format for Slack
            color_map = {
                'z_score': '#ff9800',           # Orange
                'isoforest': '#4caf50',         # Green
                'filtered_isoforest': '#f44336' # Red
            }

            slack_data = {
                "text": "Anomaly Detected",
                "attachments": [{
                    "color": color_map.get(method, '#2196f3'),
                    "fields": [
                        {"title": "Type",   "value": method.replace('_', ' ').title(), "short": True},
                        {"title": "Symbol", "value": features.get('symbol', 'BTC/USDT'), "short": True},
                        {"title": "Price",  "value": f"${features.get('price', 0):,.2f}", "short": True},
                        {"title": "Z-Score","value": f"{features.get('z_score', 0):.2f}", "short": True},
                        {"title": "Time",   "value": features.get('timestamp', 'N/A'), "short": False},
                    ],
                    "footer": "Crypto Anomaly Detection",
                    "ts": int(time.time())
                }]
            }

            response = requests.post(self.slack_webhook_url, json=slack_data, timeout=10)

            if response.status_code in [200, 204]:
                logger.debug("Slack alert sent successfully")
            else:
                logger.error(f"Failed to send Slack alert: {response.text}")

        except Exception as e:
            logger.error(f"Error sending Slack alert: {e}")
