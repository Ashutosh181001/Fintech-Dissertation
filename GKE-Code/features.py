"""
Feature Engineering Module for Anomaly Detection
Handles rolling statistics, z-scores, and technical indicators
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


def compute_features(trade: Dict, history_df: pd.DataFrame,
                    window_size: int = 1000) -> Tuple[Dict, pd.DataFrame]:
    """
    Compute comprehensive features from trade data and historical context.

    Parameters:
    -----------
    trade: Dict
        Current trade with keys: timestamp, price, quantity, is_buyer_maker
    history_df: pd.DataFrame
        Historical trades DataFrame
    window_size: int
        Rolling window size for statistics

    Returns:
    --------
    Tuple[Dict, pd.DataFrame]: (features dict, updated history DataFrame)
    """
    try:
        # Initialize feature dictionary
        features = {
            'timestamp': trade['timestamp'],
            'price': float(trade['price']),
            'quantity': float(trade['quantity']),
            'is_buyer_maker': int(trade.get('is_buyer_maker', False)),
            'injected': int(trade.get('injected', False))  # For academic evaluation
        }

        # Create new row for history
        new_row = pd.DataFrame([{
            'timestamp': trade['timestamp'],
            'price': float(trade['price']),
            'quantity': float(trade['quantity']),
            'is_buyer_maker': int(trade.get('is_buyer_maker', False))
        }])

        # Update history
        history_df = pd.concat([history_df, new_row], ignore_index=True)

        # Maintain window size
        if len(history_df) > window_size:
            history_df = history_df.iloc[-window_size:]

        history_length = len(history_df)

        # ===========================
        # BASIC FEATURES
        # ===========================
        if history_length > 1:
            # Price change
            prev_price = float(history_df.iloc[-2]['price'])
            features['price_change'] = features['price'] - prev_price
            features['price_change_pct'] = (
                (features['price'] - prev_price) / prev_price * 100
                if prev_price > 0 else 0
            )

            # Time gap
            try:
                prev_time = pd.to_datetime(history_df.iloc[-2]['timestamp'])
                curr_time = pd.to_datetime(trade['timestamp'])
                features['time_gap_sec'] = max((curr_time - prev_time).total_seconds(), 0)
            except:
                features['time_gap_sec'] = 0
        else:
            features['price_change'] = 0
            features['price_change_pct'] = 0
            features['time_gap_sec'] = 0

        # ===========================
        # ROLLING STATISTICS
        # ===========================
        if history_length >= 50:  # Minimum for meaningful statistics
            prices = history_df['price'].astype(float)
            quantities = history_df['quantity'].astype(float)

            # Rolling mean and standard deviation
            features['rolling_mean'] = float(prices.mean())
            features['rolling_std'] = float(prices.std())

            # Z-score (critical for anomaly detection)
            if features['rolling_std'] > 0:
                features['z_score'] = (
                    (features['price'] - features['rolling_mean']) /
                    features['rolling_std']
                )
            else:
                features['z_score'] = 0

            # Volume features
            mean_quantity = quantities.mean()
            features['volume_ratio'] = (
                features['quantity'] / mean_quantity
                if mean_quantity > 0 else 1
            )

            # VWAP (Volume Weighted Average Price)
            if history_length >= 100:
                recent = history_df.tail(100)
                total_value = (
                    recent['price'].astype(float) *
                    recent['quantity'].astype(float)
                ).sum()
                total_quantity = recent['quantity'].astype(float).sum()

                if total_quantity > 0:
                    features['vwap'] = float(total_value / total_quantity)
                    features['vwap_deviation'] = (
                        (features['price'] - features['vwap']) /
                        features['vwap'] * 100
                    )
                else:
                    features['vwap'] = features['rolling_mean']
                    features['vwap_deviation'] = 0
            else:
                features['vwap'] = features['rolling_mean']
                features['vwap_deviation'] = 0

            # Price velocity
            if history_length >= 20:
                lookback = history_df.tail(20)
                try:
                    timestamps = pd.to_datetime(lookback['timestamp'])
                    time_span = (timestamps.iloc[-1] - timestamps.iloc[0]).total_seconds()

                    if time_span > 0:
                        price_change = (
                            float(lookback['price'].iloc[-1]) -
                            float(lookback['price'].iloc[0])
                        )
                        features['price_velocity'] = price_change / time_span
                    else:
                        features['price_velocity'] = 0
                except:
                    features['price_velocity'] = 0
            else:
                features['price_velocity'] = 0

            # Volume spike detection
            if history_length >= 50:
                recent_volume_mean = quantities.tail(50).mean()
                features['volume_spike'] = (
                    features['quantity'] / recent_volume_mean
                    if recent_volume_mean > 0 else 1
                )
            else:
                features['volume_spike'] = 1

            # Buy/Sell pressure
            if history_length >= 10:
                recent = history_df.tail(10)
                buy_volume = recent[
                    recent['is_buyer_maker'] == 0
                ]['quantity'].astype(float).sum()
                sell_volume = recent[
                    recent['is_buyer_maker'] == 1
                ]['quantity'].astype(float).sum()
                total_volume = buy_volume + sell_volume

                features['buy_pressure'] = (
                    buy_volume / total_volume
                    if total_volume > 0 else 0.5
                )
            else:
                features['buy_pressure'] = 0.5

        else:
            # Not enough data - use defaults
            features['rolling_mean'] = features['price']
            features['rolling_std'] = 0
            features['z_score'] = 0
            features['volume_ratio'] = 1
            features['vwap'] = features['price']
            features['vwap_deviation'] = 0
            features['price_velocity'] = 0
            features['volume_spike'] = 1
            features['buy_pressure'] = 0.5

        # ===========================
        # CONTEXTUAL FEATURES
        # ===========================
        try:
            dt = pd.to_datetime(trade['timestamp'])
            features['hour'] = dt.hour
            features['day_of_week'] = dt.dayofweek
            features['is_weekend'] = 1 if dt.dayofweek >= 5 else 0

            # Trading session
            if 23 <= dt.hour or dt.hour < 8:
                features['trading_session'] = 'asian'
            elif 7 <= dt.hour < 16:
                features['trading_session'] = 'european'
            else:
                features['trading_session'] = 'us'
        except:
            features['hour'] = 0
            features['day_of_week'] = 0
            features['is_weekend'] = 0
            features['trading_session'] = 'unknown'

        return features, history_df

    except Exception as e:
        logger.error(f"Error computing features: {e}")

        # Return minimal valid features on error
        return {
            'timestamp': trade.get('timestamp', datetime.now().isoformat()),
            'price': float(trade.get('price', 0)),
            'quantity': float(trade.get('quantity', 0)),
            'is_buyer_maker': 0,
            'z_score': 0,
            'rolling_mean': float(trade.get('price', 0)),
            'rolling_std': 0,
            'price_change_pct': 0,
            'time_gap_sec': 0,
            'volume_ratio': 1,
            'buy_pressure': 0.5,
            'vwap': float(trade.get('price', 0)),
            'vwap_deviation': 0,
            'price_velocity': 0,
            'volume_spike': 1,
            'hour': 0,
            'day_of_week': 0,
            'is_weekend': 0,
            'trading_session': 'unknown',
            'injected': 0
        }, history_df