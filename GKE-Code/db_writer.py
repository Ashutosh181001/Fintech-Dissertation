"""
Database Writer Module
Handles data persistence to SQLite or CSV with multi-symbol support
+ I/O timing + bytes metrics via optional hook.
"""

import sqlite3
import pandas as pd
import json
import os
import time
from datetime import datetime
from typing import Dict, Optional, Callable
from contextlib import contextmanager
import logging
import config

logger = logging.getLogger(__name__)


class DataWriter:
    """Handles data persistence with SQLite preference and CSV fallback - Multi-symbol support.

    Optional:
      metrics_hook(write_ms: float, bytes_written: int)
        Called on each successful write (trade or anomaly).
    """

    def __init__(self, metrics_hook: Optional[Callable[[float, int], None]] = None):
        self.use_db = config.STORAGE_CONFIG["use_database"]
        self.db_path = config.STORAGE_CONFIG["database_path"]
        self.trades_csv = config.STORAGE_CONFIG["trades_csv"]
        self.anomalies_csv = config.STORAGE_CONFIG["anomalies_csv"]
        self.metrics_hook = metrics_hook

        if self.use_db:
            self._init_database()

    def set_metrics_hook(self, hook: Optional[Callable[[float, int], None]]):
        """Allow setting/replacing the metrics hook after construction."""
        self.metrics_hook = hook

    def _estimate_bytes(self, record: Dict) -> int:
        try:
            return len(json.dumps(record).encode("utf-8"))
        except Exception:
            return 0

    def _init_database(self):
        """Initialize SQLite database with tables including symbol column"""
        try:
            with self.get_connection() as conn:

                # For concurrent access
                conn.execute('PRAGMA journal_mode=WAL')
                conn.execute('PRAGMA busy_timeout=5000')

                # Trades table with symbol column
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        timestamp DATETIME NOT NULL,
                        price REAL NOT NULL,
                        quantity REAL NOT NULL,
                        is_buyer_maker INTEGER,
                        z_score REAL,
                        rolling_mean REAL,
                        rolling_std REAL,
                        price_change_pct REAL,
                        time_gap_sec REAL,
                        volume_ratio REAL,
                        buy_pressure REAL,
                        vwap REAL,
                        vwap_deviation REAL,
                        price_velocity REAL,
                        volume_spike REAL,
                        hour INTEGER,
                        day_of_week INTEGER,
                        trading_session TEXT,
                        is_weekend INTEGER,
                        injected INTEGER DEFAULT 0,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                # Anomalies table with symbol column
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS anomalies (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        trade_id INTEGER,
                        symbol TEXT NOT NULL,
                        timestamp DATETIME NOT NULL,
                        anomaly_type TEXT NOT NULL,
                        price REAL,
                        z_score REAL,
                        price_change_pct REAL,
                        volume_spike REAL,
                        vwap_deviation REAL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (trade_id) REFERENCES trades(id)
                    )
                ''')

                # Indexes
                conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_trades_symbol_timestamp ON trades(symbol, timestamp)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_anomalies_symbol ON anomalies(symbol)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_anomalies_timestamp ON anomalies(timestamp)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_anomalies_symbol_timestamp ON anomalies(symbol, timestamp)')
                conn.execute('CREATE INDEX IF NOT EXISTS idx_anomalies_type ON anomalies(anomaly_type)')

                conn.commit()
                logger.info("Database initialized successfully with multi-symbol support")

        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            self.use_db = False

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def log_trade(self, features: Dict) -> Optional[int]:
        """
        Log trade to database or CSV with symbol support.

        Returns trade_id if using database, None otherwise.
        """
        symbol = features.get('symbol', 'BTC/USDT')  # Default to BTC/USDT if missing
        start = time.perf_counter()

        if self.use_db:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO trades (
                            symbol, timestamp, price, quantity, is_buyer_maker,
                            z_score, rolling_mean, rolling_std,
                            price_change_pct, time_gap_sec, volume_ratio,
                            buy_pressure, vwap, vwap_deviation,
                            price_velocity, volume_spike,
                            hour, day_of_week, trading_session, is_weekend,
                            injected
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        symbol,
                        features['timestamp'],
                        features['price'],
                        features['quantity'],
                        features.get('is_buyer_maker', 0),
                        features.get('z_score', 0),
                        features.get('rolling_mean', 0),
                        features.get('rolling_std', 0),
                        features.get('price_change_pct', 0),
                        features.get('time_gap_sec', 0),
                        features.get('volume_ratio', 1),
                        features.get('buy_pressure', 0.5),
                        features.get('vwap', features['price']),
                        features.get('vwap_deviation', 0),
                        features.get('price_velocity', 0),
                        features.get('volume_spike', 1),
                        features.get('hour', 0),
                        features.get('day_of_week', 0),
                        features.get('trading_session', 'unknown'),
                        features.get('is_weekend', 0),
                        features.get('injected', 0)
                    ))
                    conn.commit()

                    # Metrics
                    write_ms = (time.perf_counter() - start) * 1000.0
                    bytes_written = self._estimate_bytes({
                        **{k: features.get(k) for k in [
                            'symbol','timestamp','price','quantity','is_buyer_maker',
                            'z_score','rolling_mean','rolling_std','price_change_pct',
                            'time_gap_sec','volume_ratio','buy_pressure','vwap','vwap_deviation',
                            'price_velocity','volume_spike','hour','day_of_week',
                            'trading_session','is_weekend','injected'
                        ] if k in features},
                    })
                    if self.metrics_hook:
                        try:
                            self.metrics_hook(write_ms, bytes_written)
                        except Exception:
                            pass

                    return cursor.lastrowid

            except Exception as e:
                logger.error(f"Database trade insertion failed: {e}")
                self.use_db = False  # Fallback to CSV
                # fall through to CSV

        # CSV fallback - ensure symbol is included
        try:
            features_copy = features.copy()
            features_copy['symbol'] = symbol  # Ensure symbol is in CSV

            start = time.perf_counter()
            pd.DataFrame([features_copy]).to_csv(
                self.trades_csv,
                mode='a',
                header=not os.path.exists(self.trades_csv),
                index=False
            )
            write_ms = (time.perf_counter() - start) * 1000.0
            bytes_written = self._estimate_bytes(features_copy)
            if self.metrics_hook:
                try:
                    self.metrics_hook(write_ms, bytes_written)
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"CSV trade logging failed: {e}")

        return None

    def log_anomaly(self, features: Dict, method: str, trade_id: Optional[int] = None):
        """Log anomaly to database or CSV with symbol support"""
        symbol = features.get('symbol', 'BTC/USDT')

        anomaly_record = {
            'symbol': symbol,
            'timestamp': features['timestamp'],
            'anomaly_type': method + ("_injected" if features.get('injected') else ""),
            'price': features['price'],
            'z_score': features.get('z_score', 0),
            'price_change_pct': features.get('price_change_pct', 0),
            'volume_spike': features.get('volume_spike', 1),
            'vwap_deviation': features.get('vwap_deviation', 0),
        }

        start = time.perf_counter()
        if self.use_db and trade_id is not None:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO anomalies (
                            trade_id, symbol, timestamp, anomaly_type,
                            price, z_score, price_change_pct,
                            volume_spike, vwap_deviation
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        trade_id,
                        symbol,
                        anomaly_record['timestamp'],
                        anomaly_record['anomaly_type'],
                        anomaly_record['price'],
                        anomaly_record['z_score'],
                        anomaly_record['price_change_pct'],
                        anomaly_record['volume_spike'],
                        anomaly_record['vwap_deviation']
                    ))
                    conn.commit()

                    write_ms = (time.perf_counter() - start) * 1000.0
                    bytes_written = self._estimate_bytes(anomaly_record)
                    if self.metrics_hook:
                        try:
                            self.metrics_hook(write_ms, bytes_written)
                        except Exception:
                            pass
                    return

            except Exception as e:
                logger.error(f"Database anomaly insertion failed: {e}")
                self.use_db = False  # fallback to CSV

        # CSV fallback
        try:
            start = time.perf_counter()
            pd.DataFrame([anomaly_record]).to_csv(
                self.anomalies_csv,
                mode='a',
                header=not os.path.exists(self.anomalies_csv),
                index=False
            )
            write_ms = (time.perf_counter() - start) * 1000.0
            bytes_written = self._estimate_bytes(anomaly_record)
            if self.metrics_hook:
                try:
                    self.metrics_hook(write_ms, bytes_written)
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"CSV anomaly logging failed: {e}")

    # --- Convenience readers (unchanged) ---
    def get_recent_trades(self, minutes: int = 60, symbol: Optional[str] = None) -> pd.DataFrame:
        """Get recent trades from storage, optionally filtered by symbol"""
        if self.use_db:
            try:
                with self.get_connection() as conn:
                    if symbol:
                        query = '''
                            SELECT * FROM trades
                            WHERE symbol = ? 
                            AND datetime(timestamp) >= datetime('now', '-{} minutes')
                            ORDER BY timestamp DESC
                        '''.format(minutes)
                        return pd.read_sql_query(query, conn, params=[symbol])
                    else:
                        query = '''
                            SELECT * FROM trades
                            WHERE datetime(timestamp) >= datetime('now', '-{} minutes')
                            ORDER BY timestamp DESC
                        '''.format(minutes)
                        return pd.read_sql_query(query, conn)
            except Exception as e:
                logger.error(f"Failed to read from database: {e}")

        # CSV fallback
        if os.path.exists(self.trades_csv):
            try:
                df = pd.read_csv(self.trades_csv)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                cutoff = pd.Timestamp.now() - pd.Timedelta(minutes=minutes)
                df = df[df['timestamp'] >= cutoff]
                if symbol and 'symbol' in df.columns:
                    df = df[df['symbol'] == symbol]
                return df.sort_values('timestamp', ascending=False)
            except Exception as e:
                logger.error(f"Failed to read from CSV: {e}")

        return pd.DataFrame()

    def get_recent_anomalies(self, minutes: int = 60, symbol: Optional[str] = None) -> pd.DataFrame:
        """Get recent anomalies from storage, optionally filtered by symbol"""
        if self.use_db:
            try:
                with self.get_connection() as conn:
                    if symbol:
                        query = '''
                            SELECT * FROM anomalies
                            WHERE symbol = ?
                            AND datetime(timestamp) >= datetime('now', '-{} minutes')
                            ORDER BY timestamp DESC
                        '''.format(minutes)
                        return pd.read_sql_query(query, conn, params=[symbol])
                    else:
                        query = '''
                            SELECT * FROM anomalies
                            WHERE datetime(timestamp) >= datetime('now', '-{} minutes')
                            ORDER BY timestamp DESC
                        '''.format(minutes)
                        return pd.read_sql_query(query, conn)
            except Exception as e:
                logger.error(f"Failed to read anomalies from database: {e}")

        # CSV fallback
        if os.path.exists(self.anomalies_csv):
            try:
                df = pd.read_csv(self.anomalies_csv)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                cutoff = pd.Timestamp.now() - pd.Timedelta(minutes=minutes)
                df = df[df['timestamp'] >= cutoff]
                if symbol and 'symbol' in df.columns:
                    df = df[df['symbol'] == symbol]
                return df.sort_values('timestamp', ascending=False)
            except Exception as e:
                logger.error(f"Failed to read anomalies from CSV: {e}")

        return pd.DataFrame()
