# dashboard.py
import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

import pandas as pd
import streamlit as st

# -----------------------------
# Basic setup
# -----------------------------
st.set_page_config(page_title="Crypto Anomaly Dashboard", layout="wide")
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s",
    stream=sys.stdout,
)

# -----------------------------
# Time interval configuration
# -----------------------------
TIME_INTERVALS = {
    "Live": {"minutes": 10, "refresh": 5, "candle_interval": "1min"},
    "15m": {"minutes": 15, "refresh": None, "candle_interval": "1min"},
    "1h": {"minutes": 60, "refresh": None, "candle_interval": "1min"},
    "4h": {"minutes": 240, "refresh": None, "candle_interval": "5min"},
    "24h": {"minutes": 1440, "refresh": None, "candle_interval": "15min"},
    "1W": {"minutes": 10080, "refresh": None, "candle_interval": "1H"},
}

# -----------------------------
# Environment / Paths
# -----------------------------
DATA_SOURCE = os.environ.get("DASHBOARD_SOURCE", "parquet").lower()
PARQUET_ROOT = os.environ.get("LOCAL_OUTPUT_DIR", "./output")
PARQUET_TRADES_DIR = os.path.join(PARQUET_ROOT, "trades")
PARQUET_ANOM_DIR = os.path.join(PARQUET_ROOT, "anomalies")

# -----------------------------
# Helpers
# -----------------------------
def _to_utc_timestamp(series: pd.Series) -> pd.Series:
    """Robust timestamp parser: strings/datetimes → UTC; numeric epochs → auto unit."""
    if pd.api.types.is_datetime64_any_dtype(series):
        dt = pd.to_datetime(series, errors="coerce", utc=True)
        return dt.dt.tz_convert(None)

    if series.dtype == object:
        dt = pd.to_datetime(series, errors="coerce", utc=True)
        return dt.dt.tz_convert(None)

    s = pd.to_numeric(series, errors="coerce")
    m = s.dropna().abs().max()

    # Heuristics for unix epoch unit
    if pd.isna(m):
        # All NaNs
        return pd.to_datetime(s, errors="coerce", utc=True).dt.tz_localize(None)

    if m < 1e11:
        unit = "s"     # seconds
    elif m < 1e14:
        unit = "ms"    # milliseconds
    elif m < 1e17:
        unit = "us"    # microseconds
    else:
        unit = "ns"    # nanoseconds

    dt = pd.to_datetime(s, unit=unit, utc=True)

    # Safety net: if everything looks ancient, try finer units
    try_threshold = pd.Timestamp("2005-01-01", tz="UTC")
    if (dt.max() is not pd.NaT) and (dt.max() < try_threshold):
        for alt in ("ms", "us", "ns"):
            dt2 = pd.to_datetime(s, unit=alt, utc=True)
            if dt2.max() >= try_threshold:
                dt = dt2
                break

    return dt.dt.tz_convert(None)


def _norm_symbol(val: str) -> str:
    """BTCUSDT -> BTC/USDT; leave others as-is."""
    if isinstance(val, str) and '/' not in val and val.endswith('USDT'):
        return f"{val[:-4]}/USDT"
    return val


def _pick_timestamp_col(df: pd.DataFrame) -> Optional[str]:
    for c in ("timestamp", "timestamp_ms", "event_time", "ts", "time"):
        if c in df.columns:
            return c
    return None


def _read_parquet_dir(dir_path: str, sample_limit: Optional[int] = None) -> Tuple[pd.DataFrame, List[str]]:
    """
    Recursively read all parquet files under dir_path into a single DataFrame.
    Returns (df, file_list).
    """
    files = []
    for root, _, fnames in os.walk(dir_path):
        for f in fnames:
            if f.endswith(".parquet"):
                files.append(os.path.join(root, f))
    if not files:
        return pd.DataFrame(), []

    if sample_limit is not None:
        files_sorted = sorted(files)[-sample_limit:]
    else:
        files_sorted = sorted(files)

    dfs = []
    for fp in files_sorted:
        try:
            dfp = pd.read_parquet(fp)
            if not dfp.empty:
                dfs.append(dfp)
        except Exception as e:
            logger.warning(f"Failed to read parquet {fp}: {e}")

    if not dfs:
        return pd.DataFrame(), files_sorted
    return pd.concat(dfs, ignore_index=True), files_sorted


def load_parquet_data(kind: str, symbol: Optional[str] = None, sample_limit: Optional[int] = None) -> pd.DataFrame:
    """
    kind ∈ {"trades", "anomalies"}
    Reads parquet into a combined df (no time filter). If symbol is provided, normalizes and filters.
    """
    dir_map = {"trades": PARQUET_TRADES_DIR, "anomalies": PARQUET_ANOM_DIR}
    dir_path = dir_map.get(kind)
    if not dir_path or not os.path.isdir(dir_path):
        logger.info(f"No directory for {kind}: {dir_path}")
        return pd.DataFrame()

    logger.info(f"Looking for parquet files: {dir_path}/**/*.parquet")
    df, files = _read_parquet_dir(dir_path, sample_limit=sample_limit)
    logger.info(f"Found {len(files)} files")
    if df.empty:
        return df

    logger.info(f"Combined {len(df)} total rows from {len(files)} files")

    # Normalize symbol then filter
    if symbol and "symbol" in df.columns:
        df["symbol"] = df["symbol"].apply(_norm_symbol)
        df = df[df["symbol"] == symbol]
        logger.info(f"Filtered to {len(df)} rows for {symbol}")

    return df


def load_trades(minutes: int, symbol: str) -> pd.DataFrame:
    """Load trades within the last `minutes` minutes for a given symbol from parquet."""
    logger.info(f"Loading trades for last {minutes} minutes for {symbol}")
    now_utc = datetime.utcnow()
    cutoff = now_utc - timedelta(minutes=minutes)
    logger.info(f"Current time: {now_utc}")
    logger.info(f"Cutoff time: {cutoff}")

    if DATA_SOURCE != "parquet":
        st.warning("This dashboard build currently supports parquet source only.")
        return pd.DataFrame()

    df = load_parquet_data("trades", symbol)
    if df.empty:
        logger.warning("No trade files / empty trades dataframe")
        return df

    ts_col = _pick_timestamp_col(df)
    if not ts_col:
        logger.error("No timestamp-like column found in trades parquet")
        return pd.DataFrame()

    df["timestamp"] = _to_utc_timestamp(df[ts_col])

    if df["timestamp"].isna().all():
        logger.error("All parsed trade timestamps are NaT (check epoch units or column)")
        return pd.DataFrame()

    # Filter by time window
    df = df[df["timestamp"] >= cutoff].sort_values("timestamp")
    if df.empty:
        logger.warning(f"No trades found after filtering (all trades older than {cutoff})")
        # Diagnose: what's the latest overall timestamp available?
        full_df = load_parquet_data("trades", symbol)
        if not full_df.empty:
            ts2 = _pick_timestamp_col(full_df)
            if ts2:
                full_df["timestamp"] = _to_utc_timestamp(full_df[ts2])
                latest = full_df["timestamp"].max()
                logger.info(f"Latest available timestamp: {latest}")
        return df

    logger.info(f"Loaded {len(df)} trades after filtering; Latest: {df['timestamp'].max()}")
    return df


def load_anomalies(minutes: int, symbol: str, anomaly_types: Optional[List[str]] = None) -> pd.DataFrame:
    """Load anomalies within the last `minutes` minutes for a given symbol from parquet."""
    logger.info(f"Loading anomalies for last {minutes} minutes for {symbol}")
    if DATA_SOURCE != "parquet":
        return pd.DataFrame()

    df = load_parquet_data("anomalies", symbol)
    if df.empty:
        logger.info("No anomaly files / empty anomalies dataframe")
        return df

    ts_col = _pick_timestamp_col(df)
    if not ts_col:
        logger.error("No timestamp-like column found in anomalies parquet")
        return pd.DataFrame()

    df["timestamp"] = _to_utc_timestamp(df[ts_col])

    if anomaly_types and "anomaly_type" in df.columns:
        df = df[df["anomaly_type"].isin(anomaly_types)]

    cutoff = datetime.utcnow() - timedelta(minutes=minutes)
    df = df[df["timestamp"] >= cutoff].sort_values("timestamp", ascending=False)
    logger.info(f"Found {len(df)} anomalies after filtering")
    return df


def _discover_symbols() -> List[str]:
    """Scan a few parquet files and infer available symbols."""
    symbols = set()
    for kind in ("trades", "anomalies"):
        df = load_parquet_data(kind, symbol=None, sample_limit=20)
        if not df.empty and "symbol" in df.columns:
            ss = df["symbol"].dropna().astype(str).map(_norm_symbol).unique().tolist()
            symbols.update(ss)
    # Fallback common list if empty
    if not symbols:
        return ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]
    return sorted(symbols)


def _render_interval_content(label: str, selected_symbol: str):
    cfg = TIME_INTERVALS[label]
    minutes = cfg["minutes"]

    if label == "Live" and cfg.get("refresh"):
        st_autorefresh = st.empty()
        st_autorefresh.write(f"Auto-refresh every {cfg['refresh']}s enabled.")
        st.experimental_set_query_params(_=datetime.utcnow().timestamp())

    trades = load_trades(minutes, selected_symbol)
    anomalies = load_anomalies(minutes, selected_symbol)

    c1, c2 = st.columns([2, 1])
    with c1:
        st.subheader(f"Trades — {label} • {selected_symbol}")
        if trades.empty:
            st.info("No trades in this window.")
        else:
            st.dataframe(trades.tail(500), use_container_width=True)

    with c2:
        st.subheader(f"Anomalies — {label} • {selected_symbol}")
        if anomalies.empty:
            st.info("No anomalies in this window.")
        else:
            st.dataframe(anomalies.head(200), use_container_width=True)

    with st.expander("Debug information"):
        st.write("Configuration:")
        st.write(f"- Selected Symbol: {selected_symbol}")
        st.write(f"- Data Source: {DATA_SOURCE}")
        st.write(f"- Output Directory: {PARQUET_ROOT}")

        # Show some file counts and samples
        def _sample_files(dir_path: str, n=5) -> List[str]:
            out = []
            for root, _, fnames in os.walk(dir_path):
                for f in fnames:
                    if f.endswith(".parquet"):
                        out.append(os.path.join(root, f))
            # sort by modification time (newest first)
            out.sort(key=lambda p: os.path.getmtime(p), reverse=True)
            return out[:n]

        # Count all parquet files
        all_trade_files = []
        for root, _, fnames in os.walk(PARQUET_TRADES_DIR):
            for f in fnames:
                if f.endswith(".parquet"):
                    all_trade_files.append(os.path.join(root, f))
        st.write(f"- Trade files found (total): {len(all_trade_files)}")

        # Show newest 5 by modification time
        newest = _sample_files(PARQUET_TRADES_DIR, n=5) if os.path.isdir(PARQUET_TRADES_DIR) else []
        if newest:
            st.write("Newest files:")
            for f in newest:
                mtime = datetime.utcfromtimestamp(os.path.getmtime(f))
                st.write(f"{os.path.basename(f)}  —  mtime UTC: {mtime}")

            # Load a small sample to show timestamp sanity
            # Compute latest timestamp across ALL trades for the selected symbol
            df_all = load_parquet_data("trades", selected_symbol, sample_limit=None)
            ts_col = _pick_timestamp_col(df_all) if not df_all.empty else None
            if ts_col:
                df_all["__ts"] = _to_utc_timestamp(df_all[ts_col])
                latest_time = df_all["__ts"].max()
                now = datetime.utcnow()
                st.write(f"\n**Latest timestamp (ALL files):** {latest_time}")
                st.write(f"**Current time:** {now}")
                st.write(f"**Time difference:** {now - latest_time}")
            else:
                st.write("No timestamp-like column found in trades.")


def main():
    st.title("Real-Time Crypto Trades & Anomalies (Parquet)")

    # Top bar: Source and Symbol
    st.caption("Point this dashboard at your Spark output parquet directory via LOCAL_OUTPUT_DIR.")

    if DATA_SOURCE != "parquet":
        st.warning("Only 'parquet' source is supported in this build. Set DASHBOARD_SOURCE=parquet.")
        st.stop()

    if not os.path.isdir(PARQUET_ROOT):
        st.error(f"Output directory not found: {PARQUET_ROOT}")
        st.stop()

    # Discover symbols from data
    symbols = _discover_symbols()
    col_left, col_right = st.columns([1, 3])
    with col_left:
        selected_symbol = st.selectbox("Symbol", symbols, index=0)
    with col_right:
        st.write(f"Parquet root: `{PARQUET_ROOT}`")
        st.write(f"Trades dir: `{PARQUET_TRADES_DIR}`")
        st.write(f"Anomalies dir: `{PARQUET_ANOM_DIR}`")

    # Tabs for time windows
    tabs = st.tabs(list(TIME_INTERVALS.keys()))
    # Live
    with tabs[0]:
        st.session_state["window_label"] = "Live"
        _render_interval_content("Live", selected_symbol)
    # 15m
    with tabs[1]:
        st.session_state["window_label"] = "15m"
        _render_interval_content("15m", selected_symbol)
    # 1h
    with tabs[2]:
        st.session_state["window_label"] = "1h"
        _render_interval_content("1h", selected_symbol)
    # 4h
    with tabs[3]:
        st.session_state["window_label"] = "4h"
        _render_interval_content("4h", selected_symbol)
    # 24h
    with tabs[4]:
        st.session_state["window_label"] = "24h"
        _render_interval_content("24h", selected_symbol)
    # 1W
    with tabs[5]:
        st.session_state["window_label"] = "1W"
        _render_interval_content("1W", selected_symbol)

if __name__ == "__main__":
    main()
