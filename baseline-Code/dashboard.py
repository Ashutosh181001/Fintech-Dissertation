"""
BTC/USDT Anomaly Detection Dashboard
Streamlit dashboard with time tabs and visualization
"""

import os
from datetime import datetime, timedelta
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import sqlite3
import logging
import config

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Storage configuration
USE_DATABASE = config.STORAGE_CONFIG["use_database"]
DATABASE_PATH = config.STORAGE_CONFIG["database_path"]
TRADE_LOG = config.STORAGE_CONFIG["trades_csv"]
ANOMALY_LOG = config.STORAGE_CONFIG["anomalies_csv"]

# Time interval configuration
TIME_INTERVALS = {
    "Live": {"minutes": 10, "refresh": 5, "candle_interval": "1min"},
    "15m": {"minutes": 15, "refresh": None, "candle_interval": "1min"},
    "1h": {"minutes": 60, "refresh": None, "candle_interval": "1min"},
    "4h": {"minutes": 240, "refresh": None, "candle_interval": "5min"},
    "24h": {"minutes": 1440, "refresh": None, "candle_interval": "15min"},
    "1W": {"minutes": 10080, "refresh": None, "candle_interval": "1H"},
}


def load_trades(minutes: int, symbol: str = "BTC/USDT") -> pd.DataFrame:
    """Load trades within the specified time window for a specific symbol"""
    logger.info(f"Loading trades for last {minutes} minutes for {symbol}")

    # Try database first if configured
    if USE_DATABASE and os.path.exists(DATABASE_PATH):
        logger.info(f"Attempting to load from database: {DATABASE_PATH}")
        try:
            conn = sqlite3.connect(DATABASE_PATH)

            # Check if symbol column exists
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(trades)")
            columns = [column[1] for column in cursor.fetchall()]
            has_symbol_column = 'symbol' in columns

            # Calculate cutoff time in milliseconds
            current_time_ms = int(datetime.utcnow().timestamp() * 1000)
            cutoff_time_ms = current_time_ms - (minutes * 60 * 1000)

            logger.info(f"Current time (ms): {current_time_ms}")
            logger.info(f"Cutoff time (ms): {cutoff_time_ms}")
            logger.info(f"Time window: {minutes} minutes")

            if has_symbol_column:
                # First check what we have in database
                test_query = """
                    SELECT MIN(CAST(timestamp AS INTEGER)) as min_ts, 
                           MAX(CAST(timestamp AS INTEGER)) as max_ts,
                           COUNT(*) as total
                    FROM trades 
                    WHERE symbol = ?
                """
                test_result = pd.read_sql_query(test_query, conn, params=[symbol])
                if not test_result.empty:
                    logger.info(
                        f"DB stats - Min timestamp: {test_result['min_ts'][0]}, Max timestamp: {test_result['max_ts'][0]}, Total: {test_result['total'][0]}")

                query = """
                    SELECT timestamp, price, quantity, z_score, rolling_mean, rolling_std,
                           price_change_pct, volume_spike, is_buyer_maker
                    FROM trades
                    WHERE symbol = ?
                    AND CAST(timestamp AS INTEGER) >= ?
                    ORDER BY timestamp ASC
                """
                df = pd.read_sql_query(query, conn, params=[symbol, cutoff_time_ms])
            else:
                # Old database without symbol column
                query = """
                    SELECT timestamp, price, quantity, z_score, rolling_mean, rolling_std,
                           price_change_pct, volume_spike, is_buyer_maker
                    FROM trades
                    WHERE CAST(timestamp AS INTEGER) >= ?
                    ORDER BY timestamp ASC
                """
                df = pd.read_sql_query(query, conn, params=[cutoff_time_ms])

            conn.close()

            if not df.empty:
                # Convert timestamp from milliseconds to datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float), unit='ms')

                # Additional filtering after conversion to ensure we have the right time window
                cutoff_datetime = datetime.utcnow() - timedelta(minutes=minutes)
                df = df[df['timestamp'] >= cutoff_datetime]

                logger.info(f"Loaded {len(df)} trades from database after filtering")

                # Log sample of data for debugging
                if len(df) > 0:
                    logger.info(f"  Latest trade: {df.iloc[-1]['timestamp']} @ ${df.iloc[-1]['price']:.2f}")
                    logger.info(f"  Oldest trade: {df.iloc[0]['timestamp']} @ ${df.iloc[0]['price']:.2f}")

                return df
            else:
                logger.warning("No trades found in database for the specified time window")

        except Exception as e:
            logger.error(f"Error loading from database: {e}")
            import traceback
            logger.error(traceback.format_exc())

    # Fall back to CSV
    if os.path.exists(TRADE_LOG):
        logger.info(f"Loading from CSV: {TRADE_LOG}")
        try:
            df = pd.read_csv(TRADE_LOG)

            # Check if file has data
            if df.empty:
                logger.warning("CSV file is empty")
                return pd.DataFrame()

            logger.info(f"CSV has {len(df)} total rows")

            # Filter by symbol if column exists
            if 'symbol' in df.columns:
                df = df[df['symbol'] == symbol]
                logger.info(f"Filtered to {len(df)} rows for {symbol}")

            df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')

            # Filter by time window
            cutoff = datetime.utcnow() - timedelta(minutes=minutes)
            logger.info(f"Filtering trades after {cutoff}")

            df = df[df['timestamp'] >= cutoff]

            if df.empty:
                logger.warning(f"No trades found after filtering (all trades older than {cutoff})")
            else:
                logger.info(f"Found {len(df)} trades in time window")

            # Convert numeric columns
            for col in ['price', 'quantity', 'z_score', 'rolling_mean', 'rolling_std']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            return df.sort_values('timestamp')

        except Exception as e:
            logger.error(f"Error loading from CSV: {e}")
            import traceback
            logger.error(traceback.format_exc())
    else:
        logger.warning(f"Trade log file not found: {TRADE_LOG}")

    return pd.DataFrame()


def load_anomalies(minutes: int, symbol: str = "BTC/USDT", anomaly_types: list = None) -> pd.DataFrame:
    """Load anomalies within the specified time window for a specific symbol"""
    logger.info(f"Loading anomalies for last {minutes} minutes for {symbol}")

    # Try database first if configured
    if USE_DATABASE and os.path.exists(DATABASE_PATH):
        logger.info(f"Attempting to load anomalies from database")
        try:
            conn = sqlite3.connect(DATABASE_PATH)

            # Check if symbol column exists
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(anomalies)")
            columns = [column[1] for column in cursor.fetchall()]
            has_symbol_column = 'symbol' in columns

            # Calculate cutoff time in milliseconds
            current_time_ms = int(datetime.utcnow().timestamp() * 1000)
            cutoff_time_ms = current_time_ms - (minutes * 60 * 1000)

            if has_symbol_column:
                query = """
                    SELECT timestamp, anomaly_type, price, z_score, price_change_pct, volume_spike
                    FROM anomalies
                    WHERE symbol = ?
                    AND CAST(timestamp AS INTEGER) >= ?
                    ORDER BY timestamp DESC
                """
                df = pd.read_sql_query(query, conn, params=[symbol, cutoff_time_ms])
            else:
                # Old database without symbol column
                query = """
                    SELECT timestamp, anomaly_type, price, z_score, price_change_pct, volume_spike
                    FROM anomalies
                    WHERE CAST(timestamp AS INTEGER) >= ?
                    ORDER BY timestamp DESC
                """
                df = pd.read_sql_query(query, conn, params=[cutoff_time_ms])

            conn.close()

            if not df.empty:
                # Convert timestamp from milliseconds to datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float), unit='ms')

                # Additional filtering after conversion
                cutoff_datetime = datetime.utcnow() - timedelta(minutes=minutes)
                df = df[df['timestamp'] >= cutoff_datetime]

                # Filter by type if specified
                if anomaly_types:
                    df = df[df['anomaly_type'].isin(anomaly_types)]

                logger.info(f"Loaded {len(df)} anomalies from database after filtering")
                return df
            else:
                logger.info("No anomalies found in database")

        except Exception as e:
            logger.error(f"Error loading anomalies from database: {e}")

    # Fall back to CSV
    if os.path.exists(ANOMALY_LOG):
        logger.info(f"Loading anomalies from CSV: {ANOMALY_LOG}")
        try:
            df = pd.read_csv(ANOMALY_LOG)

            if df.empty:
                logger.warning("Anomaly CSV is empty")
                return pd.DataFrame()

            # Filter by symbol if column exists
            if 'symbol' in df.columns:
                df = df[df['symbol'] == symbol]

            df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')

            # Filter by time window
            cutoff = datetime.utcnow() - timedelta(minutes=minutes)
            df = df[df['timestamp'] >= cutoff]

            # Filter by type if specified
            if anomaly_types:
                df = df[df['anomaly_type'].isin(anomaly_types)]

            # Convert numeric columns
            for col in ['price', 'z_score']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            logger.info(f"Found {len(df)} anomalies in time window")
            return df.sort_values('timestamp')

        except Exception as e:
            logger.error(f"Error loading anomalies from CSV: {e}")
    else:
        logger.info(f"Anomaly log file not found: {ANOMALY_LOG}")

    return pd.DataFrame()


def aggregate_to_candlesticks(trades_df: pd.DataFrame, interval: str) -> pd.DataFrame:
    """Aggregate trades into OHLCV candlesticks"""
    if trades_df.empty:
        return pd.DataFrame()

    try:
        df = trades_df.set_index('timestamp').sort_index()

        ohlc = df['price'].resample(interval).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last'
        })

        volume = df['quantity'].resample(interval).sum()
        volume.name = 'volume'

        candlesticks = pd.concat([ohlc, volume], axis=1)
        candlesticks = candlesticks.dropna().reset_index()

        return candlesticks
    except Exception:
        return pd.DataFrame()


def create_metrics_row(trades_df: pd.DataFrame, anomalies_df: pd.DataFrame, interval_name: str, symbol: str = "BTC/USDT"):
    """Display key metrics"""
    if trades_df.empty:
        st.warning("No trade data available")
        return

    col1, col2, col3, col4, col5 = st.columns(5)

    # Current price and change
    current_price = trades_df.iloc[-1]['price']
    first_price = trades_df.iloc[0]['price']
    price_change = ((current_price - first_price) / first_price) * 100

    with col1:
        st.metric("Current Price", f"${current_price:,.2f}", f"{price_change:+.2f}%")

    # High/Low
    with col2:
        st.metric(f"{interval_name} High", f"${trades_df['price'].max():,.2f}")

    with col3:
        st.metric(f"{interval_name} Low", f"${trades_df['price'].min():,.2f}")

    # Volume - extract base currency from symbol
    total_volume = trades_df['quantity'].sum() if 'quantity' in trades_df.columns else 0
    base_currency = symbol.split('/')[0] if '/' in symbol else 'BTC'

    with col4:
        st.metric("Volume", f"{total_volume:.4f} {base_currency}")

    # Anomalies
    with col5:
        st.metric("Anomalies", len(anomalies_df))


def create_candlestick_chart(candlesticks: pd.DataFrame, trades_df: pd.DataFrame,
                            anomalies_df: pd.DataFrame, show_ma: bool) -> go.Figure:
    """Create candlestick chart with indicators"""
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.7, 0.3],
        subplot_titles=('Price', 'Volume')
    )

    # Candlestick chart
    if not candlesticks.empty:
        fig.add_trace(
            go.Candlestick(
                x=candlesticks['timestamp'],
                open=candlesticks['open'],
                high=candlesticks['high'],
                low=candlesticks['low'],
                close=candlesticks['close'],
                name='Price',
                increasing=dict(line=dict(color='#26a69a')),
                decreasing=dict(line=dict(color='#ef5350'))
            ),
            row=1, col=1
        )

        # Volume bars
        colors = ['#26a69a' if c >= o else '#ef5350'
                 for c, o in zip(candlesticks['close'], candlesticks['open'])]
        fig.add_trace(
            go.Bar(
                x=candlesticks['timestamp'],
                y=candlesticks['volume'],
                marker_color=colors,
                name='Volume',
                opacity=0.7
            ),
            row=2, col=1
        )

    # Moving average
    if show_ma and 'rolling_mean' in trades_df.columns:
        ma_data = trades_df[['timestamp', 'rolling_mean']].dropna()
        if not ma_data.empty:
            fig.add_trace(
                go.Scatter(
                    x=ma_data['timestamp'],
                    y=ma_data['rolling_mean'],
                    name='MA',
                    line=dict(color='#ffa726', width=2)
                ),
                row=1, col=1
            )

    # Anomaly markers
    if not anomalies_df.empty:
        fig.add_trace(
            go.Scatter(
                x=anomalies_df['timestamp'],
                y=anomalies_df['price'],
                mode='markers',
                name='Anomalies',
                marker=dict(
                    symbol='triangle-up',
                    size=12,
                    color='#ff1744',
                    line=dict(width=2, color='white')
                ),
                text=[f"Type: {t}<br>Z: {z:.2f}"
                     for t, z in zip(anomalies_df['anomaly_type'], anomalies_df['z_score'])],
                hoverinfo='text+x+y'
            ),
            row=1, col=1
        )

    # Layout
    fig.update_layout(
        template='plotly_dark',
        height=700,
        showlegend=True,
        hovermode='x unified',
        xaxis_rangeslider_visible=False,
        plot_bgcolor='#0e1117',
        paper_bgcolor='#0e1117'
    )

    fig.update_xaxes(gridcolor='rgba(128,128,128,0.2)')
    fig.update_yaxes(gridcolor='rgba(128,128,128,0.2)', title_text="Price (USDT)", row=1, col=1)
    fig.update_yaxes(gridcolor='rgba(128,128,128,0.2)', title_text="Volume (BTC)", row=2, col=1)

    return fig


def create_line_chart(trades_df: pd.DataFrame, anomalies_df: pd.DataFrame,
                     show_ma: bool) -> go.Figure:
    """Create line chart for longer timeframes"""
    fig = go.Figure()

    # Price line
    if not trades_df.empty:
        fig.add_trace(
            go.Scatter(
                x=trades_df['timestamp'],
                y=trades_df['price'],
                mode='lines',
                name='Price',
                line=dict(color='#f0b90b', width=2)
            )
        )

        # Moving average
        if show_ma and 'rolling_mean' in trades_df.columns:
            ma_data = trades_df[['timestamp', 'rolling_mean']].dropna()
            if not ma_data.empty:
                fig.add_trace(
                    go.Scatter(
                        x=ma_data['timestamp'],
                        y=ma_data['rolling_mean'],
                        mode='lines',
                        name='MA',
                        line=dict(color='#ffa726', width=2, dash='dash')
                    )
                )

    # Anomaly markers
    if not anomalies_df.empty:
        fig.add_trace(
            go.Scatter(
                x=anomalies_df['timestamp'],
                y=anomalies_df['price'],
                mode='markers',
                name='Anomalies',
                marker=dict(
                    symbol='triangle-up',
                    size=12,
                    color='#ff1744'
                ),
                text=[f"Type: {t}<br>Z: {z:.2f}"
                     for t, z in zip(anomalies_df['anomaly_type'], anomalies_df['z_score'])],
                hoverinfo='text+x+y'
            )
        )

    # Layout
    fig.update_layout(
        template='plotly_dark',
        height=600,
        showlegend=True,
        hovermode='x unified',
        plot_bgcolor='#0e1117',
        paper_bgcolor='#0e1117',
        xaxis=dict(gridcolor='rgba(128,128,128,0.2)'),
        yaxis=dict(gridcolor='rgba(128,128,128,0.2)', title='Price (USDT)')
    )

    return fig


def create_zscore_chart(trades_df: pd.DataFrame) -> go.Figure:
    """Create Z-score chart"""
    if 'z_score' not in trades_df.columns:
        return go.Figure()

    fig = go.Figure()

    # Z-score line
    fig.add_trace(
        go.Scatter(
            x=trades_df['timestamp'],
            y=trades_df['z_score'],
            mode='lines',
            name='Z-Score',
            line=dict(color='#2ca02c', width=2)
        )
    )

    # Threshold lines
    fig.add_hline(y=3.5, line_dash="dash", line_color="red",
                 annotation_text="Upper Threshold")
    fig.add_hline(y=-3.5, line_dash="dash", line_color="red",
                 annotation_text="Lower Threshold")
    fig.add_hline(y=0, line_dash="dot", line_color="gray")

    # Layout
    fig.update_layout(
        height=300,
        template='plotly_dark',
        xaxis_title='',
        yaxis_title='Z-Score',
        plot_bgcolor='#0e1117',
        paper_bgcolor='#0e1117'
    )

    return fig


def display_interval(interval_name: str, interval_config: dict,
                    show_ma: bool, selected_anomaly_types: list, chart_type: str, selected_symbol: str):
    """Display content for a single time interval"""
    minutes = interval_config["minutes"]

    # Use fragment for Live tab auto-refresh
    if interval_config.get("refresh"):
        @st.fragment(run_every=interval_config["refresh"])
        def render_interval():
            _render_interval_content(interval_name, minutes, interval_config,
                                    show_ma, selected_anomaly_types, chart_type, selected_symbol)
        render_interval()
    else:
        _render_interval_content(interval_name, minutes, interval_config,
                                show_ma, selected_anomaly_types, chart_type, selected_symbol)


def _render_interval_content(interval_name: str, minutes: int, interval_config: dict,
                            show_ma: bool, selected_anomaly_types: list, chart_type: str, selected_symbol: str):
    """Render the actual interval content"""
    # Load data
    trades_df = load_trades(minutes, selected_symbol)
    anomalies_df = load_anomalies(minutes, selected_symbol, selected_anomaly_types)

    if trades_df.empty:
        st.warning(f"No trade data available for {interval_name}")

        # Show debugging information
        with st.expander("🔍 Debug Information"):
            st.write("**Configuration:**")
            st.write(f"- Selected Symbol: {selected_symbol}")
            st.write(f"- Using Database: {USE_DATABASE}")
            st.write(f"- Database Path: {DATABASE_PATH} (exists: {os.path.exists(DATABASE_PATH)})")
            st.write(f"- CSV Path: {TRADE_LOG} (exists: {os.path.exists(TRADE_LOG)})")

            # Check if files exist and show sample data
            if os.path.exists(TRADE_LOG):
                try:
                    sample_df = pd.read_csv(TRADE_LOG, nrows=5)
                    if not sample_df.empty:
                        st.write(f"**Sample data from CSV (first 5 rows):**")
                        st.dataframe(sample_df)

                        # Check timestamp format
                        sample_df['timestamp'] = pd.to_datetime(sample_df['timestamp'])
                        latest_time = sample_df['timestamp'].max()
                        st.write(f"**Latest timestamp in file:** {latest_time}")
                        st.write(f"**Current time:** {datetime.utcnow()}")
                        st.write(f"**Time difference:** {datetime.utcnow() - latest_time}")
                except Exception as e:
                    st.error(f"Error reading sample data: {e}")

            if USE_DATABASE and os.path.exists(DATABASE_PATH):
                try:
                    conn = sqlite3.connect(DATABASE_PATH)

                    # Check if symbol column exists
                    cursor = conn.cursor()
                    cursor.execute("PRAGMA table_info(trades)")
                    columns = [column[1] for column in cursor.fetchall()]

                    if 'symbol' in columns:
                        count_query = "SELECT COUNT(*) as count FROM trades WHERE symbol = ?"
                        result = pd.read_sql_query(count_query, conn, params=[selected_symbol])
                        st.write(f"**Total {selected_symbol} trades in database:** {result['count'][0]}")

                        # Get latest trade
                        latest_query = "SELECT * FROM trades WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1"
                        latest = pd.read_sql_query(latest_query, conn, params=[selected_symbol])
                    else:
                        count_query = "SELECT COUNT(*) as count FROM trades"
                        result = pd.read_sql_query(count_query, conn)
                        st.write(f"**Total trades in database:** {result['count'][0]}")

                        # Get latest trade
                        latest_query = "SELECT * FROM trades ORDER BY timestamp DESC LIMIT 1"
                        latest = pd.read_sql_query(latest_query, conn)

                    if not latest.empty:
                        st.write(f"**Latest trade in database:**")
                        st.write(f"- Timestamp: {latest['timestamp'][0]}")
                        st.write(f"- Price: ${latest['price'][0]:.2f}")
                    conn.close()
                except Exception as e:
                    st.error(f"Error reading database: {e}")
        return

    # Metrics row
    create_metrics_row(trades_df, anomalies_df, interval_name, selected_symbol)

    st.markdown("<br>", unsafe_allow_html=True)

    # Main chart - candlestick for <= 1h, line for others
    if interval_name in ["Live", "15m", "1h"]:
        if chart_type == "Candlestick":
            candlesticks = aggregate_to_candlesticks(
                trades_df,
                interval_config.get("candle_interval", "1min")
            )
            fig = create_candlestick_chart(candlesticks, trades_df, anomalies_df, show_ma)
        else:
            fig = create_line_chart(trades_df, anomalies_df, show_ma)
    else:
        # Line chart only for longer timeframes
        fig = create_line_chart(trades_df, anomalies_df, show_ma)

    # Add unique key to prevent duplicate element ID error
    st.plotly_chart(fig, use_container_width=True, key=f"main_chart_{interval_name}_{selected_symbol}")

    # Z-score chart
    st.subheader("📊 Z-Score Analysis")
    z_fig = create_zscore_chart(trades_df)
    # Add unique key to prevent duplicate element ID error
    st.plotly_chart(z_fig, use_container_width=True, key=f"zscore_chart_{interval_name}_{selected_symbol}")

    # Recent anomalies table
    if not anomalies_df.empty:
        st.subheader("🚨 Recent Anomalies")

        display_df = anomalies_df.head(20).copy()
        display_df['timestamp'] = display_df['timestamp'].dt.strftime('%H:%M:%S')
        display_df['price'] = display_df['price'].apply(lambda x: f"${x:,.2f}")
        display_df['z_score'] = display_df['z_score'].apply(lambda x: f"{x:.2f}")

        display_df = display_df[['timestamp', 'anomaly_type', 'price', 'z_score']]
        display_df.columns = ['Time', 'Type', 'Price', 'Z-Score']

        st.dataframe(display_df, use_container_width=True)

    # Last update
    st.caption(f"Last updated: {datetime.utcnow().strftime('%H:%M:%S UTC')}")


def main():
    """Main dashboard function"""
    st.set_page_config(
        page_title="Crypto Anomaly Detection",
        layout="wide",
        page_icon="📊"
    )

    # Custom CSS
    st.markdown("""
        <style>
        .stApp { background-color: #0b0e11; }
        .stTabs [data-baseweb="tab-list"] { 
            gap: 2px; 
            background-color: #1e2329; 
            padding: 4px; 
            border-radius: 4px; 
        }
        .stTabs [data-baseweb="tab"] { 
            background-color: transparent; 
            color: #848e9c; 
            padding: 8px 16px; 
        }
        .stTabs [aria-selected="true"] { 
            background-color: #2b3139; 
            color: #f0b90b; 
            border-radius: 4px; 
        }
        </style>
    """, unsafe_allow_html=True)

    st.title("📊 Crypto Anomaly Detection Dashboard")

    # Add symbol selector
    available_symbols = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT"]
    selected_symbol = st.selectbox(
        "Select Cryptocurrency",
        available_symbols,
        index=0,
        key="symbol_selector"
    )

    # Quick data availability check
    data_status = check_data_availability(selected_symbol)
    if data_status:
        st.success(data_status)
    else:
        st.error(f"⚠️ No data sources available for {selected_symbol}. Please ensure the detector is running.")

    # Sidebar settings
    with st.sidebar:
        st.header("⚙️ Settings")

        # Show selected symbol
        st.info(f"Currently viewing: **{selected_symbol}**")

        st.subheader("Indicators")
        show_ma = st.checkbox("Show Moving Average", value=True)

        st.subheader("Chart Type")
        chart_type = st.radio(
            "Select chart type for short timeframes",
            ["Candlestick", "Line"],
            help="Applies to Live, 15m, and 1h views"
        )

        st.subheader("Anomaly Filters")

        # Get all anomaly types
        all_anomalies = load_anomalies(10080, selected_symbol)  # Last week
        if not all_anomalies.empty and 'anomaly_type' in all_anomalies.columns:
            anomaly_types = sorted(all_anomalies['anomaly_type'].unique())
            selected_types = st.multiselect(
                "Select anomaly types to display",
                anomaly_types,
                default=anomaly_types
            )
        else:
            selected_types = []

        # Data source info
        st.subheader("📁 Data Source")
        if USE_DATABASE:
            st.write(f"**Mode:** SQLite Database")
            st.write(f"**Path:** {DATABASE_PATH}")
            if os.path.exists(DATABASE_PATH):
                st.write(f"**Status:** ✅ Found")
            else:
                st.write(f"**Status:** ❌ Not Found")
        else:
            st.write(f"**Mode:** CSV Files")
            st.write(f"**Trades:** {TRADE_LOG}")
            st.write(f"**Anomalies:** {ANOMALY_LOG}")

    # Time interval tabs
    tabs = st.tabs(list(TIME_INTERVALS.keys()))

    for i, (interval_name, interval_config) in enumerate(TIME_INTERVALS.items()):
        with tabs[i]:
            display_interval(interval_name, interval_config, show_ma, selected_types, chart_type, selected_symbol)


def check_data_availability(symbol: str = "BTC/USDT"):
    """Check if data is available and return status message"""
    if USE_DATABASE and os.path.exists(DATABASE_PATH):
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            cursor = conn.cursor()

            # Check if symbol column exists
            cursor.execute("PRAGMA table_info(trades)")
            columns = [column[1] for column in cursor.fetchall()]

            if 'symbol' in columns:
                cursor.execute("SELECT COUNT(*) FROM trades WHERE symbol = ?", [symbol])
            else:
                cursor.execute("SELECT COUNT(*) FROM trades")

            count = cursor.fetchone()[0]
            conn.close()

            if count > 0:
                return f"✅ Database connected: {count:,} trades"
        except:
            pass

    if os.path.exists(TRADE_LOG):
        try:
            df = pd.read_csv(TRADE_LOG, nrows=1)
            if not df.empty:
                total_rows = sum(1 for _ in open(TRADE_LOG)) - 1  # Subtract header
                return f"✅ CSV data found: {total_rows:,} trades available"
        except:
            pass

    return None


if __name__ == "__main__":
    main()