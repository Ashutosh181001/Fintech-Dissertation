# Crypto Anomaly Detection System

A real-time cryptocurrency anomaly detection pipeline that monitors multiple trading pairs (BTC/USDT, ETH/USDT, BNB/USDT, SOL/USDT, XRP/USDT) using machine learning.

## Prerequisites

- Python 3.9+
- Docker and Docker Compose
- Git

## Installation

 **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```


## Configuration

1. **Set up environment variables** (optional)
   ```bash
   export TELEGRAM_TOKEN="your_bot_token"
   export TELEGRAM_CHAT_ID="your_chat_id"
   ```

2. **Verify configuration**
   ```bash
   python config.py
   ```

## Running the System

### Step 1: Start Docker Services

1. **Start Docker**
   - On termimal: Make sure to set working directory as ./baseline-Code 

2. **Start Kafka**
   ```bash
   docker-compose -f baseline-docker.yml up -d

   ```

3. **Verify Kafka is running**
   ```bash
   docker-compose ps
   ```
   Both kafka and zookeeper should show "Up" status.

### Step 2: Start the Data Producer

Open a new terminal and run:
```bash
python kafka_producer.py
```

You should see messages like:
- "Connected to Binance combined WebSocket"
- "BTC/USDT trade #1: $43,250.00"

### Step 3: Start the Anomaly Detector

Open another terminal and run:
```bash
python detect.py
```

Expected output:
- "Kafka consumer initialized successfully"
- Trade processing messages
- Anomaly detection results

### Step 4: View Live Dashboard

Open a third terminal and run:
```bash
streamlit run dashboard.py
```

The dashboard will open at: http://localhost:8501

## Dashboard Features

- **Live data**: Real-time price charts and anomaly markers
- **Multiple timeframes**: Live, 15m, 1h, 4h, 24h, 1W
- **Symbol selection**: Switch between BTC/USDT, ETH/USDT, etc.
- **Anomaly filtering**: Filter by detection method
- **Z-score analysis**: Statistical anomaly detection metrics

## Optional: Model Tuning

To optimize detection parameters:
```bash
python tune_model.py
```

## Troubleshooting

### Kafka Issues
- Streaming old data: Always make sure to remove old Docker container and re-compose as old messages can clog in the topic
- Ensure Docker is running: `docker ps`
- Restart Kafka: `docker-compose restart kafka`
- Check logs: `docker-compose logs kafka`

### No Data in Dashboard
- Verify producer is running and connected to Binance
- Check detect.py is processing trades
- Confirm database/CSV files are being created


### Connection Errors
- Check internet connection for Binance WebSocket
- Verify Kafka broker is accessible on localhost:9092

## Stopping the System

1. **Stop dashboard**: Press Ctrl+C in streamlit terminal
2. **Stop detector**: Press Ctrl+C in detect.py terminal  
3. **Stop producer**: Press Ctrl+C in kafka_producer.py terminal
4. **Stop Docker services**: `docker-compose down`

## File Structure

```
├── config.py           # Configuration settings
├── kafka_producer.py   # Binance data producer
├── detect.py          # Main anomaly detection
├── dashboard.py       # Streamlit dashboard
├── features.py        # Feature engineering
├── db_writer.py       # Data persistence
├── alerts.py          # Alert notifications
├── tune_model.py      # Model optimization
└── docker-compose.yml # Docker configuration
```

## Performance Monitoring

The system generates performance metrics in:
- `benchmark_summary.json` - Real-time stats
- `performance_log.csv` - Detailed metrics log
<img width="451" height="688" alt="image" src="https://github.com/user-attachments/assets/c89bd9df-2ff0-46eb-bb15-b7858fd8a1cb" />
