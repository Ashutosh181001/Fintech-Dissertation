# Spark-based Crypto Anomaly Detection System

A distributed real-time cryptocurrency anomaly detection pipeline using Apache Spark Structured Streaming. Monitors multiple trading pairs with scalable parallel processing and advanced windowing operations.

## Prerequisites

- Python 3.9+
- Apache Spark 3.4+ with PySpark
- Docker and Docker Compose
- Git

**Common setup steps**: Refer to the baseline implementation README for Docker/Kafka setup, as these steps are identical.

## Installation

1. **Install additional Spark dependencies**
   ```bash
   pip install pyspark findspark
   # Optional for cloud storage
   pip install gcsfs google-cloud-bigquery google-cloud-storage
   ```

2. **Create Spark-specific directories**
   ```bash
   mkdir -p output models logs
   mkdir -p output/trades output/anomalies
   ```

## Configuration

Edit `config.py` - all settings are config-only (no environment variables):

```python
# Core settings
OUTPUT_MODE = "local"  # local | gcs | bigquery
SUPPORTED_SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "ADA/USDT"]

# Per-symbol thresholds
ANOMALY_CONFIG = {
    "BTC/USDT": {"z_score_threshold": 3.5, "volume_spike_ratio": 3.0},
    "ETH/USDT": {"z_score_threshold": 3.8, "volume_spike_ratio": 3.5},
    # ... customize per symbol
}
```

## Running the Spark System

### Step 1-2: Docker and Producer Setup

Follow Steps 1-2 from the baseline implementation README (Docker + `kafka_producer.py`).

### Step 3: Start Spark Anomaly Detection

```bash
python main_detect_spark.py
```

**Expected output:**
- Spark job initialization with timezone (UTC)
- Streaming query startup messages
- Real-time processing statistics
- Console anomaly previews (if enabled)

**Key features:**
- Windowed aggregations (60s duration, 20s slide)
- Per-symbol detection thresholds
- Parquet output with partitioning
- Isolation Forest + rule-based detection

### Step 4: Optional Alert Dispatcher

For notifications from stored anomalies:
```bash
python alert_dispatcher.py
```

This polls `./output/anomalies/` and sends alerts via configured channels.

### Step 5: View Dashboard

```bash
streamlit run dashboard.py
```

Dashboard opens at: http://localhost:8501 (same interface as baseline system).

## Spark-specific Features

### Advanced Detection Pipeline

- **Filtered Detection**: Requires both z-score AND Isolation Forest agreement
- **Per-symbol Thresholds**: Different sensitivity per trading pair
- **Windowed Features**: Rolling statistics computed via Spark windows
- **Quality Scoring**: High/medium/low confidence levels

### Model Management

**Pre-trained Models** (optional):
```bash
# Place models in ./models/ directory
models/
├── BTC_USDT.pkl
├── ETH_USDT.pkl
└── _default_.pkl
```

If no models exist, system automatically uses dummy models for development.

### Output Structure

```
output/
├── trades/
│   ├── hour=2025-08-30-14/
│   └── part-*.parquet
├── anomalies/
│   ├── hour=2025-08-30-14/
│   └── part-*.parquet
└── _checkpoints/
    ├── trades/
    └── anomalies/
```

### Spark Configuration

**Memory settings** (if needed):
```bash
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=4g
export SPARK_DRIVER_MAX_RESULT_SIZE=2g
```

**Cluster mode** (optional):
```bash
spark-submit --master spark://master:7077 main_detect_spark.py
```

## Monitoring

### Spark Web UI
Visit http://localhost:4040 during execution for:
- Streaming query metrics
- Job execution details
- Memory usage and performance

### Query Progress Logs
```
🔊 trades — batch=123, in=45.2 r/s, proc=44.8 r/s
🔊 anomalies — batch=123, in=2.1 r/s, proc=2.1 r/s
```

## Troubleshooting

### Spark-specific Issues

**Out of Memory Errors:**
```bash
# Reduce window size
WINDOWS = {"duration": "30 seconds", "slide": "10 seconds"}
# Or increase memory allocation
export SPARK_DRIVER_MEMORY=8g
```

**Checkpoint Recovery Issues:**
```bash
# Clear checkpoints to reset state
rm -rf ./output/*/_checkpoints
```

**Model Loading Failures:**
```bash
# System will log warnings and use dummy models
# Check models directory permissions and file formats
ls -la ./models/
```

**Slow Processing:**
```bash
# Reduce input rate in config.py
KAFKA_CONFIG = {"max_offsets_per_trigger": "500"}
```

**Parquet Write Failures:**
```bash
# Check output directory permissions
chmod 755 ./output/
# Or switch to single file mode
STREAM_SINGLE_FILE = True
```


## Performance Tuning

### Throughput Optimization
```python
# config.py adjustments
KAFKA_CONFIG = {
    "max_offsets_per_trigger": "2000",  # Higher batch sizes
    "num_partitions": 6,                # More parallelism
}

WINDOWS = {
    "duration": "120 seconds",          # Larger windows
    "slide": "30 seconds",              # Less frequent updates
}
```

### Resource Management
```python
# Reduce state size
PROCESSING_TRIGGER = "30 seconds"      # Less frequent processing
WATERMARK_DELAY = "60 seconds"         # More aggressive state cleanup
```

## Stopping the System

1. **Stop dashboard**: Ctrl+C in streamlit terminal
2. **Stop alert dispatcher**: Ctrl+C in alert_dispatcher.py terminal
3. **Stop Spark job**: Ctrl+C in main_detect_spark.py terminal (graceful shutdown)
4. **Stop producer**: Ctrl+C in kafka_producer.py terminal
5. **Stop Docker**: `docker-compose down`

**Clean shutdown sequence ensures:**
- Streaming queries stop gracefully
- Final micro-batches are processed
- Checkpoints are properly saved

## Cloud Deployment

### GCS Output
```python
OUTPUT_MODE = "gcs"
GCS_BUCKET = "your-crypto-bucket"
```

### BigQuery Output
```python
OUTPUT_MODE = "bigquery"  
GCP_PROJECT = "your-project"
BQ_DATASET = "crypto_anomalies"
```
