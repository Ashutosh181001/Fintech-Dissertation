# GKE Crypto Anomaly Detection System

Production-ready cryptocurrency anomaly detection system deployed on Google Kubernetes Engine with Confluent Cloud Kafka integration.

## Architecture Overview

- **Producer**: Binance WebSocket → Confluent Cloud Kafka
- **Detector**: Kafka Consumer → SQLite → Real-time anomaly detection  
- **Dashboard**: Streamlit visualization with live data
- **Infrastructure**: GKE Autopilot with persistent storage
- **Monitoring**: Comprehensive performance metrics and alerting

## Prerequisites

### Local Tools
- `gcloud` CLI configured with appropriate project access
- `kubectl` installed
- Docker with buildx support
- Git

### GCP Project Setup
- Active GCP project with billing enabled
- Required APIs enabled (detailed in setup steps)
- Sufficient quotas for GKE Autopilot

## Part 1: GCP Infrastructure Setup

### 1.1 Project Configuration

```bash
# Set project variables
export PROJECT_ID="crypto-anomaly-detection"
export REGION="europe-west2"
export CLUSTER_NAME="crypto-realtime"

# Configure gcloud
gcloud config set project $PROJECT_ID
gcloud config set compute/region $REGION
```

### 1.2 Enable Required APIs

```bash
# Enable all necessary GCP APIs
gcloud services enable \
  container.googleapis.com \
  artifactregistry.googleapis.com \
  compute.googleapis.com \
  cloudbuild.googleapis.com \
  secretmanager.googleapis.com \
  monitoring.googleapis.com
```

**APIs Explained:**
- `container.googleapis.com`: GKE cluster management
- `artifactregistry.googleapis.com`: Container image registry
- `compute.googleapis.com`: Compute resources for nodes
- `cloudbuild.googleapis.com`: Automated image building
- `secretmanager.googleapis.com`: Secure credential storage
- `monitoring.googleapis.com`: System monitoring and alerting

### 1.3 Create Artifact Registry

```bash
# Create container registry for images
gcloud artifacts repositories create crypto \
  --repository-format=docker \
  --location=$REGION \
  --description="Crypto anomaly detection containers"

# Configure Docker authentication
gcloud auth configure-docker ${REGION}-docker.pkg.dev
```

**Registry Structure:**
```
europe-west2-docker.pkg.dev/crypto-anomaly-detection/crypto/
├── crypto-producer:latest
├── crypto-detector:latest
└── crypto-dashboard:latest
```

### 1.4 Create GKE Autopilot Cluster

```bash
# Create optimized Autopilot cluster
gcloud container clusters create-auto $CLUSTER_NAME \
  --region=$REGION \
  --release-channel=regular \
  --enable-network-policy \
  --enable-autorepair \
  --enable-autoupgrade \
  --disk-type=pd-ssd \
  --cluster-version="1.27"

# Get cluster credentials
gcloud container clusters get-credentials $CLUSTER_NAME --region=$REGION
```

**Autopilot Benefits:**
- Fully managed nodes with automatic scaling
- Built-in security and compliance features
- Optimized resource allocation
- No node maintenance required

## Part 2: Confluent Cloud Kafka Setup

### 2.1 Create Confluent Cloud Account
1. Visit [Confluent Cloud](https://confluent.cloud)
2. Create account and select Basic cluster in `europe-west2`
3. Note the bootstrap server endpoint

### 2.2 Configure Kafka Resources

**Create Topic:**
```bash
# Via Confluent Cloud Console or CLI
Topic: crypto_trades
Partitions: 6
Retention: 7 days
Cleanup Policy: delete
```

**Create API Keys:**
1. Navigate to API Keys in Confluent Cloud Console
2. Create cluster-scoped API key
3. Save the Key and Secret securely

**Example Configuration Values:**
```
Bootstrap Server: pkc-l6wr6.europe-west2.gcp.confluent.cloud:9092  
Username: TID2HZFAIXB5IWN5
Password: cflt3LaBgYbfB9Qaby4f7A3shWgqYFe6vps2TuDY8RIVFYm/xuNvXOGqIg63voxg
Security Protocol: SASL_SSL
SASL Mechanism: PLAIN
```

### 2.3 Network Configuration
- Ensure GKE cluster can reach Confluent Cloud (public internet access)
- No additional firewall rules needed for Autopilot
- Confluent Cloud handles TLS/SSL termination

## Part 3: Deployment Automation

### 3.1 Deploy Script Overview

The `crypto-deploy.sh` script provides complete deployment automation:

**Core Functions:**
- Image building with multi-platform support
- Kubernetes manifest generation with resource limits  
- Secret management for Kafka credentials
- Rolling deployments with zero downtime
- Comprehensive monitoring and data extraction

### 3.2 Resource Specifications

**Standard Mode (Default):**
```yaml
Producer:  200m-1000m CPU, 512Mi-2Gi RAM
Detector:  500m-2000m CPU, 1Gi-4Gi RAM  
Dashboard: 200m-1000m CPU, 512Mi-2Gi RAM
```

**High-Performance Mode:**
```yaml
Producer:  1000m-4000m CPU, 1Gi-4Gi RAM
Detector:  2000m-8000m CPU, 4Gi-16Gi RAM
Dashboard: 500m-2000m CPU, 1Gi-4Gi RAM
```

### 3.3 Storage Configuration

**Persistent Volume:**
- **Type**: `premium-rwo` (SSD-backed)
- **Size**: 20Gi
- **Access**: ReadWriteOnce
- **Mount**: `/data` in detector and dashboard pods

**Data Structure:**
```
/data/output/
├── trading_anomalies.db    # SQLite database
├── trades.csv             # Trade backup
├── anomalies.csv          # Anomaly backup  
├── performance_log.csv    # Metrics
└── benchmark_summary.json # Real-time stats
```

## Part 4: Complete Deployment Process

### 4.1 Initial Deployment

```bash
# Make script executable
chmod +x crypto-deploy.sh

# Standard deployment
./crypto-deploy.sh build-deploy

# High-performance deployment
PERFORMANCE_MODE=high-performance ./crypto-deploy.sh build-deploy
```

**Build Process:**
1. **Multi-platform images** built with `docker buildx`
2. **Automatic tagging** with timestamp and `:latest`
3. **Registry push** to Artifact Registry
4. **Manifest generation** with dynamic resource limits
5. **Secret creation** for Kafka credentials
6. **Rolling deployment** with health checks

### 4.2 Deployment Verification

```bash
# Check system status
./crypto-deploy.sh status

# View real-time logs
./crypto-deploy.sh logs

# Access dashboard
./crypto-deploy.sh dashboard
# Opens port-forward to http://localhost:8501
```

**Expected Output:**
- All pods in `Running` state with `1/1` ready
- Producer connected to Binance WebSocket
- Detector processing trades with anomaly detection
- Dashboard displaying live data

## Part 5: Configuration Details

### 5.1 Kafka Integration

**confluent_kafka Library:**
- Native librdkafka client for high performance
- Built-in SASL/SSL support for Confluent Cloud
- Automatic reconnection and error handling
- Optimized for cloud-native deployments

**Connection Configuration:**
```python
{
    "bootstrap.servers": "pkc-l6wr6.europe-west2.gcp.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "API_KEY",
    "sasl.password": "API_SECRET",
    "ssl.ca.location": "/path/to/ca-certificates.crt"
}
```

### 5.2 Application Configuration

**Multi-Symbol Support:**
- BTC/USDT, ETH/USDT, BNB/USDT, SOL/USDT, XRP/USDT
- Per-symbol anomaly detection models
- Independent cooldown periods per symbol
- Symbol-specific alerting thresholds

**Anomaly Detection Methods:**
1. **Z-Score**: Statistical deviation detection (threshold: 3.5σ)
2. **Isolation Forest**: ML-based outlier detection  
3. **Filtered**: Combined approach requiring both methods

### 5.3 Monitoring and Metrics

**Performance Tracking:**
```json
{
  "total_trades": 571734,
  "total_anomalies": 1907,
  "detection_rate_pct": 0.333547,
  "avg_processing_time_sec": 0.018293,
  "e2e_p95_ms": 616707976.932,
  "memory_mb": 563.16,
  "cpu_pct": 12.0
}
```

**Key Metrics:**
- End-to-end latency percentiles (P50, P95, P99)
- Per-symbol processing rates
- Kafka consumer lag monitoring
- Resource utilization tracking
- Model retraining frequency

## Part 6: Production Operations

### 6.1 Daily Operations

```bash
# Health check
./crypto-deploy.sh status

# Download performance data  
./crypto-deploy.sh data

# View anomaly patterns
./crypto-deploy.sh dashboard
```

### 6.2 Maintenance

```bash
# Rolling restart (zero downtime)
./crypto-deploy.sh restart

# Update with latest images
./crypto-deploy.sh update

# Performance tuning
PERFORMANCE_MODE=high-performance ./crypto-deploy.sh update
```

### 6.3 Troubleshooting

**Common Issues:**

**Kafka Connection Failures:**
```bash
# Check credentials in secret
kubectl get secret kafka-credentials -n crypto -o yaml

# Verify network connectivity
kubectl exec deployment/producer -n crypto -- nslookup pkc-l6wr6.europe-west2.gcp.confluent.cloud
```

**Pod Resource Constraints:**
```bash
# Check resource usage
kubectl top pods -n crypto

# Scale to high-performance mode
PERFORMANCE_MODE=high-performance ./crypto-deploy.sh update
```

**Data Issues:**
```bash
# Verify persistent volume
kubectl get pvc -n crypto

# Check database integrity
kubectl exec deployment/detector -n crypto -- sqlite3 /data/output/trading_anomalies.db ".schema"
```

## Part 7: Advanced Configuration

### 7.1 Custom Resource Limits

Edit resource specifications in `crypto-deploy.sh`:

```bash
# Modify get_resource_limits() function
DETECTOR_CPU_REQUEST="4000m"    # 4 cores
DETECTOR_CPU_LIMIT="8000m"      # 8 cores  
DETECTOR_MEM_REQUEST="8Gi"      # 8GB
DETECTOR_MEM_LIMIT="16Gi"       # 16GB
```


### 7.2 Scaling Configuration

**Horizontal Scaling:**
```yaml
# Modify replica count in manifests
spec:
  replicas: 3  # Multiple detector instances
```

**Vertical Scaling:**
```bash
# Use high-performance mode
PERFORMANCE_MODE=high-performance ./crypto-deploy.sh deploy
```

## 8 External Dependencies

**Required Services:**
1. **Binance API**: Real-time market data source
2. **Confluent Cloud**: Managed Kafka infrastructure
3. **GKE Autopilot**: Container orchestration platform

**Service Reliability:**
- Multiple availability zones for GKE
- Confluent Cloud SLA: 99.95% uptime
- Binance WebSocket: Built-in reconnection logic
<img width="451" height="690" alt="image" src="https://github.com/user-attachments/assets/7e235c5a-3884-4e65-90d5-812a74f8d83a" />
