# Crypto Anomaly Detection System

Real-time cryptocurrency anomaly detection with multiple implementation approaches for different deployment scenarios and performance requirements.

## Project Structure

This repository contains three distinct implementations of the same anomaly detection system, each optimized for different use cases:

```
crypto-anomaly-detection/
├── baseline-code/          # Python-based single-machine implementation
├── spark-code/            # Apache Spark distributed processing
├── gke-code/             # Google Kubernetes Engine production deployment
└── README.md             # This overview
```

## Quick Start Guide

### 1. Baseline Implementation

**Key Features:**
- Single Python process
- SQLite database
- Real-time model retraining
- Streamlit dashboard
- Docker + Kafka setup

### 2. Spark Implementation  

**Key Features:**
- Distributed Spark Structured Streaming
- Windowed aggregations
- Parquet/BigQuery output
- Pre-trained model inference
- Cloud-ready architecture

### 3. GKE Cloud Implementation

**Key Features:**
- Kubernetes orchestration
- Confluent Cloud Kafka
- Auto-scaling infrastructure
- Production monitoring
- Zero-downtime deployments

## Prerequisites by Implementation

### Common Requirements
- Python 3.9+
- Docker and Docker Compose
- Git

### Additional Requirements

**Spark:**
- Apache Spark 3.4+
- PySpark libraries

**GKE:**
- Google Cloud Platform account
- `gcloud` CLI configured
- `kubectl` installed
- Confluent Cloud account (for managed Kafka)

## Getting Started

1. **Choose ab implementation** 
2. **Navigate to the appropriate folder**
3. **Follow the README.md** in that specific folder
4. **For GKE**: Also reference the `crypto-deploy.sh` script for automated deployment commands

## System Architecture

All implementations share the same core architecture:

```
Binance WebSocket → Kafka → Anomaly Detector → Storage → Dashboard
                             ↓
                         Alert System
```

**Components:**
- **Producer**: Streams live crypto trade data from Binance
- **Detector**: Applies ML-based anomaly detection algorithms
- **Storage**: Persists trades and detected anomalies
- **Dashboard**: Real-time visualization with Streamlit
- **Alerts**: Optional notifications via Telegram/Slack

**Supported Cryptocurrencies:**
- BTC/USDT, ETH/USDT, BNB/USDT, SOL/USDT, XRP/USDT

**Detection Methods:**
- Z-score statistical analysis
- Isolation Forest machine learning
- Combined filtered detection

## Documentation Structure

Each implementation folder contains:
- **README.md**: Complete setup and usage instructions
- **requirements.txt**: Python dependencies
- **config.py**: Centralized configuration
- **Source code**: Implementation-specific modules

**GKE Additional Files:**
- **crypto-deploy.sh**: Automated deployment script with comprehensive commands
- **Dockerfile**: Container build configuration
- **Kubernetes manifests**: Infrastructure as code

## Support and Troubleshooting

For implementation-specific issues:
1. Check the README.md in the relevant folder
2. Review configuration settings in config.py
3. For GKE: Use the built-in troubleshooting commands in crypto-deploy.sh

## Next Steps

1. **Start with Baseline** to understand the system fundamentals
2. **Progress to Spark** for distributed processing needs
3. **Finally with GKE** for fully clode deployed infrastructure

Each implementation builds upon the same core concepts while providing different deployment and scaling characteristics suitable for various operational requirements.
