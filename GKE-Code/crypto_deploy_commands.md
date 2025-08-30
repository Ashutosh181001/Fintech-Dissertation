# Crypto Anomaly Detection - Deployment Script Commands

## Overview

This document provides a comprehensive list of commands for the `crypto-deploy.sh` script, which handles the complete deployment and management of a crypto anomaly detection system on Kubernetes.

## Prerequisites

- `kubectl` installed and configured
- `docker` installed
- Active Kubernetes cluster connection
- Appropriate GCP permissions for the project

---

## 🚀 Deployment Commands

### Primary Deployment
```bash
./crypto-deploy.sh build-deploy
```
**Description**: Build new Docker images and deploy the complete system from scratch. This is the most comprehensive deployment option.

**Use when**: 
- First-time deployment
- Code changes require new images
- Complete system refresh needed

---

### Quick Deployment
```bash
./crypto-deploy.sh deploy
# OR
./crypto-deploy.sh start
```
**Description**: Deploy system using existing Docker images. Faster than `build-deploy` as it skips the image building process.

**Use when**: 
- Images are already built and available
- Quick restart after system stop
- Configuration-only changes

---

### Rolling Update
```bash
./crypto-deploy.sh update
```
**Description**: Perform rolling update of all components without rebuilding images. Zero-downtime deployment.

**Use when**: 
- System is running and needs restart
- Configuration changes applied
- Want to refresh without full rebuild

---

## 📊 Monitoring Commands

### System Status
```bash
./crypto-deploy.sh status
```
**Description**: Show comprehensive system status including:
- Deployment status
- Pod health and resource usage
- Service availability
- Storage information
- Performance metrics
- Quick anomaly detection statistics

**Output includes**:
- 🗂️ Deployments status
- 🚀 Pods status and locations
- 🌐 Services and networking
- 💾 Storage utilization
- ⚡ Performance mode indicator
- 📈 Real-time metrics (trades processed, anomalies detected)

---

### Live Logs
```bash
./crypto-deploy.sh logs
```
**Description**: Interactive log viewer with options:
1. All components (parallel)
2. Producer only
3. Detector only  
4. Dashboard only
5. Recent Kubernetes events

**Features**:
- Real-time log streaming
- Component-specific filtering
- Event troubleshooting

---

### Dashboard Access
```bash
./crypto-deploy.sh dashboard
```
**Description**: Creates port-forward to access the Streamlit dashboard locally.

**Access**: `http://localhost:8501`

**Features**:
- Real-time anomaly visualization
- Trading data analysis
- System performance metrics
- Interactive charts and graphs

---

### Data Download
```bash
./crypto-deploy.sh data
```
**Description**: Downloads performance and benchmark data from the running system.

**Downloads**:
- `trading_anomalies.db` - SQLite database with all data
- `performance_log.csv` - Detailed performance metrics
- `benchmark_summary.json` - System performance summary
- Creates timestamped backup directory
- Updates latest files for quick access

---

## 🔄 Management Commands

### System Restart
```bash
./crypto-deploy.sh restart
```
**Description**: Gracefully restart all system components by scaling down to 0 replicas, then back to 1.

**Process**:
1. Scale deployments to 0
2. Wait for pod termination
3. Scale back to 1 replica each
4. Wait for readiness
5. Show updated status

---

### System Stop
```bash
./crypto-deploy.sh stop
```
**Description**: Stop the system with data preservation options.

**Interactive options**:
1. **Keep all data** (recommended) - Preserves database and secrets
2. **Delete database only** - Removes storage but keeps Kafka credentials  
3. **Delete everything** - Complete cleanup including secrets

**Use when**: 
- Maintenance required
- Cost optimization (stopping compute)
- Troubleshooting storage issues

---

### Complete Cleanup
```bash
./crypto-deploy.sh clean
```
**Description**: Nuclear option - deletes the entire Kubernetes namespace and all associated resources.

**⚠️ Warning**: This removes ALL data, configurations, and secrets permanently.

**Use when**: 
- Project decommissioning
- Complete fresh start needed
- Development environment reset

---

### Help
```bash
./crypto-deploy.sh help
# OR
./crypto-deploy.sh
```
**Description**: Display comprehensive help information with usage examples and command descriptions.

---

## ⚡ Performance Mode

### Standard Mode (Default)
```bash
./crypto-deploy.sh [command]
```
**Resource allocation**:
- Producer: 200m-1000m CPU, 512Mi-2Gi RAM
- Detector: 500m-2000m CPU, 1Gi-4Gi RAM  
- Dashboard: 200m-1000m CPU, 512Mi-2Gi RAM

---

### High-Performance Mode
```bash
PERFORMANCE_MODE=high-performance ./crypto-deploy.sh [command]
```
**Resource allocation**:
- Producer: 1000m-4000m CPU, 1Gi-4Gi RAM
- Detector: 2000m-8000m CPU, 4Gi-16Gi RAM
- Dashboard: 500m-2000m CPU, 1Gi-4Gi RAM

**Use when**:
- High-volume trading data
- Real-time performance critical
- Sufficient cluster resources available

---

## 📋 Common Usage Patterns

### Initial Deployment
```bash
# Standard performance
./crypto-deploy.sh build-deploy

# High performance  
PERFORMANCE_MODE=high-performance ./crypto-deploy.sh build-deploy
```

### Daily Operations
```bash
# Check system health
./crypto-deploy.sh status

# View logs if issues
./crypto-deploy.sh logs

# Access dashboard
./crypto-deploy.sh dashboard
```

### Maintenance
```bash
# Download data backup
./crypto-deploy.sh data

# Restart system
./crypto-deploy.sh restart

# Apply updates
./crypto-deploy.sh update
```

### Development Cycle
```bash
# Deploy changes
./crypto-deploy.sh build-deploy

# Test and monitor
./crypto-deploy.sh status
./crypto-deploy.sh logs

# Download results
./crypto-deploy.sh data
```

---

## 🔧 Configuration Details

### Project Configuration
- **Project ID**: `crypto-anomaly-detection`
- **Region**: `europe-west2`
- **Cluster**: `crypto-realtime`
- **Namespace**: `crypto`
- **Registry**: `europe-west2-docker.pkg.dev/crypto-anomaly-detection/crypto`

### Kafka Configuration
- **Bootstrap Server**: `pkc-l6wr6.europe-west2.gcp.confluent.cloud:9092`
- **Topic**: `crypto_trades`
- **Consumer Group**: `anomaly-detector`
- **Security**: SASL_SSL with PLAIN mechanism

---

## 🚨 Troubleshooting

### System Not Starting
1. Check prerequisites: `./crypto-deploy.sh help`
2. Verify cluster connection: `kubectl cluster-info`
3. Check resource availability: `kubectl describe nodes`
4. View events: `./crypto-deploy.sh logs` → option 5

### Performance Issues
1. Check current performance mode: `./crypto-deploy.sh status`
2. Switch to high-performance mode if needed
3. Monitor resource usage in status output
4. Download performance data for analysis

### Data Access Issues
1. Verify pod status: `./crypto-deploy.sh status`
2. Check storage: `kubectl get pvc -n crypto`
3. Test database access through logs
4. Download data to verify integrity

---

## 📞 Support Information

For issues with the deployment script:
1. Run `./crypto-deploy.sh status` for system health
2. Use `./crypto-deploy.sh logs` for detailed troubleshooting
3. Check Kubernetes events for infrastructure issues
4. Verify GCP permissions and quotas

---

*Last updated: August 2025*