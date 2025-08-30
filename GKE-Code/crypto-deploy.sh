#!/bin/bash
# =============================================================================
# ULTIMATE CRYPTO ANOMALY DETECTION DEPLOYMENT SCRIPT
# =============================================================================
# One script to rule them all! Handles everything from building to monitoring.
#
# Usage:
#   ./crypto.sh build-deploy    # Build new images and deploy
#   ./crypto.sh deploy          # Deploy with existing images
#   ./crypto.sh start           # Alias for deploy
#   ./crypto.sh stop            # Stop system (with data options)
#   ./crypto.sh restart         # Restart all components
#   ./crypto.sh status          # Show detailed system status
#   ./crypto.sh dashboard       # Open dashboard port-forward
#   ./crypto.sh logs            # Show live logs from all components
#   ./crypto.sh update          # Update images without full rebuild
#   ./crypto.sh data            # Download performance/benchmark data
#   ./crypto.sh clean           # Complete cleanup
#   ./crypto.sh help            # Show help
# =============================================================================

set -e

# =============================================================================
# CONFIGURATION
# =============================================================================
PROJECT_ID="crypto-anomaly-detection"
REGION="europe-west2"
CLUSTER_NAME="crypto-realtime"
NAMESPACE="crypto"
REGISTRY="europe-west2-docker.pkg.dev/${PROJECT_ID}/crypto"

# Kafka Credentials
KAFKA_BOOTSTRAP="pkc-l6wr6.europe-west2.gcp.confluent.cloud:9092"
KAFKA_USERNAME="TID2HZFAIXB5IWN5"
KAFKA_PASSWORD="cflt3LaBgYbfB9Qaby4f7A3shWgqYFe6vps2TuDY8RIVFYm/xuNvXOGqIg63voxg"

# Default resource settings (can be overridden)
PERFORMANCE_MODE=${PERFORMANCE_MODE:-"standard"}  # "standard" or "high-performance"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
log() {
    echo "🚀 $(date '+%H:%M:%S') - $1"
}

error() {
    echo "❌ $(date '+%H:%M:%S') - ERROR: $1" >&2
    exit 1
}

success() {
    echo "✅ $(date '+%H:%M:%S') - $1"
}

check_prerequisites() {
    log "Checking prerequisites..."

    if ! command -v kubectl &> /dev/null; then
        error "kubectl is not installed"
    fi

    if ! command -v docker &> /dev/null; then
        error "docker is not installed"
    fi

    if ! kubectl cluster-info >/dev/null 2>&1; then
        error "Cannot connect to Kubernetes cluster"
    fi

    success "Prerequisites check passed"
}

wait_for_deployment() {
    local deployment=$1
    local timeout=${2:-300}

    log "Waiting for $deployment to be ready..."
    if kubectl wait --for=condition=available --timeout=${timeout}s deployment/$deployment -n $NAMESPACE >/dev/null 2>&1; then
        success "$deployment is ready!"
    else
        error "$deployment failed to become ready"
    fi
}

get_resource_limits() {
    if [[ "$PERFORMANCE_MODE" == "high-performance" ]]; then
        echo "High-performance mode enabled"
        PRODUCER_CPU_REQUEST="1000m"
        PRODUCER_CPU_LIMIT="4000m"
        PRODUCER_MEM_REQUEST="1Gi"
        PRODUCER_MEM_LIMIT="4Gi"

        DETECTOR_CPU_REQUEST="2000m"
        DETECTOR_CPU_LIMIT="8000m"
        DETECTOR_MEM_REQUEST="4Gi"
        DETECTOR_MEM_LIMIT="16Gi"

        DASHBOARD_CPU_REQUEST="500m"
        DASHBOARD_CPU_LIMIT="2000m"
        DASHBOARD_MEM_REQUEST="1Gi"
        DASHBOARD_MEM_LIMIT="4Gi"
    else
        echo "Standard performance mode"
        PRODUCER_CPU_REQUEST="200m"
        PRODUCER_CPU_LIMIT="1000m"
        PRODUCER_MEM_REQUEST="512Mi"
        PRODUCER_MEM_LIMIT="2Gi"

        DETECTOR_CPU_REQUEST="500m"
        DETECTOR_CPU_LIMIT="2000m"
        DETECTOR_MEM_REQUEST="1Gi"
        DETECTOR_MEM_LIMIT="4Gi"

        DASHBOARD_CPU_REQUEST="200m"
        DASHBOARD_CPU_LIMIT="1000m"
        DASHBOARD_MEM_REQUEST="512Mi"
        DASHBOARD_MEM_LIMIT="2Gi"
    fi
}

# =============================================================================
# BUILD IMAGES
# =============================================================================
build_images() {
    log "🔨 Building Docker images..."

    # Generate version
    VERSION=$(date +"%Y%m%d-%H%M%S")
    log "Building with version: $VERSION"

    # Build all images with both version tag and latest tag
    log "Building producer image..."
    docker buildx build --platform linux/amd64 \
        -t ${REGISTRY}/crypto-producer:${VERSION} \
        -t ${REGISTRY}/crypto-producer:latest \
        --push . || error "Failed to build producer image"

    log "Building detector image..."
    docker buildx build --platform linux/amd64 \
        -t ${REGISTRY}/crypto-detector:${VERSION} \
        -t ${REGISTRY}/crypto-detector:latest \
        --push . || error "Failed to build detector image"

    log "Building dashboard image..."
    docker buildx build --platform linux/amd64 \
        -t ${REGISTRY}/crypto-dashboard:${VERSION} \
        -t ${REGISTRY}/crypto-dashboard:latest \
        --push . || error "Failed to build dashboard image"

    # Save version for reference
    echo "$VERSION" > .last-version
    success "Images built successfully with version: $VERSION"
}

# =============================================================================
# CREATE KUBERNETES MANIFESTS
# =============================================================================
create_manifests() {
    log "Creating Kubernetes manifests..."

    get_resource_limits

    cat > crypto-deployment.yaml << EOF
# Namespace
apiVersion: v1
kind: Namespace
metadata:
  name: crypto
---
# Storage
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: crypto-storage
  namespace: crypto
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 20Gi
  storageClassName: premium-rwo
---
# Producer Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: producer
  namespace: crypto
  labels:
    app: producer
    tier: ingestion
spec:
  replicas: 1
  selector:
    matchLabels:
      app: producer
  template:
    metadata:
      labels:
        app: producer
        tier: ingestion
    spec:
      containers:
      - name: producer
        image: ${REGISTRY}/crypto-producer:latest
        imagePullPolicy: Always
        command: ["python", "kafka_producer.py"]
        envFrom:
        - secretRef:
            name: kafka-credentials
        env:
        - name: ENV_MODE
          value: "GKE"
        - name: REGION
          value: "${REGION}"
        - name: PERFORMANCE_MODE
          value: "${PERFORMANCE_MODE}"
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        resources:
          requests:
            cpu: ${PRODUCER_CPU_REQUEST}
            memory: ${PRODUCER_MEM_REQUEST}
          limits:
            cpu: ${PRODUCER_CPU_LIMIT}
            memory: ${PRODUCER_MEM_LIMIT}
        livenessProbe:
          exec:
            command:
              - /bin/sh
              - -c
              - "test -f kafka_producer.py && test -f /proc/1/cmdline"
          initialDelaySeconds: 30
          periodSeconds: 30
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          exec:
            command:
              - /bin/sh
              - -c
              - "test -f kafka_producer.py"
          initialDelaySeconds: 10
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
---
# Detector Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: detector
  namespace: crypto
  labels:
    app: detector
    tier: processing
spec:
  replicas: 1
  selector:
    matchLabels:
      app: detector
  template:
    metadata:
      labels:
        app: detector
        tier: processing
    spec:
      initContainers:
      - name: init-data-dir
        image: busybox:1.36
        command: ["sh", "-c", "mkdir -p /data/output && chmod -R 777 /data"]
        volumeMounts:
        - name: storage
          mountPath: /data
        resources:
          requests:
            cpu: 100m
            memory: 128Mi
      containers:
      - name: detector
        image: ${REGISTRY}/crypto-detector:latest
        imagePullPolicy: Always
        command: ["python", "detect.py"]
        envFrom:
        - secretRef:
            name: kafka-credentials
        env:
        - name: DATABASE_PATH
          value: "/data/output/trading_anomalies.db"
        - name: TRADES_CSV
          value: "/data/output/trades.csv"
        - name: ANOMALIES_CSV
          value: "/data/output/anomalies.csv"
        - name: ENV_MODE
          value: "GKE"
        - name: REGION
          value: "${REGION}"
        - name: PERFORMANCE_MODE
          value: "${PERFORMANCE_MODE}"
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        volumeMounts:
        - name: storage
          mountPath: /data
        resources:
          requests:
            cpu: ${DETECTOR_CPU_REQUEST}
            memory: ${DETECTOR_MEM_REQUEST}
          limits:
            cpu: ${DETECTOR_CPU_LIMIT}
            memory: ${DETECTOR_MEM_LIMIT}
        livenessProbe:
          exec:
            command:
              - /bin/sh
              - -c
              - "test -f detect.py && test -f /proc/1/cmdline"
          initialDelaySeconds: 60
          periodSeconds: 30
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          exec:
            command:
              - /bin/sh
              - -c
              - "test -f detect.py"
          initialDelaySeconds: 30
          periodSeconds: 15
          timeoutSeconds: 5
          failureThreshold: 3
      volumes:
      - name: storage
        persistentVolumeClaim:
          claimName: crypto-storage
---
# Dashboard Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dashboard
  namespace: crypto
  labels:
    app: dashboard
    tier: visualization
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dashboard
  template:
    metadata:
      labels:
        app: dashboard
        tier: visualization
    spec:
      containers:
      - name: dashboard
        image: ${REGISTRY}/crypto-dashboard:latest
        imagePullPolicy: Always
        command: ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
        ports:
        - containerPort: 8501
          name: http
        volumeMounts:
        - name: storage
          mountPath: /data
        env:
        - name: ENV_MODE
          value: "GKE"
        - name: REGION
          value: "${REGION}"
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        resources:
          requests:
            cpu: ${DASHBOARD_CPU_REQUEST}
            memory: ${DASHBOARD_MEM_REQUEST}
          limits:
            cpu: ${DASHBOARD_CPU_LIMIT}
            memory: ${DASHBOARD_MEM_LIMIT}
        livenessProbe:
          httpGet:
            path: /
            port: 8501
          initialDelaySeconds: 60
          periodSeconds: 30
          timeoutSeconds: 10
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /
            port: 8501
          initialDelaySeconds: 30
          periodSeconds: 15
          timeoutSeconds: 10
          failureThreshold: 3
      volumes:
      - name: storage
        persistentVolumeClaim:
          claimName: crypto-storage
---
# Dashboard Service
apiVersion: v1
kind: Service
metadata:
  name: dashboard
  namespace: crypto
  labels:
    app: dashboard
spec:
  selector:
    app: dashboard
  ports:
  - port: 8501
    targetPort: 8501
    name: http
  type: ClusterIP
EOF

    success "Manifests created"
}

# =============================================================================
# DEPLOY SYSTEM
# =============================================================================
deploy_system() {
    local skip_build=${1:-false}

    if [[ "$skip_build" == "false" ]]; then
        build_images
    fi

    log "🚀 Deploying crypto anomaly detection system..."

    create_manifests

    # Create namespace
    kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

    # Create/update Kafka secret
    log "Setting up Kafka credentials..."
    kubectl delete secret kafka-credentials -n $NAMESPACE 2>/dev/null || true
    kubectl create secret generic kafka-credentials -n $NAMESPACE \
        --from-literal=KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" \
        --from-literal=KAFKA_USERNAME="$KAFKA_USERNAME" \
        --from-literal=KAFKA_PASSWORD="$KAFKA_PASSWORD" \
        --from-literal=KAFKA_TOPIC_TRADES="crypto_trades" \
        --from-literal=KAFKA_CONSUMER_GROUP="anomaly-detector" \
        --from-literal=KAFKA_SECURITY_PROTOCOL="SASL_SSL" \
        --from-literal=KAFKA_SASL_MECHANISM="PLAIN"

    # Check if deployments exist
    if kubectl get deployment producer -n $NAMESPACE >/dev/null 2>&1; then
        log "🔄 System exists - performing rolling update..."
        kubectl apply -f crypto-deployment.yaml
        kubectl rollout restart deployment/producer deployment/detector deployment/dashboard -n $NAMESPACE
    else
        log "🆕 Fresh deployment..."
        kubectl apply -f crypto-deployment.yaml
    fi

    # Wait for deployments
    log "⏳ Waiting for deployments to be ready..."
    wait_for_deployment "producer" 300
    wait_for_deployment "detector" 300
    wait_for_deployment "dashboard" 300

    success "System deployed successfully!"
    show_status
    show_access_info
}

# =============================================================================
# STATUS AND MONITORING
# =============================================================================
show_status() {
    echo ""
    echo "📊 SYSTEM STATUS"
    echo "================"

    # Deployments
    echo ""
    echo "🗂️ DEPLOYMENTS:"
    kubectl get deployments -n $NAMESPACE 2>/dev/null || echo "No deployments found"

    # Pods
    echo ""
    echo "🚀 PODS:"
    kubectl get pods -n $NAMESPACE -o wide 2>/dev/null || echo "No pods found"

    # Services
    echo ""
    echo "🌐 SERVICES:"
    kubectl get services -n $NAMESPACE 2>/dev/null || echo "No services found"

    # Storage
    echo ""
    echo "💾 STORAGE:"
    kubectl get pvc -n $NAMESPACE 2>/dev/null || echo "No storage found"

    # Performance mode
    echo ""
    echo "⚡ PERFORMANCE MODE: $PERFORMANCE_MODE"

    # System health
    echo ""
    if kubectl get pods -n $NAMESPACE 2>/dev/null | grep -q "Running.*1/1"; then
        echo "✅ SYSTEM STATUS: HEALTHY"
        show_quick_metrics
    else
        echo "⚠️ SYSTEM STATUS: STARTING/ISSUES"
        echo ""
        echo "🔍 Recent events:"
        kubectl get events -n $NAMESPACE --sort-by='.lastTimestamp' | tail -5 2>/dev/null || true
    fi
    echo ""
}

show_quick_metrics() {
    if kubectl get pods -n $NAMESPACE -l app=detector 2>/dev/null | grep -q "Running"; then
        echo ""
        echo "📈 QUICK METRICS:"
        kubectl exec deployment/detector -n $NAMESPACE -- python3 -c "
import sqlite3
import os
import json
if os.path.exists('/data/output/trading_anomalies.db'):
    conn = sqlite3.connect('/data/output/trading_anomalies.db')
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM trades')
    trades = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM anomalies')
    anomalies = cursor.fetchone()[0]
    print(f'  📊 Trades Processed: {trades:,}')
    print(f'  🚨 Anomalies Detected: {anomalies:,}')
    if trades > 0:
        rate = (anomalies/trades)*100
        print(f'  📈 Detection Rate: {rate:.2f}%')

    # Check for symbols
    cursor.execute('SELECT symbol, COUNT(*) FROM trades GROUP BY symbol ORDER BY COUNT(*) DESC LIMIT 5')
    symbols = cursor.fetchall()
    if symbols:
        print(f'  📋 Active Symbols:')
        for symbol, count in symbols:
            print(f'    - {symbol}: {count:,} trades')

    conn.close()

    # Check benchmark file
    if os.path.exists('/data/output/benchmark_summary.json'):
        with open('/data/output/benchmark_summary.json', 'r') as f:
            summary = json.load(f)
        print(f'  ⚡ Avg Processing: {summary.get(\"avg_processing_time_sec\", 0):.4f}s')
        print(f'  💾 Memory Usage: {summary.get(\"memory_mb\", 0):.1f} MB')
else:
    print('  📊 Database initializing...')
" 2>/dev/null || echo "  📊 Metrics not available yet"
    fi
}

show_access_info() {
    echo ""
    echo "🎯 ACCESS INFORMATION"
    echo "===================="
    echo ""
    echo "📊 Dashboard:"
    echo "   kubectl port-forward service/dashboard 8501:8501 -n $NAMESPACE"
    echo "   Then open: http://localhost:8501"
    echo ""
    echo "📈 Quick commands:"
    echo "   ./crypto.sh status      # Check system status"
    echo "   ./crypto.sh logs        # View live logs"
    echo "   ./crypto.sh dashboard   # Open dashboard"
    echo "   ./crypto.sh data        # Download performance data"
    echo ""
}

# =============================================================================
# LOGS
# =============================================================================
show_logs() {
    echo "📋 Select component to view logs:"
    echo "1) All (parallel)"
    echo "2) Producer only"
    echo "3) Detector only"
    echo "4) Dashboard only"
    echo "5) Recent events"

    read -p "Choice [1-5]: " choice

    case $choice in
        1)
            log "Opening logs for all components (Ctrl+C to exit)..."
            kubectl logs -f deployment/producer -n $NAMESPACE &
            kubectl logs -f deployment/detector -n $NAMESPACE &
            kubectl logs -f deployment/dashboard -n $NAMESPACE &
            wait
            ;;
        2)
            kubectl logs -f deployment/producer -n $NAMESPACE --tail=50
            ;;
        3)
            kubectl logs -f deployment/detector -n $NAMESPACE --tail=50
            ;;
        4)
            kubectl logs -f deployment/dashboard -n $NAMESPACE --tail=50
            ;;
        5)
            kubectl get events -n $NAMESPACE --sort-by='.lastTimestamp' | tail -20
            ;;
        *)
            echo "Invalid choice"
            ;;
    esac
}

# =============================================================================
# DATA MANAGEMENT
# =============================================================================
download_data() {
    log "📊 Downloading performance and benchmark data..."

    # Get detector pod name
    DETECTOR_POD=$(kubectl get pods -n $NAMESPACE -l app=detector -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

    if [[ -z "$DETECTOR_POD" ]]; then
        error "No detector pod found. Is the system running?"
    fi

    # Create timestamp folder
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    BACKUP_DIR="data_backup_${TIMESTAMP}"
    mkdir -p "$BACKUP_DIR"

    log "Downloading to: $BACKUP_DIR/"

    # Download files
    kubectl cp $NAMESPACE/$DETECTOR_POD:/data/output/trading_anomalies.db $BACKUP_DIR/trading_anomalies.db 2>/dev/null || log "Database not found"
    kubectl cp $NAMESPACE/$DETECTOR_POD:/data/output/performance_log.csv $BACKUP_DIR/performance_log.csv 2>/dev/null || log "Performance log not found"
    kubectl cp $NAMESPACE/$DETECTOR_POD:/data/output/benchmark_summary.json $BACKUP_DIR/benchmark_summary.json 2>/dev/null || log "Benchmark summary not found"

    # Also download to current directory with latest names
    kubectl cp $NAMESPACE/$DETECTOR_POD:/data/output/performance_log.csv ./performance_log_latest.csv 2>/dev/null || true
    kubectl cp $NAMESPACE/$DETECTOR_POD:/data/output/benchmark_summary.json ./benchmark_summary_latest.json 2>/dev/null || true

    success "Data downloaded to $BACKUP_DIR/ and latest files updated"

    # Show quick summary
    if [[ -f "$BACKUP_DIR/benchmark_summary.json" ]]; then
        echo ""
        echo "📈 Quick Summary:"
        cat "$BACKUP_DIR/benchmark_summary.json" | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(f'  Trades: {data.get(\"total_trades\", 0):,}')
print(f'  Anomalies: {data.get(\"total_anomalies\", 0):,}')
print(f'  Detection Rate: {data.get(\"detection_rate_pct\", 0):.2f}%')
print(f'  Avg Processing: {data.get(\"avg_processing_time_sec\", 0):.4f}s')
" 2>/dev/null || true
    fi
}

# =============================================================================
# STOP SYSTEM
# =============================================================================
stop_system() {
    log "🛑 Stopping crypto anomaly detection system..."

    # Scale down deployments
    kubectl scale deployment producer detector dashboard --replicas=0 -n $NAMESPACE 2>/dev/null || true

    log "Waiting for pods to terminate..."
    sleep 10

    # Delete deployments and services
    kubectl delete deployment producer detector dashboard -n $NAMESPACE 2>/dev/null || true
    kubectl delete service dashboard -n $NAMESPACE 2>/dev/null || true

    # Ask about data
    echo ""
    echo "💾 Data Options:"
    echo "1) Keep all data (recommended)"
    echo "2) Delete database only"
    echo "3) Delete everything (storage + secrets)"
    echo ""
    read -p "Choice [1-3]: " choice

    case $choice in
        1)
            log "💾 All data preserved"
            ;;
        2)
            kubectl delete pvc crypto-storage -n $NAMESPACE 2>/dev/null || true
            log "💾 Database deleted, secrets preserved"
            ;;
        3)
            kubectl delete pvc crypto-storage -n $NAMESPACE 2>/dev/null || true
            kubectl delete secret kafka-credentials -n $NAMESPACE 2>/dev/null || true
            log "💾 Everything deleted"
            ;;
    esac

    success "System stopped successfully"
}

# =============================================================================
# DASHBOARD ACCESS
# =============================================================================
open_dashboard() {
    log "🎯 Opening dashboard..."

    if ! kubectl get service dashboard -n $NAMESPACE >/dev/null 2>&1; then
        error "Dashboard service not found. Is the system running?"
    fi

    success "Dashboard will be available at: http://localhost:8501"
    log "🛑 Press Ctrl+C to stop port-forward"

    kubectl port-forward service/dashboard 8501:8501 -n $NAMESPACE
}

# =============================================================================
# MAIN SCRIPT LOGIC
# =============================================================================
show_help() {
    echo ""
    echo "🚀 Crypto Anomaly Detection - Ultimate Deployment Script"
    echo "========================================================"
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "🔧 Deployment Commands:"
    echo "  build-deploy    Build new images and deploy complete system"
    echo "  deploy          Deploy system with existing images"
    echo "  start           Alias for deploy"
    echo "  update          Rolling update with latest images"
    echo ""
    echo "📊 Monitoring Commands:"
    echo "  status          Show detailed system status and metrics"
    echo "  logs            Show live logs (interactive)"
    echo "  dashboard       Open dashboard port-forward"
    echo "  data            Download performance/benchmark data"
    echo ""
    echo "🔄 Management Commands:"
    echo "  restart         Restart all components"
    echo "  stop            Stop system (with data options)"
    echo "  clean           Complete cleanup"
    echo ""
    echo "⚡ Performance Options:"
    echo "  Set PERFORMANCE_MODE=high-performance for maximum resources"
    echo "  Example: PERFORMANCE_MODE=high-performance ./crypto.sh build-deploy"
    echo ""
    echo "🔍 Examples:"
    echo "  $0 build-deploy                    # Full deployment with new images"
    echo "  $0 deploy                          # Deploy with existing images"
    echo "  $0 status                          # Check system health"
    echo "  PERFORMANCE_MODE=high-performance $0 build-deploy  # High-performance mode"
    echo ""
}

# Main execution
case "${1:-help}" in
    build-deploy)
        check_prerequisites
        deploy_system false
        ;;
    deploy|start)
        check_prerequisites
        deploy_system true
        ;;
    update)
        check_prerequisites
        log "🔄 Performing rolling update..."
        kubectl rollout restart deployment/producer deployment/detector deployment/dashboard -n $NAMESPACE
        wait_for_deployment "producer" 180
        wait_for_deployment "detector" 180
        wait_for_deployment "dashboard" 180
        success "Rolling update complete"
        show_status
        ;;
    restart)
        log "🔄 Restarting system..."
        kubectl scale deployment producer detector dashboard --replicas=0 -n $NAMESPACE 2>/dev/null || true
        sleep 10
        kubectl scale deployment producer detector dashboard --replicas=1 -n $NAMESPACE 2>/dev/null || true
        wait_for_deployment "producer" 180
        wait_for_deployment "detector" 180
        wait_for_deployment "dashboard" 180
        success "System restarted"
        show_status
        ;;
    stop)
        stop_system
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    dashboard)
        open_dashboard
        ;;
    data)
        download_data
        ;;
    clean)
        log "🧹 Complete cleanup..."
        kubectl delete namespace $NAMESPACE 2>/dev/null || true
        success "Complete cleanup done"
        ;;
    help|*)
        show_help
        ;;
esac