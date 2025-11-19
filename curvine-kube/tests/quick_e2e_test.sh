# Copyright 2025 OPPO.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

# Quick E2E Test for Curvine on minikube
# Simplified version for rapid testing

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[✓]${NC} $1"; }
log_error() { echo -e "${RED}[✗]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[!]${NC} $1"; }

# Setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# Use curvine-cli directly to avoid curvine-env.sh setting hostname env vars
KUBE_BIN="$PROJECT_ROOT/build/dist/lib/curvine-cli"
CURVINE_HOME="$PROJECT_ROOT/build/dist"

export CURVINE_HOME
# Clear hostname environment variables to avoid conflicts
unset CURVINE_MASTER_HOSTNAME
unset CURVINE_WORKER_HOSTNAME
unset CURVINE_CLIENT_HOSTNAME

CLUSTER_ID="test"
NAMESPACE="default"
TIMEOUT=300

# Cleanup on exit
cleanup() {
    log_info "Cleaning up..."
    export CURVINE_CONF_FILE="${CURVINE_HOME}/conf/curvine-cluster.toml"
    $KUBE_BIN k8s delete -c $CLUSTER_ID -n $NAMESPACE 2>/dev/null || true
}
trap cleanup EXIT

# Check prerequisites
log_info "Checking prerequisites..."
[ -f "$KUBE_BIN" ] || { log_error "Curvine binary not found at $KUBE_BIN"; exit 1; }
kubectl cluster-info &>/dev/null || { log_error "kubectl not connected"; exit 1; }
log_success "Prerequisites OK"

# Create config
log_info "Creating cluster config..."
CONFIG_DIR=$(mktemp -d -t curvine-e2e-XXXXXX)
CONFIG_FILE="$CONFIG_DIR/curvine-cluster.toml"

cat > "$CONFIG_FILE" << 'EOF'
format_master = false
format_worker = false

[master]
meta_dir = "testing/meta"
rpc_port = 8900
log = { level = "info", log_dir = "stdout", file_name = "master.log" }

[journal]
journal_addrs = []
journal_dir = "testing/journal"
rpc_port = 8996

[worker]
dir_reserved = "0"
data_dir = ["[MEM:100M]/data/mem"]
log = { level = "info", log_dir = "stdout", file_name = "worker.log" }
enable_s3_gateway = false

[client]
master_addrs = []
[client.kubernetes]
namespace = "default"
cluster_id = "test"
cluster_domain = "cluster.local"

[fuse]

[log]
level = "info"
log_dir = "stdout"
file_name = "curvine.log"

[s3_gateway]
listen = "0.0.0.0:9900"
region = "us-east-1"
EOF

log_success "Config created at $CONFIG_FILE"

# Deploy
log_info "Deploying cluster..."
export CURVINE_CONF_FILE="$CONFIG_FILE"

# Unset hostname environment variables before deployment
unset CURVINE_MASTER_HOSTNAME
unset CURVINE_WORKER_HOSTNAME  
unset CURVINE_CLIENT_HOSTNAME

$KUBE_BIN k8s deploy -c $CLUSTER_ID -n $NAMESPACE --master-replicas 1 --worker-replicas 1 || {
    log_error "Deployment failed"
    exit 1
}
log_success "Deployment command executed"

# Wait for Master
log_info "Waiting for Master pod (timeout: ${TIMEOUT}s)..."
start_time=$(date +%s)
while true; do
    elapsed=$(($(date +%s) - start_time))
    [ $elapsed -gt $TIMEOUT ] && { log_error "Timeout waiting for Master"; exit 1; }
    
    if kubectl get pod -n $NAMESPACE ${CLUSTER_ID}-master-0 &>/dev/null; then
        status=$(kubectl get pod -n $NAMESPACE ${CLUSTER_ID}-master-0 -o jsonpath='{.status.phase}')
        ready=$(kubectl get pod -n $NAMESPACE ${CLUSTER_ID}-master-0 -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || echo "false")
        
        if [ "$status" = "Running" ] && [ "$ready" = "true" ]; then
            log_success "Master pod ready"
            break
        fi
        log_info "Master status: $status (${elapsed}s)"
    else
        log_info "Waiting for Master pod (${elapsed}s)"
    fi
    sleep 3
done

# Wait for Worker (Deployment uses label selector, not fixed pod name)
log_info "Waiting for Worker pod (timeout: ${TIMEOUT}s)..."
start_time=$(date +%s)
while true; do
    elapsed=$(($(date +%s) - start_time))
    [ $elapsed -gt $TIMEOUT ] && { log_error "Timeout waiting for Worker"; exit 1; }
    
    # Get worker pod name using label selector (Deployment creates pods with random suffix)
    WORKER_POD=$(kubectl get pod -n $NAMESPACE -l app=${CLUSTER_ID},component=worker -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    
    if [ -n "$WORKER_POD" ]; then
        status=$(kubectl get pod -n $NAMESPACE $WORKER_POD -o jsonpath='{.status.phase}' 2>/dev/null)
        ready=$(kubectl get pod -n $NAMESPACE $WORKER_POD -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || echo "false")
        
        if [ "$status" = "Running" ] && [ "$ready" = "true" ]; then
            log_success "Worker pod ready ($WORKER_POD)"
            break
        fi
        log_info "Worker status: $status (${elapsed}s)"
    else
        log_info "Waiting for Worker pod (${elapsed}s)"
    fi
    sleep 3
done

# Verify resources
log_info "Verifying Kubernetes resources..."
kubectl get statefulset -n $NAMESPACE ${CLUSTER_ID}-master &>/dev/null || { log_error "Master StatefulSet not found"; exit 1; }
kubectl get deployment -n $NAMESPACE ${CLUSTER_ID}-worker &>/dev/null || { log_error "Worker Deployment not found"; exit 1; }
kubectl get svc -n $NAMESPACE ${CLUSTER_ID}-master &>/dev/null || { log_error "Master Service not found"; exit 1; }
kubectl get configmap -n $NAMESPACE ${CLUSTER_ID}-config &>/dev/null || { log_error "ConfigMap not found"; exit 1; }
log_success "All resources verified"

# Show pod info
log_info "Pod information:"
kubectl get pod -n $NAMESPACE -l app=$CLUSTER_ID

# Show logs
log_info "Master pod logs (last 10 lines):"
kubectl logs -n $NAMESPACE ${CLUSTER_ID}-master-0 --tail=10 2>/dev/null || log_warning "Could not fetch Master logs"

log_info "Worker pod logs (last 10 lines):"
kubectl logs -n $NAMESPACE $WORKER_POD --tail=10 2>/dev/null || log_warning "Could not fetch Worker logs"

echo ""
echo "=========================================="
log_success "All tests passed!"
echo "=========================================="
echo ""
echo "Cluster is running successfully!"
echo ""
echo "Useful commands:"
echo "  View pods:       kubectl get pod -n $NAMESPACE -l app=$CLUSTER_ID"
echo "  Master logs:     kubectl logs -n $NAMESPACE ${CLUSTER_ID}-master-0 -f"
echo "  Worker logs:     kubectl logs -n $NAMESPACE $WORKER_POD -f"
echo "  Port forward:    kubectl port-forward -n $NAMESPACE svc/${CLUSTER_ID}-master 9000:9000"
echo "  Delete cluster:  $KUBE_BIN k8s delete -c $CLUSTER_ID -n $NAMESPACE"
echo ""
