#!/bin/bash

# Kubernetes Deployment Script for Curvine
# This script deploys Curvine master and worker containers to a local Kubernetes cluster

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="${SCRIPT_DIR}/k8s"

echo_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

echo_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

echo_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

echo_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Check if kubectl is available
check_kubectl() {
    if ! command -v kubectl &> /dev/null; then
        echo_error "kubectl is not installed or not in PATH"
        exit 1
    fi
}

# Check if cluster is running
check_cluster() {
    if ! kubectl cluster-info &> /dev/null; then
        echo_error "Kubernetes cluster is not running or not accessible"
        echo_info "Run './k8s-setup.sh' to create a local cluster"
        exit 1
    fi
    echo_info "Connected to cluster: $(kubectl config current-context)"
}

# Check if Docker image exists locally
check_images() {
    echo_step "Checking Docker images..."

    if docker images curvine:latest --format "{{.Repository}}:{{.Tag}}" | grep -q "curvine:latest"; then
        echo_info "Docker image 'curvine:latest' found locally"
        docker images curvine:latest
        return 0
    else
        echo_error "Docker image 'curvine:latest' not found locally"
        echo_info "Please build the image first using: ./build-img.sh"
        exit 1
    fi
}

# Push image to local kind registry
push_to_local_registry() {
    local reg_port='5001'
    local local_image="curvine:latest"
    local registry_image="localhost:${reg_port}/curvine:latest"

    echo_step "Pushing image to local kind registry..."

    # Check if registry is running
    if ! docker ps --format '{{.Names}}' | grep -q "kind-registry"; then
        echo_error "Local registry 'kind-registry' is not running"
        echo_info "Please run ./k8s-setup.sh first to create the registry"
        exit 1
    fi

    # Tag the image for the local registry
    echo_info "Tagging image: ${local_image} -> ${registry_image}"
    docker tag ${local_image} ${registry_image}

    # Push to the local registry (with platform flag to avoid attestation issues)
    echo_info "Pushing image to registry..."
    docker push ${registry_image} --platform linux/amd64 2>&1 | grep -v "Waiting\|Pushing" || docker push ${registry_image}

    echo_info "Successfully pushed image to local registry: ${registry_image}"
}

# Apply Kubernetes manifests
apply_manifests() {
    echo_step "Applying Kubernetes manifests..."

    # Create namespace
    echo_info "Creating namespace..."
    kubectl apply -f "${K8S_DIR}/namespace.yaml"

    # Apply ConfigMap
    echo_info "Creating ConfigMap..."
    kubectl apply -f "${K8S_DIR}/configmap.yaml"

    # Apply Master deployment
    echo_info "Deploying Curvine Master..."
    kubectl apply -f "${K8S_DIR}/master-deployment.yaml"

    # Wait for master to be ready
    echo_info "Waiting for Master to be ready..."
    kubectl wait --for=condition=ready pod -l component=master -n curvine --timeout=300s || true

    # Apply Worker deployment
    echo_info "Deploying Curvine Workers..."
    kubectl apply -f "${K8S_DIR}/worker-deployment.yaml"

    # Wait for workers to be ready
    echo_info "Waiting for Workers to be ready..."
    kubectl wait --for=condition=ready pod -l component=worker -n curvine --timeout=300s || true
}

# Show deployment status
show_status() {
    echo ""
    echo_step "Deployment Status"
    echo "=================="

    echo ""
    echo_info "Pods:"
    kubectl get pods -n curvine -l app=curvine

    echo ""
    echo_info "Services:"
    kubectl get svc -n curvine -l app=curvine

    echo ""
    echo_info "Deployments:"
    kubectl get deployments -n curvine -l app=curvine

    echo ""
    echo_info "PersistentVolumeClaims:"
    kubectl get pvc -n curvine -l app=curvine
}

# Show access information
show_access_info() {
    echo ""
    echo_step "Access Information"
    echo "=================="

    # Get NodePort for master web service
    MASTER_WEB_PORT=$(kubectl get svc cv-master-web -n curvine -o jsonpath='{.spec.ports[?(@.name=="web")].nodePort}' 2>/dev/null)

    echo ""
    if [ -n "$MASTER_WEB_PORT" ]; then
        echo_info "Master Web UI: http://localhost:${MASTER_WEB_PORT}"
    fi
    echo_info "Master RPC (internal): cv-master.curvine.svc.cluster.local:8995"
    echo_info "Worker RPC (internal): cv-worker.curvine.svc.cluster.local:8997"

    echo ""
    echo_info "Useful commands:"
    echo "  # View logs"
    echo "  kubectl logs -l component=master -n curvine --tail=100 -f"
    echo "  kubectl logs -l component=worker -n curvine --tail=100 -f"
    echo ""
    echo "  # Execute commands in master pod"
    echo "  kubectl exec -it \$(kubectl get pod -n curvine -l component=master -o jsonpath='{.items[0].metadata.name}') -n curvine -- /bin/bash"
    echo ""
    echo "  # Use Curvine CLI from master pod"
    echo "  kubectl exec -it \$(kubectl get pod -n curvine -l component=master -o jsonpath='{.items[0].metadata.name}') -n curvine -- /app/curvine/bin/cv report"
    echo ""
    echo "  # Port-forward to access services"
    echo "  kubectl port-forward -n curvine svc/cv-master 8995:8995"
    echo "  kubectl port-forward -n curvine svc/cv-master 9000:9000"
    echo ""
    echo "  # Scale workers"
    echo "  kubectl scale deployment cv-worker -n curvine --replicas=5"
    echo ""
    echo "  # Delete deployment"
    echo "  kubectl delete -f ${K8S_DIR}/"
}

# Cleanup deployment
cleanup() {
    echo_step "Cleaning up existing deployment..."

    kubectl delete -f "${K8S_DIR}/worker-deployment.yaml" --ignore-not-found=true || true
    kubectl delete -f "${K8S_DIR}/master-deployment.yaml" --ignore-not-found=true || true
    kubectl delete -f "${K8S_DIR}/configmap.yaml" --ignore-not-found=true || true

    # Wait for pods to terminate
    echo_info "Waiting for pods to terminate..."
    kubectl wait --for=delete pod -l app=curvine -n curvine --timeout=60s 2>/dev/null || true

    # Delete PVCs
    echo_info "Deleting PersistentVolumeClaims..."
    kubectl delete pvc -l app=curvine -n curvine --ignore-not-found=true

    echo_info "Cleanup completed"
}

# Main execution
main() {
    local action="${1:-deploy}"

    case "$action" in
        deploy)
            echo_info "Deploying Curvine to Kubernetes..."
            check_kubectl
            check_cluster
            check_images
            push_to_local_registry
            apply_manifests
            show_status
            show_access_info
            echo ""
            echo_info "✓ Deployment complete!"
            ;;
        cleanup|clean|delete)
            echo_info "Cleaning up Curvine deployment..."
            check_kubectl
            check_cluster
            cleanup
            echo_info "✓ Cleanup complete!"
            ;;
        status)
            check_kubectl
            check_cluster
            show_status
            show_access_info
            ;;
        *)
            echo "Usage: $0 {deploy|cleanup|status}"
            echo ""
            echo "Commands:"
            echo "  deploy  - Deploy Curvine to Kubernetes (default)"
            echo "  cleanup - Remove Curvine deployment"
            echo "  status  - Show deployment status"
            exit 1
            ;;
    esac
}

main "$@"
