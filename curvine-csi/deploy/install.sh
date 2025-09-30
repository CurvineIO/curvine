#!/bin/bash

# Curvine CSI Driver Installation Script
# This script deploys Curvine CSI Driver to the curvine-system namespace

set -euo pipefail

NAMESPACE="curvine-system"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Curvine CSI Driver Installation Script ==="
echo "Target Namespace: $NAMESPACE"
echo ""

# Function to check if kubectl is available
check_kubectl() {
    if ! command -v kubectl &> /dev/null; then
        echo "Error: kubectl command not found, please install kubectl first"
        exit 1
    fi
}

# Function to check if namespace exists
check_namespace() {
    if kubectl get namespace "$NAMESPACE" &> /dev/null; then
        echo "✓ Namespace '$NAMESPACE' already exists"
    else
        echo "× Namespace '$NAMESPACE' does not exist, creating..."
        kubectl apply -f "$SCRIPT_DIR/namespace.yaml"
        echo "✓ Namespace '$NAMESPACE' created successfully"
    fi
}

# Function to deploy resources
deploy_resources() {
    local resources=(
        "serviceaccount.yaml"
        "clusterrole.yaml"
        "clusterrolebinding.yaml"
        "configmap.yaml"
        "csidriver.yaml"
        "deployment.yaml"
        "daemonset.yaml"
    )

    echo ""
    echo "Starting to deploy CSI Driver components..."

    for resource in "${resources[@]}"; do
        echo "Deploying $resource..."
        kubectl apply -f "$SCRIPT_DIR/$resource"
        echo "✓ $resource deployed successfully"
    done
}

# Function to verify installation
verify_installation() {
    echo ""
    echo "Verifying installation status..."

    # Check CSI Driver
    if kubectl get csidriver curvine &> /dev/null; then
        echo "✓ CSI Driver 'curvine' registered successfully"
    else
        echo "× CSI Driver 'curvine' registration failed"
    fi

    # Check Controller Pod
    echo "Checking controller Pod status..."
    kubectl get pods -n "$NAMESPACE" -l app=curvine-csi-controller

    # Check Node Pods
    echo ""
    echo "Checking node Pod status..."
    kubectl get pods -n "$NAMESPACE" -l app=curvine-csi-node

    # Check Storage Class
    echo ""
    if [ -f "$SCRIPT_DIR/../examples/storage-class.yaml" ]; then
        echo "Do you want to deploy example storage class? (y/n)"
        read -r deploy_sc
        if [[ "$deploy_sc" == "y" || "$deploy_sc" == "Y" ]]; then
            kubectl apply -f "$SCRIPT_DIR/../examples/storage-class.yaml"
            echo "✓ Example storage class deployed successfully"
        fi
    fi
}

# Main execution
main() {
    echo "Checking environment..."
    check_kubectl

    echo "Checking/Creating Namespace..."
    check_namespace

    deploy_resources

    verify_installation

    echo ""
    echo "=== Installation Complete ==="
    echo ""
    echo "Next steps:"
    echo "1. Check if all Pods are running: kubectl get pods -n $NAMESPACE"
    echo "2. View logs: kubectl logs -n $NAMESPACE -l app=curvine-csi-controller"
    echo "3. Create test PVC: kubectl apply -f examples/pvc-curvine.yaml"
    echo ""
    echo "To uninstall, run: kubectl delete namespace $NAMESPACE"
}

# Run main function
main "$@"
