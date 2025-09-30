#!/bin/bash

# Curvine CSI Driver Uninstallation Script
# This script removes Curvine CSI Driver from the curvine-system namespace

set -euo pipefail

NAMESPACE="curvine-system"

echo "=== Curvine CSI Driver Uninstallation Script ==="
echo "Target Namespace: $NAMESPACE"
echo ""

# Function to check if kubectl is available
check_kubectl() {
    if ! command -v kubectl &> /dev/null; then
        echo "Error: kubectl command not found, please install kubectl first"
        exit 1
    fi
}

# Function to remove CSI Driver
remove_csi_driver() {
    echo "Removing CSI Driver..."
    if kubectl get csidriver curvine &> /dev/null; then
        kubectl delete csidriver curvine
        echo "✓ CSI Driver removed successfully"
    else
        echo "× CSI Driver does not exist"
    fi
}

# Function to remove cluster-scoped resources
remove_cluster_resources() {
    echo ""
    echo "Removing cluster-scoped resources..."

    # Remove ClusterRoleBindings
    local cluster_role_bindings=("curvine-csi-controller-binding" "curvine-csi-node-binding")
    for crb in "${cluster_role_bindings[@]}"; do
        if kubectl get clusterrolebinding "$crb" &> /dev/null; then
            kubectl delete clusterrolebinding "$crb"
            echo "✓ ClusterRoleBinding '$crb' removed successfully"
        else
            echo "× ClusterRoleBinding '$crb' does not exist"
        fi
    done

    # Remove ClusterRoles
    local cluster_roles=("curvine-csi-controller-role" "curvine-csi-node-role")
    for cr in "${cluster_roles[@]}"; do
        if kubectl get clusterrole "$cr" &> /dev/null; then
            kubectl delete clusterrole "$cr"
            echo "✓ ClusterRole '$cr' removed successfully"
        else
            echo "× ClusterRole '$cr' does not exist"
        fi
    done
}

# Function to remove namespace and all resources
remove_namespace() {
    echo ""
    echo "Removing Namespace and all its resources..."

    if kubectl get namespace "$NAMESPACE" &> /dev/null; then
        kubectl delete namespace "$NAMESPACE"
        echo "✓ Namespace '$NAMESPACE' and all its resources removed successfully"
    else
        echo "× Namespace '$NAMESPACE' does not exist"
    fi
}

# Function to remove storage class
remove_storage_class() {
    echo ""
    echo "Removing storage class..."

    if kubectl get storageclass curvine-sc &> /dev/null; then
        echo "Found storage class 'curvine-sc', do you want to remove it? (y/n)"
        read -r remove_sc
        if [[ "$remove_sc" == "y" || "$remove_sc" == "Y" ]]; then
            kubectl delete storageclass curvine-sc
            echo "✓ Storage class 'curvine-sc' removed successfully"
        else
            echo "Keeping storage class 'curvine-sc'"
        fi
    else
        echo "× Storage class 'curvine-sc' does not exist"
    fi
}

# Function to check for existing PVCs
check_existing_pvcs() {
    echo ""
    echo "Checking existing PVCs..."

    local pvcs
    pvcs=$(kubectl get pvc --all-namespaces -o jsonpath='{.items[?(@.spec.storageClassName=="curvine-sc")].metadata.name}' 2>/dev/null || true)

    if [ -n "$pvcs" ]; then
        echo "Warning: Found PVCs using curvine-sc storage class:"
        kubectl get pvc --all-namespaces -o wide | grep curvine-sc || true
        echo ""
        echo "Please delete these PVCs before uninstalling, or they may enter Pending state"
    else
        echo "✓ No PVCs found using curvine-sc"
    fi
}

# Main execution
main() {
    echo "Checking environment..."
    check_kubectl

    check_existing_pvcs

    echo ""
    echo "Are you sure you want to uninstall Curvine CSI Driver? This will remove all related resources (y/n)"
    read -r confirm

    if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
        echo "Uninstallation cancelled"
        exit 0
    fi

    remove_csi_driver
    remove_cluster_resources
    remove_namespace
    remove_storage_class

    echo ""
    echo "=== Uninstallation Complete ==="
    echo ""
    echo "Curvine CSI Driver has been successfully uninstalled"
    echo "If there are any remaining PV/PVCs, please clean them up manually"
}

# Run main function
main "$@"
