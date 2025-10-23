#!/bin/bash

# Local Kubernetes Cluster Setup for Curvine
# This script sets up a local Kubernetes cluster using kind (Kubernetes in Docker)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

echo_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

echo_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if kind is installed
check_kind() {
    if ! command -v kind &> /dev/null; then
        echo_error "kind is not installed. Installing kind..."

        # Detect OS
        OS=$(uname -s | tr '[:upper:]' '[:lower:]')
        ARCH=$(uname -m)

        if [ "$ARCH" = "x86_64" ]; then
            ARCH="amd64"
        elif [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
            ARCH="arm64"
        fi

        # Install kind
        curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.20.0/kind-${OS}-${ARCH}
        chmod +x ./kind
        sudo mv ./kind /usr/local/bin/kind

        echo_info "kind installed successfully"
    else
        echo_info "kind is already installed: $(kind version)"
    fi
}

# Check if kubectl is installed
check_kubectl() {
    if ! command -v kubectl &> /dev/null; then
        echo_error "kubectl is not installed. Please install kubectl first."
        echo "Visit: https://kubernetes.io/docs/tasks/tools/"
        exit 1
    else
        echo_info "kubectl is installed: $(kubectl version --client --short 2>/dev/null || kubectl version --client)"
    fi
}

# Create local registry for kind
create_local_registry() {
    local reg_name='kind-registry'
    local reg_port='5001'

    echo_info "Setting up local Docker registry..."

    # Create registry container unless it already exists
    if [ "$(docker inspect -f '{{.State.Running}}' "${reg_name}" 2>/dev/null || true)" != 'true' ]; then
        echo_info "Creating registry container '${reg_name}' on port ${reg_port}..."
        docker run \
            -d --restart=always -p "127.0.0.1:${reg_port}:5000" \
            --network bridge \
            --name "${reg_name}" \
            registry:2
        echo_info "Registry container created"
    else
        echo_info "Registry container '${reg_name}' already running"
    fi
}

# Create kind cluster configuration
create_cluster_config() {
    cat > /tmp/curvine-kind-config.yaml <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: curvine-cluster
containerdConfigPatches:
- |-
  [plugins."io.containerd.grpc.v1.cri".registry]
    config_path = "/etc/containerd/certs.d"
nodes:
  - role: control-plane
    kubeadmConfigPatches:
      - |
        kind: InitConfiguration
        nodeRegistration:
          kubeletExtraArgs:
            node-labels: "ingress-ready=true"
    extraPortMappings:
      # Master RPC port (client-master communication)
      - containerPort: 8995
        hostPort: 8995
        protocol: TCP
      # Raft/Journal port (master HA consensus)
      - containerPort: 8996
        hostPort: 8996
        protocol: TCP
      # Worker RPC port (client-worker data operations)
      - containerPort: 8997
        hostPort: 8997
        protocol: TCP
      # Master Web UI port
      - containerPort: 9000
        hostPort: 9000
        protocol: TCP
      # Worker Web UI port
      - containerPort: 9001
        hostPort: 9001
        protocol: TCP
      # S3 Gateway port (optional)
      - containerPort: 9900
        hostPort: 9900
        protocol: TCP
  - role: worker
  - role: worker
EOF

    echo_info "Cluster configuration created at /tmp/curvine-kind-config.yaml"
}

# Configure registry in kind nodes
configure_registry_in_nodes() {
    local reg_name='kind-registry'
    local reg_port='5001'

    echo_info "Configuring registry in kind nodes..."

    # Add the registry config to the nodes
    local REGISTRY_DIR="/etc/containerd/certs.d/localhost:${reg_port}"
    for node in $(kind get nodes); do
        echo_info "  Configuring ${node}..."
        docker exec "${node}" mkdir -p "${REGISTRY_DIR}"
        cat <<EOF | docker exec -i "${node}" cp /dev/stdin "${REGISTRY_DIR}/hosts.toml"
server = "http://localhost:${reg_port}"

[host."http://${reg_name}:5000"]
  capabilities = ["pull", "resolve"]
EOF
    done

    # Connect the registry to the cluster network if not already connected
    if [ "$(docker inspect -f='{{json .NetworkSettings.Networks.kind}}' "${reg_name}")" = 'null' ]; then
        echo_info "Connecting registry to kind network..."
        docker network connect "kind" "${reg_name}"
    fi

    # Document the local registry
    echo_info "Creating local registry ConfigMap..."
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: local-registry-hosting
  namespace: kube-public
data:
  localRegistryHosting.v1: |
    host: "localhost:${reg_port}"
    help: "https://kind.sigs.k8s.io/docs/user/local-registry/"
EOF

    echo_info "Local registry configured successfully"
    echo_info "Registry available at: localhost:${reg_port}"
}

# Create the kind cluster
create_cluster() {
    CLUSTER_NAME="curvine-cluster"

    # Check if cluster already exists
    if kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
        echo_warn "Cluster '${CLUSTER_NAME}' already exists"
        read -p "Do you want to delete and recreate it? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo_info "Deleting existing cluster..."
            kind delete cluster --name ${CLUSTER_NAME}
        else
            echo_info "Using existing cluster"
            return
        fi
    fi

    echo_info "Creating Kubernetes cluster '${CLUSTER_NAME}'..."
    create_cluster_config
    kind create cluster --config /tmp/curvine-kind-config.yaml

    echo_info "Waiting for cluster to be ready..."
    kubectl wait --for=condition=Ready nodes --all --timeout=300s

    echo_info "Cluster created successfully!"
}

# Display cluster info
show_cluster_info() {
    echo_info "Cluster Information:"
    echo "===================="
    kubectl cluster-info
    echo ""
    echo_info "Nodes:"
    kubectl get nodes
    echo ""
    echo_info "Cluster context: $(kubectl config current-context)"
}

# Main execution
main() {
    echo_info "Setting up local Kubernetes cluster for Curvine..."
    echo ""

    check_kind
    check_kubectl
    create_local_registry
    create_cluster
    configure_registry_in_nodes
    show_cluster_info

    echo ""
    echo_info "✓ Kubernetes cluster setup complete!"
    echo ""
    echo_info "Local Registry Information:"
    echo "  Registry URL: localhost:5001"
    echo "  From cluster: kind-registry:5000"
    echo ""
    echo_info "Next steps:"
    echo "  1. Tag your image: docker tag curvine:latest localhost:5001/curvine:latest"
    echo "  2. Push to registry: docker push localhost:5001/curvine:latest"
    echo "  3. Deploy Curvine: ./k8s-deploy.sh"
    echo "  4. Update deployments to use: localhost:5001/curvine:latest"
}

main "$@"
