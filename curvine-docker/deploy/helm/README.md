# Curvine Helm Chart

A production-ready Helm Chart for deploying Curvine distributed storage clusters on Kubernetes.

## Features

- **One-Click Deployment**: Deploy a complete Curvine cluster with a single Helm command
- **Dynamic Configuration**: Automatically generates journal_addrs and master_addrs
- **Flexible Storage**: Supports PVC, hostPath, and emptyDir storage modes
- **High Availability**: Supports odd-numbered Master replicas with Raft consensus
- **Hot Configuration Updates**: ConfigMap changes automatically trigger Pod rolling updates
- **Production-Ready**: Built-in resource limits, health checks, and RBAC
- **Master Replica Protection**: Prevents accidental Master replica changes during upgrades

## Prerequisites

- Kubernetes 1.20+
- Helm 3.0+
- PV provisioner (if using PVC storage)

## Quick Start

### 1. Add Helm Repository (Optional)

```bash
# If the chart is published to a repository
helm repo add curvine https://curvineio.github.io/helm/charts
helm repo update
```

### 2. Install the Chart

#### Option A: From Helm Repository (Recommended)

```bash
# Install with default configuration
helm install curvine curvine/curvine -n curvine --create-namespace

# Install with custom replica counts
helm install curvine curvine/curvine -n curvine --create-namespace \
  --set master.replicas=5 \
  --set worker.replicas=10

# Install using a custom values file
helm install curvine curvine/curvine -n curvine --create-namespace \
  -f https://curvineio.github.io/helm/charts/examples/values-prod.yaml
```

#### Option B: From Local Chart

```bash
# Install with default configuration
helm install curvine ./curvine -n curvine --create-namespace

# Install with custom replica counts
helm install curvine ./curvine -n curvine --create-namespace \
  --set master.replicas=5 \
  --set worker.replicas=10

# Install using a custom values file
helm install curvine ./curvine -n curvine --create-namespace \
  -f examples/values-prod.yaml
```

### 3. Verify Deployment

```bash
# Check Pod status
kubectl get pods -n curvine

# View Services
kubectl get svc -n curvine

# View PersistentVolumeClaims
kubectl get pvc -n curvine

# Run Helm tests
helm test curvine -n curvine
```

### 4. Access the Cluster

```bash
# Port-forward to access Master Web UI
kubectl port-forward -n curvine svc/curvine-master 9000:9000

# Visit http://localhost:9000
```

## Configuration

### Core Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `cluster.id` | Cluster identifier | `curvine` |
| `master.replicas` | Number of Master replicas (must be odd) | `3` |
| `worker.replicas` | Number of Worker replicas | `3` |
| `image.repository` | Container image repository | `docker.io/curvine` |
| `image.tag` | Container image tag | `latest` |

### Master Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `master.rpcPort` | RPC port | `8995` |
| `master.journalPort` | Journal/Raft port | `8996` |
| `master.webPort` | Web UI port | `9000` |
| `master.storage.meta.storageClass` | Storage class for metadata | `""` (default) |
| `master.storage.meta.size` | Metadata storage size | `10Gi` |
| `master.storage.journal.storageClass` | Storage class for journal | `""` (default) |
| `master.storage.journal.size` | Journal storage size | `50Gi` |

### Worker Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `worker.rpcPort` | RPC port | `8997` |
| `worker.webPort` | Web UI port | `9001` |
| `worker.hostNetwork` | Use host network | `false` |
| `worker.privileged` | Privileged mode (required for FUSE) | `true` |
| `worker.antiAffinity.enabled` | Enable Pod anti-affinity | `true` |
| `worker.antiAffinity.type` | Anti-affinity type | `preferred` |

For a complete list of parameters, see `values.yaml`.

### View Current Configuration

```bash
# View all current values
helm get values curvine -n curvine

# View values from a specific release
helm get values curvine -n curvine -o yaml

# View the rendered manifest
helm get manifest curvine -n curvine

# View the values.yaml from the chart
cat ./curvine/values.yaml

# View specific parameter
helm get values curvine -n curvine | grep master.replicas
```

## Configuration Examples

### Development Environment (Minimal)

```bash
# From Helm Repository
helm install curvine curvine/curvine -n curvine --create-namespace \
  --set master.replicas=1 \
  --set worker.replicas=1

# From Local Chart
helm install curvine ./curvine -n curvine --create-namespace \
  -f examples/values-dev.yaml
```

### Production Environment (High Availability)

```bash
# From Helm Repository
helm install curvine curvine/curvine -n curvine --create-namespace \
  --set master.replicas=5 \
  --set worker.replicas=10 \
  --set master.storage.meta.storageClass=fast-ssd \
  --set master.storage.journal.storageClass=fast-ssd

# From Local Chart
helm install curvine ./curvine -n curvine --create-namespace \
  -f examples/values-prod.yaml
```

### Bare Metal Environment (Using hostPath)

```bash
# From Local Chart
helm install curvine ./curvine -n curvine --create-namespace \
  -f examples/values-baremetal.yaml
```

### Custom Configuration

```bash
# From Helm Repository
helm install curvine curvine/curvine -n curvine --create-namespace \
  --set master.replicas=5 \
  --set worker.replicas=10 \
  --set master.storage.meta.storageClass=fast-ssd \
  --set worker.storage.dataDirs[0].storageClass=fast-ssd \
  --set worker.storage.dataDirs[0].size=500Gi

# From Local Chart
helm install curvine ./curvine -n curvine --create-namespace \
  --set master.replicas=5 \
  --set worker.replicas=10 \
  --set master.storage.meta.storageClass=fast-ssd \
  --set worker.storage.dataDirs[0].storageClass=fast-ssd \
  --set worker.storage.dataDirs[0].size=500Gi
```

## Storage Configuration

### Using PVC (Recommended for Cloud)

```yaml
master:
  storage:
    meta:
      storageClass: "fast-ssd"
      size: "20Gi"
    journal:
      storageClass: "fast-ssd"
      size: "100Gi"

worker:
  storage:
    dataDirs:
      - name: "data1"
        type: "SSD"
        enabled: true
        size: "100Gi"
        storageClass: "fast-ssd"
        mountPath: "/data/data1"
```

### Using hostPath (Recommended for Bare Metal)

```yaml
master:
  storage:
    meta:
      storageClass: ""
      hostPath: "/mnt/curvine/master/meta"
    journal:
      storageClass: ""
      hostPath: "/mnt/curvine/master/journal"

worker:
  storage:
    dataDirs:
      - name: "data1"
        type: "SSD"
        enabled: true
        size: "100Gi"
        storageClass: ""
        hostPath: "/mnt/nvme0n1/curvine"
        mountPath: "/data/data1"
```

### Using emptyDir (For Testing)

```yaml
master:
  storage:
    meta:
      storageClass: ""
      hostPath: ""
    journal:
      storageClass: ""
      hostPath: ""

worker:
  storage:
    dataDirs:
      - name: "data1"
        storageClass: ""
        hostPath: ""
```

## Upgrading

### Update Configuration

```bash
# Scale Worker replicas (From Helm Repository)
helm upgrade curvine curvine/curvine -n curvine \
  --set worker.replicas=15

# Upgrade image version (From Helm Repository)
helm upgrade curvine curvine/curvine -n curvine \
  --set image.tag=v1.1.0

# Upgrade using a new values file (From Local Chart)
helm upgrade curvine ./curvine -n curvine \
  -f values-new.yaml
```

> **Note**: Master replicas cannot be changed during upgrade. To modify Master replicas, delete and redeploy the cluster.

### View Release History

```bash
helm history curvine -n curvine
```

### Rollback

```bash
# Rollback to previous version
helm rollback curvine -n curvine

# Rollback to specific version
helm rollback curvine 2 -n curvine
```

## Uninstall

```bash
# Uninstall Chart (preserves PVCs)
helm uninstall curvine -n curvine

# Delete PersistentVolumeClaims
kubectl delete pvc -n curvine -l app.kubernetes.io/instance=curvine

# Delete namespace
kubectl delete namespace curvine
```

## Troubleshooting

### Check Pod Status

```bash
kubectl get pods -n curvine
kubectl describe pod <pod-name> -n curvine
kubectl logs <pod-name> -n curvine
```

### View ConfigMap

```bash
kubectl get configmap -n curvine
kubectl describe configmap curvine-config -n curvine
```

### View Events

```bash
kubectl get events -n curvine --sort-by='.lastTimestamp'
```

### Common Issues

1. **Master Replica Validation Failed**
   - Error: `master.replicas must be an odd number`
   - Solution: Ensure Master replicas is odd (1, 3, 5, 7...)

2. **PVC Cannot Bind**
   - Check if StorageClass exists
   - Verify PV provisioner is working correctly

3. **Pod Fails to Start**
   - Verify container image exists
   - Check resource quotas are sufficient
   - Review Pod logs for details

## Support

For issues, questions, or contributions, please visit:
- GitHub: https://github.com/CurvineIO/curvine
- Documentation: https://curvineio.github.io/
