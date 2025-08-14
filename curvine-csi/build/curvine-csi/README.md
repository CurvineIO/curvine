# Curvine CSI Driver Helm Chart

This Helm chart deploys the Curvine CSI (Container Storage Interface) driver on a Kubernetes cluster.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+

## Installation

### Add Helm Repository (if available)

```bash
helm repo add curvine https://charts.curvine.io
helm repo update
```

### Install from Local Chart

```bash
# Install with default values
helm install curvine-csi ./curvine-csi

# Install with custom values
helm install curvine-csi ./curvine-csi -f custom-values.yaml

# Install in specific namespace
helm install curvine-csi ./curvine-csi --namespace curvine-system --create-namespace
```

## Configuration

The following table lists the configurable parameters and their default values:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `global.namespace` | Namespace to deploy resources | `default` |
| `image.repository` | Curvine CSI image repository | `curvine/curvine-csi` |
| `image.tag` | Curvine CSI image tag | `latest` |
| `image.pullPolicy` | Image pull policy | `Always` |
| `csiDriver.name` | CSI driver name | `curvine` |
| `csiDriver.attachRequired` | Whether attach is required | `true` |
| `csiDriver.podInfoOnMount` | Whether pod info on mount | `false` |
| `controller.replicas` | Number of controller replicas | `1` |
| `controller.priorityClassName` | Priority class for controller | `system-cluster-critical` |
| `node.priorityClassName` | Priority class for node | `system-node-critical` |
| `rbac.create` | Create RBAC resources | `true` |
| `configMap.name` | ConfigMap name | `curvine-config` |

## Customization

### Custom Curvine Configuration

You can customize the Curvine configuration by modifying the `configMap.data.curvineClusterToml` value:

```yaml
configMap:
  data:
    curvineClusterToml: |
      [client]
      master_addrs = [
          { hostname = "your-master-host", port = 8995 }
      ]
      
      [log]
      level = "debug"
      log_dir = "stdout"
      file_name = "curvine.log"
```

### Custom Images

```yaml
image:
  repository: your-registry/curvine-csi
  tag: v1.0.0
  pullPolicy: IfNotPresent

controller:
  sidecars:
    provisioner:
      image: registry.k8s.io/sig-storage/csi-provisioner:v3.6.0
    attacher:
      image: registry.k8s.io/sig-storage/csi-attacher:v4.5.0
```

### Node Tolerations

```yaml
node:
  tolerations:
    - key: "node-role.kubernetes.io/master"
      operator: "Exists"
      effect: "NoSchedule"
    - key: "node-role.kubernetes.io/control-plane"
      operator: "Exists"
      effect: "NoSchedule"
```

## Usage

After installation, create a StorageClass:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: curvine-csi
provisioner: curvine
parameters:
  # Add Curvine-specific parameters
volumeBindingMode: WaitForFirstConsumer
```

Create a PVC:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: test-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
  storageClassName: curvine-csi
```

## Uninstallation

```bash
helm uninstall curvine-csi
```

## Troubleshooting

### Check CSI Driver Status

```bash
kubectl get csidriver curvine
kubectl get pods -l app.kubernetes.io/name=curvine-csi
```

### Check Logs

```bash
# Controller logs
kubectl logs -l app=curvine-csi-controller -c csi-plugin

# Node logs
kubectl logs -l app=curvine-csi-node -c csi-plugin
```

### Common Issues

1. **CSI Driver not registered**: Check if the node-driver-registrar sidecar is running
2. **Mount failures**: Verify Curvine cluster connectivity and configuration
3. **Permission issues**: Ensure proper RBAC permissions are granted

## Support

For support and documentation, visit:
- [Curvine Documentation](https://curvineio.github.io/docs/)
- [GitHub Issues](https://github.com/CurvineIO/curvine/issues)