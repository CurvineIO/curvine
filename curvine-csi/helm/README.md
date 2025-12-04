# Curvine CSI Driver Helm Chart

This Helm chart deploys the Curvine CSI (Container Storage Interface) driver on a Kubernetes cluster.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+

## Installation

### Add Helm Repository (if available)

```bash
helm repo add curvine https://curvineio.github.io/helm-charts
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
| `global.namespace` | Namespace to deploy resources | `curvine-system` |
| `image.repository` | Curvine CSI image repository | `ghcr.io/curvineio/curvine-csi` |
| `image.tag` | Curvine CSI image tag | `latest` |
| `image.pullPolicy` | Image pull policy | `Always` |
| `csiDriver.name` | CSI driver name | `curvine` |
| `csiDriver.attachRequired` | Whether attach is required | `false` |
| `csiDriver.podInfoOnMount` | Whether pod info on mount | `false` |
| `controller.name` | Controller deployment name | `curvine-csi-controller` |
| `controller.replicas` | Number of controller replicas | `1` |
| `controller.priorityClassName` | Priority class for controller | `system-cluster-critical` |
| `node.name` | Node DaemonSet name | `curvine-csi-node` |
| `node.priorityClassName` | Priority class for node | `system-node-critical` |
| `node.dnsPolicy` | DNS policy for node | `ClusterFirstWithHostNet` |
| `rbac.create` | Create RBAC resources | `true` |
| `serviceAccount.controller.name` | Controller service account name | `curvine-csi-controller-sa` |
| `serviceAccount.node.name` | Node service account name | `curvine-csi-node-sa` |

## Customization

### Custom Images

```yaml
image:
  repository: ghcr.io/your-org/curvine-csi
  tag: v0.1.0
  pullPolicy: IfNotPresent

controller:
  sidecars:
    provisioner:
      image: quay.io/k8scsi/csi-provisioner:v1.6.0
    attacher:
      image: registry.k8s.io/sig-storage/csi-attacher:v4.5.0
    livenessProbe:
      image: registry.k8s.io/sig-storage/livenessprobe:v2.11.0

node:
  sidecars:
    nodeDriverRegistrar:
      image: quay.io/k8scsi/csi-node-driver-registrar:v2.1.0
    livenessProbe:
      image: registry.k8s.io/sig-storage/livenessprobe:v2.11.0
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

After installation, create a StorageClass with Curvine cluster configuration:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: curvine-sc
provisioner: curvine
reclaimPolicy: Delete
volumeBindingMode: Immediate
allowVolumeExpansion: true
parameters:
  # Required: Curvine cluster connection information
  master-addrs: "master1:8995,master2:8995,master3:8995"
  
  # Required: Filesystem path prefix for dynamic PV
  # Each dynamic PV will create: fs-path + pv-name
  fs-path: "/data"
  
  # Optional: Path creation strategy
  path-type: "DirectoryOrCreate"  # "DirectoryOrCreate" or "Directory"
  
  # Optional: FUSE parameters
  # io-threads: "4"
  # worker-threads: "8"
```

### StorageClass Parameters

| Parameter | Required | Description | Default |
|-----------|----------|-------------|---------|
| `master-addrs` | Yes | Curvine master node addresses, format: `host:port,host:port` | - |
| `fs-path` | Yes | Filesystem path prefix for PV. Each PV creates `fs-path + pv-name` | - |
| `path-type` | No | Path creation strategy: `DirectoryOrCreate` or `Directory` | `Directory` |
| `io-threads` | No | FUSE IO threads count | - |
| `worker-threads` | No | FUSE worker threads count | - |

### Create a PersistentVolumeClaim

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
  storageClassName: curvine-sc
```

### Use PVC in a Pod

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
spec:
  containers:
    - name: app
      image: nginx
      volumeMounts:
        - mountPath: /data
          name: curvine-volume
  volumes:
    - name: curvine-volume
      persistentVolumeClaim:
        claimName: test-pvc
```

## Uninstallation

```bash
# Uninstall the release
helm uninstall curvine-csi --namespace curvine-system

# Optionally, delete the namespace
kubectl delete namespace curvine-system
```

## Troubleshooting

### Check CSI Driver Status

```bash
# Check CSI driver registration
kubectl get csidriver curvine

# Check controller pod
kubectl get deployment -n curvine-system curvine-csi-controller
kubectl get pods -n curvine-system -l app=curvine-csi-controller

# Check node pods
kubectl get daemonset -n curvine-system curvine-csi-node
kubectl get pods -n curvine-system -l app=curvine-csi-node
```

### Check Logs

```bash
# Controller logs
kubectl logs -n curvine-system -l app=curvine-csi-controller -c csi-plugin

# Node logs
kubectl logs -n curvine-system -l app=curvine-csi-node -c csi-plugin

# Check specific sidecar logs
kubectl logs -n curvine-system -l app=curvine-csi-controller -c csi-provisioner
kubectl logs -n curvine-system -l app=curvine-csi-controller -c csi-attacher
kubectl logs -n curvine-system -l app=curvine-csi-node -c node-driver-registrar
```

### Common Issues

1. **CSI Driver not registered**
   - Check if the node-driver-registrar sidecar is running
   - Verify `/var/lib/kubelet/plugins_registry/` is accessible
   ```bash
   kubectl logs -n curvine-system -l app=curvine-csi-node -c node-driver-registrar
   ```

2. **Mount failures**
   - Verify Curvine cluster connectivity by checking master-addrs in StorageClass
   - Ensure the fs-path exists and is accessible
   - Check FUSE mount status on the node
   ```bash
   kubectl logs -n curvine-system -l app=curvine-csi-node -c csi-plugin
   ```

3. **Permission issues**
   - Ensure proper RBAC permissions are granted
   - Verify ServiceAccounts are created correctly
   ```bash
   kubectl get clusterrole curvine-csi-controller-sa
   kubectl get clusterrole curvine-csi-node-sa
   ```

4. **Volume provisioning stuck**
   - Check controller pod status and logs
   - Verify StorageClass parameters (master-addrs, fs-path)
   ```bash
   kubectl describe pvc <pvc-name>
   kubectl logs -n curvine-system -l app=curvine-csi-controller -c csi-provisioner
   ```

## Support

For support and documentation, visit:
- [Curvine Documentation](https://curvineio.github.io/docs/)
- [GitHub Issues](https://github.com/CurvineIO/curvine/issues)