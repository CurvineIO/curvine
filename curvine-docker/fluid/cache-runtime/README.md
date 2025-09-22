# Curvine Fluid CacheRuntime Complete Deployment Guide

This guide provides comprehensive deployment, testing, and troubleshooting solutions for Curvine integration with Fluid CacheRuntime.

## Table of Contents

1. [Environment Setup](#environment-setup)
2. [Build Required Components](#build-required-components)
3. [Deploy Fluid System](#deploy-fluid-system)
4. [Deploy Curvine CacheRuntime](#deploy-curvine-cacheruntime)
5. [Storage Configuration](#storage-configuration)
6. [Testing and Validation](#testing-and-validation)
7. [Troubleshooting](#troubleshooting)
8. [Usage Guide](#usage-guide)

## Environment Setup

### Prerequisites

- Kubernetes cluster (v1.20+ recommended)
- Docker
- kubectl
- minikube (for local testing)
- Git

### Environment Verification

```bash
# Check Kubernetes cluster
kubectl cluster-info

# Check node status
kubectl get nodes

# Expected output: All nodes in Ready status
```

## Build Required Components

> **Important Note**: CacheRuntime functionality is currently in development branch and requires building components from source.

### Step 1: Get Fluid Source Code

```bash
# Clone Fluid source code
git clone https://github.com/fluid-cloudnative/fluid.git /tmp/fluid
cd /tmp/fluid

# Switch to CacheRuntime feature branch
git checkout feature/generic-cache-runtime

# Verify branch
git branch
# Expected output: * feature/generic-cache-runtime
```

### Step 2: Build CacheRuntime Controller

```bash
cd /tmp/fluid

# Build CacheRuntime controller image
make docker-build-cacheruntime-controller

# Verify image build success
docker images | grep cacheruntime-controller
# Expected output: fluidcloudnative/cacheruntime-controller   v1.0.7-da106f77

# Load image to minikube
minikube image load fluidcloudnative/cacheruntime-controller:v1.0.7-da106f77
```

### Step 3: Build CacheRuntime-enabled CSI Driver

```bash
cd /tmp/fluid

# Build CSI driver image
make docker-build-csi

# Verify image build success
docker images | grep fluid-csi
# Expected output: fluidcloudnative/fluid-csi   v1.0.7-da106f77

# Load image to minikube
minikube image load fluidcloudnative/fluid-csi:v1.0.7-da106f77
```

### Step 4: Build Curvine Image

```bash
cd /path/to/curvine/curvine-docker/fluid/cache-runtime

# Build Curvine image
./build-image.sh

# Verify image build success
docker images | grep curvine
# Expected output: curvine   latest

# Load image to minikube
minikube image load curvine-fluid-cacheruntime:latest
```

## Deploy Fluid System

### Step 1: Uninstall Existing Fluid (if exists)

```bash
# Check existing Fluid installation
helm list -n fluid-system

# If exists, uninstall first
helm uninstall fluid -n fluid-system
kubectl delete namespace fluid-system
```

### Step 2: Deploy CacheRuntime-enabled Fluid

```bash
cd /tmp/fluid

# Create namespace
kubectl create namespace fluid-system

# Install Fluid (enable CacheRuntime)
helm install fluid charts/fluid/fluid \
  --namespace fluid-system \
  --set runtime.cache.enabled=true \
  --set image.fluidcloudnative.cacheruntime-controller.repository=fluidcloudnative/cacheruntime-controller \
  --set image.fluidcloudnative.cacheruntime-controller.tag=v1.0.7-da106f77
```

### Step 3: Verify Fluid Deployment

```bash
# Wait for all pods to be ready
kubectl wait --for=condition=ready pod --all -n fluid-system --timeout=300s

# Check Fluid component status
kubectl get pods -n fluid-system

# Expected output:
# NAME                                       READY   STATUS    RESTARTS   AGE
# cacheruntime-controller-xxx                1/1     Running   0          2m
# csi-nodeplugin-fluid-xxx                   2/2     Running   0          2m
# dataset-controller-xxx                     1/1     Running   0          2m
# fluid-webhook-xxx                          1/1     Running   0          2m
# fluidapp-controller-xxx                    1/1     Running   0          2m

# Check CacheRuntime CRDs
kubectl get crd | grep cache
# Expected output:
# cacheruntimeclasses.data.fluid.io
# cacheruntimes.data.fluid.io
```

### Step 4: Update CSI Driver

```bash
# Update CSI DaemonSet to use CacheRuntime-enabled image
kubectl patch daemonset -n fluid-system csi-nodeplugin-fluid -p '{"spec":{"template":{"spec":{"containers":[{"name":"plugins","image":"fluidcloudnative/fluid-csi:v1.0.7-da106f77"}]}}}}'

# Wait for CSI pods to be recreated
kubectl rollout status daemonset/csi-nodeplugin-fluid -n fluid-system

# Verify CSI pod status
kubectl get pods -n fluid-system | grep csi
# Expected output: csi-nodeplugin-fluid-xxx   2/2   Running   0   1m
```

## Deploy Curvine CacheRuntime

### Step 1: Create CacheRuntimeClass

```bash
cd /path/to/curvine/curvine-docker/fluid/cache-runtime

# Apply CacheRuntimeClass
kubectl apply -f curvine-cache-runtime-class.yaml

# Verify creation
kubectl get cacheruntimeclass
# Expected output:
# NAME      AGE
# curvine   1m
```

### Step 2: Add Node Labels

```bash
# Add Fluid label to node (for client pod scheduling)
kubectl label nodes minikube fluid.io/f-default-curvine-demo=true

# Verify label addition
kubectl get nodes --show-labels | grep fluid
# Expected output should contain: fluid.io/f-default-curvine-demo=true
```

### Step 3: Create Dataset and CacheRuntime

```bash
# Apply Dataset and CacheRuntime
kubectl apply -f deploy/curvine-dataset.yaml

# Verify resource creation
kubectl get dataset,cacheruntime
# Expected output:
# NAME                                    UFS TOTAL SIZE   CACHED   CACHE CAPACITY   CACHED PERCENTAGE   PHASE   AGE
# dataset.data.fluid.io/curvine-demo                                                                     Bound   1m
# 
# NAME                                      MASTER PHASE   WORKER PHASE   FUSE PHASE   AGE
# cacheruntime.data.fluid.io/curvine-demo  Ready          Ready          Ready        1m
```

### Step 4: Verify Curvine Cluster Deployment

```bash
# Check all Curvine pod status
kubectl get pods | grep curvine

# Expected output:
# curvine-demo-client-xxx      1/1     Running   0   2m
# curvine-demo-master-0        1/1     Running   0   2m
# curvine-demo-worker-0        1/1     Running   0   2m

# Check PVC status
kubectl get pvc
# Expected output:
# NAME            STATUS   VOLUME                CAPACITY   ACCESS MODES   STORAGECLASS   AGE
# curvine-demo    Bound    default-curvine-demo  100Pi      RWX            fluid          2m
```

## Storage Configuration

### Storage Architecture

#### Master Components
- **Metadata Storage**: RocksDB database for file system metadata
- **Journal Storage**: Raft consensus logs for high availability
- **Log Storage**: Application and audit logs

#### Worker Components  
- **Multi-Tier Cache**: Memory, SSD, and HDD storage tiers
- **Data Storage**: Actual cached file data
- **Log Storage**: Worker operation logs

### Storage Path Configuration

#### Master Storage Paths

| Component | Default Path | Description | Recommended Storage |
|-----------|--------------|-------------|-------------------|
| Metadata | `/opt/curvine/data/meta` | RocksDB metadata | SSD (10-50GB) |
| Journal | `/opt/curvine/data/journal` | Raft consensus logs | SSD (5-10GB) |
| Logs | `/opt/curvine/logs` | Application logs | Any (5-10GB) |

#### Worker Storage Types

| Prefix | Storage Type | Use Case | Performance | Capacity |
|--------|-------------|----------|-------------|----------|
| `[MEM]` | Memory | Hot data cache | Highest | Limited by RAM |
| `[SSD]` | SSD | Warm data cache | High | 100GB-1TB |
| `[NVME]` | NVMe SSD | Ultra-fast cache | Highest | 100GB-1TB |
| `[HDD]` | Hard Disk | Cold data cache | Medium | 1TB+ |
| `[DISK]` | Generic Disk | Default storage | Medium | Variable |

#### Storage Path Format

```
[StorageType:Capacity]MountPath
```

**Examples:**
- `[MEM:8GB]/cache-mem` - 8GB memory cache
- `[SSD:100GB]/cache-ssd` - 100GB SSD cache  
- `[HDD]/cache-hdd` - Unlimited HDD cache
- `/cache-data` - Default disk storage

### Important: Memory Storage Configuration

**Just configuring `[MEM:8GB]/cache-mem` is NOT enough!**

For memory storage to work properly, you need **both**:

1. **Curvine Configuration**: `data_dir: "[MEM:8GB]/cache-mem"`
   - Tells Curvine this is high-performance memory storage
   - Used for data placement and caching strategies
   - Curvine worker creates directory structure using standard filesystem calls

2. **Kubernetes Volume Configuration**: 
   ```yaml
   volumes:
   - name: memory-cache
     emptyDir:
       medium: Memory      # Creates tmpfs memory filesystem
       sizeLimit: 8Gi
   
   volumeMounts:
   - name: memory-cache
     mountPath: /cache-mem  # Must match the path in data_dir
   ```

**Without the Kubernetes volume configuration, the path will be treated as regular disk storage!**

### Configuration Examples

All configuration examples are available in the unified `deploy/curvine-dataset.yaml` file. Simply uncomment the sections you need:

#### 1. Basic Configuration (Default - Testing/Development)

```yaml
apiVersion: data.fluid.io/v1alpha1
kind: CacheRuntime
metadata:
  name: curvine-basic
spec:
  volumes:
  - name: curvine-cache
    emptyDir:
      sizeLimit: 10Gi
  
  worker:
    options:
      data_dir: "[SSD]/cache-data"
    volumeMounts:
    - name: curvine-cache
      mountPath: /cache-data
    tieredStore:
      levels:
      - quota: 10Gi
        low: "0.5"
        high: "0.8"
        path: "/cache-data"
        medium:
          emptyDir:
            sizeLimit: 10Gi
```

#### 2. Multi-Tier Configuration (Advanced)

To enable multi-tier storage, uncomment the relevant sections in `curvine-dataset.yaml`:

```yaml
# Uncomment these volume definitions:
# - name: memory-cache
#   emptyDir:
#     medium: Memory
#     sizeLimit: 32Gi
# - name: ssd-cache
#   persistentVolumeClaim:
#     claimName: ssd-pvc

# Uncomment the multi-tier data_dir configuration:
# data_dir: "[MEM:32GB]/cache-mem,[SSD:500GB]/cache-ssd,[HDD:2TB]/cache-hdd"

# Uncomment the additional volume mounts:
# - name: memory-cache
#   mountPath: /cache-mem
# - name: ssd-cache
#   mountPath: /cache-ssd

# Uncomment the multi-tier tieredStore configuration:
# tieredStore:
#   levels:
#   - quota: 32Gi
#     low: "0.85"
#     high: "0.95"
#     path: "/cache-mem"
#     medium:
#       emptyDir:
#         medium: Memory
```

#### 3. Production Configuration

For production deployment, uncomment the production sections in `curvine-dataset.yaml`:

```yaml
# Uncomment persistent volume definitions:
# - name: master-meta
#   persistentVolumeClaim:
#     claimName: master-meta-pvc

# Uncomment production resource limits:
# resources:
#   requests:
#     memory: "2Gi"
#     cpu: "1000m"
#   limits:
#     memory: "8Gi"
#     cpu: "4000m"

# Uncomment the PVC definitions at the end of the file
```

### Performance Tuning

#### Memory Tier Optimization
- **Size**: 10-30% of node RAM
- **Watermarks**: High=0.95, Low=0.85 (aggressive eviction)
- **Use Case**: Frequently accessed hot data

#### SSD Tier Optimization  
- **Size**: 100GB-1TB depending on workload
- **Watermarks**: High=0.9, Low=0.7 (balanced)
- **Use Case**: Warm data with good performance

#### HDD Tier Optimization
- **Size**: 1TB+ for large datasets
- **Watermarks**: High=0.8, Low=0.5 (conservative)
- **Use Case**: Cold data archival

### Memory Storage Verification

To verify memory storage is working correctly:

```bash
# Check if tmpfs is mounted (inside worker pod)
kubectl exec curvine-demo-worker-0 -- df -h /cache-mem
kubectl exec curvine-demo-worker-0 -- mount | grep tmpfs

# Check Curvine recognizes memory storage
kubectl exec curvine-demo-worker-0 -- cat /opt/curvine/conf/curvine-cluster.toml | grep -A5 data_dir

# Monitor memory usage
kubectl exec curvine-demo-worker-0 -- free -h
kubectl top pod curvine-demo-worker-0
```

**Expected output for memory storage:**
```bash
# df -h should show tmpfs filesystem
tmpfs           32G     0   32G   0% /cache-mem

# mount should show tmpfs
tmpfs on /cache-mem type tmpfs (rw,nosuid,nodev,noexec,relatime,size=34359738368)

# Curvine config should show MEM type
data_dir = ["[MEM:32GB]/cache-mem"]
```

### Storage Planning Guidelines

#### Development Environment
- **Master**: 1 replica, emptyDir volumes
- **Worker**: 1 replica, single-tier SSD storage
- **Capacity**: 10-50GB total

#### Testing Environment  
- **Master**: 1 replica, emptyDir volumes
- **Worker**: 2-3 replicas, multi-tier (Memory+SSD)
- **Capacity**: 100-500GB total

#### Production Environment
- **Master**: 3 replicas, persistent volumes
- **Worker**: 5+ replicas, multi-tier (Memory+SSD+HDD)  
- **Capacity**: 1TB+ total
- **Backup**: Regular metadata backups
- **Monitoring**: Resource usage alerts

### Common Storage Issues

#### Memory Storage Not Working
1. **Symptom**: Files stored on disk instead of memory
2. **Cause**: Missing `emptyDir: {medium: Memory}` in Kubernetes
3. **Fix**: Add proper volume configuration

#### Performance Issues
1. **Symptom**: Slow cache access
2. **Cause**: Wrong storage tier configuration  
3. **Fix**: Adjust watermark levels and tier sizes

#### Capacity Issues
1. **Symptom**: Cache eviction too frequent
2. **Cause**: Undersized storage tiers
3. **Fix**: Increase tier quotas or add more tiers

## Testing and Validation

### Test 1: Direct FUSE Filesystem Access

```bash
# Get client pod name
CLIENT_POD=$(kubectl get pods | grep curvine-demo-client | awk '{print $1}')

# Test file write
kubectl exec $CLIENT_POD -- sh -c 'echo "Hello Curvine!" > /runtime-mnt/cache/default/curvine-demo/cache-fuse/test1.txt'

# Test file read
kubectl exec $CLIENT_POD -- cat /runtime-mnt/cache/default/curvine-demo/cache-fuse/test1.txt
# Expected output: Hello Curvine!

# Test directory operations
kubectl exec $CLIENT_POD -- sh -c 'mkdir -p /runtime-mnt/cache/default/curvine-demo/cache-fuse/testdir && echo "Directory test" > /runtime-mnt/cache/default/curvine-demo/cache-fuse/testdir/file.txt'

# Verify directory content
kubectl exec $CLIENT_POD -- ls -la /runtime-mnt/cache/default/curvine-demo/cache-fuse/
# Expected output should contain: test1.txt and testdir/
```

### Test 2: PVC Mount Access

```bash
# Create test pod
kubectl apply -f deploy/test_pod.yaml

# Wait for pod ready
kubectl wait --for=condition=ready pod test-curvine --timeout=60s

# Check pod status
kubectl get pod test-curvine
# Expected output: test-curvine   1/1   Running   0   1m

# View mounted filesystem content
kubectl exec test-curvine -- ls -la /data/
# Expected output should contain previously created test1.txt and testdir/

# Test writing file through PVC
kubectl exec test-curvine -- sh -c 'echo "PVC test success!" > /data/pvc-test.txt'

# Verify file content
kubectl exec test-curvine -- cat /data/pvc-test.txt
# Expected output: PVC test success!
```

### Test 3: Cross-Access Verification

```bash
# Verify files written through PVC can be accessed via direct FUSE
kubectl exec $CLIENT_POD -- cat /runtime-mnt/cache/default/curvine-demo/cache-fuse/pvc-test.txt
# Expected output: PVC test success!

# Verify data consistency
kubectl exec test-curvine -- sh -c 'echo "Cross access test" > /data/cross-test.txt'
kubectl exec $CLIENT_POD -- cat /runtime-mnt/cache/default/curvine-demo/cache-fuse/cross-test.txt
# Expected output: Cross access test
```

### Test 4: Performance Testing

```bash
# Large file write test
kubectl exec test-curvine -- dd if=/dev/zero of=/data/bigfile.dat bs=1M count=10
# Expected output: 10+0 records in/out, 10485760 bytes copied

# Verify file size
kubectl exec test-curvine -- ls -lh /data/bigfile.dat
# Expected output: -rw-r--r-- 1 root root 10M ... bigfile.dat
```

## Troubleshooting

### Issue 1: CacheRuntime Controller ImagePullBackOff

**Symptoms:**
```bash
kubectl get pods -n fluid-system
# cacheruntime-controller-xxx   0/1   ImagePullBackOff   0   5m
```

**Cause:** Official registry doesn't have CacheRuntime controller image

**Solution:**
```bash
# Confirm image is built and loaded
docker images | grep cacheruntime-controller
minikube image load fluidcloudnative/cacheruntime-controller:v1.0.7-da106f77

# Restart pod
kubectl delete pod -n fluid-system -l app=cacheruntime-controller
```

### Issue 2: CSI Mount Failure "fail to get runtimeInfo for runtime type: cache"

**Symptoms:**
```bash
kubectl describe pod test-curvine
# Warning  FailedMount  ... rpc error: code = Unknown desc = NodeStageVolume: failed to get runtime info for default/curvine-demo: fail to get runtimeInfo for runtime type: cache
```

**Cause:** CSI driver doesn't support CacheRuntime

**Solution:**
```bash
# Confirm CSI driver is updated
kubectl get pods -n fluid-system | grep csi
kubectl describe pod -n fluid-system csi-nodeplugin-fluid-xxx | grep Image
# Should show: fluidcloudnative/fluid-csi:v1.0.7-da106f77

# If not, update again
kubectl patch daemonset -n fluid-system csi-nodeplugin-fluid -p '{"spec":{"template":{"spec":{"containers":[{"name":"plugins","image":"fluidcloudnative/fluid-csi:v1.0.7-da106f77"}]}}}}'
```

### Issue 3: Client Pod CrashLoopBackOff "Mnt path is not empty"

**Symptoms:**
```bash
kubectl logs curvine-demo-client-xxx
# ERROR: Mnt /runtime-mnt/cache/default/curvine-demo/cache-fuse is not empty
```

**Cause:** FUSE mount point directory is not empty

**Solution:**
```bash
# Check Curvine image version
kubectl describe pod curvine-demo-client-xxx | grep Image
# Ensure using latest built image

# If needed, rebuild and load image
cd /path/to/curvine/curvine-docker/fluid/cache-runtime
./build-image.sh
minikube image load curvine-fluid-cacheruntime:latest

# Restart client pod
kubectl delete pod curvine-demo-client-xxx
```

### Issue 4: Missing Node Label Prevents Client Pod Scheduling

**Symptoms:**
```bash
kubectl get daemonset curvine-demo-client
# DESIRED   CURRENT   READY   UP-TO-DATE   AVAILABLE   NODE SELECTOR
# 0         0         0       0            0           fluid.io/f-default-curvine-demo=true
```

**Solution:**
```bash
# Add node label
kubectl label nodes minikube fluid.io/f-default-curvine-demo=true

# Verify label
kubectl get nodes --show-labels | grep fluid.io/f-default-curvine-demo=true
```

## Usage Guide

### Application Integration Example

Create an application that uses Curvine cache:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: my-app
spec:
  containers:
  - name: app
    image: nginx:latest
    volumeMounts:
    - mountPath: /data
      name: cache-volume
  volumes:
  - name: cache-volume
    persistentVolumeClaim:
      claimName: curvine-demo
```

### Performance Optimization Recommendations

1. **Adjust Cache Configuration**:
   ```yaml
   # In curvine-dataset.yaml adjust
   tieredStore:
     levels:
     - high: "0.8"
       low: "0.5"
       path: "/cache-data"
       quota: "100Gi"  # Adjust as needed
   ```

2. **Scale Worker Nodes**:
   ```yaml
   # Increase worker replica count
   worker:
     replicas: 4  # Adjust based on cluster size
   ```

### Monitoring and Logging

```bash
# View Master logs
kubectl logs curvine-demo-master-0

# View Worker logs
kubectl logs curvine-demo-worker-0

# View Client logs
kubectl logs curvine-demo-client-xxx

# View Fluid controller logs
kubectl logs -n fluid-system deployment/cacheruntime-controller
```

## Summary

Through this guide, you should be able to:

1. ✅ Successfully build all required components
2. ✅ Deploy complete Fluid + CacheRuntime system
3. ✅ Deploy Curvine distributed cache cluster
4. ✅ Configure multi-tier storage for optimal performance
5. ✅ Verify PVC mounting and filesystem functionality
6. ✅ Resolve common deployment issues

Curvine now provides high-performance distributed caching services as a Fluid CacheRuntime, allowing applications to seamlessly access cache through standard Kubernetes PVC methods.

If you encounter issues, please:

1. Check the troubleshooting section in this guide
2. Review relevant component logs
3. Verify all prerequisites are met
4. Confirm using correct image versions

---

**Note**: This integration is based on Fluid's development branch `feature/generic-cache-runtime`. Please ensure thorough testing before production use.
