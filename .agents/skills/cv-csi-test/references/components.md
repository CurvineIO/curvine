# CSI Components Reference

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Node                          │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐    ┌──────────────────────────────┐  │
│  │  CSI Node Pod    │    │      MountPod                │  │
│  │  (可重启)         │    │  (独立运行，持久化FUSE)       │  │
│  │                  │    │                              │  │
│  │  - NodeService   │    │  - curvine-fuse 进程         │  │
│  │  - MountPodMgr   │───▶│  - 挂载点: /mnt/curvine      │  │
│  │                  │    │  - HostPath: Bidirectional   │  │
│  └──────────────────┘    └──────────────────────────────┘  │
│           │                         │                       │
│           ▼                         ▼                       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              Host Mount Namespace                     │  │
│  │     /var/lib/kubelet/plugins/kubernetes.io/csi/...   │  │
│  └──────────────────────────────────────────────────────┘  │
│                          │                                  │
│                          ▼                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              Application Pod                          │  │
│  │     Bind Mount from host → /usr/share/nginx/html     │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

### Namespace
```yaml
# File: curvine-csi/deploy/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: curvine-system
```

### ServiceAccounts
```yaml
# File: curvine-csi/deploy/serviceaccount.yaml
# Creates:
# - curvine-csi-controller-sa
# - curvine-csi-node-sa
```

### RBAC
- Files: `curvine-csi/deploy/clusterrole.yaml`, `clusterrolebinding.yaml`
- MountPod mode also includes RBAC in `daemonset-mountpod.yaml`

## Core Components

### CSIDriver
```yaml
# File: curvine-csi/deploy/csidriver.yaml
apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: curvine
spec:
  attachRequired: false
  podInfoOnMount: false
```

### CSI Controller (Optional)
- File: `curvine-csi/deploy/deployment.yaml`
- For dynamic provisioning
- Includes csi-provisioner sidecar

### CSI Node Plugin
Two modes available:

**MountPod Mode (Recommended)**
- File: `curvine-csi/deploy/daemonset-mountpod.yaml`
- FUSE runs in separate MountPod
- Enables smooth CSI restart
- Environment variables:
  ```yaml
  env:
    - name: USE_MOUNT_POD
      value: "true"
    - name: MOUNT_POD_IMAGE
      value: "10.119.43.210:5000/curvine-csi:latest"
  ```

**Traditional Mode**
- File: `curvine-csi/deploy/daemonset.yaml`
- FUSE runs in CSI container
- Simpler but loses mounts on restart

## Storage Configuration

### StorageClass
```yaml
# File: curvine-csi/examples/storage-class.yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: curvine-sc
provisioner: curvine
reclaimPolicy: Delete
volumeBindingMode: Immediate
allowVolumeExpansion: true
parameters:
  master-addrs: "m0:8995,m1:8995,m2:8995"  # Required
  fs-path: "/test-data"                      # Required
  path-type: "DirectoryOrCreate"            # Optional
```

**Parameters:**
- `master-addrs`: Curvine master addresses (required)
- `fs-path`: Filesystem path prefix for dynamic PVs (required)
- `path-type`: "DirectoryOrCreate" or "Directory" (optional, default: "Directory")

**Path Generation:**
```
实际挂载路径 = fs-path + "/" + pv-name
示例: "/test-data/pvc-1234-5678-abcd"
```

## MountPod Details

### MountPod Lifecycle
1. Created during NodeStageVolume
2. One MountPod per Curvine cluster (master-addrs + fs-path)
3. State persisted in ConfigMap
4. Survives CSI Node Pod restart
5. Deleted when refCount=0 in NodeUnstageVolume

### MountPod Configuration
- `hostNetwork: true`
- `hostPID: true`
- `MountPropagation: Bidirectional`
- Mount point: `/mnt/curvine`
- Host path: `/var/lib/kubelet/plugins/curvine/<cluster-id>`

### MountPod Health Checks
- ReadinessProbe: `mountpoint -q /mnt/curvine`
- LivenessProbe: `mountpoint -q /mnt/curvine`

