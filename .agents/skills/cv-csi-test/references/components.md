# CSI Components Reference

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Node                          │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐    ┌──────────────────────────────┐  │
│  │  CSI Node Pod    │    │      Standalone mount pod    │  │
│  │  (restartable)   │    │  (independent, persistent FUSE) │
│  │                  │    │                              │  │
│  │  - NodeService   │    │  - curvine-fuse process      │  │
│  │  - StandaloneMgr │───▶│  - mount: /mnt/curvine       │  │
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
│  │     Bind Mount from host → the pod's volumeMount     │  │
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
  name: curvine
```

Every manifest in `curvine-csi/deploy/` and `curvine-csi/examples/` uses this namespace.

### ServiceAccounts
```yaml
# File: curvine-csi/deploy/serviceaccount.yaml
# Creates:
# - curvine-csi-controller-sa
# - curvine-csi-node-sa
```

### RBAC
- Files: `curvine-csi/deploy/clusterrole.yaml`, `clusterrolebinding.yaml`
- `daemonset.yaml` also creates `curvine-csi-standalone-role` and its binding to
  `curvine-csi-node-sa`, which the node plugin uses to manage standalone mount pods and their
  state ConfigMaps

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

### CSI Controller
- File: `curvine-csi/deploy/deployment.yaml`
- Required for dynamic provisioning (the PVC examples)
- Includes csi-provisioner and csi-attacher sidecars

### CSI Node Plugin
- File: `curvine-csi/deploy/daemonset.yaml`
- `MOUNT_MODE` selects where curvine-fuse runs:

**Standalone mode (`MOUNT_MODE=standalone`, the default)**
- FUSE runs in a separate standalone mount pod
- Enables smooth CSI restart
- Environment variables:
  ```yaml
  env:
    - name: MOUNT_MODE
      value: "standalone"
    - name: STANDALONE_IMAGE          # defaults to the CSI node pod's own image
      value: "$CSI_REGISTRY/curvine-csi:latest"
    - name: STANDALONE_SERVICE_ACCOUNT
      value: "curvine-csi-node-sa"
  ```

  `CSI_REGISTRY` is the registry reachable from the test cluster; export it instead of
  hardcoding a host.

**Embedded mode (`MOUNT_MODE=embedded`)**
- FUSE runs in CSI container
- Simpler but loses mounts on restart
- The default of the Helm chart (`mountMode: embedded`)

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
allowVolumeExpansion: false   # expansion is not implemented yet
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
actual mount path = fs-path + "/" + pv-name
example: "/test-data/pvc-1234-5678-abcd"
```

## Standalone Mount Pod Details

### Lifecycle
1. Created during NodeStageVolume
2. Shared by volumes with the same master-addrs, fs-path prefix and passthrough parameters
   (one mount key)
3. State persisted in the ConfigMap `curvine-standalone-state-<node name>`
4. Survives CSI Node Pod restart
5. Deleted when its last volume is released in NodeUnstageVolume

### Configuration
- Name `curvine-csi-standalone-<first 8 characters of the mount key>-<5-character node hash>`,
  in the node plugin's namespace
- Labels `app=curvine-standalone`, `curvine.io/node`, `curvine.io/cluster-id`
- `hostNetwork: true`
- `hostPID: true`
- `MountPropagation: Bidirectional`
- Mount point: `/mnt/curvine`
- Host path: `/var/lib/kubelet/plugins/curvine/<mount-key>/fuse-mount`

### Health Checks
- StartupProbe, ReadinessProbe and LivenessProbe:
  `grep -qE '^[^ ]+ /mnt/curvine fuse' /proc/mounts` (not `mountpoint`, which can block on FUSE)
