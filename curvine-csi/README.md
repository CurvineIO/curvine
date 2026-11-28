# Curvine CSI driver

A [Container Storage Interface](https://github.com/container-storage-interface/spec) driver that exposes Curvine directories to Kubernetes workloads as persistent volumes. For end-user setup, see the [CSI driver guide](https://curvineio.github.io/docs/User-Manuals/K8S-CSI-Driver/). This page is the map for working in this directory.

## What the driver does

- **Registers as `curvine`.** That name is used by the `CSIDriver` object in [`deploy/csidriver.yaml`](deploy/csidriver.yaml) and by the `provisioner` field of a StorageClass.
- **Dynamic provisioning.** Each volume is the Curvine directory `<fs-path>/<pv-name>`. `CreateVolume` creates it, and a missing `fs-path`, only when the StorageClass sets `path-type: DirectoryOrCreate`. With the default, `Directory`, both must already exist or provisioning fails ([`pkg/csi/validator.go`](pkg/csi/validator.go), [`pkg/csi/controller.go`](pkg/csi/controller.go)). When a PV with the `Delete` reclaim policy is released, `DeleteVolume` removes the directory, using the `master-addrs` stored in the PV to reach the cluster.
- **Static provisioning.** A PersistentVolume can point at an existing Curvine directory.
- **Access modes.** `ReadWriteOnce`, `ReadWriteMany` and `ReadOnlyMany`.
- **Mounting.** The node plugin stages a `curvine-fuse` mount and bind-mounts each volume's directory into the pod. On each node, volumes with the same `master-addrs`, `fs-path` and FUSE parameters share one FUSE mount.

### Mount modes

The `MOUNT_MODE` environment variable of the node plugin chooses where `curvine-fuse` runs:

| `MOUNT_MODE` | Where `curvine-fuse` runs | Code |
| --- | --- | --- |
| `standalone` (default) | In a separate mount pod per FUSE mount, created by the node plugin in its own namespace from `STANDALONE_IMAGE` with `STANDALONE_SERVICE_ACCOUNT`. Mount state is kept in a ConfigMap, and the pod restarts if its FUSE mount becomes unhealthy. | [`pkg/csi/node_standalone.go`](pkg/csi/node_standalone.go), [`pkg/csi/standalone_manager.go`](pkg/csi/standalone_manager.go) |
| `embedded` | Inside the CSI node container (legacy) | [`pkg/csi/node.go`](pkg/csi/node.go) |

### Abandoned staging in standalone mode

New volume references carry a durable pending-stage attempt ID until readiness
and its state update succeed. A failed or cancelled attempt can be reclaimed by
the node plugin's existing garbage collector even if its PV still exists:
each category starts two minutes after startup and waits ten minutes between
completed passes. Pending stages, orphan Pods and deleted-PV references run
independently, with up to four concurrent Pod groups per category. Operations
sharing a physical Pod remain serialized. Cleanup
waits for in-flight staging and rechecks the attempt ID, preserving successful
restaging and other volumes sharing the FUSE Pod. Publish requires an established
reference for that volume. Failed deletion, host unmount or state persistence
retains cleanup ownership for retry. Annotated Pods created after a timed-out
Create are discovered independently of reference state on a later pass. Before
deleting an orphan Pod, cleanup persists an empty state entry so host unmount
can still be retried after the Pod disappears or the plugin restarts.

Pod adoption and deletion validate the node binding and, when present, the full
mount-key annotation. Shortened Pod-name collisions are rejected before new
stage intent is recorded. Previously recorded rejected attempts clean their own
local mount and reference without deleting the other owner's Pod. Snapshot-pending
references are excluded from PV-based collection and use attempt-checked cleanup.

This applies to newly tracked, incomplete stages, not all unused PVs. Established
and legacy references still require unstage or PV deletion; they are never
expired merely because a consumer disappears. If promotion committed but its
response was lost and the plugin restarted, recovery conservatively treats that
reference as established. Older binaries ignore the optional pending-stage field;
downgrading can lose automatic cleanup eligibility. No consumer-data deletion or
embedded-mode behavior changes are involved.

## Layout

| Path | Contents |
| --- | --- |
| `main.go` | Entry point. Flags: `--endpoint`, `--nodeid`, `--version`, `--version-json` |
| `pkg/csi/` | Driver: identity, controller and node services, mount modes, FUSE command line, Kubernetes clients |
| `deploy/` | Manifests that install the driver in the `curvine` namespace |
| `examples/` | StorageClass, PVC, PV, Pod, Deployment and StatefulSet examples, see [examples/README.md](examples/README.md) |
| `Dockerfile` | Full image build: compiles Curvine (`curvine-fuse`, `curvine-cli`) and the CSI binary from the repository root |
| `Dockerfile.quick` | Development image: layers locally built binaries over the published image |
| `go.mod` | Go module `github.com/curvineio/curvine-csi` |

## Build and test

You need the Go version in [`go.mod`](go.mod) (currently 1.26). The driver builds on any platform, but it mounts filesystems, so it only runs on Linux nodes.

```bash
cd curvine-csi
go build -o bin/csi .
go test ./...

# List unformatted files and vet, as build/format-csi.sh does
gofmt -l .
go vet ./...
```

The container image needs the rest of the repository, because it also builds `curvine-fuse` and `curvine-cli`. Build it from the repository root:

```bash
docker build -f curvine-csi/Dockerfile -t curvine-csi:dev .
```

For a faster loop, `Dockerfile.quick` starts from `ghcr.io/curvineio/curvine-csi:latest` and copies in `curvine-csi/csi-binary`, `curvine-csi/quick-build/curvine-cli` and `curvine-csi/quick-build/curvine-fuse`, which you build first.

## Deploy

Apply the manifests in this order:

```bash
kubectl apply -f curvine-csi/deploy/namespace.yaml
kubectl apply -f curvine-csi/deploy/serviceaccount.yaml
kubectl apply -f curvine-csi/deploy/clusterrole.yaml
kubectl apply -f curvine-csi/deploy/clusterrolebinding.yaml
kubectl apply -f curvine-csi/deploy/csidriver.yaml
kubectl apply -f curvine-csi/deploy/deployment.yaml   # controller: CSI plugin, csi-provisioner, csi-attacher
kubectl apply -f curvine-csi/deploy/daemonset.yaml    # node plugin, plus the ClusterRole and binding for mount pods
```

Check that the driver is registered and the pods are ready:

```bash
kubectl get csidriver curvine
kubectl -n curvine get pods
```

What to edit before you apply, if your cluster differs:

- **Namespace.** Everything uses `curvine`. Change the `namespace` fields and the ClusterRoleBinding subjects in `clusterrolebinding.yaml` and `daemonset.yaml` together.
- **Kubelet directory.** The manifests assume `/var/lib/kubelet`, in the node plugin's `hostPath` volumes and in `DRIVER_REG_SOCK_PATH`. The driver code assumes it too: standalone mount pods mount under `/var/lib/kubelet/plugins/curvine` (`HostMountBaseDir` in [`pkg/csi/standalone_manager.go`](pkg/csi/standalone_manager.go)), and embedded mode mounts under `/var/lib/kubelet/plugins/kubernetes.io/csi/curvine` (`fuseKubeletPluginBase` in [`pkg/csi/fuse_cli.go`](pkg/csi/fuse_cli.go)). For another kubelet root, change the manifests and these constants together, then rebuild the image.
- **Images.** `deployment.yaml` and `daemonset.yaml` use `ghcr.io/curvineio/curvine-csi:latest`. `STANDALONE_IMAGE` sets the mount pod image; when it is empty, the node plugin uses its own image. Mount pods run on the node that needs the mount and use `imagePullPolicy: Never`, so their image must already be on every node. The node plugin's own image is, because the DaemonSet pulled it; load any other image onto every node first.
- **Mount mode.** Set `MOUNT_MODE` in `daemonset.yaml`. `STANDALONE_SERVICE_ACCOUNT` must stay equal to the node plugin's ServiceAccount, `curvine-csi-node-sa`.

## Use

[examples/README.md](examples/README.md) walks through dynamic provisioning with a StorageClass, static provisioning with a PV, and Pod, Deployment and StatefulSet workloads, plus troubleshooting and clean-up.

## Configuration

Volumes are configured through StorageClass `parameters` for dynamic provisioning and PersistentVolume `volumeAttributes` for static provisioning. The driver handles `master-addrs`, `fs-path` and `path-type` itself, a static PV also takes `curvine-path`, and both reject `mnt-path`. Any other key is passed to `curvine-fuse` as `--key value`, for example FUSE flags such as `io-threads` or client overrides such as `client.block-size`. The comments at the top of [`examples/storage-class.yaml`](examples/storage-class.yaml) and [`examples/pv-curvine.yaml`](examples/pv-curvine.yaml) explain the rules, and the [CSI driver guide](https://curvineio.github.io/docs/User-Manuals/K8S-CSI-Driver/) covers the rest.

## Known limitations

- **No volume expansion.** `ControllerExpandVolume` in [`pkg/csi/controller.go`](pkg/csi/controller.go) and `NodeExpandVolume` in both node services return `Unimplemented`, and `deploy/` has no `csi-resizer` sidecar, so a PVC cannot be resized. [`examples/storage-class.yaml`](examples/storage-class.yaml) sets `allowVolumeExpansion: false` for this reason.
- **No volume stats.** `NodeGetVolumeStats` returns `Unimplemented`, so kubelet reports no capacity or usage metrics for these volumes.
- **No snapshots or capacity reporting.** The snapshot RPCs, `ListVolumes`, `GetCapacity` and `ControllerGetVolume` return `Unimplemented`.

#### Ownership-state loss and host mount verification

If the node's state ConfigMap or `state.json` is missing during recovery, the
plugin records an uncertain-ownership recovery epoch. It does not adopt or
reclaim Pods from before that epoch merely because their references are absent.
New Pods carry the epoch annotation and remain eligible for normal cleanup.
The uncertainty and epoch survive subsequent state writes and restarts.
Previously existing Pods require operator reconciliation of their volume owners
and restoration of valid ownership state; do not clear the uncertainty flag on
an empty state file as a cleanup shortcut. Downgrading to a version unaware of
these fields loses this protection.

Stage and publish require a Curvine FUSE mount visible in the node plugin's
mount namespace, not just a Ready Pod or an existing directory. Legacy
`fuse` mounts with source `curvinefs` are accepted alongside `fuse.curvinefs`.
Missing, malformed, foreign, or ambiguous stacked mounts fail closed. Publishing
holds the mount lifecycle lock through the bind operation. A failed new stage
keeps its pending intent so garbage collection can reclaim a late Ready Pod
whose host mount has disappeared and a subsequent stage can recreate it.
