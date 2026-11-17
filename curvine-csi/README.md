# Curvine CSI driver

A [Container Storage Interface](https://github.com/container-storage-interface/spec) driver that exposes Curvine directories to Kubernetes workloads as persistent volumes. For end-user setup, see the [CSI driver guide](https://curvineio.github.io/docs/User-Manuals/K8S-CSI-Driver/). This page is the map for working in this directory.

## What the driver does

- **Registers as `curvine`.** That name is used by the `CSIDriver` object in [`deploy/csidriver.yaml`](deploy/csidriver.yaml) and by the `provisioner` field of a StorageClass.
- **Dynamic provisioning.** `CreateVolume` creates `<fs-path>/<pv-name>` in Curvine, and `DeleteVolume` removes it.
- **Static provisioning.** A PersistentVolume can point at an existing Curvine directory.
- **Access modes.** `ReadWriteOnce`, `ReadWriteMany` and `ReadOnlyMany`.
- **Mounting.** The node plugin stages a `curvine-fuse` mount and bind-mounts each volume's directory into the pod. On each node, volumes with the same `master-addrs`, `fs-path` and FUSE parameters share one FUSE mount.

### Mount modes

The `MOUNT_MODE` environment variable of the node plugin chooses where `curvine-fuse` runs:

| `MOUNT_MODE` | Where `curvine-fuse` runs | Code |
| --- | --- | --- |
| `standalone` (default) | In a separate mount pod per FUSE mount, created by the node plugin in its own namespace from `STANDALONE_IMAGE` with `STANDALONE_SERVICE_ACCOUNT`. Mount state is kept in a ConfigMap, and the pod restarts if its FUSE mount becomes unhealthy. | [`pkg/csi/node_standalone.go`](pkg/csi/node_standalone.go), [`pkg/csi/standalone_manager.go`](pkg/csi/standalone_manager.go) |
| `embedded` | Inside the CSI node container (legacy) | [`pkg/csi/node.go`](pkg/csi/node.go) |

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

# Format and vet, as build/format-csi.sh does
go fmt ./... && go vet ./...
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
- **Kubelet directory.** The node plugin assumes `/var/lib/kubelet`, in its `hostPath` volumes and in `DRIVER_REG_SOCK_PATH`. Distributions with another kubelet root need all of these changed.
- **Images.** `deployment.yaml` and `daemonset.yaml` use `ghcr.io/curvineio/curvine-csi:latest`, and `STANDALONE_IMAGE` sets the mount pod image.
- **Mount mode.** Set `MOUNT_MODE` in `daemonset.yaml`. `STANDALONE_SERVICE_ACCOUNT` must stay equal to the node plugin's ServiceAccount, `curvine-csi-node-sa`.

## Use

[examples/README.md](examples/README.md) walks through dynamic provisioning with a StorageClass, static provisioning with a PV, and Pod, Deployment and StatefulSet workloads, plus troubleshooting and clean-up.

## Configuration

Volumes are configured through StorageClass `parameters` for dynamic provisioning and PersistentVolume `volumeAttributes` for static provisioning. The driver handles `master-addrs`, `fs-path` and `path-type` itself, a static PV also takes `curvine-path`, and both reject `mnt-path`. Any other key is passed to `curvine-fuse` as `--key value`, for example FUSE flags such as `io-threads` or client overrides such as `client.block-size`. The comments at the top of [`examples/storage-class.yaml`](examples/storage-class.yaml) and [`examples/pv-curvine.yaml`](examples/pv-curvine.yaml) explain the rules, and the [CSI driver guide](https://curvineio.github.io/docs/User-Manuals/K8S-CSI-Driver/) covers the rest.

## Known limitations

- **No volume expansion.** `ControllerExpandVolume` in [`pkg/csi/controller.go`](pkg/csi/controller.go) and `NodeExpandVolume` in both node services return `Unimplemented`, and `deploy/` has no `csi-resizer` sidecar. Resizing a PVC therefore does not complete, even though [`examples/storage-class.yaml`](examples/storage-class.yaml) sets `allowVolumeExpansion: true`.
- **No volume stats.** `NodeGetVolumeStats` returns `Unimplemented`, so kubelet reports no capacity or usage metrics for these volumes.
- **No snapshots or capacity reporting.** The snapshot RPCs, `ListVolumes`, `GetCapacity` and `ControllerGetVolume` return `Unimplemented`.
