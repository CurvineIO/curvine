# Curvine CSI examples

These manifests show how to use the Curvine CSI driver from Kubernetes workloads.
They assume that a Curvine cluster is running and that the CSI driver is installed
(see the [CSI guide](https://curvineio.github.io/docs/User-Manuals/K8S-CSI-Driver/)).

All namespaced examples use the `curvine` namespace, which is the namespace created
by `curvine-csi/deploy/namespace.yaml`. Create it first if you installed the driver
somewhere else:

```bash
kubectl create namespace curvine
```

## Files

| File | Kind | What it shows |
| --- | --- | --- |
| `storage-class.yaml` | StorageClass `curvine-sc` | Dynamic provisioning: each PVC gets its own directory under `fs-path` |
| `pvc-curvine.yaml` | PVC `curvine-pvc` | A dynamically provisioned `ReadWriteMany` volume |
| `pod-curvine.yaml` | Pod `curvine-test-pod` | A single pod that mounts `curvine-pvc` at `/data` |
| `deployment-curvine.yaml` | PVC `curvine-shared-pvc` + Deployment | Three replicas sharing one `ReadWriteMany` volume |
| `statefulset-curvine.yaml` | StatefulSet + headless Service | One `ReadWriteOnce` volume per pod through `volumeClaimTemplates` |
| `pv-curvine.yaml` | PersistentVolume `curvine-pv` | Static provisioning: expose an existing Curvine directory |
| `pvc-static.yaml` | PVC `curvine-pvc-static` | Binds to `curvine-pv` by name |

## Values to edit

Change these before applying anything:

- **`master-addrs`** in `storage-class.yaml` and in `pv-curvine.yaml`: the
  comma-separated `host:port` list of your Curvine masters. The examples use
  `m0:8995,m1:8995,m2:8995`.
- **`fs-path`** in `storage-class.yaml`: the directory prefix for dynamic volumes.
  A PV named `pvc-xxx` is mounted from `<fs-path>/pvc-xxx`.
- **`curvine-path`** in `pv-curvine.yaml`: the existing Curvine directory to
  expose. With the default `path-type: Directory` the directory must already exist.
- **`volumeHandle`** in `pv-curvine.yaml`: any string that is unique among your
  static PVs.
- Storage sizes and `namespace` fields, if you are not using `curvine`.

The comments at the top of `storage-class.yaml` and `pv-curvine.yaml` describe the
reserved keys and the optional passthrough parameters (`io-threads`,
`client.block-size`, ...).

## Dynamic provisioning

Apply the StorageClass once, then any of the workloads that use it:

```bash
kubectl apply -f storage-class.yaml

# Single pod
kubectl apply -f pvc-curvine.yaml
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/curvine-pvc -n curvine --timeout=120s
kubectl apply -f pod-curvine.yaml
kubectl wait --for=condition=Ready pod/curvine-test-pod -n curvine --timeout=120s

# Shared volume across replicas (the PVC is defined in the same file)
kubectl apply -f deployment-curvine.yaml

# One volume per replica
kubectl apply -f statefulset-curvine.yaml
```

Verify the mount by writing and reading a file:

```bash
kubectl exec -n curvine curvine-test-pod -- sh -c 'echo hello > /data/hello.txt'
kubectl exec -n curvine curvine-test-pod -- ls -l /data
kubectl exec -n curvine curvine-test-pod -- cat /data/hello.txt
```

The Deployment and the StatefulSet mount their volume at `/usr/share/nginx/html`
and write an `index.html` there on startup:

```bash
# curvine-test-pod also carries app=curvine-test, so select a Deployment pod
# by its pod-template-hash label instead of using deploy/curvine-test-deployment.
POD=$(kubectl get pod -n curvine -l 'app=curvine-test,pod-template-hash' \
  -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n curvine "$POD" -- cat /usr/share/nginx/html/index.html
kubectl exec -n curvine curvine-statefulset-0 -- cat /usr/share/nginx/html/index.html
```

## Static provisioning

Use this to mount data that already exists in Curvine. The PV is cluster-scoped;
the PVC binds to it by name through `volumeName`:

```bash
kubectl apply -f storage-class.yaml   # the PV and PVC refer to curvine-sc
kubectl apply -f pv-curvine.yaml
kubectl apply -f pvc-static.yaml
kubectl get pv curvine-pv
kubectl get pvc curvine-pvc-static -n curvine
```

To use it from a pod, copy `pod-curvine.yaml` to `pod-static.yaml`, change
`metadata.name` to `curvine-test-pod-static`, and set
`spec.volumes[0].persistentVolumeClaim.claimName` to `curvine-pvc-static`.
Give the copy a new name: a Pod's volumes cannot be changed in place, so
re-applying `curvine-test-pod` with a different claim is rejected if the
dynamic example is still running.

```bash
kubectl apply -f pod-static.yaml
kubectl wait --for=condition=Ready pod/curvine-test-pod-static -n curvine --timeout=120s
```

## Troubleshooting

- PVC stuck in `Pending`: `kubectl describe pvc <name> -n curvine` and check the
  controller logs (`kubectl logs -n curvine deploy/curvine-csi-controller --all-containers`).
  `CreateVolume` validates the StorageClass parameters before it creates the PV,
  so a wrong `master-addrs` or an unknown passthrough key shows up there.
- Pod stuck in `ContainerCreating`: `kubectl describe pod <name> -n curvine` and
  check the node plugin logs (`kubectl logs -n curvine ds/curvine-csi-node --all-containers`).

## Clean up

Delete workloads before their claims, and claims before the StorageClass:

```bash
kubectl delete -f statefulset-curvine.yaml -f deployment-curvine.yaml -f pod-curvine.yaml --ignore-not-found
kubectl delete pod curvine-test-pod-static -n curvine --ignore-not-found
kubectl delete pvc -n curvine -l app=curvine-stateful --ignore-not-found
kubectl delete -f pvc-curvine.yaml -f pvc-static.yaml --ignore-not-found
kubectl delete -f pv-curvine.yaml -f storage-class.yaml --ignore-not-found
```

`curvine-sc` uses `reclaimPolicy: Delete`, so deleting a dynamic PVC also removes
its directory under `fs-path`. The static PV uses `Retain`, so deleting it leaves
the data in Curvine untouched.
