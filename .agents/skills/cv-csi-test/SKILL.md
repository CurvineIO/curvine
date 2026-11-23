---
name: cv-csi-test
description: End-to-end Curvine CSI driver testing guide for Kubernetes deployment, StorageClass and standalone mount pod setup, resilience scenarios, and scripted test suites. Use when validating CSI code changes, running integration tests, debugging mount failures, or verifying standalone mount pod recovery after node or CSI restarts.
---

# cv-csi-test

This skill provides comprehensive guidance for testing Curvine CSI driver, including component deployment, test pod configuration, and complete testing workflows.

## Quick Start

### 1. Deploy Components

Deploy in order:
```bash
kubectl apply -f curvine-csi/deploy/namespace.yaml
kubectl apply -f curvine-csi/deploy/serviceaccount.yaml
kubectl apply -f curvine-csi/deploy/clusterrole.yaml
kubectl apply -f curvine-csi/deploy/clusterrolebinding.yaml
kubectl apply -f curvine-csi/deploy/csidriver.yaml
kubectl apply -f curvine-csi/deploy/deployment.yaml   # controller: provisions dynamic PVs
kubectl apply -f curvine-csi/deploy/daemonset.yaml    # node plugin, MOUNT_MODE=standalone
kubectl apply -f curvine-csi/examples/storage-class.yaml
```

The manifests and the examples use the `curvine` namespace. The Helm chart
(`curvineio/curvine-csi`) installs the driver in its release namespace instead and defaults to
`mountMode: embedded`, which creates no standalone mount pods: the restart tests below need
standalone mode.

### 2. Verify Deployment

```bash
kubectl get pods -n curvine -l app=curvine-csi-controller
kubectl get pods -n curvine -l app=curvine-csi-node
kubectl get csidriver curvine
kubectl get storageclass curvine-sc
```

### 3. Run Basic Test

`pod-curvine.yaml` mounts the PVC at `/data`:

```bash
kubectl apply -f curvine-csi/examples/pvc-curvine.yaml
kubectl apply -f curvine-csi/examples/pod-curvine.yaml
kubectl exec curvine-test-pod -n curvine -- sh -c 'echo "Test" > /data/test.txt'
kubectl exec curvine-test-pod -n curvine -- cat /data/test.txt
```

## Core Workflow

### Phase 1: Build and Deploy

1. **Build CSI Binary**
   ```bash
   cd curvine-csi
   CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/csi .
   ```

2. **Build Docker Image**
   ```bash
   mkdir -p /tmp/csi-build && cp bin/csi /tmp/csi-build/
   cat > /tmp/csi-build/Dockerfile << 'EOF'
   FROM ghcr.io/curvineio/curvine-csi:latest
   COPY csi /opt/curvine/csi
   RUN chmod +x /opt/curvine/csi
   EOF
   cd /tmp/csi-build && docker build -t curvine-csi:latest .
   docker tag curvine-csi:latest "$CSI_REGISTRY/curvine-csi:latest"
   docker push "$CSI_REGISTRY/curvine-csi:latest"
   ```

   `CSI_REGISTRY` is the registry reachable from the test cluster (for example an in-cluster
   registry, a Harbor project or a cloud ACR). Export it before building — never hardcode a
   registry host in this file.

3. **Deploy Components** (see Quick Start)

### Phase 2: Basic Functionality Tests

#### Test 1: Create PVC and Pod
```bash
kubectl apply -f curvine-csi/examples/pvc-curvine.yaml
# A PVC has no Bound condition: wait on its phase
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/curvine-pvc -n curvine --timeout=60s
kubectl apply -f curvine-csi/examples/pod-curvine.yaml
kubectl wait --for=condition=Ready pod/curvine-test-pod -n curvine --timeout=60s
```

#### Test 2: Write and Read Data
```bash
kubectl exec curvine-test-pod -n curvine -- \
  sh -c 'echo "Hello Curvine CSI" > /data/test.txt'
kubectl exec curvine-test-pod -n curvine -- \
  cat /data/test.txt
```

#### Test 3: Multi-Pod Shared Access
```bash
kubectl apply -f curvine-csi/examples/deployment-curvine.yaml
kubectl rollout status deployment/curvine-test-deployment -n curvine --timeout=120s
# Its pods mount the shared PVC at /usr/share/nginx/html: write from pod 1, read from pod 2
```

### Phase 3: Resilience Tests

#### Test 4: CSI Node Pod Restart (Standalone Mode)
```bash
# Write data before restart
kubectl exec curvine-test-pod -n curvine -- \
  sh -c 'echo "Before restart: $(date)" > /data/restart-test.txt'

# Restart CSI Node Pod
kubectl delete pod -l app=curvine-csi-node -n curvine
kubectl rollout status daemonset/curvine-csi-node -n curvine --timeout=120s

# Verify the standalone mount pod still exists
kubectl get pods -n curvine -l app=curvine-standalone

# Read data after restart (should still be accessible)
kubectl exec curvine-test-pod -n curvine -- \
  cat /data/restart-test.txt
```

#### Test 5: Standalone Mount Pod Recovery
```bash
kubectl get pods -n curvine -l app=curvine-standalone
kubectl logs -n curvine -l app=curvine-standalone --tail=50
```

#### Test 6: Pod Deletion and Cleanup
```bash
kubectl delete pod curvine-test-pod -n curvine
kubectl wait --for=delete pod/curvine-test-pod -n curvine --timeout=30s
kubectl delete pvc curvine-pvc -n curvine
```

## Test Pods

### Simple Test Pod
- File: `curvine-csi/examples/pod-curvine.yaml`
- Single pod with nginx, mounts PVC at `/data`

### Multi-Pod Deployment
- File: `curvine-csi/examples/deployment-curvine.yaml`
- 3 replicas sharing same PVC

### StatefulSet
- File: `curvine-csi/examples/statefulset-curvine.yaml`
- For stateful workloads

## Components Reference

See `references/components.md` for detailed component descriptions:
- Namespace, ServiceAccounts, RBAC
- CSIDriver, Controller, Node Plugin
- StorageClass configuration

## Troubleshooting

### Common Issues

**PVC Stuck in Pending**
```bash
kubectl describe pvc curvine-pvc -n curvine
kubectl logs -n curvine -l app=curvine-csi-controller --tail=100
```

**Pod Cannot Mount Volume**
```bash
kubectl logs -n curvine -l app=curvine-csi-node --tail=100
kubectl get pods -n curvine -l app=curvine-standalone
kubectl logs -n curvine -l app=curvine-standalone --tail=100
```

**Standalone Mount Pod Not Created**
```bash
kubectl logs -n curvine -l app=curvine-csi-node --tail=100 | grep -i standalone
kubectl auth can-i create pods -n curvine --as=system:serviceaccount:curvine:curvine-csi-node-sa
kubectl get configmap -n curvine | grep curvine-standalone-state
```

**Pod Deletion Hangs**
```bash
kubectl get pv -o json | jq '.items[] | select(.spec.csi.driver=="curvine") | {name: .metadata.name, finalizers: .metadata.finalizers}'
# Manual cleanup if needed:
kubectl patch pv <pv-name> -p '{"metadata":{"finalizers":[]}}' --type=merge
```

## Test Scripts

See `scripts/test-suite.sh` for automated test script template.

## Verification Checklist

- [ ] CSI Node Pods running on all nodes
- [ ] Standalone mount pods created for each mount
- [ ] CSIDriver registered
- [ ] StorageClass available
- [ ] PVC creation succeeds
- [ ] Pod can mount volume
- [ ] Data write/read works
- [ ] Multiple pods can share same volume
- [ ] CSI restart doesn't affect mounted volumes (standalone mode)

## Notes
If a Pod is stuck in Terminating, wait at most 2 minutes, then inspect why deletion is blocked. If it cannot be fixed, use `--force` to delete it safely.
- The test cluster has a single master; read its address from your environment (`CV_TEST_MASTER`, for example `master.example.com:8995`) instead of hardcoding one.
- Before testing, replace the ghcr image with `$CSI_REGISTRY/curvine-csi`. Always restore the original image after the test.
