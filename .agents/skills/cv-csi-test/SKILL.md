---
name: cv-csi-test
description: End-to-end Curvine CSI driver testing guide for Kubernetes deployment, StorageClass and MountPod setup, resilience scenarios, volume expansion, and scripted test suites. Use when validating CSI code changes, running integration tests, debugging mount failures, or verifying MountPod recovery after node or CSI restarts.
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
kubectl apply -f curvine-csi/deploy/daemonset-mountpod.yaml  # MountPod mode (recommended)
kubectl apply -f curvine-csi/examples/storage-class.yaml
```

### 2. Verify Deployment

```bash
kubectl get pods -n curvine-system -l app=curvine-csi-node
kubectl get csidriver curvine
kubectl get storageclass curvine-sc
```

### 3. Run Basic Test

```bash
kubectl apply -f curvine-csi/examples/pvc-curvine.yaml
kubectl apply -f curvine-csi/examples/pod-curvine.yaml
kubectl exec curvine-test-pod -n curvine-system -- sh -c 'echo "Test" > /usr/share/nginx/html/test.txt'
kubectl exec curvine-test-pod -n curvine-system -- cat /usr/share/nginx/html/test.txt
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
   docker tag curvine-csi:latest 10.119.43.210:5000/curvine-csi:latest
   docker push 10.119.43.210:5000/curvine-csi:latest
   ```

3. **Deploy Components** (see Quick Start)

### Phase 2: Basic Functionality Tests

#### Test 1: Create PVC and Pod
```bash
kubectl apply -f curvine-csi/examples/pvc-curvine.yaml
kubectl wait --for=condition=Bound pvc/curvine-pvc -n curvine-system --timeout=60s
kubectl apply -f curvine-csi/examples/pod-curvine.yaml
kubectl wait --for=condition=Ready pod/curvine-test-pod -n curvine-system --timeout=60s
```

#### Test 2: Write and Read Data
```bash
kubectl exec curvine-test-pod -n curvine-system -- \
  sh -c 'echo "Hello Curvine CSI" > /usr/share/nginx/html/test.txt'
kubectl exec curvine-test-pod -n curvine-system -- \
  cat /usr/share/nginx/html/test.txt
```

#### Test 3: Multi-Pod Shared Access
```bash
kubectl apply -f curvine-csi/examples/deployment-curvine.yaml
kubectl wait --for=condition=Ready pod -l app=curvine-test -n curvine-system --timeout=60s
# Write from pod 1, read from pod 2
```

### Phase 3: Resilience Tests

#### Test 4: CSI Node Pod Restart (MountPod Mode)
```bash
# Write data before restart
kubectl exec curvine-test-pod -n curvine-system -- \
  sh -c 'echo "Before restart: $(date)" > /usr/share/nginx/html/restart-test.txt'

# Restart CSI Node Pod
kubectl delete pod -l app=curvine-csi-node -n curvine-system
kubectl wait --for=condition=Ready pod -l app=curvine-csi-node -n curvine-system --timeout=60s

# Verify MountPod still exists
kubectl get pods -n curvine-system | grep curvine-mountpod

# Read data after restart (should still be accessible)
kubectl exec curvine-test-pod -n curvine-system -- \
  cat /usr/share/nginx/html/restart-test.txt
```

#### Test 5: MountPod Recovery
```bash
kubectl get pods -n curvine-system | grep curvine-mountpod
kubectl logs -n curvine-system -l app=curvine-mountpod --tail=50
```

#### Test 6: Pod Deletion and Cleanup
```bash
kubectl delete pod curvine-test-pod -n curvine-system
kubectl wait --for=delete pod/curvine-test-pod -n curvine-system --timeout=30s
kubectl delete pvc curvine-pvc -n curvine-system
```

## Test Pods

### Simple Test Pod
- File: `curvine-csi/examples/pod-curvine.yaml`
- Single pod with nginx, mounts PVC

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
kubectl describe pvc curvine-pvc -n curvine-system
kubectl logs -n curvine-system -l app=curvine-csi-controller --tail=100
```

**Pod Cannot Mount Volume**
```bash
kubectl logs -n curvine-system -l app=curvine-csi-node --tail=100
kubectl get pods -n curvine-system | grep curvine-mountpod
kubectl logs -n curvine-system -l app=curvine-mountpod --tail=100
```

**MountPod Not Created**
```bash
kubectl logs -n curvine-system -l app=curvine-csi-node --tail=100 | grep -i mountpod
kubectl auth can-i create pods --as=system:serviceaccount:curvine-system:curvine-csi-node-sa
kubectl get configmap -n curvine-system | grep curvine-mountpod
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
- [ ] MountPods created for each Curvine cluster
- [ ] CSIDriver registered
- [ ] StorageClass available
- [ ] PVC creation succeeds
- [ ] Pod can mount volume
- [ ] Data write/read works
- [ ] Multiple pods can share same volume
- [ ] CSI restart doesn't affect mounted volumes (MountPod mode)

## 注意事项
如果遇到Pod Terminating，最多等待2min，就要停止等待，并仔细检查pod删除卡主的原因，如果无法修复，可以采用--force的方式确保安全删除。
- 测试集群的master addr为10.119.43.210:8995, 且只有一个。 
- 测试之前需要将gchr镜像替换为10.119.43.210:5000/curvine-csi,  测试完成后总是要恢复为原始镜像。