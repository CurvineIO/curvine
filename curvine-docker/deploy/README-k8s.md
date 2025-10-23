# Curvine Kubernetes Deployment Guide

This guide explains how to deploy Curvine to a local Kubernetes cluster using the provided scripts.

## Prerequisites

- Docker installed and running
- kubectl installed (https://kubernetes.io/docs/tasks/tools/)
- kind will be automatically installed by the setup script if not present

## Quick Start

### 1. Create Local Kubernetes Cluster

```bash
cd curvine-docker/deploy
./k8s-setup.sh
```

This will:
- Install kind if not present
- Create a 3-node Kubernetes cluster named "curvine-cluster"
- Configure port mappings for Curvine services
- Set up kubectl context

### 2. Build Docker Images (if not already built)

```bash
./build-img.sh
```

### 3. Deploy Curvine to Kubernetes

```bash
./k8s-deploy.sh deploy
```

This will:
- Load Docker images into the kind cluster
- Create ConfigMap with Curvine configuration
- Deploy 1 Master node (StatefulSet)
- Deploy 3 Worker nodes (StatefulSet)
- Create necessary Services and PersistentVolumeClaims

### 4. Check Deployment Status

```bash
./k8s-deploy.sh status
```

Or manually:

```bash
# Check pods
kubectl get pods -l app=curvine

# Check services
kubectl get svc -l app=curvine

# View master logs
kubectl logs -l component=master --tail=100 -f

# View worker logs
kubectl logs -l component=worker --tail=100 -f
```

## Accessing Curvine

### Web UI

Access the Curvine Web UI at: http://localhost:30900

### RPC Access

Master RPC is exposed at: localhost:30700

### Execute Commands in Pods

```bash
# Access master pod
kubectl exec -it curvine-master-0 -- /bin/sh

# Access worker pod
kubectl exec -it curvine-worker-0 -- /bin/sh

# Run Curvine CLI in master pod
kubectl exec -it curvine-master-0 -- /app/cv report
```

## Architecture

### Deployment Layout

- **Master Node**: 1 StatefulSet replica
  - RPC Port: 7000 (NodePort: 30700)
  - Web UI Port: 9000 (NodePort: 30900)
  - Journal Port: 19000
  - Storage: 10Gi PersistentVolume

- **Worker Nodes**: 3 StatefulSet replicas
  - RPC Port: 8000
  - Storage: 50Gi PersistentVolume per worker
  - Multi-tier storage: Memory/SSD/HDD

### Kubernetes Resources

```
curvine-docker/deploy/k8s/
├── configmap.yaml           # Curvine cluster configuration
├── master-deployment.yaml   # Master StatefulSet and Service
└── worker-deployment.yaml   # Worker StatefulSet and Service
```

## Configuration

Configuration is managed via ConfigMap (`k8s/configmap.yaml`). To update:

1. Edit `k8s/configmap.yaml`
2. Apply changes:
   ```bash
   kubectl apply -f k8s/configmap.yaml
   ```
3. Restart pods to pick up new config:
   ```bash
   kubectl rollout restart statefulset/curvine-master
   kubectl rollout restart statefulset/curvine-worker
   ```

## Scaling

### Scale Workers

```bash
# Scale to 5 workers
kubectl scale statefulset curvine-worker --replicas=5

# Scale down to 2 workers
kubectl scale statefulset curvine-worker --replicas=2
```

### Scale Master (High Availability)

For production, you can scale the master to 3 replicas for Raft consensus:

```bash
kubectl scale statefulset curvine-master --replicas=3
```

Note: Ensure journal configuration in ConfigMap is updated with all master addresses.

## Monitoring

### View Logs

```bash
# Master logs
kubectl logs -f curvine-master-0

# Worker logs (all workers)
kubectl logs -l component=worker --tail=50 -f

# Specific worker
kubectl logs -f curvine-worker-0
```

### Metrics

Access Prometheus metrics:

```bash
# Port-forward master web UI
kubectl port-forward svc/curvine-master 9000:9000

# Access metrics at http://localhost:9000/metrics
```

## Troubleshooting

### Pods Not Starting

```bash
# Check pod status
kubectl describe pod curvine-master-0
kubectl describe pod curvine-worker-0

# Check events
kubectl get events --sort-by=.metadata.creationTimestamp
```

### Image Pull Issues

If using kind, ensure images are loaded:

```bash
kind load docker-image curvine/curvine-server:latest --name curvine-cluster
```

### Storage Issues

```bash
# Check PVCs
kubectl get pvc

# Check PVs
kubectl get pv

# Describe PVC
kubectl describe pvc data-curvine-master-0
```

### Network Issues

```bash
# Test master connectivity
kubectl run -it --rm debug --image=busybox --restart=Never -- nc -zv curvine-master 7000

# Test worker connectivity
kubectl run -it --rm debug --image=busybox --restart=Never -- nc -zv curvine-worker-0.curvine-worker 8000
```

## Cleanup

### Remove Deployment (Keep Cluster)

```bash
./k8s-deploy.sh cleanup
```

This removes Curvine resources but keeps the cluster running.

### Delete Entire Cluster

```bash
kind delete cluster --name curvine-cluster
```

## Advanced Usage

### Custom Namespace

Deploy to a custom namespace:

```bash
# Create namespace
kubectl create namespace curvine-prod

# Update manifests to use new namespace
sed -i 's/namespace: default/namespace: curvine-prod/g' k8s/*.yaml

# Deploy
kubectl apply -f k8s/ -n curvine-prod
```

### Persistent Storage Class

For production, use a proper StorageClass:

```yaml
# In volumeClaimTemplates
storageClassName: fast-ssd  # Your StorageClass name
```

### Resource Limits

Adjust resource limits in deployment manifests based on your workload:

```yaml
resources:
  requests:
    cpu: "1000m"
    memory: "4Gi"
  limits:
    cpu: "4000m"
    memory: "16Gi"
```

## Testing the Deployment

```bash
# Access master pod
kubectl exec -it curvine-master-0 -- /bin/sh

# Inside the pod, run:
/app/cv report                    # Cluster status
/app/cv fs mkdir /test           # Create directory
/app/cv fs ls /                  # List root
echo "Hello Curvine" > /tmp/test.txt
/app/cv fs put /tmp/test.txt /test/hello.txt  # Upload file
/app/cv fs cat /test/hello.txt   # Read file
```

## Production Considerations

1. **High Availability**: Deploy 3 master replicas with proper Raft configuration
2. **Storage**: Use production-grade StorageClass (e.g., SSD-backed)
3. **Resources**: Adjust CPU/memory based on workload
4. **Monitoring**: Integrate with Prometheus/Grafana
5. **Backup**: Implement regular backups of PersistentVolumes
6. **Security**: Enable RBAC, NetworkPolicies, and Pod Security Standards
7. **LoadBalancer**: Use LoadBalancer service type for external access

## References

- Curvine Documentation: See CLAUDE.md
- Kubernetes Documentation: https://kubernetes.io/docs/
- kind Documentation: https://kind.sigs.k8s.io/
