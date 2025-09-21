# Curvine Fluid CacheRuntime 完整部署指南

本指南提供 Curvine 与 Fluid CacheRuntime 集成的完整部署、测试和问题解决方案。

## 📋 目录

1. [环境准备](#环境准备)
2. [构建必要组件](#构建必要组件)
3. [部署 Fluid 系统](#部署-fluid-系统)
4. [部署 Curvine CacheRuntime](#部署-curvine-cacheruntime)
5. [测试验证](#测试验证)
6. [问题排查](#问题排查)
7. [使用指南](#使用指南)

## 🚀 环境准备

### 前置条件

- Kubernetes 集群（推荐 v1.20+）
- Docker
- kubectl
- minikube（本地测试）
- Git

### 验证环境

```bash
# 检查 Kubernetes 集群
kubectl cluster-info

# 检查节点状态
kubectl get nodes

# 预期输出：所有节点状态为 Ready
```

## 🔨 构建必要组件

> **重要说明**：CacheRuntime 功能目前在开发分支中，需要从源码构建相关组件。

### 步骤 1：获取 Fluid 源码

```bash
# 克隆 Fluid 源码
git clone https://github.com/fluid-cloudnative/fluid.git /tmp/fluid
cd /tmp/fluid

# 切换到 CacheRuntime 功能分支
git checkout feature/generic-cache-runtime

# 验证分支
git branch
# 预期输出：* feature/generic-cache-runtime
```

### 步骤 2：构建 CacheRuntime 控制器

```bash
cd /tmp/fluid

# 构建 CacheRuntime 控制器镜像
make docker-build-cacheruntime-controller

# 验证镜像构建成功
docker images | grep cacheruntime-controller
# 预期输出：fluidcloudnative/cacheruntime-controller   v1.0.7-da106f77

# 加载镜像到 minikube
minikube image load fluidcloudnative/cacheruntime-controller:v1.0.7-da106f77
```

### 步骤 3：构建支持 CacheRuntime 的 CSI 驱动

```bash
cd /tmp/fluid

# 构建 CSI 驱动镜像
make docker-build-csi

# 验证镜像构建成功
docker images | grep fluid-csi
# 预期输出：fluidcloudnative/fluid-csi   v1.0.7-da106f77

# 加载镜像到 minikube
minikube image load fluidcloudnative/fluid-csi:v1.0.7-da106f77
```

### 步骤 4：构建 Curvine 镜像

```bash
cd /path/to/curvine/curvine-docker/fluid/cache-runtime

# 构建 Curvine 镜像
./build-image.sh

# 验证镜像构建成功
docker images | grep curvine
# 预期输出：curvine   latest

# 加载镜像到 minikube
minikube image load curvine:latest
```

## 🎯 部署 Fluid 系统

### 步骤 1：卸载现有 Fluid（如果存在）

```bash
# 检查现有 Fluid 安装
helm list -n fluid-system

# 如果存在，先卸载
helm uninstall fluid -n fluid-system
kubectl delete namespace fluid-system
```

### 步骤 2：部署支持 CacheRuntime 的 Fluid

```bash
cd /tmp/fluid

# 创建命名空间
kubectl create namespace fluid-system

# 安装 Fluid（启用 CacheRuntime）
helm install fluid charts/fluid/fluid \
  --namespace fluid-system \
  --set runtime.cache.enabled=true \
  --set image.fluidcloudnative.cacheruntime-controller.repository=fluidcloudnative/cacheruntime-controller \
  --set image.fluidcloudnative.cacheruntime-controller.tag=v1.0.7-da106f77
```

### 步骤 3：验证 Fluid 部署

```bash
# 等待所有 Pod 就绪
kubectl wait --for=condition=ready pod --all -n fluid-system --timeout=300s

# 检查 Fluid 组件状态
kubectl get pods -n fluid-system

# 预期输出：
# NAME                                       READY   STATUS    RESTARTS   AGE
# cacheruntime-controller-xxx                1/1     Running   0          2m
# csi-nodeplugin-fluid-xxx                   2/2     Running   0          2m
# dataset-controller-xxx                     1/1     Running   0          2m
# fluid-webhook-xxx                          1/1     Running   0          2m
# fluidapp-controller-xxx                    1/1     Running   0          2m

# 检查 CacheRuntime CRD
kubectl get crd | grep cache
# 预期输出：
# cacheruntimeclasses.data.fluid.io
# cacheruntimes.data.fluid.io
```

### 步骤 4：更新 CSI 驱动

```bash
# 更新 CSI DaemonSet 使用支持 CacheRuntime 的镜像
kubectl patch daemonset -n fluid-system csi-nodeplugin-fluid -p '{"spec":{"template":{"spec":{"containers":[{"name":"plugins","image":"fluidcloudnative/fluid-csi:v1.0.7-da106f77"}]}}}}'

# 等待 CSI Pod 重新创建
kubectl rollout status daemonset/csi-nodeplugin-fluid -n fluid-system

# 验证 CSI Pod 状态
kubectl get pods -n fluid-system | grep csi
# 预期输出：csi-nodeplugin-fluid-xxx   2/2   Running   0   1m
```

## 🏗️ 部署 Curvine CacheRuntime

### 步骤 1：创建 CacheRuntimeClass

```bash
cd /path/to/curvine/curvine-docker/fluid/cache-runtime

# 应用 CacheRuntimeClass
kubectl apply -f curvine-cache-runtime-class.yaml

# 验证创建成功
kubectl get cacheruntimeclass
# 预期输出：
# NAME      AGE
# curvine   1m
```

### 步骤 2：给节点添加标签

```bash
# 为节点添加 Fluid 标签（用于 client Pod 调度）
kubectl label nodes minikube fluid.io/f-default-curvine-demo=true

# 验证标签添加成功
kubectl get nodes --show-labels | grep fluid
# 预期输出应包含：fluid.io/f-default-curvine-demo=true
```

### 步骤 3：创建 Dataset 和 CacheRuntime

```bash
# 应用 Dataset 和 CacheRuntime
kubectl apply -f curvine-dataset-example.yaml

# 验证资源创建
kubectl get dataset,cacheruntime
# 预期输出：
# NAME                                    UFS TOTAL SIZE   CACHED   CACHE CAPACITY   CACHED PERCENTAGE   PHASE   AGE
# dataset.data.fluid.io/curvine-demo                                                                     Bound   1m
# 
# NAME                                      MASTER PHASE   WORKER PHASE   FUSE PHASE   AGE
# cacheruntime.data.fluid.io/curvine-demo  Ready          Ready          Ready        1m
```

### 步骤 4：验证 Curvine 集群部署

```bash
# 检查所有 Curvine Pod 状态
kubectl get pods | grep curvine

# 预期输出：
# curvine-demo-client-xxx      1/1     Running   0   2m
# curvine-demo-master-0        1/1     Running   0   2m
# curvine-demo-worker-0        1/1     Running   0   2m
# curvine-demo-worker-1        1/1     Running   0   2m

# 检查 PVC 状态
kubectl get pvc
# 预期输出：
# NAME            STATUS   VOLUME                CAPACITY   ACCESS MODES   STORAGECLASS   AGE
# curvine-demo    Bound    default-curvine-demo  100Pi      RWX            fluid          2m
```

## 🧪 测试验证

### 测试 1：直接 FUSE 文件系统访问

```bash
# 获取 client Pod 名称
CLIENT_POD=$(kubectl get pods | grep curvine-demo-client | awk '{print $1}')

# 测试文件写入
kubectl exec $CLIENT_POD -- sh -c 'echo "Hello Curvine!" > /runtime-mnt/cache/default/curvine-demo/cache-fuse/test1.txt'

# 测试文件读取
kubectl exec $CLIENT_POD -- cat /runtime-mnt/cache/default/curvine-demo/cache-fuse/test1.txt
# 预期输出：Hello Curvine!

# 测试目录操作
kubectl exec $CLIENT_POD -- sh -c 'mkdir -p /runtime-mnt/cache/default/curvine-demo/cache-fuse/testdir && echo "Directory test" > /runtime-mnt/cache/default/curvine-demo/cache-fuse/testdir/file.txt'

# 验证目录内容
kubectl exec $CLIENT_POD -- ls -la /runtime-mnt/cache/default/curvine-demo/cache-fuse/
# 预期输出应包含：test1.txt 和 testdir/
```

### 测试 2：PVC 挂载访问

```bash
# 创建测试 Pod
kubectl apply -f test_pod.yaml

# 等待 Pod 就绪
kubectl wait --for=condition=ready pod test-curvine --timeout=60s

# 检查 Pod 状态
kubectl get pod test-curvine
# 预期输出：test-curvine   1/1   Running   0   1m

# 查看挂载的文件系统内容
kubectl exec test-curvine -- ls -la /data/
# 预期输出应包含之前创建的 test1.txt 和 testdir/

# 测试通过 PVC 写入文件
kubectl exec test-curvine -- sh -c 'echo "PVC test success!" > /data/pvc-test.txt'

# 验证文件内容
kubectl exec test-curvine -- cat /data/pvc-test.txt
# 预期输出：PVC test success!
```

### 测试 3：跨访问验证

```bash
# 验证通过 PVC 写入的文件可以通过直接 FUSE 访问
kubectl exec $CLIENT_POD -- cat /runtime-mnt/cache/default/curvine-demo/cache-fuse/pvc-test.txt
# 预期输出：PVC test success!

# 验证数据一致性
kubectl exec test-curvine -- sh -c 'echo "Cross access test" > /data/cross-test.txt'
kubectl exec $CLIENT_POD -- cat /runtime-mnt/cache/default/curvine-demo/cache-fuse/cross-test.txt
# 预期输出：Cross access test
```

### 测试 4：性能测试

```bash
# 大文件写入测试
kubectl exec test-curvine -- dd if=/dev/zero of=/data/bigfile.dat bs=1M count=10
# 预期输出：10+0 records in/out, 10485760 bytes copied

# 验证文件大小
kubectl exec test-curvine -- ls -lh /data/bigfile.dat
# 预期输出：-rw-r--r-- 1 root root 10M ... bigfile.dat
```

## 🔍 问题排查

### 问题 1：CacheRuntime 控制器 ImagePullBackOff

**现象**：
```bash
kubectl get pods -n fluid-system
# cacheruntime-controller-xxx   0/1   ImagePullBackOff   0   5m
```

**原因**：官方镜像仓库没有 CacheRuntime 控制器镜像

**解决方案**：
```bash
# 确认已构建并加载镜像
docker images | grep cacheruntime-controller
minikube image load fluidcloudnative/cacheruntime-controller:v1.0.7-da106f77

# 重启 Pod
kubectl delete pod -n fluid-system -l app=cacheruntime-controller
```

### 问题 2：CSI 挂载失败 "fail to get runtimeInfo for runtime type: cache"

**现象**：
```bash
kubectl describe pod test-curvine
# Warning  FailedMount  ... rpc error: code = Unknown desc = NodeStageVolume: failed to get runtime info for default/curvine-demo: fail to get runtimeInfo for runtime type: cache
```

**原因**：CSI 驱动不支持 CacheRuntime

**解决方案**：
```bash
# 确认已更新 CSI 驱动
kubectl get pods -n fluid-system | grep csi
kubectl describe pod -n fluid-system csi-nodeplugin-fluid-xxx | grep Image
# 应该显示：fluidcloudnative/fluid-csi:v1.0.7-da106f77

# 如果不是，重新更新
kubectl patch daemonset -n fluid-system csi-nodeplugin-fluid -p '{"spec":{"template":{"spec":{"containers":[{"name":"plugins","image":"fluidcloudnative/fluid-csi:v1.0.7-da106f77"}]}}}}'
```

### 问题 3：Client Pod CrashLoopBackOff "Mnt path is not empty"

**现象**：
```bash
kubectl logs curvine-demo-client-xxx
# ERROR: Mnt /runtime-mnt/cache/default/curvine-demo/cache-fuse is not empty
```

**原因**：FUSE 挂载点目录不为空

**解决方案**：
```bash
# 检查 Curvine 镜像版本
kubectl describe pod curvine-demo-client-xxx | grep Image
# 确保使用最新构建的镜像

# 如果需要，重新构建并加载镜像
cd /path/to/curvine/curvine-docker/fluid/cache-runtime
./build-image.sh
minikube image load curvine:latest

# 重启 client Pod
kubectl delete pod curvine-demo-client-xxx
```

### 问题 4：节点标签缺失导致 Client Pod 无法调度

**现象**：
```bash
kubectl get daemonset curvine-demo-client
# DESIRED   CURRENT   READY   UP-TO-DATE   AVAILABLE   NODE SELECTOR
# 0         0         0       0            0           fluid.io/f-default-curvine-demo=true
```

**解决方案**：
```bash
# 添加节点标签
kubectl label nodes minikube fluid.io/f-default-curvine-demo=true

# 验证标签
kubectl get nodes --show-labels | grep fluid.io/f-default-curvine-demo=true
```

## 📖 使用指南

### 应用集成示例

创建使用 Curvine 缓存的应用：

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

### 性能优化建议

1. **调整缓存配置**：
   ```yaml
   # 在 curvine-dataset-example.yaml 中调整
   tieredStore:
     levels:
     - high: "0.8"
       low: "0.5"
       path: "/cache-data"
       quota: "100Gi"  # 根据需要调整
   ```

2. **扩展 Worker 节点**：
   ```yaml
   # 增加 Worker 副本数
   worker:
     replicas: 4  # 根据集群规模调整
   ```

### 监控和日志

```bash
# 查看 Master 日志
kubectl logs curvine-demo-master-0

# 查看 Worker 日志
kubectl logs curvine-demo-worker-0

# 查看 Client 日志
kubectl logs curvine-demo-client-xxx

# 查看 Fluid 控制器日志
kubectl logs -n fluid-system deployment/cacheruntime-controller
```

## 🎯 总结

通过本指南，您应该能够：

1. ✅ 成功构建所有必要的组件
2. ✅ 部署完整的 Fluid + CacheRuntime 系统
3. ✅ 部署 Curvine 分布式缓存集群
4. ✅ 验证 PVC 挂载和文件系统功能
5. ✅ 解决常见部署问题

Curvine 现在作为 Fluid CacheRuntime 提供高性能的分布式缓存服务，应用可以通过标准的 Kubernetes PVC 方式无缝访问。

## 📞 支持

如遇到问题，请：

1. 检查本指南的问题排查部分
2. 查看相关组件的日志
3. 验证所有前置条件是否满足
4. 确认使用正确的镜像版本

---

**注意**：本集成基于 Fluid 的开发分支 `feature/generic-cache-runtime`，在生产环境使用前请确保充分测试。