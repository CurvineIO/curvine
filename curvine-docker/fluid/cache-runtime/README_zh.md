# Curvine Fluid CacheRuntime 完整部署指南

本指南提供 Curvine 与 Fluid CacheRuntime 集成的完整部署、测试和问题解决方案。

## 目录

1. [环境准备](#环境准备)
2. [构建必要组件](#构建必要组件)
3. [部署 Fluid 系统](#部署-fluid-系统)
4. [部署 Curvine CacheRuntime](#部署-curvine-cacheruntime)
5. [存储配置](#存储配置)
6. [测试验证](#测试验证)
7. [问题排查](#问题排查)
8. [使用指南](#使用指南)

## 环境准备

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

## 构建必要组件

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
minikube image load curvine-fluid-cacheruntime:latest
```

## 部署 Fluid 系统

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

## 部署 Curvine CacheRuntime

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

## 存储配置

### 存储架构

#### Master 组件
- **元数据存储**：RocksDB 数据库，用于文件系统元数据
- **日志存储**：Raft 共识日志，用于高可用
- **日志存储**：应用程序和审计日志

#### Worker 组件  
- **多级缓存**：内存、SSD 和 HDD 存储层
- **数据存储**：实际缓存的文件数据
- **日志存储**：Worker 操作日志

### 存储路径配置

#### Master 存储路径

| 组件 | 默认路径 | 描述 | 推荐存储 |
|------|---------|------|----------|
| 元数据 | `/opt/curvine/data/meta` | RocksDB 元数据 | SSD (10-50GB) |
| 日志 | `/opt/curvine/data/journal` | Raft 共识日志 | SSD (5-10GB) |
| 日志 | `/opt/curvine/logs` | 应用程序日志 | 任意 (5-10GB) |

#### Worker 存储类型

| 前缀 | 存储类型 | 用途 | 性能 | 容量 |
|------|---------|------|------|------|
| `[MEM]` | 内存 | 热数据缓存 | 最高 | 受 RAM 限制 |
| `[SSD]` | SSD | 温数据缓存 | 高 | 100GB-1TB |
| `[NVME]` | NVMe SSD | 超快缓存 | 最高 | 100GB-1TB |
| `[HDD]` | 硬盘 | 冷数据缓存 | 中等 | 1TB+ |
| `[DISK]` | 通用磁盘 | 默认存储 | 中等 | 可变 |

#### 存储路径格式

```
[存储类型:容量]挂载路径
```

**示例：**
- `[MEM:8GB]/cache-mem` - 8GB 内存缓存
- `[SSD:100GB]/cache-ssd` - 100GB SSD 缓存  
- `[HDD]/cache-hdd` - 无限制硬盘缓存
- `/cache-data` - 默认磁盘存储

### 重要：内存存储配置

**仅配置 `[MEM:8GB]/cache-mem` 是不够的！**

要让内存存储正常工作，您需要**两个配置**：

1. **Curvine 配置**：`data_dir: "[MEM:8GB]/cache-mem"`
   - 告诉 Curvine 这是高性能内存存储
   - 用于数据放置和缓存策略
   - Curvine worker 使用标准文件系统调用创建目录结构

2. **Kubernetes 卷配置**： 
   ```yaml
   volumes:
   - name: memory-cache
     emptyDir:
       medium: Memory      # 创建 tmpfs 内存文件系统
       sizeLimit: 8Gi
   
   volumeMounts:
   - name: memory-cache
     mountPath: /cache-mem  # 必须与 data_dir 中的路径匹配
   ```

**没有 Kubernetes 卷配置，路径将被视为普通磁盘存储！**

### 配置示例

所有配置示例都在统一的 `deploy/curvine-dataset.yaml` 文件中。只需取消注释您需要的部分：

#### 1. 基础配置（默认 - 测试/开发）

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

#### 2. 多级配置（高级）

要启用多级存储，请在 `curvine-dataset.yaml` 中取消注释相关部分：

```yaml
# 取消注释这些卷定义：
# - name: memory-cache
#   emptyDir:
#     medium: Memory
#     sizeLimit: 32Gi
# - name: ssd-cache
#   persistentVolumeClaim:
#     claimName: ssd-pvc

# 取消注释多级 data_dir 配置：
# data_dir: "[MEM:32GB]/cache-mem,[SSD:500GB]/cache-ssd,[HDD:2TB]/cache-hdd"

# 取消注释额外的卷挂载：
# - name: memory-cache
#   mountPath: /cache-mem
# - name: ssd-cache
#   mountPath: /cache-ssd

# 取消注释多级 tieredStore 配置：
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

#### 3. 生产配置

对于生产部署，请在 `curvine-dataset.yaml` 中取消注释生产部分：

```yaml
# 取消注释持久卷定义：
# - name: master-meta
#   persistentVolumeClaim:
#     claimName: master-meta-pvc

# 取消注释生产资源限制：
# resources:
#   requests:
#     memory: "2Gi"
#     cpu: "1000m"
#   limits:
#     memory: "8Gi"
#     cpu: "4000m"

# 取消注释文件末尾的 PVC 定义
```

### 性能调优

#### 内存层优化
- **大小**：节点 RAM 的 10-30%
- **水位**：高=0.95，低=0.85（激进驱逐）
- **用途**：频繁访问的热数据

#### SSD 层优化  
- **大小**：根据工作负载 100GB-1TB
- **水位**：高=0.9，低=0.7（平衡）
- **用途**：具有良好性能的温数据

#### HDD 层优化
- **大小**：大数据集 1TB+
- **水位**：高=0.8，低=0.5（保守）
- **用途**：冷数据归档

### 内存存储验证

要验证内存存储是否正常工作：

```bash
# 检查是否挂载了 tmpfs（在 worker pod 内）
kubectl exec curvine-demo-worker-0 -- df -h /cache-mem
kubectl exec curvine-demo-worker-0 -- mount | grep tmpfs

# 检查 Curvine 是否识别内存存储
kubectl exec curvine-demo-worker-0 -- cat /opt/curvine/conf/curvine-cluster.toml | grep -A5 data_dir

# 监控内存使用
kubectl exec curvine-demo-worker-0 -- free -h
kubectl top pod curvine-demo-worker-0
```

**内存存储的预期输出：**
```bash
# df -h 应显示 tmpfs 文件系统
tmpfs           32G     0   32G   0% /cache-mem

# mount 应显示 tmpfs
tmpfs on /cache-mem type tmpfs (rw,nosuid,nodev,noexec,relatime,size=34359738368)

# Curvine 配置应显示 MEM 类型
data_dir = ["[MEM:32GB]/cache-mem"]
```

### 存储规划指南

#### 开发环境
- **Master**：1 副本，emptyDir 卷
- **Worker**：1 副本，单级 SSD 存储
- **容量**：总计 10-50GB

#### 测试环境  
- **Master**：1 副本，emptyDir 卷
- **Worker**：2-3 副本，多级（内存+SSD）
- **容量**：总计 100-500GB

#### 生产环境
- **Master**：3 副本，持久卷
- **Worker**：5+ 副本，多级（内存+SSD+HDD）  
- **容量**：总计 1TB+
- **备份**：定期元数据备份
- **监控**：资源使用警报

### 常见存储问题

#### 内存存储不工作
1. **症状**：文件存储在磁盘而不是内存中
2. **原因**：Kubernetes 中缺少 `emptyDir: {medium: Memory}`
3. **修复**：添加适当的卷配置

#### 性能问题
1. **症状**：缓存访问缓慢
2. **原因**：错误的存储层配置  
3. **修复**：调整水位级别和层大小

#### 容量问题
1. **症状**：缓存驱逐过于频繁
2. **原因**：存储层大小不足
3. **修复**：增加层配额或添加更多层

**多级存储示例：**
```yaml
worker:
  options:
    # 多个存储层级
    data_dir: "[MEM:8GB]/cache-mem,[SSD:100GB]/cache-ssd,[HDD]/cache-hdd"
  volumeMounts:
  - name: memory-cache
    mountPath: /cache-mem
  - name: ssd-cache
    mountPath: /cache-ssd
  - name: hdd-cache
    mountPath: /cache-hdd
  tieredStore:
    levels:
    - quota: 8Gi
      low: "0.9"
      high: "0.95"
      path: "/cache-mem"
      medium:
        emptyDir:
          medium: Memory
    - quota: 100Gi
      low: "0.7"
      high: "0.9"
      path: "/cache-ssd"
      medium:
        persistentVolumeClaim:
          claimName: ssd-storage
    - quota: 1Ti
      low: "0.5"
      high: "0.8"
      path: "/cache-hdd"
      medium:
        persistentVolumeClaim:
          claimName: hdd-storage
```

### 生产环境存储规划

**Master 存储需求：**
- **元数据卷**：10-50GB SSD（取决于文件数量）
- **日志卷**：5-10GB SSD（用于 Raft 日志）
- **备份策略**：定期元数据快照

**Worker 存储需求：**
- **内存层**：节点 RAM 的 10-30%，用于热数据
- **SSD 层**：主要缓存存储（100GB-1TB）
- **HDD 层**：大容量冷数据存储（1TB+）

**生产环境配置示例：**
```yaml
volumes:
- name: master-meta
  persistentVolumeClaim:
    claimName: master-meta-pvc
- name: master-journal
  persistentVolumeClaim:
    claimName: master-journal-pvc
- name: worker-memory
  emptyDir:
    medium: Memory
    sizeLimit: 16Gi
- name: worker-ssd
  persistentVolumeClaim:
    claimName: worker-ssd-pvc
- name: worker-hdd
  persistentVolumeClaim:
    claimName: worker-hdd-pvc

master:
  volumeMounts:
  - name: master-meta
    mountPath: /opt/curvine/data/meta
  - name: master-journal
    mountPath: /opt/curvine/data/journal
  options:
    meta_dir: "/opt/curvine/data/meta"

worker:
  volumeMounts:
  - name: worker-memory
    mountPath: /cache-mem
  - name: worker-ssd
    mountPath: /cache-ssd
  - name: worker-hdd
    mountPath: /cache-hdd
  options:
    data_dir: "[MEM:16GB]/cache-mem,[SSD:500GB]/cache-ssd,[HDD:2TB]/cache-hdd"
    dir_reserved: "10GB"
```

### 配置文件

项目提供了统一的配置文件，包含全面的选项：

- **`deploy/curvine-dataset.yaml`** - 完整配置文件，基础设置已启用，高级选项以注释形式提供
  - 基础单级存储用于测试/开发（默认启用）
  - 高级多级存储（内存+SSD+HDD）通过注释提供
  - 生产就绪配置通过注释提供
  - 只需取消注释您部署场景所需的部分即可

## 测试验证

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

## 问题排查

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
minikube image load curvine-fluid-cacheruntime:latest

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

## 使用指南

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

## 总结

通过本指南，您应该能够：

1. ✅ 成功构建所有必要的组件
2. ✅ 部署完整的 Fluid + CacheRuntime 系统
3. ✅ 部署 Curvine 分布式缓存集群
4. ✅ 验证 PVC 挂载和文件系统功能
5. ✅ 解决常见部署问题

Curvine 现在作为 Fluid CacheRuntime 提供高性能的分布式缓存服务，应用可以通过标准的 Kubernetes PVC 方式无缝访问。


如遇到问题，请：

1. 检查本指南的问题排查部分
2. 查看相关组件的日志
3. 验证所有前置条件是否满足
4. 确认使用正确的镜像版本

---

**注意**：本集成基于 Fluid 的开发分支 `feature/generic-cache-runtime`，在生产环境使用前请确保充分测试。