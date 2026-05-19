# install curvine


```bash
helm repo add curvine https://curvineio.github.io/helm-charts

helm install curvine curvine/curvine -n curvine --create-namespace --devel \
  --set image.pullPolicy=Always \
  --set master.replicas=1 \
  --set worker.replicas=1 \
  --set "master.nodeSelector.beta\.kubernetes\.io/instance-type=ml.r7i.xlarge"  \
  --set "worker.nodeSelector.sagemaker\.amazonaws\.com/compute-type=hyperpod"  \
  --set master.resources.requests.cpu=1000m \
  --set master.resources.requests.memory=2Gi \
  --set master.resources.limits.cpu=1000m \
  --set master.resources.limits.memory=2Gi \
  --set worker.resources.requests.cpu=1000m \
  --set worker.resources.requests.memory=2Gi \
  --set worker.resources.limits.cpu=1000m \
  --set worker.resources.limits.memory=2Gi
# 注意，这里因为测试的机型资源较小，所以调小了，生产环境请一定根据实际情况调整
# 调度到 HyperPod 节点：使用 sagemaker.amazonaws.com/compute-type=hyperpod 选择器
# 可选进一步通过 sagemaker.amazonaws.com/instance-group-name 区分实例组
```




# install curvine-csi

```bash
helm install curvine curvine/curvine -n curvine --create-namespace --devel -f ./value.yaml
```


# 创建 pvc 和 pv
curvine-sc.yaml

```yaml
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: curvine-sc  # StorageClass 名称，后续 PVC 需引用
provisioner: curvine  # CSI 驱动名称，固定为 curvine
reclaimPolicy: Delete  # 存储卷回收策略（Delete/Retain）
volumeBindingMode: Immediate  # 绑定模式（即时绑定）
allowVolumeExpansion: true  # 允许存储卷扩容
parameters:
  # 必需：Curvine Master 节点地址（多个用逗号分隔）
  master-addrs: "xxxx:8995"
  # master-addrs: "curvine-master.curvine.svc.cluster.local:8995"
  # 必需：Curvine 文件系统路径前缀（所有 PV 会自动创建在此路径下）
  fs-path: "/test-data"
  # 可选：路径创建策略（DirectoryOrCreate=不存在则创建）
  path-type: "DirectoryOrCreate"
  # 可选：FUSE 性能调优参数（根据业务需求调整）
  io-threads: "4"
  worker-threads: "8"
```

```
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: curvine-pvc
  namespace: default
spec:
  storageClassName: curvine-sc
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
```


# vllm 推理部署打 patch
## InferenceEndpointConfig
deploy-llama-kvcache.yaml
```yaml
apiVersion: inference.sagemaker.aws.amazon.com/v1
kind: InferenceEndpointConfig
metadata:
  name: ${ENDPOINT_NAME}
  namespace: ${NAMESPACE}
spec:
  modelName: ${MODEL_NAME}
  instanceType: ${INSTANCE_TYPE}
  invocationEndpoint: v1/chat/completions
  replicas: ${REPLICAS}

  modelSourceConfig:
    modelSourceType: s3
    s3Storage:
      bucketName: ${MODEL_BUCKET}
      region: ${REGION}
    modelLocation: ${MODEL_LOCATION}
    prefetchEnabled: false

  kvCacheSpec:
    enableL1Cache: true
    enableL2Cache: true #这里后来是让打开了，但是跑不起来
    l2CacheSpec:
      l2CacheBackend: "tieredstorage"
      # 使用 Redis 作为 L2 Cache 后端时，取消注释以下两行并注释上方 tieredstorage：
      # l2CacheBackend: "redis"
      # l2CacheLocalUrl: "redis://redis.redis-system.svc.cluster.local:6379/path1/"  /opt/dlami/nvme/
      # lmcacheConfigPath: "/mnt/s3/lmcache-config.yaml"

  intelligentRoutingSpec:
    enabled: true
    routingStrategy: ${ROUTING_STRATEGY}

  tlsConfig:
    tlsCertificateOutputS3Uri: ${CERT_S3_URI}

  metrics:
    enabled: true
    modelMetrics:
      port: 8000

  loadBalancer:
    healthCheckPath: /health

  worker:
    image: ${MODEL_IMAGE}
    args:
      - "--model"
      - "/opt/ml/model"
      - "--max-model-len"
      - "${MAX_MODEL_LEN}"
      - "--tensor-parallel-size"
      - "${TENSOR_PARALLEL_SIZE}"
    resources:
      limits:
        nvidia.com/gpu: "${GPU_COUNT}"
      requests:
        cpu: "${CPU_REQUEST}"
        memory: ${MEMORY_REQUEST}
        nvidia.com/gpu: "${GPU_COUNT}"
    modelInvocationPort:
      containerPort: 8000
      name: http
    modelVolumeMount:
      name: model-weights
      mountPath: /opt/ml/model
    environmentVariables:
      - name: OPTION_ROLLING_BATCH
        value: "vllm"
      - name: SAGEMAKER_SUBMIT_DIRECTORY
        value: "/opt/ml/model/code"
      - name: MODEL_CACHE_ROOT
        value: "/opt/ml/model"
      - name: SAGEMAKER_MODEL_SERVER_WORKERS
        value: "1"
      - name: SAGEMAKER_MODEL_SERVER_TIMEOUT
        value: "3600"
      - name: LMCACHE_REMOTE_URL
        value: "file:///mnt/curvine/l2cache/"
```

部署完成后，打 patch 的方式来挂curvine
```bash
kubectl patch deployment qwen2-7b-instruct-kvcache -n default --type=json -p='[
  {
    "op": "add",
    "path": "/spec/template/spec/volumes/-",
    "value": {
      "name": "curvine-cache",
      "persistentVolumeClaim": {"claimName": "curvine-pvc"}
    }
  },
  {
    "op": "add",
    "path": "/spec/template/spec/containers/0/volumeMounts/-",
    "value": {
      "name": "curvine-cache",
      "mountPath": "/mnt/curvine/l2cache"
    }
  }
]'
```
