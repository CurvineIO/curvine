# CPU 推理镜像：vLLM + Curvine（环境变量对接）

本文说明如何 **构建** 带 Curvine Python SDK 的 **CPU 推理 Docker 镜像**，以及如何在 **部署阶段用环境变量** 把 vLLM 接到 Curvine（通过 **`CurvineKVConnector`** 的 **`--kv-transfer-config`**），而无需手写一长串 JSON。

> **相关文档**：仅验证 **Native 写块 + `cv fs` 列目录**、不跑完整 vLLM 服务时，见同目录 **`testing-native-kv-e2e.md`**（仓库路径：`doc/testing-native-kv-e2e.md`）。

---

## 一、整体流程概览

```mermaid
flowchart LR
  A[构建 Curvine wheel] --> B[构建 vLLM CPU 底镜像 vllm-openai]
  B --> C[在 curvine-vllm 打 Curvine 层镜像]
  C --> D[docker run 设置环境变量]
  D --> E[vllm serve 带 Curvine KV]
```

---

## 二、先构建 Curvine Python SDK 安装包（wheel）

在 **curvine-vllm 仓库根目录** 执行（需已安装 Docker 以外的前置：如 `make`、`protoc` 等，详见仓库 **`README` / `AGENTS`** 说明）：

```bash
cd /path/to/curvine-vllm
make build
```

完成后请确认存在（文件名随版本与 CPU 架构变化）：

- **`build/dist/lib/curvine_libsdk-*-cp38-abi3-*.whl`**（`linux_aarch64` 或 `linux_x86_64` 等）  
- **`build/dist/conf/curvine-cluster.toml`**（若你计划在容器内用 **Native** 后端，需将该文件挂载进容器）

> **注意**：`docker/Dockerfile.vllm-curvine-cpu` 要求 **`build/dist/lib` 下仅有一个** 符合通配符的 wheel；若存在多个，请只保留目标架构的那一个，或修改 Dockerfile 为固定文件名。

---

## 三、两仓库构建顺序（重要）

**`vllm-project/vllm` 与 `curvine-vllm` 是两个独立 Git 仓库**：底镜像在 **vLLM 仓库** 构建，Curvine 层在 **curvine-vllm 仓库** 构建。请先完成 **第二节 wheel**，再在 vLLM 仓库打 **`vllm-openai-cpu:local`**，最后在 curvine-vllm 仓库打 **`vllm-curvine-cpu:latest`**。

**基础镜像 `ubuntu:22.04` 与 BuildKit：** 若使用 **`docker build --pull`** 时出现 **`unable to fetch descriptor ... content size of zero`**，多为 BuildKit **强制重拉** manifest 时 Docker Hub / 镜像站返回异常，**不是** Dockerfile 写错。推荐流程：

1. 在宿主机执行 **`docker pull ubuntu:22.04`**（单独拉 tag，走普通 pull 路径，通常可成功）。  
2. 再执行下面的 **`docker build` 时不要加 `--pull`**，让 BuildKit 使用本机已有 **`ubuntu:22.04`** 元数据。  
3. 若仍失败：尝试 **`docker builder prune -f`**、更换网络或临时关闭 **`registry-mirrors`** 后重试；需要「始终强制更新基础镜像」时再考虑 **`--pull`**。

---

## 四、构建 vLLM 官方 CPU「OpenAI 服务」底镜像

在 **vLLM 源码仓库根目录**（`Dockerfile.cpu` 所在仓库）执行，目标阶段 **`vllm-openai`**，产出 **`vllm-openai-cpu:local`**：

```bash
cd /path/to/vllm
docker pull ubuntu:22.04
docker build -f docker/Dockerfile.cpu --target vllm-openai -t vllm-openai-cpu:local .
```

（若你确认网络与镜像源稳定，可改用 **`docker build --pull ...`**；遇到上一节所述 **descriptor / content size of zero** 错误时，请改回 **先 `pull`、构建时不带 `--pull`**。）

说明：

- 该阶段镜像内 **`ENTRYPOINT`** 原为 **`vllm serve`**。  
- 下一节 **Curvine 层** 会替换为 **`entrypoint-vllm-serve-curvine.sh`**：在未开启 Curvine 开关时，行为与 **`vllm serve`** 一致；开启时自动追加 **`--kv-transfer-config`**。
- 基础镜像已包含 **`util-linux`**（提供 **`lscpu`**），供 vLLM CPU V1 引擎初始化 **OMPProcessManager** 使用；若你使用极旧底镜像，Curvine 层 Dockerfile 内仍会再装一层 **`util-linux`** 作为兜底。

> **构建耗时**：从源码编译 vLLM CPU 版本通常较长，请预留足够时间与磁盘。

---

## 五、构建 Curvine 层镜像（curvine-vllm 仓库）

在 **curvine-vllm 仓库根目录**（本仓库，含 **`docker/Dockerfile.vllm-curvine-cpu`**）执行；**`--build-arg VLLM_BASE_IMAGE`** 指向上一节在同一台机器上已构建的 **`vllm-openai-cpu:local`**：

```bash
cd /path/to/curvine-vllm
docker build -f docker/Dockerfile.vllm-curvine-cpu \
  --build-arg VLLM_BASE_IMAGE=vllm-openai-cpu:local \
  -t vllm-curvine-cpu:latest .
```

**`Dockerfile` 做了什么（摘要）**：

1. 以 **`VLLM_BASE_IMAGE`** 为基础镜像（默认 **`vllm-openai-cpu:local`**）。  
2. **`apt-get install util-linux`**：确保存在 **`lscpu`**（vLLM CPU V1 **`OMPProcessManager`** 依赖）；与上游 **`Dockerfile.cpu`** 中已包含的 **`util-linux`** 叠加时通常仅为幂等安装。  
3. 将 **`build/dist/lib/curvine_libsdk-*-cp38-abi3-*.whl`** 拷入镜像并 **`uv pip install`**，同时固定 **`protobuf==3.20.3`**。  
4. 从本仓库拷贝 **`curvine-libsdk/python/curvinefs`** 与 **`curvine_libsdk/_proto`** 到 **`/opt/venv/lib/python3.12/site-packages`**（与上游 CPU 镜像默认 Python 3.12 一致；若你自行改了底镜像 Python 版本，需同步修改 Dockerfile 中的 **`PYTHON_SITE`**）。  
5. 安装入口脚本 **`/usr/local/bin/entrypoint-vllm-serve-curvine.sh`** 并设为 **`ENTRYPOINT`**。

---

## 六、部署：环境变量说明

### 5.1 总开关

| 变量 | 含义 |
|------|------|
| **`VLLM_USE_CURVINE_KV=1`** | 启用 Curvine KV；为 **`0`** 或未设置时，入口脚本直接 **`exec vllm serve`**，与官方镜像行为一致。 |

### 5.2 POSIX 后端（FUSE / 共享目录，CPU 场景常见）

| 变量 | 默认 | 说明 |
|------|------|------|
| **`CURVINE_KV_BACKEND`** | `posix` | 使用 **`PosixCurvineStoreClient`**。 |
| **`CURVINE_STORE_ROOT`** / **`CURVINE_FUSE_MOUNT_ROOT`** | `/mnt/curvine` | KV 块根目录。 |
| **`CURVINE_MODEL_ID`** | 空 | 不传则使用 vLLM 的 **`model`** 名（经 sanitize 写入路径）。 |
| **`CURVINE_TP_RANK`** | 不传则不出现在 extra | 整数。 |
| **`CURVINE_KV_GROUP_ID`** | 同上 | 整数。 |

### 5.3 Native 后端（Python SDK 直连 Curvine 集群）

| 变量 | 默认 | 说明 |
|------|------|------|
| **`CURVINE_KV_BACKEND=native`** | — | 使用 **`NativeCurvineStoreClient`**。 |
| **`CURVINE_SDK_CONFIG_PATH`** | **必填** | 容器内 **`curvine-cluster.toml`** 路径（建议 **`volume` 挂载**）。 |
| **`CURVINE_NATIVE_ROOT`** | `/curvine_kv` | KV 在 Curvine 命名空间下的前缀。 |
| **`CURVINE_WRITE_CHUNK_NUM` / `CURVINE_WRITE_CHUNK_SIZE`** | 不传则用代码默认 | 与 **`CurvineClient`** 构造参数一致。 |

### 5.4 可选：Native 扩展 static TLS

若进程内导入 **`curvine_libsdk._native`** 报 **`cannot allocate memory in static TLS block`**，可设置：

```text
CURVINE_LD_PRELOAD=/opt/venv/lib/python3.12/site-packages/curvine_libsdk/_native.abi3.so
```

（若底镜像 Python 不是 3.12，请进入容器用 **`python -c "import site; print(site.getsitepackages())"`** 确认路径后修改。）

---

## 七、运行示例（`docker run`）

### 6.1 POSIX：挂载 FUSE 或共享存储目录

```bash
docker run --rm -p 8000:8000 \
  -e VLLM_USE_CURVINE_KV=1 \
  -e CURVINE_KV_BACKEND=posix \
  -e CURVINE_STORE_ROOT=/mnt/curvine \
  -v /宿主机/curvine挂载点:/mnt/curvine:rw \
  -v /宿主机/模型目录:/models:ro \
  vllm-curvine-cpu:latest \
  /models/你的模型目录或HF-ID \
  --host 0.0.0.0 --port 8000
```

### 6.2 Native：挂载集群配置 + 可选 LD_PRELOAD

```bash
docker run --rm -p 8000:8000 \
  -e VLLM_USE_CURVINE_KV=1 \
  -e CURVINE_KV_BACKEND=native \
  -e CURVINE_SDK_CONFIG_PATH=/etc/curvine/curvine-cluster.toml \
  -e CURVINE_NATIVE_ROOT=/curvine_kv \
  -e CURVINE_LD_PRELOAD=/opt/venv/lib/python3.12/site-packages/curvine_libsdk/_native.abi3.so \
  -v /宿主机/curvine-cluster.toml:/etc/curvine/curvine-cluster.toml:ro \
  -v /宿主机/模型目录:/models:ro \
  vllm-curvine-cpu:latest \
  /models/你的模型目录或HF-ID \
  --host 0.0.0.0 --port 8000
```

入口脚本会把上述变量折叠为一条 **`--kv-transfer-config '<json>'`**，再执行 **`vllm serve`**。

---

## 八、镜像与 Curvine 集成自测（建议步骤）

以下不涉及真实权重推理负载时，可只做「镜像是否可用、Curvine 包是否可导入」级别检查。

### 7.1 检查镜像内 Curvine Python 包

不经过自定义 **`ENTRYPOINT`**，直接指定解释器（避免启动完整 **`vllm serve`**）：

```bash
docker run --rm --entrypoint /opt/venv/bin/python vllm-curvine-cpu:latest \
  -c "import curvinefs.curvineClient; import curvine_libsdk; print('curvine sdk ok')"
```

若此处失败，请对照 **第六节 LD_PRELOAD**、**protobuf**、以及 **`curvinefs/_proto`** 是否随镜像拷贝成功。

### 7.2 检查入口脚本是否注入 `--kv-transfer-config`（可选）

进入容器后手动打印环境并试运行（需你本机已有 **`/tmp/dummy`** 等占位，或改为真实模型路径）：

```bash
docker run --rm -it \
  -e VLLM_USE_CURVINE_KV=1 \
  -e CURVINE_KV_BACKEND=posix \
  -e CURVINE_STORE_ROOT=/mnt/curvine \
  --entrypoint bash vllm-curvine-cpu:latest
# 容器内：
# vllm serve --help | head
# 或用小模型试跑（耗时与资源取决于模型）
```

### 7.3 Native 写块与 `cv fs`（不经过 vLLM）

与 **`testing-native-kv-e2e.md`** 中步骤一致：在**宿主机或另一调试容器**内用 **`NativeCurvineStoreClient`** 写 **`.kvblk`**，再在 **`build/dist`** 下用 **`cv fs ls`** 核对。用于确认 **集群与配置** 正常，再叠加 **vLLM 推理**。

### 7.4 vLLM 仓库内的单元测试（可选，宿主机非容器）

在 **vLLM** 仓库、已配置 **`.venv`** 的前提下（需能 **`import vllm`**，部分环境可能受 sklearn/TLS 等影响）：

```bash
cd /path/to/vllm
PYTHONPATH=. .venv/bin/python -m unittest tests.v1.kv_connector.unit.test_curvine_store -v
```

该测试包含 **POSIX** 与 **Fake SDK 的 Native 契约**；**真实集群 Native 全链路**仍以 **`testing-native-kv-e2e.md`** 为准。

---

## 九、与手写命令行的等价关系

等价于（节选）：

```bash
vllm serve YOUR_MODEL \
  --kv-transfer-config '{"kv_connector":"CurvineKVConnector","kv_role":"kv_both","kv_connector_extra_config":{...}}'
```

其中 **`kv_connector_extra_config`** 的键与 **`make_curvine_store_client`** 实现一致（见 vLLM **`vllm/.../curvine/store.py`**）。

---

## 十、Kubernetes 部署提示

将 **`docker run`** 中的 **`-e`** 逐项写成 **`env`**；**`CURVINE_SDK_CONFIG_PATH`** 对应文件用 **`ConfigMap`/`Secret` + `volumeMount`** 挂载到容器内路径即可。

---

## 十一、附录：文件位置

| 文件 | 作用 |
|------|------|
| **`docker/Dockerfile.vllm-curvine-cpu`** | Curvine 层镜像定义。 |
| **`docker/entrypoint-vllm-serve-curvine.sh`** | 读取环境变量并 **`exec vllm serve`**。 |
