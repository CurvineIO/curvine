# Native KV 端到端自测（真实 CurvineClient + 真实集群）

本文说明如何在**本机**复现与线上一致的 **Native 路径**：使用 vLLM 侧 **`NativeCurvineStoreClient`**（见 vLLM 源码中的 `store.py`），通过官方 **Python SDK（`curvinefs.CurvineClient`）** 访问 **真实 Curvine 集群**，完成写块、读回，并用 **`cv fs`** 在集群命名空间里看到 **`.kvblk`** 文件。

> **与单元测试的区别**：vLLM 仓库里 `tests/v1/kv_connector/unit/test_curvine_store.py` 中的 **`FakeCurvineSdk`** 是内存假实现，只校验契约，**不连集群**。本文流程**不使用** Fake，全部为真实 RPC。

> **相关文档**：若需 **CPU 推理镜像**、容器内用**环境变量**对接 Curvine，见同目录 **`docker-vllm-curvine-cpu.md`**（仓库路径：`doc/docker-vllm-curvine-cpu.md`）。

---

## 一、你在验证什么

1. Python 能正常导入 **`curvine_libsdk`**、**`curvinefs`**，并能用 **`curvine-cluster.toml`** 构造 **`CurvineClient`**。  
2. **`NativeCurvineStoreClient.write_block`** 在 Curvine 上创建约定路径（根前缀默认 **`/curvine_kv`**），写入数据后经 **`rename`** 去掉 **`.tmp`**。  
3. **`read_block`** 读回的字节与写入一致。  
4. 使用发行目录下的 **`./bin/cv fs ls`**，能在集群命名空间中看到对应 **`.kvblk`**。

---

## 二、前置条件

| 项目 | 说明 |
|------|------|
| **Curvine 集群** | Master 与至少 **一个可用 Worker**；否则 flush 阶段会出现 *No available worker found*。 |
| **集群配置** | 例如本仓库构建产物：`build/dist/conf/curvine-cluster.toml`（或你自备路径）。 |
| **Python SDK 安装包** | 例如 `build/dist/lib/curvine_libsdk-*-cp38-abi3-linux_*.whl`（由根目录 **`make build`** / **`build/build.sh`** 生成，架构需与本机一致）。 |
| **Python 版本** | 建议使用 **3.8+**（如 3.12）。系统自带的 **3.6** 虚拟环境无法安装 **cp38-abi3** 类 wheel，请换用高版本解释器创建 venv。 |
| **protobuf** | 当前仓库生成的 `*_pb2` 与 **protobuf 4+/5+** 不兼容，请固定 **`protobuf==3.20.3`**，除非你已用新版 **protoc** 重新生成 Python stub。 |
| **vLLM 的 `store.py`** | 需要本机有一份 vLLM 源码树，路径形如：`vllm/vllm/distributed/kv_transfer/kv_connector/v1/curvine/store.py`。下文用 **`VLLM_ROOT`** 表示 vLLM 仓库根目录。 |

### 可选：导入 `_native` 时出现 static TLS 报错

若报错 **`cannot allocate memory in static TLS block`**，可在执行 Python **之前** 对扩展 **`.so` 做预加载**：

```bash
export LD_PRELOAD="/你的venv路径/lib/python3.12/site-packages/curvine_libsdk/_native.abi3.so"
```

（将路径中的 Python 小版本、`site-packages` 换成你本机实际路径。）

---

## 三、先准备 Curvine wheel（若尚未构建）

在 **curvine-vllm 仓库根目录**（含 `Makefile`、`build/build.sh`）执行：

```bash
make build
# 或按需跳过部分组件，例如：
# make build ARGS='--skip-java-sdk'
```

完成后应存在：

- `build/dist/lib/curvine_libsdk-*-cp38-abi3-*.whl`  
- `build/dist/conf/curvine-cluster.toml`  
- `build/dist/bin/cv`（**建议用 `build/dist` 整套**，与 `conf/curvine-env.sh`、`lib/curvine-cli` 配套）

---

## 四、步骤 1：虚拟环境与 wheel 安装

将下列路径改成你本机绝对路径：

```bash
export ROOT="/path/to/curvine-vllm"
export VLLM_ROOT="/path/to/vllm"
export WHL="$ROOT/build/dist/lib/curvine_libsdk-0.1.0-cp38-abi3-linux_aarch64.whl"   # 按实际文件名/架构修改
export CONF="$ROOT/build/dist/conf/curvine-cluster.toml"
export PY="/path/to/python3.12"    # 例如 /root/.local/bin/python3.12

rm -rf /tmp/native-kv-e2e
"$PY" -m venv /tmp/native-kv-e2e
/tmp/native-kv-e2e/bin/pip install -q --upgrade pip
/tmp/native-kv-e2e/bin/pip install -q "protobuf==3.20.3" "$WHL"
```

---

## 五、步骤 2：合并 `curvinefs` 与 `_proto`（wheel 为「仅原生库」形态时）

部分构建产物 wheel 里**只有** `curvine_libsdk` 的 **`.so`**，**不包含** **`curvinefs`** 与 **`curvine_libsdk/_proto/*_pb2.py`**。此时需从**本仓库源码**拷贝到 venv 的 **`site-packages`**：

```bash
export SITE="$(/tmp/native-kv-e2e/bin/python -c 'import site; print(site.getsitepackages()[0])')"

cp -a "$ROOT/curvine-libsdk/python/curvinefs" "$SITE/"
mkdir -p "$SITE/curvine_libsdk/_proto"
cp -a "$ROOT/curvine-libsdk/python/curvine_libsdk/_proto/"* "$SITE/curvine_libsdk/_proto/"
```

若你的 wheel 已内嵌上述包，可跳过本节。

---

## 六、步骤 3：执行端到端 Python 片段

通过 **`importlib`** 按文件路径加载 **`store.py`**（无需 **`pip install vllm`**）。**`NativeCurvineStoreClient`** 不传 **`cv_client`** 时，内部会创建**真实** **`CurvineClient`**。

```bash
export STORE_PY="$VLLM_ROOT/vllm/distributed/kv_transfer/kv_connector/v1/curvine/store.py"
# 若使用 LD_PRELOAD，请在本 shell 中先于 python 执行 export。

/tmp/native-kv-e2e/bin/python <<'PY'
import importlib.util
import sys
from pathlib import Path

conf = "/path/to/curvine-vllm/build/dist/conf/curvine-cluster.toml"
store_path = Path("/path/to/vllm/vllm/distributed/kv_transfer/kv_connector/v1/curvine/store.py")

spec = importlib.util.spec_from_file_location("curvine_store_e2e", store_path)
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules["curvine_store_e2e"] = mod
spec.loader.exec_module(mod)

block_key = "e2e-native-kv-smoke-block"
payload = b"E2E-KVBLK-" + bytes(range(64))

client = mod.NativeCurvineStoreClient(
    config_path=conf,
    root_prefix="/curvine_kv",
    model_id="e2e_test_model",
    tp_rank=0,
    kv_group_id=0,
    write_chunk_num=8,
    write_chunk_size=128 * 1024 * 1024,
)

p = client.build_block_path(block_key)
print("path:", p)
print("exists_before:", client.exists(block_key))
client.write_block(block_key, payload)
print("exists_after:", client.exists(block_key))
got = client.read_block(block_key)
assert got == payload
print("read_ok bytes:", len(got))
PY
```

请将脚本中的 **`conf`**、**`store_path`** 改为你的绝对路径（或与上面 **`export`** 一致）。

### 预期对象路径（与 vLLM `NativeCurvineStoreClient` 规则相同）

```text
/curvine_kv/{经 sanitize 的 model_id}/tp-{tp}/kg-{kg}/{sha256(block_key) 的前 2 位十六进制}/{经 sanitize 的 block_key}.kvblk
```

本示例中 **`block_key`** 为 **`e2e-native-kv-smoke-block`** 时，前两字节哈希为 **`22`**，完整路径示例：

```text
/curvine_kv/e2e_test_model/tp-0/kg-0/22/e2e-native-kv-smoke-block.kvblk
```

---

## 七、步骤 4：用 `cv` 在集群上核对

请使用 **`build/dist`** 目录下的 **`cv`**（与 **`conf/curvine-env.sh`**、**`lib/curvine-cli`** 一致）。若无可执行权限：

```bash
chmod +x "$ROOT/build/dist/bin/cv"
cd "$ROOT/build/dist"
./bin/cv fs ls /curvine_kv
./bin/cv fs ls /curvine_kv/e2e_test_model/tp-0/kg-0/22
./bin/cv fs ls -l /curvine_kv/e2e_test_model/tp-0/kg-0/22/e2e-native-kv-smoke-block.kvblk
```

应能看到 **`.kvblk`** 且大小与 payload 长度一致（示例为 74 字节）。

---

## 八、与 vLLM 运行时的对应关系

线上 vLLM 通过 **`kv_connector_extra_config`** 选择后端：

- **`curvine_backend: "native"`** → 使用 **`NativeCurvineStoreClient`**。  
- **`curvine_sdk_config_path`** → 与本指南中的 **`curvine-cluster.toml`** 作用相同。  
- **`curvine_native_root`** → 省略时默认为 **`/curvine_kv`**，与本示例 **`root_prefix`** 一致。

本指南的手动脚本等价于「不启动完整 vLLM 服务、只验证 **Native 存储路径**」的最小闭环。

---

## 九、常见问题

| 现象 | 可能原因 |
|------|-----------|
| **No available worker found** | 集群无可用 Worker 或策略未命中。 |
| **ModuleNotFoundError: curvinefs** | wheel 未带 Python 包，需执行 **第五节** 合并源码目录。 |
| **Descriptors cannot be created directly** | **protobuf** 过新，请降级到 **3.20.3** 或重新生成 `_pb2`。 |
| **`./build/bin/cv` Permission denied** | 改用 **`build/dist/bin/cv`**，并确认 **`build/dist/conf`** 存在。 |
| 导入 **`.so`** 报 **static TLS** | 使用 **第二节** 的 **`LD_PRELOAD`**。 |

---

## 十、清理（可选）

在 **`build/dist`** 下删除测试对象：

```bash
cd "$ROOT/build/dist"
./bin/cv fs rm /curvine_kv/e2e_test_model/tp-0/kg-0/22/e2e-native-kv-smoke-block.kvblk
```

或在 Python 中重新构造 **`NativeCurvineStoreClient`** 后调用 **`delete_block(block_key)`**。
