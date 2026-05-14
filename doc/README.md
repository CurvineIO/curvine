# 联调与部署说明（中文）

本目录收录 **Curvine 与 vLLM** 相关的自建说明（仓库根目录下 **`docs/`** 目录被 `.gitignore` 忽略，故使用 **`doc/`** 以便纳入版本控制）。

| 文档 | 内容 |
|------|------|
| [docker-vllm-curvine-cpu.md](docker-vllm-curvine-cpu.md) | 如何构建 **CPU 推理镜像**、环境变量对接 Curvine、运行与自测步骤。 |
| [testing-native-kv-e2e.md](testing-native-kv-e2e.md) | **Native** 路径下 **真实 CurvineClient + 真实集群** 的端到端验证（含 `cv fs`）。 |
