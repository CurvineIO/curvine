# 证据目录来源说明

本目录全部 `*.argv`/`*.status`/`*.log` 三件套（104 组）均为
**2026-11-15 12:19 起**在隔离工作树 `/tmp/cv-verify-final`（HEAD=`c0c8f783a`）
同步记录：脚本在每条 `cargo` 进程启动前写 `.argv`（含 `source_sha`/`workdir`/`env`/`argv:`），
进程结束后写 `.status`（exit code）与 `.log`（完整 stdout/stderr）。
`.log` 里的 `Running /deps/xxx-hash` 行回填进 `.argv` 末尾，形成 argv→二进制的绑定。

## 遗留文件（非本目录本次产物，已重命名标记）

| 原文件名 | 改后 | 说明 |
|---|---|---|
| `curvineraft.argv` | `curvineraft.argv.legacy-20261114T2216` | 时间戳 2026-11-14 22:16，**早于本目录创建**（11-15 10:51）。格式为 `CARGO_TARGET_DIR=… cargo test…`（无 `source_sha`/`workdir`/`argv:` 头），是旧证据目录 `cv-reg-final2`/`cv-reg-744f` 遗留物被 `cp -n` 带入；与本目录权威 `curvine_raft.argv` 并存属命名碰撞（`curvine_raft` 的连字符展开歧义）。保留原始时间戳与内容，但**不纳入统计**——raft 的权威结果以 `curvine_raft.*`（exit 101，含并发失败）为准。 |

## 数字口径（main-suite）

每条 log 的最后一条含 `filtered out` 的 `test result` 行为主 suite；
之前出现的 `running 1 test`/`running 0 tests`（在 `filtered out != 0` 之前）为子进程 harness，不计入。
