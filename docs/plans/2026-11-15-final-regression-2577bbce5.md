# 最终固定源 2577bbce5 完整回归（C2-FU 恢复后）

源码固定源：`2577bbce5f1555a246f5786a5441b08a78e19f9f`（tree `d91c98f6007e3fef61a552b21a24876c5470c785`）
本轮唯一代码提交：`2577bbce5`；随后只有 docs-only 提交（`git diff 2577bbce5..HEAD -- '*.rs'` 为空可证）

原始记录：`/tmp/cv-reg-2577/`（每 target `.log` + `.status`；`source.sha`/`source.after` 一致）
独立复验：左辅 `/tmp/cv-reg-2577-20261115T021701Z/`（fault lane，数字完全一致）

## 一、数字（main-suite 口径）

**口径定义**：lib 取 `running N tests` 对应的主 `test result` 行；`journal_uncommitted_restart_test`
等 target 内部会再起子进程 harness（`running 1 test` / `running 0 tests`），**不累加**。
上一版报告把子进程输出加进总数，导致多算 1 例（`abcd` 我报 1024，实为 1023 —— 左辅的
1023 正确）。本轮已按此口径重算。

| 组 | default | fault-injection |
|---|---|---|
| R1 lib（主 suite） | **739 passed / 0 failed / 3 ignored** | **795 passed / 0 failed / 3 ignored** |
| 43 integration targets | **220 passed / 0 failed / 1 ignored**（42 个有实际执行） | **232 passed / 0 failed / 1 ignored**（43 个有实际执行） |
| 合计 | **959 passed / 0 failed / 4 ignored** | **1027 passed / 0 failed / 4 ignored** |

相对上一源 `abcd5530f`（735+220=955 / 791+232=1023）：lib 各 **+4**
（`fenced_window_rejects_real_recursive_rpcs_by_original_epoch` 1 例 +
`covers_rejects_*` 3 例），integration 不变。

未记通过（如实）：
- `dir_tree_memory`：两模式 `0 passed / 1 ignored`（唯一用例设计性 `#[ignore]`）
- `fault_isolated_test`：default 侧 `running 0 tests`（feature 门控），fault 侧 1 passed
- R1 的 3 个 ignored：`status_probe_owned`、`bench_create_tree_large`、`dir_space_scale_one_million_inode_tree`

## 二、受影响 crate（default）

| crate | 结果 |
|---|---|
| curvine-rocksdb | 22 / 0 |
| curvine-fs-api | 26 / 0 |
| curvine-model | 134 / 0 |
| curvine-raft | 136 / 0 / 1 ignored |
| curvine-fault | 5 / 0 |
| curvine-fault `--features fault-injection` | 19 / 0 |
| curvine-tests（`replication_test` / `actual_storage_type_test`） | 4 / 0 与 3 / 0 |

## 三、门禁

| 检查 | 结果 |
|---|---|
| `cargo fmt --all -- --check` | **0** |
| `git diff --check` | **0** |
| `clippy -p curvine-master --no-deps --all-targets --deny=warnings`（default） | **0** |
| 同上 `--features fault-injection` | **0** |
| 全 workspace `clippy --all-targets` | **不通过（101）**，两侧编译失败包同为 `curvine-server`；可比范围内 only-in-candidate **0**、count-increased **0**；`curvine-server` 未扫描部分属**未验证** |

## 四、curvine-server

**139 passed / 1 failed / 1 ignored**。唯一失败 `small_file_job_transfers_without_task_rows`
（基线同位置同报错）。**不豁免**：该项**不满足验收**。

## 五、未落实/未验证

见 `2026-11-15-design-to-source-map.md` 第 F 节与第 I 节；要点：
WAL-on A/B 未启动；整体 lint 门禁不通过；`dir_tree_memory` 零执行；
`snapshot_restore_failure_test` 偶发 Timeout 未定位（本轮 10/0 与 16/0 均绿，但仍属未定位观察项）；
`small_file_job_transfers_without_task_rows` 不满足验收；C2-FU-04 按设计不要求修改。
