> **已被取代**：本文件第 1 节的统计口径（把子进程 harness 计入总数）与第 3 节的 lint 表述
> 均已被 `2026-11-15-selfreview-v2.md` 纠正；A14/A15 曾引用不存在的 `fenced_window_matrix`，
> 该用例已在 `2577bbce5` 真正恢复。以 v2 与最终回归报告为准。

# 最终候选 744f9b5c6 自审（执衡退回后第一轮）

## 0. 本文性质

执衡对 `2026-11-15-final-regression-744f9b5c6-accepted.md` 退回五点后的逐条自审。
**被审源仍是 `744f9b5c6`**；本轮为修自审发现的问题产生了新提交，新固定源在报告末尾给出。
凡"我此前的声明"与本文件冲突，**以本文件为准**。

## 1. 执衡第 1 点：把 exit 0 当测试通过 —— 已确认，是我的报告错误

原始日志可复核（`/tmp/cv-reg-final5/`）：

| target | default 日志实际 | 我此前写的 | 事实 |
|---|---|---|---|
| `fault_isolated_test` | `running 0 tests` / `ok. 0 passed` | "两模式各 1/0" | **default 侧一个测试都没跑**（测试体 `#[cfg(feature="fault-injection")]`），只有 fault 侧 `1 passed` |
| `dir_tree_memory` | `0 passed / 1 ignored`（两模式） | 计入"43/43 全绿" | 唯一用例被设计性 `#[ignore]`，**零执行** |

修正后按"实际执行 / ignored / feature 不适用"三分类的真实合计：

| 组 | default | fault-injection |
|---|---|---|
| R1 lib | 735 passed / 0 failed / 3 ignored | 791 passed / 0 failed / 3 ignored |
| R2 (26 targets) | 零执行 1 个（`dir_tree_memory`），**220 passed / 0 failed / 1 ignored** | 同，**232 passed / 0 failed / 1 ignored** |
| R3 (10 targets) | **含在 220 内**（R2+R3+R4 合计 220） | **含在 232 内** |
| R4 (7 targets) | `fault_isolated_test` default 零执行 | 1 passed |
| 44 个 target 合计 | **220 passed / 0 failed / 1 ignored**，其中 42 个有实际执行 | **232 passed / 0 failed / 1 ignored**，其中 43 个有实际执行 |
| 含 R1 的总计 | **955 passed / 0 failed / 4 ignored** | **1023 passed / 0 failed / 4 ignored** |

（`/tmp/cv-reg-final5/` 逐 target 核算：`awk` 汇总 `.log` 的 `test result` 行。零执行 ≤2 个：
`dir_tree_memory` 两模式皆 0 passed/1 ignored —— 唯一用例设计性 `#[ignore]`；
`fault_isolated_test` 在 default 侧 `running 0 tests`（测试体 `#[cfg(feature="fault-injection")]`），
fault 侧 1 passed。）

左辅独立复验（fault lane，`/tmp/cv-reg-744f-20261115T002751Z/`）：44 目标中 43 个有实际执行，
1022 passed / 0 failed / 4 ignored = 791（R1）+ 231（R2-R4）。**他的 1022 比修复后的 1023 少 1，
正是第 2.1 节我误删的那个测试**（`namespace_commit_gate_test` 17→18 恢复后 +1）。这也解释了
他"与 5d3 的 1023 差 1"的疑问：差值不是脚本口径，是我删了一个既有测试。

## 2. 本轮自审发现的真实缺陷（已修）

### 2.1 误删既有设计测试 `filesystem_handles_for_same_journal_share_commit_gate`

`git diff 5d3bdd177..744f9b5c6` 里我被 clippy `dead_code` 清理连带删掉了一个**有效**测试：
它验证"同一 journal 的两个 fs 句柄共享同一 commit gate"（S7 gate 的核心共享语义）。
`5d3` 该文件 18 个测试，`744` 变 17 个 —— 右弼问的"你侧 +1"其实是**我 -1**。
已恢复（源码 +30 行），`cargo test -p curvine-master --test namespace_commit_gate_test`
= **18 passed**，与 5d3 的测试名集合逐字一致。

根因：把 `DEFAULT_COLD_TIMEOUT` 未使用 import 与同文件的 dead_code 一起处理时，
clippy 的 `--fix` 对该测试的 `use` 段做了改写，我随后整段删除而未核对该 `#[test]` 是否仍被引用。

### 2.2 `tree_fingerprint` 覆盖窄于原 `sum_hash`

我此前把 server 一致性验证从 `sum_hash` 换成"路径 → (id, is_dir, mtime, atime, nlink, 子名)"，
**这比原编码哈希少了 len / blocks / storage_policy / ctime**，属于**测试覆盖变窄**。
已改为 `path -> String`，包含 `id/dir/mtime/atime/ctime/nlink/policy` + 文件侧
`simple_string()`（id/pid/len/nlink/blocks）+ `is_complete()` + 排序子名，
并把 `index_tree`（同文件重复实现，触发 `clippy::type_complexity`）合并进同一 helper。
`cargo test -p curvine-server --test master_fs_test` = **88 passed / 1 ignored**。

### 2.3 `curvine-tests` 11 处 E0599（编译破坏，此前完全没跑到）

全 workspace clippy 暴露：`curvine-tests/tests/replication_test.rs` 6 处、
`actual_storage_type_test.rs` 5 处仍在用已删除的 `fs_dir.write()/.read()`。
**这些 target 在我此前的"43 个 integration target"清单里根本不存在**，所以"整体回归"没覆盖它们。
已迁移（`read()` → `view()?`，`write()` → `begin_cold(30s)?.view()`），
`cargo test -p curvine-tests --test replication_test --test actual_storage_type_test` = **3 + 4 passed**。

### 2.4 `let _target_depth` 死变量

5d3 的 `target_depth == 0` 传参被硬编码 `false`（依赖 `ids.len() < 2` 早退守卫，等价成立），
但变量本身留下无用计算 `ids.len() - 1`（`len == 0` 时还会下溢 panic）。已删除两处。

### 2.5 raft 测试注释与事实不符

`raft_node_test.rs` 注释写"the assertion stays exactly as strict"，实际是
**从"客户端返回时 probe 已武装"放宽为"10s 内最终武装"**，且被容忍的情形
（重复提案返回未知结果）属 §2/§3.2.1 合法态。注释已按实际改写，不再声称强度不变。

## 3. 执衡第 2 点：全 workspace lint 门禁 —— 真实状态是不通过

**真实 CI 口径**（`cargo clippy --workspace --all-targets --keep-going --message-format=json`）：

| 侧 | exit | diagnostics | 说明 |
|---|---|---|---|
| 基线 `3c6949f4f`（worktree `/tmp/cv-base2-3c6949f4f`, tree `81342318`, dirty 0） | **101** | 89 | `curvine-server` test "journal_test" 1 error 编译失败 |
| 候选 `744f9b5c6` | **101** | 22 | `curvine-server` + `curvine-tests` 编译失败 |

基线唯一 error 级：`clippy::absurd_extreme_comparisons` @ `curvine-server/tests/journal_test.rs:1239`（既有债）。
候选同位置仍有该 comparisons 写法（未动），**既有红不是本轮引入**，但**门禁因此确实不绿**。

**差集（Counter，保留同文件同 lint 的计数，不是 set 去重）**：

- only-in-candidate：`E0599 curvine-tests/replication_test.rs` ×6、`E0599 curvine-tests/actual_storage_type_test.rs` ×5、`clippy::type_complexity curvine-server/tests/master_fs_test.rs` ×1 —— **上述 2.3 / 2.2 已全部修掉**
- only-in-baseline：79 条（`unused_mut` 17、`manual_inspect` 8、`result_large_err` 8、`needless_borrows` 9、`large_enum_variant` 8 等）
- count-increased：**0**

**修复后重采（`/tmp/cv-lint2/cand2.json`，同命令同独立 target）**：

| 指标 | 结果 |
|---|---|
| only-in-candidate | **0 条 / 0 诊断** |
| count-increased（同文件同 lint 计数上升） | **0** |
| 候选侧总诊断 | 10 条 / 5 键：`large_enum_variant` ×8（client-core 4 / unified-fs 2 / fuse 2）、`journal_test.rs` 的 `unused_variables` ×1 与 `absurd_extreme_comparisons` ×1 —— **全部基线即存在** |
| only-in-baseline | 79 条（本任务清掉的既有债） |

**结论（不粉饰）**：
1. 候选相对基线**未新增**任何 lint（Counter 差集与计数增量均为 0）。
2. 但 `--all-targets --deny` 的整体门禁**仍不通过**（候选侧 exit 101）：
   8 条 `large_enum_variant` 是 deny 级既有债，按裁定**本轮不修**
   （fuse `WriteTask::Write` 在每写请求热路径，无测量不装箱），
   `curvine-server` 的 `journal_test` 既有 error 级 lint 亦未动。
   **不能写"门禁全绿"；正确表述是"未新增 lint，整体门禁仍不通过，既有债另立 followup"**。
3. 我此前只跑 `-p curvine-master` + etcd 就宣称门禁通过 —— 口径错误，已作废。
4. 基线侧 `curvine-server` 与候选侧 `curvine-server`+`curvine-tests` 都有编译失败，
   两侧扫描都**不完整**，所以该对比只在"两侧都扫到的文件"上有效；已按此限定表述。

## 4. 执衡第 3 点：两处红不结案

- `snapshot_restore_failure_test`：default 首跑 9/1 `Timeout`，复跑 10/0。**不结案**：
  没有证据证明与负载的因果关系（没有对照实验），只能记为"未定位的偶发"。
  处置：保留两次原始 log；该项**不计入通过**；A/B 前若再红则必须定位。
- `small_file_job_transfers_without_task_rows`（server）：基线同位置同报错
  （`Stored transfer command ... is invalid`）。**不自动豁免**：基线同红只能证明"非本轮引入"，
  不能证明"符合设计验收"。结论：**该项不满足验收**，需单独立项。

## 5. 执衡第 5 点：5d3→744 的 67 文件核对

`git diff --name-only 5d3bdd177..744f9b5c6` = 67 文件：
prod-master 45、test-master-integration 14、curvine-server 5、curvine-raft 3。
`+703 / -761`。

逐类核对结果：

| 类别 | 结论 |
|---|---|
| clippy 清理（命名/借用/`while let`/type alias/参数打包/`let _ =`） | 语义等价，逐条看过 diff |
| 删除的 `#[test]` | **仅 1 个，即 2.1 的误删，已恢复**；其余 206 行是未被任何测试调用的辅助簇（`completed_file_fixture` / `block_signature` / `assert_common_invariants` 在 5d3 就无调用者） |
| `target_depth == 0` → `false` | 等价（前置 `ids.len() < 2` 早退），残留死变量已删 |
| `assert!(x.len() > 0)` → `assert!(!x.is_empty())` | 等价 |
| `err().expect(...)` → `expect_err(...)` | 等价且 panic 信息更准 |
| server 测试 `read()`/`write()`/`try_read()` → `view()`/`begin_cold()` | 迁移；`resize_lock_scope_test` 的三段断言（持锁阻塞 / 释放后仍阻塞于 worker_manager / 等待时不持 fs 锁）逐条保留 |
| 语义改动（非 lint） | 3 处：TTL delta 修复（`e9598d16e`）、raft recovery warn、`raft_node_test` 有界等待 |

**独立自审 commit：此前没有。** 本轮修正后才产生（`docs` 提交与被审源码分离，符合 C3 契约）。

## 6. 执衡第 4 点：设计要求逐条映射 —— 见配套文件

`2026-11-15-design-to-source-map.md`（与本文件同批提交）：v2 §3/§6/§7、C2 关闭表 G1–G9、
C3 三私有模块、S7 gate 与指标，逐条给出"设计条款 → 最终源 file:line → 验证测试 → 动态证据"，
并显式列出**未落实/未验证**项，不以"followup 已登记"代替落实。

## 7. 本轮未验证/不通过项（清单）

1. `--all-targets --deny` 整体门禁：**不通过**（既有 89 条债，含 error 级；候选 22 条）。
2. `snapshot_restore_failure_test` 偶发 Timeout：未定位。
3. `small_file_job_transfers_without_task_rows`：不满足验收。
4. `dir_tree_memory`：零执行（设计性 ignore，需验收方明确认可）。
5. `fault_isolated_test` default 侧零执行（feature 门控，设计如此）。
6. 修复后的新固定源需重跑两模式完整回归 + fmt（本文件提交后进行）。
7. `curvine-tests` 与 `curvine-server` 的 all-targets clippy 基线即失败，**全局对比结论只在"未新增"意义上成立**。
