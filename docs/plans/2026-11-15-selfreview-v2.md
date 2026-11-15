# 自审 v2：执衡第二次退回后的逐条处置（源 2577bbce5）

上一份自审（`2026-11-15-final-candidate-selfreview.md`，随 `abcd5530f`）仍然有实质错误。
本文件逐条纠正，凡冲突以本文件为准。

## 1. 最严重的问题：把不存在的测试写成动态证据

我在 `2026-11-15-design-to-source-map.md` 的 A14/A15 写了
「`fenced_window_matrix` / 三 op 全 NotLeader / 落实」。
执衡在固定 worktree 与全部回归日志里搜索均无匹配 —— 事实是：

- `fenced_window_matrix` 是 `e1aa12595`（G9 冻结裁定）**移出源码**的用例，
  落地为 `C2-FU-05`/`C2-FU-01` followup；
- 我引用的是 `13793b206` **提交信息**里对当时一次性诊断的描述，把它当成了现存测试。

这属于"把过去描述或想运行的测试写成实际证据"，不可接受。已按用户"设计 100% 落实"要求
**把测试真的恢复并跑绿**（见 §2），而不是改措辞。

另一个同类但更隐蔽的问题：`subtree_location_cover_test.rs` 在左辅分支 `c7b2e3cdc` 里
**从未在 `meta/tests/mod.rs` 注册** —— 即该文件从未被编译、从未执行过。
我上一版把它当作"已移出、可随时恢复"的证据也是错的。本轮把三例并入
`subtree_location_plan_test.rs`（复用同文件 fixture）后才首次真正运行。

## 2. C2-FU-01..07 逐项裁定（不再笼统）

| FU | 请求 | 处置 | 证据 |
|---|---|---|---|
| 01 | G3 冻结窗口矩阵（真实公共递归 RPC） | **恢复**：`tests/recursive_fenced_window_test.rs`，用例 `fenced_window_rejects_real_recursive_rpcs_by_original_epoch`；真实三 op（`delete_with_mode(SkipTrash)` / `free_or_defer` / `SetAttrOpts`），pre-hot hook 内 fence→unfence，断言 original-epoch 拒绝、文件与 blocks 不变、`failure_seq` 不动、hook 只消耗一次 | default 1 passed、fault 1 passed（`/tmp/cv-reg-2577/R1.log`、`R1-f.log`）；左辅独立复验同绿 |
| 02 | 1024→1025 独立 budget 例 | **确认为已实现**：`covers_rejects_a_known_worker_growing_rows_on_an_existing_block`（512 block + 512 location = 1024 刚好，行数 1025 必须被拒且 `capture` 亦返回 None） | 该例一直在 `subtree_location_plan_test.rs`；上一版把它笼统记为 followup 属登记口径错误 |
| 03 | `recursive_confirmation_test.rs` 行数收束 | **关闭**：G3 矩阵拆独立文件后 **408 行** | `wc -l` |
| 04 | 位置预算常数级额外开销 | **现行设计明确不要求修改**：登记原文"本轮不要求修改……移除条件=成为热点或需严格 I/O 计数"。它不是语义错误、不是额外解码、不是物理读盘 | `2026-11-14-c2-closure-table.md` G4 行 |
| 05 | 冻结移出的 4 例 | **全部恢复**：矩阵 1 例（见 01）+ cover 三例（并入 plan_test） | 4 例在 `R1.log` 中逐条 ok；左辅独立 grep 确认 |
| 06 | `recursive_rpc_progress_test` 可维护性 | **关闭**：删掉 `(0..200)` 轮询 → 先等 `arm_wait_reached_hook` 真实到达信号再断言（更强：信号到达后状态必须已发布）；结尾回收本 case 的 meta/journal 目录 | 1 passed |
| 07 | `walk_is_current`/`versions` 死路径 | **已闭合**（`e25eb78e8`） | 源码无该符号 |

## 3. 数字口径错误（上一版多算 1 例）

`journal_uncommitted_restart_test` 等 target 内部会再起**子进程 harness**
（`running 1 test` / `running 0 tests`），其 `test result` 行在日志里出现在主 suite 之前。
上一版按"累加所有 test result 行"统计，把子进程的 1 例加进总数：

- 我上一版 abcd：integration 233 / 合计 1024
- 实际（main-suite 口径）：integration **232** / 合计 **1023** —— 左辅的 1023 是对的

正确口径（本轮起固定使用）：lib 取 `running N tests` 对应的主 `test result`；
integration target 取主 suite（`filtered out == 0` 或 passed 最大者），子进程不计。

## 4. lint 口径的自相矛盾（已修正）

上一版同时写了"只在两侧都扫到的文件上有效"和"候选未新增任何 lint"，逻辑上不能并存。
本轮实测两侧编译失败包集合**相同**（都只有 `curvine-server`），因此：

- 可比范围 = 两侧都成功编译的包 → within: only-in-candidate **0**、count-increased **0**
- 不可比范围 = `curvine-server` 因 `journal_test` error 级 lint 中断的其它 test target
  → 明确标注为**未验证**（既非"未新增"也非"有新增"）
- 整体门禁 **不通过（101）**，既有 8 条 `large_enum_variant` + server 既有 error 级 lint；不豁免

## 5. 上一版仍未处理、本轮明确不豁免的三项

1. 全 workspace `--all-targets --deny` 门禁：**不通过**
2. `curvine-server` 的 `small_file_job_transfers_without_task_rows`：**不满足验收**
   （基线同红只证明非本轮引入）
3. `snapshot_restore_failure_test` 的偶发 `Timeout`：**未定位**，本项目本轮 10/0 与 16/0 均绿，
   但仍按"未定位观察项"记录，不计为已解决

## 6. 本轮教训（写给后续维护者）

- **删 dead_code 前必须追溯"是谁让它变 dead"**：`completed_file_fixture` /
  `block_signature` / `assert_common_invariants` 当时无调用者，是因为唯一调用它的
  `fenced_window_matrix` 被 G9 移出；我把它们当纯 dead code 删掉，等于继续拆除一个
  已冻结的测试族。恢复时才发现需要一并回补。
- **测试文件的"存在"不等于"被执行"**：`subtree_location_cover_test.rs` 物理存在但未注册，
  编译都不会发生。任何"某测试覆盖某条款"的声明，必须同时给出可复核的 `test result` 日志行。
- **统计口径必须写进报告并在两侧一致**：子进程 harness 是本次 1 例差异的唯一来源。
