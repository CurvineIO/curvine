# 设计 → 最终源 → 验证测试 映射表（执衡第 4 点）

源：`744f9b5c6` + 本轮自审修复（新固定源见文末）。行号为 `grep -n` 实测值。
"验证测试"列给出 target 与用例数（两模式均通过，除注明者）。
动态证据目录：`/tmp/cv-reg-final5/`（744f9b5c6）与本轮新源重跑目录。

## A. v2 §3.2.1 C2 递归锁集合（17 条要点）

| # | 设计条款（原文要点） | 最终源 | 验证测试 | 状态 |
|---|---|---|---|---|
| A1 | 范围含递归 Delete/SetAttr/Free 及三个 replay 入口；永久删除不改 pending purge | `write.rs:143`（`run_recursive_metadata_write`） | `recursive_confirmation_test`、`recursive_confirmation_lifecycle_test` | 落实 |
| A2 | 规划为无副作用内存 DFS：祖先 Read、目标父 Write、子树目录+文件 ID Write；不点读 nlink | `subtree_write_plan.rs:27`（`SUBTREE_PLAN_LIMIT`）、`read_plan.rs`（纯规划） | `recursive_lock_set_test`、`recursive_lock_set_topology_test` | 落实 |
| A3 | 一次按条带全序获取完整锁集合，禁止逐节点追加 | `inode_lock_manager.rs:136`（`lock_many`）、`write_locks.rs` | `recursive_lock_set_test`、`inode_lock_manager_test` | 落实 |
| A4 | `SUBTREE_PLAN_LIMIT=1024` 按路径链条目/实际子边计预算；硬链接名分别计数 | `subtree_write_plan.rs:27`、`directory_children.rs:613`（`child_edges_bounded`） | `directory_children_test`、`directory_sharding_test` | 落实 |
| A5 | 独立 `SUBTREE_LOCATION_LIMIT=1024` 全调用共享预算，只允许一项判超限 | `write.rs:225` | `recursive_location_id_only_test`、`recursive_location_lock_test` | 落实 |
| A6 | RPC/replay 共享 owned `SubtreeLocationPlan`；bounded prefix 读取；不先全量 clone | `subtree_write_plan.rs:264`（`SubtreeLocationPlan`） | `recursive_location_interleaving_test` | 落实 |
| A7 | 采样保留到同一最终 view 下 covers；读当前 Write inode 的 block 全集，一次全调用 counter | `subtree_write_plan.rs:334`（`covers`） | `recursive_location_lock_test`、`recursive_confirmation_test` | 落实 |
| A8 | 位置 capture(None) 是正常冷回退不 panic；need_locations=false 不读位置 | `write.rs`（`PlanOutcome::Fallback` 路径） | `fallback_contract_test` | 落实 |
| A9 | child_edges_bounded / 位置读取返回完整 Some 或超限 None，不改旧 API 语义 | `directory_children.rs:613` | `directory_children_test`（11） | 落实 |
| A10 | 每目录先取 shared_version 再枚举；锁后核 epoch/路径身份/同一 live 句柄/所有 token | `write.rs`（`run_recursive_metadata_write` 内 revalidate） | `recursive_confirmation_test`、`recursive_lock_set_window_test` | 落实 |
| A11 | free/最后链接删除保持 inode→worker location→block location 顺序 | `write.rs`（free 分支） | `block_location_cleanup_test`、`block_location_lock_manager_test` | 落实 |
| A12 | 等对象锁时不持 view；body/发布后释放 view/location/inode 再等确认 | `write.rs`（`drop(_locks)` 后 wait）、`fs_dir_handle.rs:439`（`MetadataConfirmation`） | `recursive_confirmation_lifecycle_test` | 落实 |
| A13 | 仅副作用前预算/覆盖不匹配走 typed Fallback，最多 3 次；body 最多一次；真实错误不重跑 | `write.rs`（`MAX_PLAN_RETRIES` 循环，仅 body 前） | `fallback_contract_test`、`recursive_confirmation_test` | 落实 |
| A14 | 复用普通准入/original epoch；hot 显式接收同一 u64；三入口不得再 `metadata_epoch()` 遮蔽 | `master_filesystem.rs` delete/free/set_attr 三入口传 outer epoch | `recursive_fenced_window_test::fenced_window_rejects_real_recursive_rpcs_by_original_epoch`（真实三 op：DeleteMode::SkipTrash / free_or_defer / SetAttrOpts） | 落实（用例于 `2577bbce5` 从 C2-FU-01/05 恢复，此前**不在源码内**，本表上一版引用不存在的名字，已纠正） |
| A15 | helper 在最终 I/L/view 与计划重验后、body 前 `ensure_metadata_epoch(epoch)` | `journal_writer.rs` `ensure_metadata_epoch` | 同上用例：pre-hot hook 内 fence→unfence 后，三 op 都必须以 original-epoch 的 role-changed 错误被拒（`assert_common_invariants` 证文件存活、`failure_seq` 不变、hook 只消耗一次） | 落实（`2577bbce5` 恢复后实测两模式各 1 passed） |
| A16 | 规划窗口 fence 后不得换新 epoch 继续旧请求；不二次 enter、不重采 | `journal_writer.rs:615`（`ensure_metadata_epoch` 返回 typed Err） | `set_attr_lock_test`、`exists_optimistic_test`（fenced 用例） | 落实 |
| A17 | 确认守卫在 body 作用域内、晚于 I/L/view 声明；opErr 优先；Ok 才 disarm | `fs_dir_handle.rs:439` | `recursive_confirmation_lifecycle_test`、`recursive_confirmation_test` | 落实 |
| A18 | FAILED = 新 view/cold 拒绝 + writer fence/cursor 退休，非 drain/回滚 | `fs_dir_handle.rs`（`fail_closed`）、`namespace_commit_gate.rs:342` | `fs_dir_handle_test`、`lost_worker_gate_test` | 落实 |
| A19 | RPC 子树并发仅在实际 writer journal 关闭或 channel 无界时启用；否则 typed 回退 C1 | `write.rs`（`queue_never_blocks` 判定） | `fallback_contract_test` | 落实 |
| A20 | replay 复用计划/锁池/位置保护，不套 role/admission/确认，不 echo journal | `journal_loader.rs:1831`（`with_replay_locks`）+ 三调用点 2398/2423/2464 | `recursive_replay_lock_test`（11 例） | 落实 |
| A21 | replay 超预算沿既有冷桥；缺失 inode/entry 按原合同；body 已变更后不得 fallback 重跑 | `journal_loader.rs`（`ReplayStep`） | `recursive_replay_lock_test`、`replay_inode_contract_test` | 落实 |

## B. C2 关闭表 G1–G9

| 项 | 最终源证据 | 状态 |
|---|---|---|
| G1 非递归 free 不去重跳过 | `seen_inodes: Option<HashSet>` 仅递归分配；`Some` 且 contains 才跳过 | 关闭（`e1aa12595` 验收） |
| G2 预算 None → 释放 view+I → cold | `write.rs`（`PlanOutcome::Budget` 分支） | 关闭 |
| G3 hot 闭包用外层 epoch | 三入口传参 `master_filesystem.rs:2031/2656/5705` | 关闭（`13793b206`） |
| G4 常数级 KV 物化 | 已登记 followup（C2-FU-04），非本轮范围 | 登记 |
| G5–G8 | 见 `2026-11-14-c2-closure-table.md` 对应条目 | 关闭（`e1aa12595`） |
| G9 | 同上 | 关闭 |

## C. C3 三私有模块契约

| 条款 | 最终源 | 状态 |
|---|---|---|
| `write.rs` / `read_plan.rs` / `write_locks.rs` 三私有模块 | `curvine-master/src/master/fs/master_filesystem/{write,read_plan,write_locks}.rs`（496/137/48 行） | 落实 |
| `MetadataConfirmation` 移到 `fs_dir_handle.rs` | `fs_dir_handle.rs:439` | 落实 |
| op↔调用点一一对应（21 生产点 / 0 mismatch / 0 orphan） | `2026-11-14-s7-op-sites-table.md` | 落实 |

## D. S7 commit gate 与指标

| 条款 | 最终源 | 验证 | 状态 |
|---|---|---|---|
| `close_and_enter_if_open` 单 CAS 语义 | `namespace_commit_gate.rs:342` | `namespace_commit_gate_test`（18，含本轮恢复的共享 gate 用例） | 落实 |
| `wait_for_quiescence` 冷路径 | `namespace_commit_gate.rs:404` | 同上 + `mount_commit_gate_test`(2)、`ttl_atime_gate_test`(2)、`ttl_bucket_concurrency_test`(1) | 落实 |
| `commit_gate_drain_wait_us` Histogram（14 桶、无标签） | `namespace_commit_gate.rs`（wait_for_quiescence 内计时） | 指标接线 + 面板 `8e3eb15f9`/`17a3c7470` | 落实 |
| `commit_gate_exclusive_enter_total` Counter（无标签） | `close_and_enter_if_open` 成功处 | 同上 | 落实 |
| `metadata_lock_fallback_total{op}` | 21 生产调用点 | `2026-11-14-s7-op-sites-table.md` | 落实 |
| 不做 try_enter 两计数 / generation gauge | §7.1 定稿 | — | 明确不做（设计决定，非遗漏） |

## E. S4 / file_status / get_lock / rename 契约

| 条款 | 最终源 | 验证 | 状态 |
|---|---|---|---|
| dir-attr delta 不得刷新已过期父目录 mtime | `fs_dir.rs` `delete_child`（`delta_mtime` 受 `update_parent_mtime` 保护） | `delete_ttl_atomicity_test`、`directory_setattr_metadata`、`master_fs_test`(88) | **本轮修复**（`e9598d16e`） |
| file_status 在 cold 窗口有界重试 | `master_filesystem.rs`（`retry_on_holder_busy` / `FileStatusReadError`） | `file_status_cold_window_test`（default 9 / fault 10） | 落实 |
| get_lock 对 trailing FileId 行缺失报 `Failed to load inode` | `read_plan.rs`（`trailing_file_edge`）+ get_lock 锁后窗口 | `get_lock_path_test`（2） | 落实 |
| rename noop 同路径先 resolve 再返回 | `master_filesystem.rs`（cold body 内 `resolve_path` 先于 `Ok((false,None))`） | `exists_optimistic_test`（26） | 落实 |

## F. 未落实 / 未验证（不得当作已满足）— 截至 `a1272ac6b` 复核

| # | 项 | 状态 | 证据 / 说明 |
|---|---|---|---|
| 1 | **性能验收（v2 目标）** | **未启动** | WAL-on A/B 无任何 QPS/p50/p99 数据；C2 只覆盖"切片收口" |
| 2 | `--all-targets --deny` 整体门禁 | **已通过** | `a1272ac6b`：`clippy --all-targets --jobs 4 -- --deny=warnings` **exit 0**、0 error 行（`/tmp/cv-ci-a127-evidence/ci_clippy.*`）；etcd 变体同 **exit 0**。此前 §F 记的 8 条 `large_enum_variant` 与 server `journal_test` error 已在本轮修掉（`686646061`） |
| 3 | `dir_tree_memory` | **零执行** | default/fault 均 0 passed / 1 ignored（design `#[ignore]`），需验收方评估；本轮未额外运行 |
| 4 | `snapshot_restore_failure_test` 偶发 Timeout | **已定位并修** | 根因是"活性等待预算当功能判据"（`recv_timeout(5s).unwrap()`），改 60s + 明确文案，`was_early` 2s 判据与断言未改；15 次序列 + 5 次 CPU 饱和压测全 10/10（`a91998830`） |
| 5 | `small_file_job_transfers_without_task_rows` | **已修** | 根因 `command_json="{}"` 对不上 `TransferCommand` 必填 schema；改真实 command serde 后 1 passed（`c0c8f783a`） |
| 6 | C2-FU-04（常数级 KV 物化） | **现行设计明确不要求** | 原文"本轮不要求修改……移除条件=成为热点或需严格 I/O 计数" |
| 7 | `fuse WriteTask::Write` 装箱 | **未做（有意）** | 写热路径无测量不装箱；`Complete`/`Resize` 的 6 处装箱是 clippy 门禁必需（保留），**客户端层性能未测量** |
| 8 | FUSE write-back 数据安全（范围外） | **独立风险，另立任务** | 撤出后 `curvine-fuse --lib` = 634/4 红（基线既有 pcache 用例）；修复起点留档 `wip/fuse-pcache-safety`=`0ceb720ff` |
| 9 | A/B harness `close` 项 | **设计上不可比** | baseline `bff4e7dc5` 的 `MasterFilesystem` **无 `close_file`**（`grep -c` = 0），候选有 3 处；baseline 的等价 close 路径即 `complete_file(only_flush=false)`，矩阵已覆盖该 case。**不剔除、不造对照**，启动申请中明确列出 |
| 10 | 热父可比性阈值 | **待冻结** | `background.rs:43-44` 为硬编码常量（20k ops/s、depth 4），无"什么算通过"的预设标准；由右弼产出跑前固定阈值，我审核后冻结 |

## G. C2-FU-01..07 逐项裁定（用户"设计 100% 落实"口径）

| FU | 登记内容 | 最终源状态 | 裁定 |
|---|---|---|---|
| C2-FU-01 | G3 冻结窗口矩阵（真实公共递归 RPC，07:00 后新增） | `tests/recursive_fenced_window_test.rs`（`2577bbce5` 恢复，两模式 1 passed） | **已实现**（此前只在 followup 里，源码内没有） |
| C2-FU-02 | 1024→1025 独立 budget 例（block/worker 集合不变，仅行数增长） | `meta/tests/subtree_location_plan_test.rs::covers_rejects_a_known_worker_growing_rows_on_an_existing_block`（`512×2=1024` 预算，`1025` 必须拒绝且 `capture` 也返回 None） | **已实现**（该例一直在源码中；此前被笼统记为 followup，属登记口径错误） |
| C2-FU-03 | `recursive_confirmation_test.rs` 行数收束（曾 529 > 500） | 拆出 G3 矩阵后 **408 行** | **已关闭** |
| C2-FU-04 | 位置预算扫描的常数级额外开销（超限时多物化一条未解码 KV） | 代码未改 | **现行设计明确不要求**：原文"本轮不要求修改……移除条件=成为热点或需严格 I/O 计数"。它不是语义错误、不是额外解码、不是物理读盘 |
| C2-FU-05 | G9 冻结移出的 4 例（G3 矩阵 + cover 三例） | 4 例全部恢复：矩阵 1 例见 FU-01；cover 三例并入 `subtree_location_plan_test.rs`（`covers_rejects_a_block_added_after_the_sample` / `..._expanding_onto_a_new_block` / `..._growing_rows_on_an_existing_block`） | **已实现**（注意：`subtree_location_cover_test.rs` 在 c7b2e3cdc 中**从未在 mod.rs 注册**，即从未真正执行过，本轮并入后才首次运行） |
| C2-FU-06 | `recursive_rpc_progress_test` 可维护性（200 次轮询、join、临时目录） | 改为先等 `arm_wait_reached_hook` 真实到达信号再断言状态 + 结尾回收本 case 的 meta/journal 目录 | **已关闭**（断言更强：信号到达后状态必须已发布） |
| C2-FU-07 | `walk_is_current` / `versions` 死路径 | 已删除（`e25eb78e8`） | **已闭合** |

## H. 证据索引（每条都能自己复跑）

```bash
# lib 单测（含全部递归/gate/replay 单元用例）
CARGO_TARGET_DIR=<target> cargo test -p curvine-master --lib --features fault-injection -- <filter> --test-threads=1

# integration target
CARGO_TARGET_DIR=<target> cargo test -p curvine-master --test <target>

# 本报告的原始记录
/tmp/cv-reg-2577/R1.log        # lib default（main suite: running 742 tests）
/tmp/cv-reg-2577/R1-f.log      # lib fault  （main suite: running 798 tests）
/tmp/cv-reg-2577/<target>.log  # 43 个 integration target，各配 .status
/tmp/cv-reg-2577/<target>-f.log
```

**数字口径**：lib 取 `running N tests` 对应的主 `test result` 行；`journal_uncommitted_restart_test`
等 target 内部会再起子进程 harness（`running 1 test` / `running 0 tests`），**不计入**总数。
按此口径：lib default **739**、fault **795**；43 target default **220**、fault **232**；
合计 default **959**、fault **1027**，0 failed。

## I. 全 workspace lint 口径（修正上一版自相矛盾的表述）

命令（两侧同一条，独立 target，`--keep-going` 保留可编译部分）：

```bash
cargo clippy --workspace --all-targets --keep-going --jobs 4 --message-format=json
```

| 侧 | 源 | exit | 编译失败包 | 诊断 |
|---|---|---|---|---|
| 基线 | `3c6949f4f`（worktree `/tmp/cv-base2-3c6949f4f`, tree `81342318`, dirty 0） | **101** | `curvine-server` | 89 条 / 36 键 |
| 候选 | `2577bbce5` | **101** | `curvine-server` | 10 条 / 5 键 |

两侧**编译失败包集合相同**，因此"两侧都成功编译的源文件集合"是对称可比的。在该范围内：

- only-in-candidate（含同文件同 lint 的计数差）= **0 键 / 0 诊断**
- count-increased = **0**
- only-in-baseline = 79 条（本任务清掉的既有债）

**不能外推的部分（如实标注为"未验证"）**：
`curvine-server` 的其它 test target 两侧都因 `journal_test` 的 error 级 lint 而中断，
因此**这些文件在两侧都没被扫描**——对它们既不能声称"未新增"，也不能声称"有新增"。
另外候选侧残留的 10 条中，2 条恰好来自 `curvine-server/tests/journal_test.rs`
（`unused_variables:1225`、`absurd_extreme_comparisons:1248`，基线同样存在），
说明该文件被部分扫描；但同包其它 target 未扫描的部分仍属未验证。

**结论（唯一正确表述）**：
1. 在两侧编都能编译的包范围内，本轮**未新增 lint**；
2. **整体门禁不通过**（exit 101），原因是既有 8 条 `large_enum_variant`（client-core 4 /
   unified-fs 2 / fuse 2，deny 级）与 `curvine-server` 的既有 error 级 lint；
3. 既有债不因"基线同红"而豁免，另立 followup；
4. 绝不写"门禁全绿"。
