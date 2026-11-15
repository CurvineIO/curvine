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
| A14 | 复用普通准入/original epoch；hot 显式接收同一 u64；三入口不得再 `metadata_epoch()` 遮蔽 | `master_filesystem.rs:2031/2656/5705`（delete/free/set_attr 传 outer epoch） | `fenced_window_matrix`（`set_attr_lock_test` 等） | 落实（`13793b206` 修过一次重采遗漏） |
| A15 | helper 在最终 I/L/view 与计划重验后、body 前 `ensure_metadata_epoch(epoch)` | `journal_writer.rs:615` | `fenced_window_matrix` 三 op 全 NotLeader | 落实 |
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

## F. 未落实 / 未验证（不得当作已满足）

1. **性能验收（v2 "目标与验收"）**：WAL-on A/B **未启动**，无任何 QPS/p50/p99 数据。C2 验收只覆盖"切片收口"，不覆盖性能门槛。
2. **`--all-targets --deny` 整体门禁**：不通过（既有 8 条 `large_enum_variant` + server journal_test 既有 error）。候选未新增 lint，但门禁本身红。
3. **`dir_tree_memory`**：零执行（设计性 ignore），若验收要求每 target ≥1 例需明确认可。
4. **`snapshot_restore_failure_test`**：偶发 Timeout 未定位，不计通过。
5. **`small_file_job_transfers_without_task_rows`**：不满足验收（基线同红只说明非新增）。
6. **C2-FU-04/05/06/07**：按登记处理，其中 04（常数级 KV 物化）仍未实现。
7. **`fuse WriteTask::Write` 装箱**：无测量不做（设计裁定），大 enum 债仍在。
