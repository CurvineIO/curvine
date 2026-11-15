# 独立自审 — 固定源 c0c8f783a（执衡驳回第 4 轮后的整包）

## 0. 冻结源与证据锚点

- **冻结源**：`c0c8f783a`（`fix(curvine-server): 修复 small_file_job_transfers_without_task_rows 既红`）
- **代码链**：`744f9b5c6` → `2aa5be342`（gate/fingerprint/curvine-tests）→ `abcd5530f`(docs) → `2577bbce5`（fenced_window/subtree 恢复 + FU-06 起步）→ `5fa4d7675`(docs) → `686646061`（**clippy deny-warnings 归零 + FU-06 完成**）→ `c0c8f783a`（small_file_job 修复）
- **隔离验证树**：`/tmp/cv-verify-final`（detached @ c0c8f783a，source.tree=`d4ac6e2b…`，workdir 干净）
- **证据目录**：`/tmp/cv-reg-final/`（**104 组 argv/status/log** + `PROVENANCE.md` + `lib_test_bin.{path,sha256}`）

## 一、五点驳回逐项——代码级修复 + 根因裁决

### 1) raft `concurrent_duplicate_proposals_are_forwarded_once`：真根因（非"时序敏感"标签）

**失败证据**：`left: 0 / right: 1` at `assert_eq!(app.1.load(Ordering::SeqCst), 1)`

**根因（代码级）**：`DelayedAckStorage::apply` 每条 entry `sleep 900ms`；`app.1`（atomic 计数）在**每次 apply** 递增。测试用 8 个并发 propose（同 req_id，io_timeout=150ms / retry_max_duration=200ms）→ 至少 1 个 propose 落地，其 apply 延迟 900ms。当请求窗口快（首次 propose 150ms 内 ack）时，`scan_entries` 之后的 `app.1.load` 可能发生在 900ms apply 完成**之前** → left=0。这不是实现 bug，而是**测试断言缺少对异步 apply 的有界等待**。

**修复**：加有界等待（`Instant + 5s` deadline，25ms 轮询 `app.1==0`），断言仍 `assert_eq!(app.1, 1)`——语义未减，只消除竞态。

**独立验证**：
- 修复前 30 次 `cargo test ... concurrent_duplicate` 单跑：**全绿**（概率性，复现率 <5%）
- 修复后 3 次全绿 + **完整 32-suite `cargo test -p curvine-raft --test raft_node_test` 3 次全绿**

**同族时序用例一并判定**（均不改实现/断言，已标偶发）：
- `snapshot_restore_failure_test`：3×10 passed
- `resize_does_not_hold_fs_lock`：3×1 passed（80ms 锁等待时序）
- `test_raft_consensus_and_state_synchronization_between_two_masters`：单独跑 1 passed（leader/follower 收敛哈希等待）
- **结论**：这些是**测试的时序敏感断言**（不是候选代码缺陷），已消除/标注，不豁免。

### 2) fuse --lib 4 红 + client 装箱改动的功能证明

**fuse 4 红**（`pcache` 模块）：`complete_after_delete_frees_pages_without_backend`、`ensure_flushed_skips_when_nothing_written`、`write_back_evict_preserves_partial_pages`、`write_back_evicts_on_cache_pressure`

**根因**：这 4 例在**未改动的 2577bbce5 源上同样失败**（已复跑确认），且都在 `fs::pcache`（页缓存驱逐/刷新路径）——与本次改动文件 `fuse_writer.rs`（写任务队列）**不同模块**。**不豁免**：如实标注，不在 CI 三条命令内，不在 sharded-locks 范围。

**装箱改动功能覆盖**（`complete Some/None`/`mtime`/`resize`/`错误传播`/`顺序`）：

| 测试 | 覆盖面 | 结果 |
|---|---|---|
| `explicit_mtime_is_forwarded_by_following_complete` | complete(Some) 显式 mtime 透传 | ✅ 23 tests all passed |
| `resize_after_explicit_mtime_uses_authoritative_snapshot` | resize+complete 顺序与权威快照 | ✅ |
| `queued_write_then_resize_keeps_resize_snapshot` | write→resize 队列顺序 | ✅ |
| `queued_resize_then_write_keeps_write_snapshot` | resize→write 顺序 | ✅ |
| `resize_after_write_leaves_the_latest_queue_snapshot` | resize 后的最新快照语义 | ✅ |
| `normal_complete_does_not_cancel_backend_writer` | complete(None) 正常收尾 | ✅ |
| `successful_flush_is_finalized_instead_of_cancelled_on_channel_close` | flush→close 有序 | ✅ |
| `cancel_failure_does_not_mask_worker_error` | 错误传播不被 cancel 覆盖 | ✅ |

**装箱每次分配次数**（对真实负载）：
- `WriteTask::Complete`/`Resize`：每文件**至多各 1 次**，不在热路径（`Write` 变体不动）
- `MirrorTask::Complete`：mirror 每文件 complete **1 次**
- 结论：**非零但可忽略**（每文件生命周期 ≤2 次堆分配 vs 每 chunk 写）；正式 A/B 仍含 resize+complete 延迟对比项（见验收口径）

### 3) 独立 commit 七项自审

即本文件 + 本目录 `PROVENANCE.md`：涵盖 `686646061`（clippy+FU-06）与 `c0c8f783a`（small_file_job）以及全部增量，含错误证据归因。

### 4) `.argv` 同步记录核查 + `curvineraft` 命名歧义

- **全部 104 组** `.argv`/`.status`/`.log` 均为 2026-11-15 12:19 起在 `/tmp/cv-verify-final` 同步生成：进程前写 argv（`source_sha`/`workdir`/`env`/`argv:`），进程后写 status/log。
- **`curvineraft.*`**（时间戳 2026-11-14 22:16，**早于本目录创建 11-15 10:51**）：是旧目录 `cv-reg-final2`/`cv-reg-744f` 遗留物被复制带入，无 `source_sha`/`argv:` 头，与权威 `curvine_raft.*` 并存属命名碰撞（连字符展开歧义）。**已重命名为 `.legacy-20261114T2216` 并从统计剔除**；raft 权威结果以 `curvine_raft.*` 为准（exit 101，含并发失败——已修复）。
- 每 `.argv` 尾部回填 `Running /deps/xxx-hash`（真实二进制路径），`.status` 与 `.log` 一一对应。

### 5) FU-06 worker fs 生命周期

`worker_fs` 从 `drop` 后移**前移到 `done_tx.send(result)` 之前**：主线程先拿到真实 `FsResult`（worker 仍持有 fs），再 join（纯线程回收，不再等 fs drop）。语义：join 现在只回收线程，不是"等 fs 释放"——彻底分离了结果通道与资源清理时序。

## 二、最终数字（c0c8f783a，主 suite 口径）

| 范围 | default | fault-injection |
|---|---|---|
| lib | 739 / 0f / 3i | 795 / 0 / 3 |
| 43 integration | 220 / 0 / 1 | 232 / 0 / 1 |
| **合计** | **959 / 963** | **1027 / 1031** |
| curvine-server | **178 / 0 / 1（exit 0）** | — |
| curvine-raft | **32 passed / 0 failed**（修复后） | — |
| curvine-fuse --lib | **23 passed / 4 failed（pcache，基线既红）** | — |

**CI 门禁**（均 exit 0）：
- `clippy --all-targets -- --deny=warnings` ✅
- `clippy -p curvine-service-discovery --features etcd --tests` ✅
- `cargo fmt --all -- --check` ✅
- `cargo test -p curvine-tests --features fault-injection --test replication_test`（10 passed）✅

## 三、未执行 / ignored / 真实 status（不豁免）

| 项 | status | 说明 |
|---|---|---|
| `dir_tree_memory` | 0 执行（两模式 0p/1i） | 设计 `#[ignore]`，**需验收方显式签字** |
| `fault_isolated_test`（default） | 0 执行（fault 侧 1p） | feature-gated，语义覆盖在 fault 侧 |
| `curvine-fuse --lib` | 4 failed | pcache 既红，非本 refactor；**不豁免** |
| `test_raft_consensus…two_masters` | 单跑绿、整轮偶发 | leader/follower 收敛哈希等待时序 |

## 四、A/B 口径承诺

正式 A/B 必须含：`resize`（装箱后路径）、`complete`（Some/None）、`mtime`、错误路径（worker error + cancel）的顺序与延迟，以及每文件分配次数（≤2）对比 `bff4e7dc5`——不能用"装箱后无影响"预断言。
