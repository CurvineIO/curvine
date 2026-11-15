# 独立自审 — 冻结源 c1c4f3734（执衡第 5 轮驳回后）

本文件是 **docs-only commit**，追加在代码冻结源之后；不含任何 `.rs`/Cargo 改动，
未使用 amend/reset。

## 0. 提交边界（时间顺序，可复核）

| 提交 | 类型 | 内容 |
|---|---|---|
| `686646061` | code | CI clippy deny-warnings 归零（`wait==0` + 低频变体装箱）+ C2-FU-06 硬化 |
| `c0c8f783a` | code | `small_file_job_transfers_without_task_rows` 根因修复（command_json schema） |
| `c82014dec` | code+docs | raft 竞态（**错误根因版**）+ FU-06 时序 + 自审文档（混提交） |
| `3381d350a` | revert | 撤回上面混入的两份 docs，恢复 code-only |
| `302c63881` | code | raft 竞态（正确根因版：投递竞态） |
| `2c797bb56` | docs | 独立自审（针对 302c） |
| `bf56ceb55` | code | **fuse pcache 4 红真根因修复** + raft 恰好一次 sett le 窗口 |
| `a91998830` | code | snapshot 等待语义分离 + A/B 补 `set_attr_mtime`/`complete_file_mtime` |
| `c1c4f3734` | code | **fuse 全局 metrics 并发 flaky 真根因修复** ← 代码冻结源 |
| 本文件 | docs | 最终自审（覆盖上述全部增量） |

## 一、第 5 轮四条逐项裁决

### 1) raft `concurrent_duplicate`：可证机制 + 不夸大为"历史唯一根因"

**已证明的（代码级事实）**：`DelayedAckStorage::apply` 的
`self.1.fetch_add(1, SeqCst)` 位于 `apply_entry` 与 900ms `tokio::spawn` **之前**，
所以 900ms 不会延迟该计数——第 4 轮把"sleep 导致计数 0"当根因是**错的**，已撤回。

**可证机制（不是历史还原）**：`EntryWithAck` 由 raft 驱动的 apply loop
**异步投递**到 `AppStorage::apply`；`scan_entries` 读的是 RocksDB log 存储，
不经过 FSM。因此存在"日志已持久化、apply 尚未投递"的窗口，此时探针计数为 0。

**明确不宣称**：没有受控事件门闩证明当次历史失败的精确交错，因此不写
"历史唯一根因是 X"。自审口径为：**这是可发生的机制**，测试需要不依赖瞬时读数的判据。

**判据改进（不降断言）**：`assert_eq!(app.1, 1)` 前加有界等待解决"未到达"，
断言后再加 settle 窗口（`200ms + 100ms` ≥ `rpc_retry_max_duration_ms=200` + 余量），
等所有在途重试收敛后仍要求计数为 1——这才能拒绝"迟到的第二次
`EntryWithAck`"，与 retry-cache 去重合同一致。原失败记录保留，不靠无界重跑。

### 2) fuse pcache 4 红：定位到引入提交并修复（不再是"基线红"）

**定位方法**：worktree 二分 `a642df02c`（4 红）→ `eea4c901f`（全绿），
再逐条读 diff。两个真实回归：

- **`evict_inodes` 丢了 flush 步骤**：`a642` 把 `Self::evict`（`flush_dirty` →
  `free_pages`）改成直接 `free_pages(false)`。后果：脏页从未写到 backend
  （日志实证 `backend_evict.bin mismatch: got len=0`），`free_pages(false)` 又对
  脏页不释放 → `cm.used()` 停在 528384。修复：恢复 `flush_dirty(false)` →
  `free_pages(force)`，flush 失败置 `force=true` 并记 `write_back_error_count{evict}`。
- **`flush`/`ensure_flushed`/`complete` 在"无 writer 且无新版本"时误建 writer**：
  `a642` 统一走 `flush_all()`，else 分支无条件 `get_writer()`；未绑定的测试缓存
  （`cache manager state not bound`）与已删除 inode 都会因此报错。修复：
  前两者在该条件下直接返回；`complete` 对 `is_deleted()` 短路（删除后无 backend
  可 complete，页面由随后的 `free_pages(true)` 回收）。

**结果**：`curvine-fuse --lib` **638 passed / 0 failed**。

**另修一个高复现 flaky（同一模块，证据充分）**：
`local_writer_task_body_observes_io_with_local_path_type` 在默认多线程 harness 下
`io_size_bytes observed once` left=4/right=1。对照实验：**未改动的基准源
302c63881 同命令 10 次里 7 次失败**。根因是 `FuseMetrics` 是进程级单例，
而该测试做 before/after 增量断言；`write_back_integration`(29 个测试) 与
`node_state`(18 个) 同样写 `[write, "local"]` label。修复：`lib.rs` 加
`#[cfg(test)] test_support::METRICS_LOCK`，读写两侧共 49 个测试持同一把锁
（同步位获取，不进 async 体）。**断言一条未改、无 `#[ignore]`、无新依赖**。
修复后 6/6 次全绿。

### 3) snapshot / raft-consensus 历史首红：给出证据与定性

- **`snapshot_restore_failure_test`**：历史 panic 值为
  `RecvTimeoutError::Timeout`，位置是 `recv_timeout(5s).unwrap()`。该测试的**功能断言**
  是 `!was_early`（restore 必须等已准入 commit）与 `applied.is_ok()`；两处 5s 只是
  "worker 线程起来了 / guard 释放后完成了"的**活性上限**。并行 suite 下
  current-thread tokio runtime 的创建本身可能超过 5s，于是被误报成功能失败。
  **定性：等待预算被当成判据的测试缺陷，不是产品缺陷。**
  处置：两处 5s → 60s 并带明确文案；`was_early` 的 2s 判断窗口与断言未改。
  证据：15 次序列 + 5 次 CPU 饱和压测均 10/10 绿。
- **`test_raft_consensus_and_state_synchronization_between_two_masters`**：
  历史失败为 180s 收敛 deadline 用尽后 `last_inode_id` 不等
  （`left: 229594383484 / right: 234255967293`）。该值远超 inode 计数（从 ROOT 递增），
  量级与 `id << 24` 吻合，但**没有日均可复核的日志**能证明是哪个 entry 写入了该值。
  **结论：未确定（不写推测为根因）**。保留 180s deadline 轮询与 `print_tree` 诊断；
  独立复跑（含 3 个并行实例）均绿，但不据此宣称已定位。

### 4) A/B checklist 与真实 harness 对齐（已核代码）

核对 `curvine-master/tests/metadata_ab_bench/ops.rs` 后发现：此前只有
`set_attr`(mode) 与 `complete_file`(`None`)，而候选侧装箱的正是 `SetAttrOpts`——
最该比较的两种形状**没测**。已新增两个 op（`main.rs` 遍历 `ops::CASES`，无需改调度）：

| op | 载荷 | 覆盖点 |
|---|---|---|
| `set_attr` | `SetAttrOptsBuilder::new().mode()` | 原有 |
| **`set_attr_mtime`** | `SetAttrOptsBuilder::new().mtime(常量)` | mtime 透传 |
| `complete_file` | `None` | close 无属性 |
| **`complete_file_mtime`** | `Some(opts_with_mtime)` | close 带属性（装箱载荷） |
| `resize` | `FileAllocOpts::with_truncate` | resize（装箱载荷） |

烟测 `BENCH_OPS=8`：两个新 op 在 same/multi × 1/8/32 线程共 12 组合全部 `err=0`。

**分配次数（修正先前"≤1 次/文件"的说法）**：同一文件可多次 `resize`/`complete`，
所以正确表述是**每次携带选项的 API 调用至多一次相关堆分配**，随调用次数线性增长；
写热路径（`Write`/`Data` 变体）不动。A/B 不含错误路径（会污染延迟分布），
错误传播由 `cancel_failure_does_not_mask_worker_error` 等功能测试覆盖。

## 二、最终回归（冻结源 c1c4f3734，main-suite 口径）

证据目录 `/tmp/cv-reg-c1c4`（108 组 argv/status/log，均自 16:51 起在
`/tmp/cv-verify-final2` 同步生成；`source.sha`/`source.tree`/`dirty=0` 绑定、
`.argv` 尾部回填 `bin:` 真实测试二进制；`lib_test_bin.{path,sha256}` 绑定 lib 二进制）。

| 范围 | default | fault-injection |
|---|---|---|
| lib | 739 / 0f / 3i | 795 / 0 / 3 |
| 43 integration | 220 / 0 / 1 | 232 / 0 / 1 |
| **合计** | **959 / 963** | **1027 / 1031** |
| curvine-server | 178 / 0 / 1（exit 0） | — |
| curvine-raft | 136 / 0 / 1 | — |
| **curvine-fuse --lib** | **638 / 0 / 0** | — |
| client-core / unified-fs | 73 / 0 · 33 / 0 | — |
| rocksdb / fs-api / model / fault / fault-fi | 22 / 26 / 134 / 5 / 19 | — |
| curvine-tests（replication-fault / storage） | 10 / 3 | — |

**门禁（全部 exit 0）**：`clippy --all-targets -- --deny=warnings`、
`clippy -p curvine-service-discovery --features etcd --tests`、
`clippy -p curvine-master`（default 与 fault-injection）、`cargo fmt --check`、
`git diff --check`、`cargo test -p curvine-config`、
`cargo test -p curvine-service-discovery --features etcd`、
`cargo test -p curvine-tests --features fault-injection --test replication_test`。

## 三、未执行 / ignored（不豁免、不代签）

| 项 | 状态 | 说明 |
|---|---|---|
| `dir_tree_memory`（default + fault） | **0 执行**（0p/1i） | 设计 `#[ignore]`；本次未运行，需验收方评估覆盖，**不由我代签** |
| `fault_isolated_test`（default） | **0 执行** | feature-gated；语义覆盖在 fault 侧（1p） |
| A/B（WAL-on 候选 vs `bff4e7dc5`） | **未启动** | 未获授权；checklist 已按真实 harness 补齐 |

## 四、本轮对先前说法的修正清单

1. "900ms sleep 导致计数 0" → **撤回**，改为"apply loop 异步投递窗口"，且不宣称历史唯一根因。
2. "pcache 4 红是基线红、非本轮范围" → **撤回**：已定位到 `a642df02c` 两个真实回归并修复。
3. "`≤1 次/文件`" → 改为"每次携带选项的调用至多一次，随调用次数线性增长"。
4. "A/B 覆盖 complete Some/None + mtime" → 此前 harness **确实没覆盖**，现已新增两个 op 并用真实运行验证。
