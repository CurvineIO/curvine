# 独立自审 — 冻结源 c1a8bb3f3（执衡第 8 轮驳回后）

docs-only，无 `.rs`/`Cargo.toml` 改动，未 amend/reset。

## 1. 取消/panic 不安全（驳回成立，已改为 RAII）

**驳回事实**：上一版恢复写在 `match res` 分支里，而 snapshot 与 taken pages 是
future 的局部。任务取消或 unwind 时 future 被 drop，`match` 不执行 →
live page 只剩 `flushing=true`、`dirty_ranges` 空。`Page::take_dirty_inner` 见
flushing 永久返回 `None`，于是：数据存在但取不出来、容量永久占用、
`flush_all` 可能把空页当成功推进 `all_ver`、`complete` 成功后再
`free_pages(true)` 直接抹掉。**"flushing 保留即保守安全侧"的说法不成立。**

**修法**：`FlushGuard { cache, pages, snapshot, committed }`
- `take_*_guarded()` 返回 guard，拥有整个 flush+publish 窗口；
- `guard.commit()` 是唯一的成功出口（清 flushing + 解除武装）；
- **`Drop`** 覆盖 Err / 取消 / unwind 三条路径，`restore_flushed_dirty(&snapshot)`
  同步加锁、不跨 await（不会死锁）；页不存在时 `warn!` + `restore_lost` 指标。
- 三处调用点（`flush_all` / `flush_dirty` / `publish_read_range`）统一使用 guard。
- `flush_dirty` 使用 `take_dirty_guarded`（full_only）：partial 页必须继续留在缓存，
  这是 `write_back_evict_preserves_partial_pages` 的语义（用 `take_all` 会破坏它，
  本轮实现中曾出现该回归并已修正）。

**删除/truncate 与 ABA**：`truncate`/删除走 `free_pages(true)` 的明确丢弃契约，
不受 guard 影响；guard 的 `restore_flushed_dirty` 用 `writer_ver` 之外的 page_id
定位，版本推进（`all_ver.set`）只在 commit 之后发生，因此 ABA 场景下要么页还在
（restore 命中），要么页已被显式丢弃（`restore_lost` 上报，不静默）。

## 2. 测试：真实取消路径，不是助手式模拟

- `cancelled_flush_leaves_retryable_state`：构造"IO 已开始但未 commit"状态
  （`take_all_guarded` → `take_pages`），期间 `free_pages(false)` 必须为 0、
  usage 不降；作用域结束 guard 被 drop —— 这就是取消/panic 走的**同一条 Drop 路径**；
  随后断言 usage 不变、重试后后端落盘 **4096** 字节。
- `aborted_real_flush_is_either_done_or_retryable`：对**真实** `cache.flush()`
  施加 `tokio::time::timeout(1ns)` 取消；若被取消则断言 usage 未释放；
  无论是否取消，重试后后端必须是 4096 字节。
- `complete_failure_keeps_dirty_pages`：`/dev/full` 下 complete 必失败且 usage 不降。

**未覆盖（如实）**：`take_dirty_pages_inner`/`take_dirty_pages_range` 循环中途
`take_dirty` 失败的回滚路径，无法在不改产品代码的前提下注入该错误，**无直接测试**；
该路径的代码已按 snapshot+restore 处理，属"已实现、未动态验证"。

## 3. complete 失败在生产调用链的可见性

`complete` 失败时：错误原样返回给调用者（`FileHandle`/FUSE 层得到 Err），
`free_pages(true)` 不再执行，usage 保留；`set_error` 只由 evict 路径设置，
complete 失败本身不 fence inode，因此**上层可重试 complete**。
cache manager 对该 inode 的引用不受影响（`get_cache` 未移除）。
本轮的测试断言 usage 不降 + 错误可见；未对 FUSE 层端到端错误码做断言（无挂载环境）。

## 4. 撤回"恢复需重启/重开"的无据承诺

上一版写"恢复需重建 writer（重启/重开/管理员）"，这缺乏证据且方向错误：
缓存是进程内内存 + 可选磁盘 store，**重启本身会导致缓存内容丢失**，不能作为恢复手段。
实际生命周期（据代码）：
- 失败后数据保留在缓存内（dirty，可被下次 `flush`/`complete` 重试）；
- `set_error`（仅 evict 路径设置）后该 inode 的写入被 fence，错误向调用者传播，
  **没有**"错误自动清除后续传"的通路；
- 进程生命周期结束，未持久化的缓存内容随之消失。
以上为事实边界，不做任何"自愈"承诺。

## 5. 统一最终源的回归（不再分源）

最终冻结源 **`c1a8bb3f3`**，worktree **`/tmp/cv-verify-f482`**（detached，dirty=0），
证据 **`/tmp/cv-reg-final4`**（每个 `.argv` 记录 `source_sha`/`source_tree`/`dirty=0`/
`argv`/`recorded_at`，`.status` 与 `.log` 一一对应）：

| 命令 | 结果 |
|---|---|
| `cargo test -p curvine-fuse --lib` | **641 passed / 0 failed**（exit 0） |
| `cargo test -p curvine-master --lib` | 739 / 0 failed |
| `cargo test -p curvine-client-core --lib` | 73 / 0 failed |
| `cargo test -p curvine-unified-fs --lib` | 33 / 0 failed |
| `cargo clippy --all-targets -- --deny=warnings` | **exit 0** |
| `cargo clippy -p curvine-master --no-deps --all-targets`（default / fault-injection） | 0 / 0 |
| `cargo clippy -p curvine-service-discovery --features etcd --tests` | 0 |
| `cargo fmt --all -- --check` | 0 |

**覆盖范围声明**：本轮改动只涉及 `curvine-fuse/src/fs/pcache/{inode_cache,write_back_integration}.rs`。
43 个 master integration target、server、raft 的全量结果来自同一 worktree 在
`f48290373` 的实测（`/tmp/cv-reg-f482`，959/963 与 1027/1031、server 178/0）；
两源之间 master/server/raft 源码零改动，故结论可传递。仍**不称"最终源单次全覆盖"**，
并保留该缺口供验收方判断。

## 6. 性能

`FlushGuard` 只额外持有 `Vec<Page>` 的所有权转移与 ranges 快照（`dirty_snapshot`），
页数据零复制；`flush_dirty` 由 `take_all` 改回 `take_dirty`（full_only），
比上一版少处理 partial 页。客户端层性能仍未测量（A/B harness 在 master 元数据层）。

## 7. 未执行

| 项 | 状态 |
|---|---|
| `dir_tree_memory`（两模式） | 0 执行（design `#[ignore]`） |
| `fault_isolated_test`（default） | 0 执行（feature-gated，fault 侧 1p） |
| `take_*` 中途失败回滚 | 无直接测试（无法注入） |
| 客户端层性能 / 正式 A/B | 未测量 / 未启动 |
