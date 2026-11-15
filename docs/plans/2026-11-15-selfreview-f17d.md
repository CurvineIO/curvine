# 自审（阶段性，未完成整包）— 源 f17d6e08c

docs-only。本文件**不代表整包完成**，只记录架构师裁决后已落地的一项与剩余缺口。

## 已落地：release 失败保留引用（裁决 1 的 A 方向）

**核实**：`async_map.rs::release_with_cleanup` 在 `last && cleanup 失败` 时仍
`refs-=1` → `resource.take()` → `remove_entry`；`execute` 只持局部 `Arc`，返回后最后
Arc 释放 → `InodeCache::drop` 丢脏页与 usage。

**修复**（不改 map 既有契约）：新增只读 `refs()`；新增 `InodeCache::has_dirty()`；
`release_cache` 改为"先判 last → 先执行 cleanup → 成功才 `release_with_cleanup`"，
**失败且仍有脏页则引用不减**（entry 保留、`get_or_create` 复用同一实例）。

**有界性（代码依据，非假设）**：`PageStore::add_usage` 在超出 capacity 时返回
**ENOSPC** 硬拒绝 ⇒ 保留集合的 usage 全部落在同一 page-store 预算内，
总量 ≤ capacity；预算耗尽时新写得到既有 ENOSPC 背压，不淘汰未持久化数据。
因此不需要独立 pending 表，也不存在无界增长。

**证据**：`curvine-fuse --lib` 641 passed / 0 failed；`curvine-runtime --lib` 40 / 0；
`clippy --all-targets --deny=warnings` exit 0；`fmt --check` 0
（`/tmp/cv-reg-f17`，argv 含 source_sha/source_tree/dirty=0/argv/recorded_at）。

## 未完成（按架构师三、四项，属明确缺口）

1. **真实 release 链故障注入测试**：`FileHandle`→`DirtyMessage`→`WriteBackWork::execute`
   →`release_cache`，last 引用 + complete 失败 + reopen/并发 release/删除。
   障碍：`release_cache` 需已绑定的 `NodeState`，`NodeState::new` 依赖
   `UnifiedFileSystem`，测试侧构造成本高。**当前无直接用例**。
2. **`truncate` 契约与 FlushGuard ABA**：需先核 `truncate` 的调用契约、cache 与 backend
   同步顺序、已入队写；decision 2 要求的方案未实现（目前仅 `restore_lost` 上报）。
3. **确定性取消/注入测试（D1）**：写入中 / publish 等待中的事件门闩、并发同页写、
   普通 evict、内容/dirty/usage 校验；多页第 N 页 `take_*` 真实注入。
   需要最小 `cfg(test)` 注入能力，尚未建设。
4. **同源整体回归（裁决 4）**：43 master integration / server / raft 仍沿用
   `f48290373` 的实测结果，最终源未重跑全部。

## 保留

`dir_tree_memory`（design `#[ignore]`，两模式 0 执行）与 `fault_isolated_test`(default)
未执行；A/B 未启动；历史红与 STOP 保留；客户端层性能未测量。
