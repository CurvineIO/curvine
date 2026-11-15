# 范围纠偏后的最终交付 — 源 a1272ac6b（tree 见下）

docs-only 提交，追加于代码冻结源之后。

## 1. 范围裁定与撤出（执衡 2026-11-16 裁定）

**裁定依据**：`git diff 3c6949f4f 5d3bdd177 -- curvine-fuse/src/fs/pcache
crates/core/runtime/src/sync/async_map.rs` **为空** —— v2 设计未要求 FUSE pcache 的
orphan 缓存或周期重试；写缓存问题在已验收的 Master 锁改造之前就存在。
把 pcache 作为本任务阻断项属越界，已撤回。

**撤出方式**（正向提交 `bcf993319`，无 reset/rebase/restore）：
- `curvine-fuse/src/fs/pcache/**` 与 `crates/core/runtime/src/sync/async_map.rs`
  恢复到基线 `5d3bdd177`（`git diff 5d3bdd177 -- <这两处>` = **0 行**）
- 删除新增的 `curvine-fuse/src/fs/pcache/tests/`（基线无此目录）

**保留（与分片锁相关或门禁必需）**：
- master/raft/server/journal 的修复、验收测试、自审文档
- `curvine-fuse/src/lib.rs::test_support::METRICS_LOCK`
- `curvine-fuse/src/fs/fuse_writer.rs`（`WriteTask` 装箱 = clippy deny 门禁必需）+ 其 metrics 锁
- `curvine-fuse/src/fs/state/node_state.rs` 的 metrics 锁
- `curvine-master/tests/metadata_ab_bench/ops.rs` 的 21 个 case（含两个 mtime 变体）

**现场保全**：全部 pcache 相关提交留在分支 **`wip/fuse-pcache-safety` = `0ceb720ff`**
（`bf56ceb55`、`c1c4f3734`、`f48290373`、`12d16e7af`、`74f880d14`、`c1a8bb3f3`、
`f17d6e08c`、`fee0fd7e7`、`765a7054f`、`9e57b3272`、`0ceb720ff`）；
左辅相关半成品在其分支 `feat/s7-release-orphan-0ceb`。

## 2. 最终回归（default lane，源 `bcf993319`）

证据 `/tmp/cv-reg-bcf`（55 组 argv/status/log，argv 含
`source_sha`/`source_tree`/`workdir`/`dirty=0`/`CARGO_TARGET_DIR`/`recorded_at` + `bin:`）：

| 命令 | 结果 |
|---|---|
| `cargo test -p curvine-master --lib` | 739 passed / 0 failed / 3 ignored |
| 43 个 master integration（default） | 220 passed / 0 failed / 1 ignored |
| `cargo test -p curvine-server` | 178 passed / 0 failed / 1 ignored |
| `cargo test -p curvine-raft` | 136 passed / 0 failed / 1 ignored |
| rocksdb / fs-api / model / fault | 22 / 26 / 134 / 5（全 0 failed） |
| curvine-tests（replication-fault / storage） | 10 / 3 |
| curvine-config / service-discovery(etcd) | 238 / 38（9 ignored） |
| `cargo fmt --all -- --check` | 0 |
| `cargo clippy --all-targets -- --deny=warnings` | **exit 0** |

零执行（如实）：`dir_tree_memory`（两模式 0 执行，design `#[ignore]`）、
`fault_isolated_test`（default 0 执行，feature-gated）。

**fault lane（`--features fault-injection`，同源 `bcf993319`，独立两条 lane 交叉一致）**：

| 命令 | 我的 lane（`/tmp/cv-reg-bcf`） | 左辅的 lane（`/tmp/cv-reg-fault-rightyi/`） |
|---|---|---|
| `curvine-master --lib` | 795 / 0 failed / 3 ignored | 795 / 0 / 3 |
| 43 master integration | **232 passed / 0 failed** / 1 ignored | 232 / 0 / 1 |
| `curvine-fault --features fault-injection` | 19 / 0 failed | （其 lane 未单列，语义一致） |
| 合计 | **1027 / 1031** | 一致 |

零执行（fault）：`dir_tree_memory`（0p/1i，design `#[ignore]`）；`fault_isolated_test`
在 fault 侧真实执行（1 passed，`CURVINE_LIB_TEST_BIN` 绑定 lib 二进制）。
无红、无首红定位需求。左辅记录了一次 runner 外层 timeout 中断（非测试失败），
其中 2 个 target 从其原 log backfilled 并标 `note=backfilled`，无红被掩盖。

**两源差异说明**：default/fault 两条 lane 均在 `bcf993319` 上执行；之后的
`a1272ac6b` 只改 `curvine-master/tests/metadata_ab_bench/ops.rs` 的 12 行注释
（`git diff bcf993319..a1272ac6b --stat` = 1 file, +6/−6，无代码语义），
故未重跑；**不把 `bcf993319` 称作最终源**，最终代码源为 `a1272ac6b`。

## 3. 独立已知问题（撤出后暴露，不关闭不掩盖）

`cargo test -p curvine-fuse --lib` 在撤出 pcache 修复后 = **634 passed / 4 failed**：

| 失败用例 | 性质 |
|---|---|
| `pcache::inode_cache::ensure_flushed_skips_when_nothing_written` | 基线既有 |
| `pcache::inode_cache::complete_after_delete_frees_pages_without_backend` | 基线既有 |
| `pcache::write_back_integration::write_back_evict_preserves_partial_pages` | 基线既有 |
| `pcache::write_back_integration::write_back_evicts_on_cache_pressure` | 基线既有 |

**这是 FUSE write-back 缓存的独立问题，不属于分片锁验收范围**；
本轮已把修复与测试完整留档在 `wip/fuse-pcache-safety`，供另立任务接续。
按要求：**不关闭测试、不改 CI、不降低 `deny`**，原状态保留。

## 4. 数据安全风险单列（另立任务，不在本任务关闭）

WIP 分支内已定位但**未纳入本候选**的 FUSE 写回数据安全缺陷（供后续任务）：
1. `AsyncSharedMap::release_with_cleanup` 在"最后引用 + cleanup 失败"时仍消费引用并
   `remove_entry`，`InodeCache::drop` 会丢弃未持久化脏页（`async_map.rs:232-264`）。
2. `evict_inodes` 历史上曾以 `free_pages(force=true)` 处理 flush 失败，可强制清掉未落盘页。
3. `complete` 失败后原实现无条件 `free_pages(true)`。
4. `FlushGuard`/`page_epoch`（ABA）与 orphan 复用方案已在 WIP 上实现并验证
   （右弼 `truncate_aba_test` 2/2、`flush_cancel_test` 2/2），可直接作为后续任务起点。

## 5. 未执行 / 未授权

| 项 | 状态 |
|---|---|
| `dir_tree_memory`（两模式） | 0 执行（design `#[ignore]`），需验收方评估 |
| `fault_isolated_test`（default） | 0 执行（feature-gated） |
| 客户端层性能（writer 装箱影响） | 未测量；A/B harness 在 master 元数据层，不覆盖 client/fuse |
| 正式 A/B（WAL-on vs `bff4e7dc5`） | **未启动**（未授权） |

## 6. ab_bench 注释修正

右弼核查发现两个 mtime case 的注释理由（"candidate boxes SetAttrOpts"）在 pcache
撤出后不再成立。case **保留**（`set_attr` → `set_attr_single_path_sharded`，
`complete_file` → `complete_file_sharded`，都走分片锁热路径；mtime 与 mode-only
payload 不同，仍有独立测量价值），仅改注释为 payload/路径理由。
