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

右弼核查发现两个 mtime case 的注释理由（"candidate boxes SetAttrOpts"）不成立。
**更正一处早先表述**：生产装箱**并未**随 pcache 撤出 —— 相对基线 `5d3bdd177`，
`crates/client/curvine-client-core`（`WriterTask::Complete/Resize`）、
`crates/client/curvine-unified-fs`（`MirrorTask::Complete`）、
`curvine-fuse/src/fs/fuse_writer.rs`（`WriteTask::Complete/Resize`）共 **6 处
`Box<SetAttrOpts>` / `Box<FileAllocOpts>` 仍在**，它们是 `clippy --deny=warnings`
门禁所必需，属保留项。ab_bench 注释里"boxed payload"不成立的真实原因只是：
该 harness 直调 `MasterFilesystem`，未经这些 writer，与装箱无关。case **保留**（`set_attr` → `set_attr_single_path_sharded`，
`complete_file` → `complete_file_sharded`，都走分片锁热路径；mtime 与 mode-only
payload 不同，仍有独立测量价值），仅改注释为 payload/路径理由。

## 7. 可定位事实索引（集中核验用，逐项给出真实路径与缺口）

### 7.1 门禁记录（已按授权在冻结源补跑，证据独立）

**补跑源**：`a1272ac6b`（`HEAD`=a1272ac6b，`tree`=`afa6f63558c54dabffcb7402d183c1f526407401`，
`dirty=0` 前后一致），worktree `/tmp/cv-ci-a127`，
证据目录 **`/tmp/cv-ci-a127-evidence/`**（未覆盖任何旧日志）。
命令参数取自 `.github/workflows/build.yml` 原文（`CARGO_BUILD_JOBS=4`、`CLIPPY_LEVEL=deny`）。

| 命令（argv 见对应 `.argv`） | status | error 行 | 证据 |
|---|---|---|---|
| `cargo clippy --all-targets --jobs 4 -- --deny=warnings --allow clippy::uninlined-format-args`（build.yml:96） | **0** | 0 | `ci_clippy.{argv,status,stdout.log,stderr.log}` |
| `cargo clippy -p curvine-service-discovery --features etcd --tests --jobs 4 -- --deny=warnings --allow clippy::uninlined-format-args`（build.yml:99） | **0** | 0 | `ci_clippy_etcd.{argv,status,stdout.log,stderr.log}` |
| `cargo fmt --all -- --check` | **0** | — | `/tmp/cv-reg-bcf/fmt.{argv,log,status}`（default lane 同步产生） |

**历史缺口的更正说明（保留记录）**：在补跑之前，`/tmp/cv-reg-bcf` 只有 `fmt.*`，
workspace clippy 仅有 `/tmp/clip-after-revert.log`（无 argv/status），etcd clippy 在最终源
**无记录**（最后一次在 `c1a8bb3f3`：`/tmp/cv-reg-final4/ci_clippy_etcd.*`）。上一版索引已如实
标注该缺口、未补造；本版为补跑后的真实结果。status 取自进程退出码文件，不以日志
`Finished` 推断。

### 7.2 fault lane：哪条是有效证明
| lane | 路径 | 完整性 | 是否作为证明 |
|---|---|---|---|
| **我的 lane** | `/tmp/cv-reg-bcf/*-f.{argv,status,log}`（44 组）+ `lib_fault_bin.{path,sha256}` | 44/44 同源一次跑完（`FAULT-DONE 20:09:11`，`FAILURES` 空） | **是（主证明）** |
| 左辅 lane | `/tmp/cv-reg-fault-rightyi/`（44 log + `results.csv` + `libbin.sha256`） | 外层 runner timeout 中断后**续跑**；`results.csv` 中 **仅 1 条** `backfilled`：`journal_uncommitted_restart_test`（log 时间 19:51，中断前） | 交叉验证 |

**更正左辅口头描述**：backfilled 是 **1 项**（`journal_uncommitted_restart_test`），
`metadata_snapshot_rename_test` 的 log 时间 20:13（续跑后真实执行），**不是** backfilled。
中断为 runner 外层 timeout 杀脚本，非测试失败；两 lane 数字一致（795 / 232 / 1027）。

### 7.3 未执行与红
- `dir_tree_memory`：**未执行**（default 与 fault 均 0p/1i，design `#[ignore]`），未额外运行
- `fault_isolated_test`：default 0 执行（feature-gated），fault 侧 1 passed
- `curvine-fuse --lib`：**4 failed（基线既有 pcache 用例）**，因此
  **不得表述为"全项目绿"**；本任务结论仅限分片锁范围内的 959/963 与 1027/1031
- 客户端层性能：**未测量**（6 处生产装箱仍在，见 §6 更正）

## 8. 同一模块两个数字的源归属（避免读成统计错误或掩盖红）

| 源 | pcache 状态 | 执行者 | `curvine-fuse --lib` | 证据 |
|---|---|---|---|---|
| `4be509e9d` | 含 `765a7054f` 的 `page_epoch`/`EpochSnapshot` 修复 + 右弼 2 个测试 + `METRICS_LOCK` | 右弼 | **645 passed / 0 failed** | `/tmp/cv-fuse-lock-*/full.log` |
| `bcf993319` / `a1272ac6b` | pcache 撤出、回到基线 `5d3bdd177` | 承栋 | **634 passed / 4 failed** | 本轮 default lane 报告 |

两者**不矛盾，源不同**：撤出 pcache 修复时一并撤掉了 `page_epoch`，基线 4 个用例的
失败因此重新出现。**可验证的推断（未坐实）**：那 4 红很可能正是被 `page_epoch`
（`765a7054f`）顺带治好的 —— 坐实需要在 WIP 分支上跑一次 `curvine-fuse --lib`；
按暂停令未执行，故只记为推断，不写入结论。

## 9. 冻结链（两侧独立核对一致）
`5d3bdd177`（基线，pcache 未动）→ … → `bcf993319`（撤出 pcache）→
`a1272ac6b`（ab_bench 注释修正，+6/−6）＝**代码候选冻结**
→ `3bdb5ca43` / `4f7cdcdae` / `b1b9fd526`（docs-only，`git diff a1272ac6b..HEAD -- '*.rs' 'Cargo.toml'` = 0）
核对项：`5d3bdd177..a1272ac6b -- curvine-fuse/src/fs/pcache/` 与 `async_map.rs` 均为空。
WIP 留档：`wip/fuse-pcache-safety` = `0ceb720ff`；右弼分支 `4be509e9d`；
左辅分支 `feat/s7-review-fixes` 链尾 `49788f4df`。
