# PR #1005 Final Split Record

本文档记录 PR #1005 以及本地 `hotfix/load-job-queue-cv-path` 分支有效 diff 的最终拆分结果。结论是：#1005 不再作为合并入口；所有有效修复已拆到独立 PR，过小或重复的 PR 已关闭并合并到对应完整 PR。

拆分原则：

- 一个 PR 只解决一个明确问题，必须能独立 review 和验证。
- 公共底座能力只能由一个 PR 承载；后续 PR 如果复用该能力，必须显式声明 base/dependency。
- 宁可保留同文件不同 hunk 的小冲突，也不能把无关语义塞回一个大 PR。
- 生产故障相关结论必须来自源码、日志或可复现验证，不能用猜测替代证据。

## 背景

PR #1005 同时包含 Docker 构建优化、TTL 路径解析死锁修复、`fs_dir` 锁观测、load job 风暴治理、客户端重试、ORPC 错误返回、block report 修复等多个方向。这个范围过大，review、回滚和上线风险都不可控，因此拆成下面的独立 PR。

拆分前生产集群已确认可用：

- `cv-curvine-master`：3/3 Ready，active master 上 `cv fs stat /` 和 `cv fs ls /` 成功。
- `cv-curvine-worker`：20/20 Ready。
- 最近观测窗口内 master/worker 日志未发现新的 panic、ERROR 或心跳批量丢失。

## 最终结论

- #1005 已作为源 PR/对照 PR 关闭，不再合并。
- 当前有效功能已覆盖到 #1012、#1013、#1014、#1015、#1017、#1019、#1020、#1021、#1024、#1026、#1027、#1029。
- #1016、#1018、#1022、#1030、#1031 已关闭，因为它们的能力已并入更完整的 PR。
- #1026 显式基于 #1017，复用 `FsError::ResourceExhausted` 和 master admission 能力。
- #1029 显式基于 #1024，复用 DeleteBlock response 处理，只保留 worker 调度修复。
- 当前主工作区相对 `github-https/main` 仍显示 diff，但其中 tracked dirty 文件已映射到下表 PR；`curvine-cli/src/cmds/load*.rs` 和 `orpc/src/io/spdk_poller.rs` 是落后 main 的反向差异，不属于 #1005 拆分内容，不能提交。

## 最终 PR 清单

| PR | 状态 | Base | 改动规模 | 解决的问题 | 关键验证 |
| --- | --- | --- | --- | --- | --- |
| [#1012](https://github.com/CurvineIO/curvine/pull/1012) | OPEN, mergeable | `main` | +14/-53, 3 files | Docker runtime image 不再重新构建完整 workspace，降低镜像构建时间和资源消耗。 | `bash -n build/build.sh`; `git diff --cached --check` |
| [#1013](https://github.com/CurvineIO/curvine/pull/1013) | OPEN, mergeable | `main` | +135/-23, 2 files | TTL executor 不再持有 `fs_dir.read()` 后递归解析父路径，避免同线程读锁重入导致 writer 饥饿或死锁。 | `cargo fmt --check`; `cargo clippy -p curvine-server --all-targets --jobs 2 -- --deny=warnings --allow clippy::uninlined-format-args`; `cargo test -p curvine-server --test master_fs_test -- --test-threads=1` |
| [#1014](https://github.com/CurvineIO/curvine/pull/1014) | OPEN, mergeable | `main` | +160/-0, 4 files | 增加 `fs_dir` 全局锁 watchdog 和指标，暴露元数据锁不可获取导致的控制面卡死信号，不改变恢复语义。 | `cargo fmt --check`; `cargo clippy -p curvine-server --all-targets --jobs 2 -- --deny=warnings --allow clippy::uninlined-format-args`; `git diff --check` |
| [#1015](https://github.com/CurvineIO/curvine/pull/1015) | OPEN, mergeable | `main` | +94/-3, 2 files | ORPC async handler 业务错误也返回标准 RPC response，和 sync handler 语义一致。 | `cargo fmt --check`; `cargo test -p orpc --test stream_handler_test`; `cargo clippy -p orpc --all-targets --jobs 2 -- --deny=warnings --allow clippy::uninlined-format-args`; `git diff --check` |
| [#1017](https://github.com/CurvineIO/curvine/pull/1017) | OPEN, mergeable | `main` | +899/-58, 9 files | Master load job admission 完整闭环：结构化 `ResourceExhausted`、有界 FIFO、后台 runtime、missing-source 快速失败/短负缓存、指标、shutdown hook 和测试。 | `cargo fmt --check`; `cargo test -p curvine-common --test fs_error_test --test job_conf_test`; `cargo test -p curvine-server --test load_job_submit_test -- --test-threads=1`; `cargo clippy -p curvine-common -p curvine-server --all-targets --jobs 2 -- --deny=warnings --allow clippy::uninlined-format-args`; `git diff --check` |
| [#1019](https://github.com/CurvineIO/curvine/pull/1019) | OPEN, mergeable | `main` | +338/-27, 6 files | Master control-plane RPC lane 隔离：新增 ORPC `try_spawn`，并把 heartbeat、block report、master info、list、get block locations 路由到独立有界 executor。 | `cargo fmt --check`; `cargo test -p orpc --test executor_test single_executor_try_spawn_returns_error_when_queue_is_full`; `cargo test -p curvine-server --test master_fs_test -- --test-threads=1`; `cargo clippy -p orpc -p curvine-server --all-targets --jobs 2 -- --deny=warnings --allow clippy::uninlined-format-args`; `git diff --check` |
| [#1020](https://github.com/CurvineIO/curvine/pull/1020) | OPEN, mergeable | `main` | +177/-23, 3 files | 修正 CV 路径 load/export 方向：UFS-only 元数据 load 走 UFS -> Curvine；fs_mode journal replay 仍走 Curvine -> UFS export。 | `cargo fmt --check`; `cargo test -p curvine-server --test load_job_submit_test fs_mode_cv_path_load_uses_ufs_source_when_metadata_is_ufs_only`; `cargo test -p curvine-server --test load_job_submit_test direct_export_task_keeps_cv_source_for_fs_mode_journal_sync`; `cargo clippy -p curvine-server --all-targets --jobs 2 -- --deny=warnings --allow clippy::uninlined-format-args`; `git diff --check` |
| [#1021](https://github.com/CurvineIO/curvine/pull/1021) | OPEN, mergeable | `main` | +182/-9, 2 files | `ArcRwLock` 在 debug/test build 下检测同线程重入，提前暴露“持有读锁又拿同一把锁”的问题；release 无线程本地检测开销。 | `cargo fmt --check`; `cargo test -p orpc --test arc_rw_lock_test`; `cargo clippy -p orpc --all-targets --jobs 2 -- --deny=warnings --allow clippy::uninlined-format-args`; `cargo clippy -p curvine-server --all-targets --jobs 2 -- --deny=warnings --allow clippy::uninlined-format-args`; `git diff --check` |
| [#1024](https://github.com/CurvineIO/curvine/pull/1024) | OPEN, mergeable | `main` | +442/-65, 8 files | Master block report 修复：missing/non-file inode 对应 block 返回 DeleteBlock；worker 对增量和全量 response 都执行删除；completed full report stale-location reconcile 后台执行并保留 under-replicated 上报。 | `cargo fmt --check`; `cargo test -p curvine-server --test master_fs_test block_report_for_non_file_inode_schedules_worker_delete -- --test-threads=1`; `cargo test -p curvine-server --test master_fs_test full_block_report_reconcile_removes_stale_location_async -- --test-threads=1`; `cargo test -p curvine-server --test lock_order_deadlock_stress_test -- --test-threads=1`; `cargo test -p curvine-server --test master_fs_test -- --test-threads=1`; `cargo clippy -p curvine-server --all-targets --jobs 2 -- --deny=warnings --allow clippy::uninlined-format-args`; `git diff --check` |
| [#1025](https://github.com/CurvineIO/curvine/pull/1025) | OPEN, mergeable | `main` | +93/-0, 1 file | 记录 #1005 最终拆分结果、依赖关系和关闭 PR 原因。 | `git diff --check` |
| [#1026](https://github.com/CurvineIO/curvine/pull/1026) | OPEN, draft, mergeable | `codex/split-master-load-job-conf` (#1017) | +466/-68, 4 files | 客户端 cache miss 不再无界 spawn SubmitJob；本地 FIFO、pending 去重、`ResourceExhausted` 重试、UFS source 校验、fs-mode UFS-only job id 修正组成完整客户端闭环。 | `cargo fmt --check`; `git diff --check`; `cargo test -p curvine-common --test client_conf_cli_test validates_auto_cache_submit_backpressure_conf` |
| [#1027](https://github.com/CurvineIO/curvine/pull/1027) | OPEN, draft, mergeable | `main` | +82/-32, 3 files | Master heartbeat checker 缩短 `worker_manager.write()` 临界区，日志和 `delete_locations` 后续任务移出锁外，blacklist 状态迁移改成幂等。 | `cargo fmt --check`; `git diff --check`; `cargo test -p curvine-server --test worker_manager_test add_blacklist_worker_is_idempotent -- --test-threads=1`; `cargo clippy -p curvine-server --all-targets --jobs 2 -- --deny=warnings --allow clippy::uninlined-format-args` |
| [#1029](https://github.com/CurvineIO/curvine/pull/1029) | OPEN, mergeable | `codex/split-block-report-delete-cmd` (#1024) | +105/-29, 2 files | Worker block report 调度修复：注册成功后先启动 heartbeat，后台周期重试 full block report；增量 report 异步提交且单 worker 只保留一个 in-flight，失败回填 pending。 | `cargo fmt --check`; `cargo clippy -p curvine-server --all-targets --jobs 2 -- --deny=warnings --allow clippy::uninlined-format-args`; `cargo test -p curvine-server --test master_fs_test block_report_for_non_file_inode_schedules_worker_delete -- --test-threads=1`; `cargo test -p curvine-server --test master_fs_test full_block_report_reconcile_removes_stale_location_async -- --test-threads=1`; `git diff --check` |

## 已关闭 PR

| PR | 关闭原因 |
| --- | --- |
| [#1016](https://github.com/CurvineIO/curvine/pull/1016) | 只新增 `FsError::ResourceExhausted`，已并入完整 master admission PR #1017，避免公共错误类型重复提交。 |
| [#1018](https://github.com/CurvineIO/curvine/pull/1018) | 只新增客户端 backpressure 配置，已并入完整客户端 auto-cache PR #1026，避免配置和行为分离。 |
| [#1022](https://github.com/CurvineIO/curvine/pull/1022) | missing-source fast fail 已并入 #1017，单独保留会重复修改 `job_runner.rs` 和测试。 |
| [#1030](https://github.com/CurvineIO/curvine/pull/1030) | worker incremental block report async 提交已并入 #1029，避免过小 PR 分开 review。 |
| [#1031](https://github.com/CurvineIO/curvine/pull/1031) | master full block report 后台 reconcile 已并入 #1024，避免两个 PR 同时修改 `block_report()` 返回语义。 |

## 依赖关系

```text
main
├── #1012 Docker runtime build
├── #1013 TTL fs_dir read-lock reentry
├── #1014 fs_dir watchdog
├── #1015 ORPC async error response
├── #1017 master load job admission
│   └── #1026 client auto-cache flow
├── #1019 master RPC lane isolation
├── #1020 load/export direction
├── #1021 ArcRwLock debug reentry guard
├── #1024 stale block report reconcile
│   └── #1029 worker block report scheduling
├── #1025 split record
└── #1027 heartbeat checker lock scope
```

## Review 结论

最终 review 后，剩余 open PR 之间仍有少量同文件重叠，但没有发现同一能力在多个 PR 中重复实现：

- `curvine-server/src/master/fs/mod.rs`：#1014 增加 watchdog module export，#1024 导出 block report 类型，语义不同。
- `curvine-server/src/master/job/job_runner.rs` 和 `load_job_submit_test.rs`：#1017 是 admission/backpressure，#1020 是 CV 路径 load/export 方向，语义不同。
- `curvine-server/src/master/master_handler.rs`：#1019 是 RPC lane 隔离，#1024 是 block report DeleteBlock response，语义不同。
- `curvine-server/src/master/master_server.rs`：#1017 注入 JobManager shutdown/background runtime，#1019 注入 RPC lane executor，语义不同。
- `curvine-server/src/worker/block/block_actor.rs` 和 `heartbeat_task.rs`：#1029 显式基于 #1024，复用 #1024 的 DeleteBlock response 处理，不重复实现。
- `curvine-server/tests/master_fs_test.rs`：多个 PR 添加不同回归用例，测试模块相同但覆盖点不同。

因此，当前拆分结果符合“小而完整、依赖显式、功能独立”的要求。后续 review/merge 应以表格中的 open PR 为准，不再回到 #1005 合并。

## 长期问题

`fs_dir` 全局锁仍是 master 元数据路径的扩展性天花板。短期 PR 只做可验证的小修和观测增强；更细粒度锁、索引路径或元数据分片属于长期设计，已由 issue [#1011](https://github.com/CurvineIO/curvine/issues/1011) 跟踪。
