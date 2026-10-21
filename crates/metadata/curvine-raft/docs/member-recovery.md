# 单个 Master 丢失本地状态后的恢复（#1718）

> 本文描述本分支新增的显式恢复协议，不适用于未升级的旧版 Curvine。
> 操作对象只限一个已经离线的故障成员；不要同时清理、格式化或重启健康成员。

## 问题与修复范围

一个 Master 的 meta 和 journal 全部丢失后，同一 `raft_id` 对当前 Leader 来说
仍然可能是“日志已经同步到很后面”的成员。Leader 的旧 `Progress.matched`
使心跳携带的 commit 超过空节点的 `last_index=0`，触发 raft-rs 的范围断言。
单独截断心跳不能清除 Leader 的旧进度，也不一定触发重新复制。

本补丁不修改 raft-rs 的 `commit_to` 断言，也不把任意越界提交当作合法状态：

- 每次进程启动生成独立会话；Leader 通过相关联的心跳 RPC 确认对端会话。
- 每个成员同一时刻只使用一个心跳做会话发现，其他心跳正常发送。
  不能仅靠发送序号排序，因为不同请求可能以相反顺序抵达旧/新进程。
- 会话变化只清除该成员的复制进度并主动探测，不回退集群 commit。
- 旧进程的复制确认、发往旧接收会话的 Append/Snapshot 不得重新污染进度。
- 显式恢复期间不 tick、不投票、不竞选、不发布 Follower 就绪状态。
  握手尚未确认当前接收会话，或心跳越界时，只允许保留本机已有 commit；
  不能凭 Leader 的旧 matched 提交尚未验证的日志，哪怕它落在本机日志范围内。
- 在收到 Leader 的任期内持久化弃权（空 vote 设置为本机 ID，不发送投票确认），
  避免恢复后在同一任期给另一候选人投第二票；已存在的非零 vote 不覆盖。
- 完成条件包括：Leader/term 与恢复目标一致，握手已确认新会话，日志、HardState
  和应用状态已落盘，应用追到本地已提交位置，快照安装已完成。
- `journal_applied`、`journal_ufs_applied` 从实际 FSM 刷新；`journal_committed`、
  `journal_term` 从实际 HardState 刷新。快照恢复后不再依赖下一笔写入更新指标。

### 自动隔离与恢复标记生命周期

本机制不仅支持显式运维配置，还提供了被动安全防御与严格的标记生命周期管理：

- **自动隔离触发（Automatic Quarantine Trigger）**：
  当清空（wiped）的成员以全新会话启动重新加入集群时，只要 Leader 发来携带旧接收会话（obsolete receiver session）的心跳 RPC，且满足以下条件之一：
  1. 心跳 commit 越界（`past_tail`）；或
  2. 本地日志完全为空（`raw.raft.raft_log.last_index() == 0`），**即使心跳携带的 commit 为 0**（前提是握手请求包含 `leader_commit` 字段）；
  在集群至少有 3 个既有投票成员且 Leader 支持握手协议时，节点将**自动进入恢复隔离**（quarantine），无需运维事先在配置中声明 `recover_from_peers`。在此期间，节点的投票和竞选被全面禁用，并在当前任期内持久化自投弃权票。
- **标记生命周期与运行时安全隔离（Marker Lifecycle & Policy A Runtime Quarantine）**：
  - **检测/激活时原子创建与存储指示器持久化**：无论是通过隐式自动检测激活，还是通过显式配置 `recover_from_peers` 启动，节点都会在 `journal_dir` 根目录下原子写入 `member-recovery-in-progress`（内容为本机 `raft_id`）并落盘，同时在 RocksDB 元数据区持久化恢复状态指示器（`RECOVERY_KEY`）。
  - **运行时丢失或操作失败保持隔离（Policy A）**：在日志和状态机追平调用 `finish()` 退出恢复时，程序严格要求该标记文件在磁盘上切实存在（除非在重试中已解绑未落盘）。若标记被外部误删，或 `remove_file()`、父目录 `sync_all()` 失败，程序**不会终止 Raft 循环（保持进程存活并在隔离区内）**，而是记录告警日志并维持隔离（`active = true`），持续禁用投票和竞选，并在后续就绪事件中安全重试退出。
  - **跨重启存储校验（Fail-Closed on Restart via Durable Storage Indicator）**：恢复中途若节点重启，`Recovery::load` 检测到标记存在仍会强制保持恢复隔离，即使配置中未包含或已移除 `recover_from_peers`。若标记被意外删除或丢失，启动阶段校验会检查底层存储中持久化的恢复状态指示器（`storage.recovery_state()`）：一旦存储记录显示该节点曾处于未完成的恢复状态，但标记文件缺失且未声明恢复目标，启动直接 fail-closed 报错并拒绝运行，杜绝未完成恢复节点作为正常 voter 参与投票。普通干净节点（即使经历选举自投且未产生提交日志）因无恢复指示器可安全正常启动。
  - **运维安全恢复规程（Operator Procedure）**：若运行时由于文件系统权限等问题导致标记无法删除，排查修复权限后系统会在下一个周期自动重试完成；若节点在标记丢失后重启触发 fail-closed 报错，运维可通过在配置中显式指定 `recover_from_peers = <raft_id>` 或在 `journal_dir` 手工重建写入本机 `raft_id` 的 `member-recovery-in-progress` 标记，使节点重新安全进入恢复隔离并追平数据。
- **何时仍需显式配置 `journal.recover_from_peers`**：
  - **本地存在非空残余日志（`last_index > 0`）**：若故障成员未完全清空（如从陈旧备份恢复或 journal 发生局部损坏/分叉，磁盘残留了部分日志），且 Leader 心跳的 commit 落在本地日志范围内（`commit <= last_index`），自动检测不会触发，必须显式配置 `recover_from_peers = <raft_id>` 强制进入隔离重建；
  - **普通非陈旧会话丢失状态**：普通运行态会话如果出现心跳 commit 超出本地日志尾部（`past_tail && !obsolete_receiver`），系统会直接报 explicit lost-state 错误并拒绝运行，必须由运维排查后显式配置 `journal.recover_from_peers`；
  - **运维主动控制**：在计划内的单成员停机重建或演练时，显式配置 `recover_from_peers` 可在节点启动的第一时间锁定恢复模式。

这不是任意多数派数据丢失、网络分区、多份同身份进程并存或备份回滚的修复协议。
这些场景应停止自动恢复并单独设计成员替换/灾备流程。

## 操作前提

1. 至少三名既有 voter；一次只恢复一个成员，另外两个成员必须维持健康多数派和稳定 Leader。
2. 确认旧进程已经完全退出，绝不能让两个进程同时使用相同 `raft_id`、hostname 或数据卷。
3. Leader 必须运行支持本恢复握手的版本。目标成员不能用旧版本执行恢复。
4. 普通 follower 在新旧版本间切换时，Leader 会通过相关联的心跳把对端标记为
   `Session(id)` 或 `Legacy` 并重置该成员复制进度；迟到的旧会话 ACK 不会被接受。
   但两个都不带 session 的旧进程之间无法可靠识别“换了进程”，这是兼容边界。
5. 核对 `raft_id -> Pod -> Kubernetes node -> meta/journal volume` 映射，并备份残留目录、
   ConfigMap 和工作负载定义。meta 与 journal 必须成对处理，不能混用不同时间点的数据。
6. 不要用于首次建群、多个成员同时丢失或多数派已经丢失的场景。

### 三节点旧版本集群的限制

如果一个 voter 已经故障，两个幸存 voter 仍是旧版本，此时重启任意幸存节点都会把
quorum 从 2/3 降为 1/3。因此“先滚动升级健康成员”**不可能零停机**。必须选择：

- 安排明确的维护窗口，停止自动 rollout，按计划升级并恢复 quorum；或者
- 从经过验证的同一时点一致备份预置目标成员的 meta+journal，再按灾备方案启动。

不要让 Deployment/StatefulSet 自动滚动健康成员，也不要承诺此场景服务不中断。

## Kubernetes 单成员恢复步骤

下面的 `raft_id=3` 只是示例。共享 ConfigMap 中的选项是“目标 ID”，不是全局布尔开关：

```toml
format_master = false

[journal]
enable = true
recover_from_peers = 3
```

1. **确认 Leader 和多数派**：从两个健康 Master 的日志/指标交叉确认当前 Leader、term、
   membership；不要只看 Pod `Running`。
2. **确认身份和卷**：记录三个 `raft_id` 对应的 Pod、hostname、Kubernetes node、
   meta 路径和 journal 路径，确认故障目标确实是 ID 3。
3. **备份并停目标成员**：暂停工作负载自动 rollout，停止目标 Pod/进程，确认端口、PID
   和容器均已退出；保存残留的 meta+journal 和配置。
4. **只清理目标成员的成对存储**：仅处理 ID 3 的 meta 与 journal。不要修改两个健康
   voter 的卷；不要把旧 meta 与另一个时间点的 journal 拼在一起。
5. **设置目标 ID**：在共享 ConfigMap 中设置 `recover_from_peers = 3`。健康节点即使读取
   这份配置也不会进入恢复；但不要重启它们，也不要触发自动滚动更新。恢复目标只在进程
   启动时读取：旧目标仍在运行或其 marker 尚未清除时，严禁把共享 target 改成另一个 ID。
   marker 会记录原目标 ID；同一节点重启时若发现配置指向另一个 ID，会直接拒绝启动。
   系统不会跨节点读取其他成员的本地 marker，因此“只恢复一个成员”依赖运维串行执行。
6. **只启动目标成员**：保持原 `raft_id`、hostname、RPC 地址和卷映射不变。严禁旧实例
   同时复活。恢复期间 startup/liveness 应探测 Raft 端口 8996，readiness 探测 Master
   RPC 端口 8995；8996 可用而 8995 未就绪表示进程仍在恢复，不应被 liveness 杀死。
7. **验证恢复**：确认出现会话握手、Append 或 Snapshot 下载/安装；不得再出现
   `to_commit ... out of range`。确认 marker 存在于恢复过程中，并在日志出现
   `member recovery completed` 后由程序删除。
8. **验证数据而非只看端口**：业务静默时核对目标成员
   `journal_applied == journal_committed`、term 与 Leader 一致，并通过 Master API 实际读取
   一个已知目录/文件的元数据。
9. **移除 opt-in**：从共享 ConfigMap 删除 `recover_from_peers`，仍不要滚动健康节点。
10. **正常重启验收**：在维护窗口只重启已恢复成员，确认它不再创建 marker、能正常加入
    quorum，并验证旧元数据和新增写入。

恢复开始（或自动检测到隔离触发）时，目标成员的 journal 根目录会生成并同步持久化标记：

```text
member-recovery-in-progress
```

这个标记记录开始恢复时的本地 `raft_id`。其完整生命周期与安全约束如下：
- **生成时机**：无论是通过隐式自动检测激活，还是通过显式配置 `recover_from_peers` 启动，均在检测到隔离后由程序原子写入并 fsync。
- **跨重启保持**：即使进程中途重启，甚至 target 已提前从配置删除，只要该标记存在，仍强制锁定在恢复模式；若共享配置被误改为另一个 target，该节点会拒绝启动。
- **完成时强校验与运行态 Policy A**：当日志与状态机追平调用 `finish()` 退出恢复时，程序要求该标记文件必须切实存在于磁盘（或在重试周期中处于已删除但父目录未落盘状态）。若标记被外部或运维提前删除，或删除/目录 fsync 出现 I/O 异常，系统**严格遵循 Policy A 准则：保持 Raft 节点存活并锁定在隔离状态**（不退出 `run0()` 主循环，记录 warn 级别日志，持续屏蔽投票和竞选），并在后续就绪事件中安全重试，绝不将节点静默转为正常投票节点。
- **两阶段删除与目录落盘重试**：程序先执行 `remove_file()` 删除标记，再调用父目录 `sync_all()` 确保目录项变更落盘。若 `remove_file` 成功而目录 `sync_all` 失败，内部状态机记录 `marker_unlinked`，下一次重试直接重试目录同步，避免因文件已不存在误报丢失；只有在标记文件及目录同步均成功后，才清除 RocksDB 中的持久化恢复指示器（`set_recovery_state(None)`），并将恢复状态标记为已完成（`active = false`）。
- **标记丢失重启防御（Startup Fail-Closed via Storage Indicator）**：若未完成恢复的节点在标记丢失后发生进程重启，启动阶段会扫描持久化存储中的恢复指示器（`RECOVERY_KEY`），识别未完成的恢复意图，在缺乏恢复标记上下文时直接拒绝启动，防止节点成为隐患投票者。普通节点（如未提交集群中的候选人重启）因无恢复指示器可安全启动。

清空目标成员重新恢复时，可以保留这个已知 marker；启动目录检查会忽略 journal 根目录中
精确匹配的 marker，但 meta 中的 marker 或任何其他未知文件仍会被拒绝。

## 观察与验收

- 不再出现 `to_commit ... out of range` panic。
- 原 Leader 持续工作，其他成员继续提供服务。
- 目标成员先保持未就绪；日志不足时应有 Append 探测及日志补齐或快照下载。
- 快照必须完成下载和安装，Raft 才能确认其复制成功。
- 日志出现 `member recovery completed` 后，目标成员才发布 Follower 状态。
- 业务静默时，核对目标成员 `journal_applied == journal_committed` 且 term 与 Leader
  一致；与健康成员对比，并实际读取已知元数据。单次指标相等不是唯一验收条件。
- `journal_ufs_applied` 有独立含义，不应为了“看起来追平”强行设为 committed。
- 核对恢复标记已由程序删除。恢复后的 snapshot 描述必须指向本机 checkpoint，
  不能仍引用原 Leader 的本地路径。
- 验收后移除 `recover_from_peers` 目标；在维护窗口验证该成员正常重启、既有元数据
  和后续写入，不能只验证进程存活。

## 失败与回滚

- 旧 Leader、缺少多数派、成员身份不明：停止尝试，先修复前置条件。
- 快照下载/安装失败：本次 Ready 不继续确认；保留日志、目标卷及恢复标记，调查后
  在支持该协议的版本和恢复模式下重试，不通过提前开放投票绕过故障。
- 恢复标记缺失或删除/同步失败：运行时保持隔离区存活重试（Policy A）；若因此重启并触发启动校验失败，运维应核查磁盘健康度，并在配置文件中配置 `recover_from_peers = <raft_id>` 或在 `journal_dir` 中手动补齐写入本机 `raft_id` 的 `member-recovery-in-progress` 文件，然后重启节点重新安全进入恢复隔离完成同步。
- 不要只回滚二进制而保留未完成的恢复状态；旧版本没有本协议的保护。
- 若必须回滚，先停止目标成员，再按独立评审的方案使用一致的备份或重新配置成员；
  保持健康多数派不变。
- 原 issue 报告过“强制 Leader 切换后恢复成功”的临时办法。但它有选举与服务中断
  风险，本次没有在真实部署执行或重新验证，不作为本补丁的必需操作步骤。

## 回归测试

```bash
# 所有 Raft 单元/集成测试；三进程用例仅 Linux 编译运行。
cargo test --locked -p curvine-raft

# 快照后的实际 Journal 指标以及现有 Journal 回归。
cargo test --locked -p curvine-master --lib journal

# 单独运行真实 TCP + RocksDB 三进程用例。
cargo test --locked -p curvine-raft --test raft_recovery_rpc_test -- --nocapture
```

三进程测试使用临时目录、自动分配的 loopback 端口和独立子进程；退出时回收子进程，
失败时保留临时证据。其应用为测试 KV 状态机，不等价于完整 Master/Worker/Kubernetes
部署验收。完整命名空间、多版本滚动兼容及生产拓扑测试仍应在预发布集群执行。
