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

这不是任意多数派数据丢失、网络分区、多份同身份进程并存或备份回滚的修复协议。
这些场景应停止自动恢复并单独设计成员替换/灾备流程。

## 操作前提

1. 至少三名既有 voter；只恢复其中一个，其他成员维持健康多数派及稳定 Leader。
2. 确认原进程已停止，绝不存在两个同时运行的相同 `raft_id` / hostname。
3. **先升级健康成员，尤其是 Leader。** 旧 Leader 不支持会话握手，显式恢复会拒绝
   继续，而不是不安全地降级。协议是 **roll-forward-only**：某个成员一旦被 Leader
   识别为带会话的新进程，就不能再回滚成不带会话的旧版本；否则该 Leader 会拒绝它的
   复制确认。应重新升级该成员；重启 Leader 进程会清空内存会话，切换到从未缓存过该
   会话的 Leader 进程也可以，但普通 Leader 切换不保证解除隔离。恢复期间不要滚动升级/
   降级其余成员。
4. 核对目标成员的配置、节点身份和卷绑定。备份残留目录与配置，保留回滚材料。
5. meta 与 journal 必须都是干净空目录，或者是同一次恢复留下的一致可恢复状态。
   不要把旧 meta 与另一个时间点的 journal 混用。
6. 不要用于首次建群，也不要为所有成员开启恢复。正常启动默认行为不变。

## 启动目标成员

仅修改故障成员的配置：

```toml
format_master = false

[journal]
enable = true
recover_from_peers = true
# hostname、rpc_port、journal_addrs、journal_dir 等保持经核对的原身份/路径。
```

不要照抄执行删除目录或删除 Pod 的命令；先按实际部署核实卷及备份。

恢复开始时，journal 根目录会生成并同步：

```text
member-recovery-in-progress
```

这个标记必须保留。即使进程中途重启，甚至配置开关已被提前移除，标记仍会让节点
保持恢复模式。只有满足落盘与应用追平条件后，程序才删除并同步该标记。
**不要手工删除它来绕过恢复检查。** 标记仍存在时，即使开关被移除，启动检查也会
拒绝 `format_master=true` 或 `journal.enable=false`，避免格式化掉恢复保护。
旧版本不识别该标记，恢复中不能降级。

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
- 验收后移除 `recover_from_peers` 开关；在维护窗口验证该成员正常重启、既有元数据
  和后续写入，不能只验证进程存活。

## 失败与回滚

- 旧 Leader、缺少多数派、成员身份不明：停止尝试，先修复前置条件。
- 快照下载/安装失败：本次 Ready 不继续确认；保留日志、目标卷及恢复标记，调查后
  在同版本恢复模式下重试，不通过提前开放投票绕过故障。
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
