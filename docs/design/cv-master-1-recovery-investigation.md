# curvine `cv-master-1` 无法恢复问题排查报告

- **排查对象**：`curvine` namespace / `cv-master-1`
- **排查时间**：2026-10-14
- **部署目录**：`/root/codespace/shuttle/curvine/aliyun-yarn`
- **参考源码**：`/root/codespace/barry/codespace/shuttle/curvine/code/curvine`
- **线上镜像**：`curvine:x86-2026-10-14-15-26`
- **结论状态**：已完成日志、Kubernetes 状态和源码联合分析；未修改线上资源，未执行 Pod 重启、数据删除或格式化操作。

## 1. 结论摘要

`cv-master-1` 本次不是普通的进程启动慢，也不是 Raft 本地日志无法读取，而是启动后成为 Raft leader，在恢复已提交 journal 时执行 UFS export。由于该进程内没有任何 worker 注册，`CompleteFile` replay 返回 `No workers available`，随后在同一批 journal 上按 10 秒间隔反复重试，恢复进度长期停在 index `54295`。

约 1 小时后，journal writer 因三节点 Raft 请求在 60 秒内无法完成而 abort master。线上 entrypoint 又通过 `tail -f /dev/null` 保持容器存活，而 `cv-master` StatefulSet 没有 readiness、startup、liveness probe，最终表现为：

- master 进程已经退出；
- 8995、8996、9001 均无监听；
- Pod 仍然是 `Running` 和 `Ready`；
- StatefulSet 可能继续把该 Pod 当作滚动部署成功节点。

### 根因链路

```text
master 滚动更新
  -> cv-master-1 启动并成为 leader
  -> 该进程 worker_map 为空，没有 Worker register
  -> journal leader replay CompleteFile
  -> UFS export 需要 worker，但返回 No workers available
  -> JournalLoader 固定重试同一批 journal
  -> journal writer Raft 请求 60s 超时
  -> master abort
  -> entrypoint 的 tail 保持容器 Running
  -> 无 Kubernetes probe，Pod 仍显示 Ready
```

## 2. 故障现象与线上证据

### 2.1 Pod 与 StatefulSet 状态

线上查询结果：

```text
NAME          READY   STATUS    RESTARTS   AGE
cv-master-0   1/1     Running   0          3h29m
cv-master-1   1/1     Running   0          3h30m
cv-master-2   1/1     Running   0          3h41m
```

`cv-master-1` 的容器状态：

```text
phase: Running
container state: running
container ready: true
restartCount: 0
startedAt: 2026-10-14T07:56:56Z
```

注意：容器实际本地时间对应 `2026-10-14 15:56:56 +0800` 启动。

### 2.2 `cv-master-1` 启动与失败时间线

日志目录为 hostPath `/data/curvine/logs`，容器内通过 `/app/curvine/logs` 链接访问。

日志文件：

```text
/data/curvine/logs/master.log
/data/curvine/logs/master.out
```

启动输出：

```text
datetime: 2026-10-14 15:56:56.527
git version: 74d40837
service: "master"
Loading config from /app/curvine/conf/curvine-cluster.toml
```

随后日志显示该节点在 leader replay 阶段反复失败：

```text
2026/10/14 15:57:24.110 INFO  journal_loader.rs:355
replay-scan, start_index: 54295, entries: 229, commit_index: 54523

2026/10/14 15:57:33.240 INFO  journal_loader.rs:364
replay-scan, start_index: 54522, entries: 2, commit_index: 54523
```

同一条 `CompleteFile` UFS export 失败：

```text
apply entry failed(retry_num=6)
...
CompleteFile(... inode id: 38105 ...)
export job failed: No workers available
```

到故障结束前，恢复进度仍然停在 `54295`：

```text
2026/10/14 16:56:48.311 replay-scan, start_index: 54295, entries: 231, commit_index: 54525
2026/10/14 16:56:48.324 apply entry failed(retry_num=353)
2026/10/14 16:57:08.340 replay-scan, start_index: 54295, entries: 231, commit_index: 54525
2026/10/14 16:57:08.353 apply entry failed(retry_num=355)
```

最终 master 被 journal writer 终止：

```text
2026/10/14 16:57:31.072 ERROR sender_task.rs:58
thread journal-writer stop:
failed to determine after 1 attempts:
deadline has elapsed:
swept 3 node(s) in 60.001891456s, rpc_timeout=60s
node cv-master-0 ... Not leader
node cv-master-2 ... Not leader
node cv-master-1 ... deadline has elapsed
aborting master
```

### 2.3 `cv-master-1` 没有 worker 注册

对三个 master 的日志统计：

```text
cv-master-0: Worker register 1176 次
cv-master-1: 当前 master.log Worker register 0 次
cv-master-2: Worker register 997 次
```

`cv-master-1` 当前 master.log 没有：

```text
Worker register: worker_id=...
```

这说明在该 master 进程的内存状态中没有建立 worker 注册信息。当前故障之后，集群查询到约 176 个 worker 为 Running，但这只能说明后续 worker 集群已恢复，不能证明故障窗口内这些 worker 已注册到 `cv-master-1`。

### 2.4 master 进程已经退出，但容器仍然存活

对 `cv-master-1` 执行进程和端口检查：

```text
PID 1  /bin/bash /entrypoint.sh master
PID 241 ... tail -f /dev/null
```

没有 `curvine-server` 进程，以下端口也没有监听：

```text
8995  master RPC
8996  Raft journal RPC
9001  metrics HTTP
```

但 Pod 仍然是：

```text
Running / ready=true / restartCount=0
```

线上镜像的 `/entrypoint.sh` 中存在多处：

```bash
tail -f /dev/null &
wait || :
```

因此不能使用容器 `Running` 或 Pod `READY 1/1` 判断 master 已成功启动。

## 3. 源码分析

### 3.1 Master 启动顺序

参考源码：

```text
curvine-master/src/master/master_server.rs
```

核心启动顺序：

```rust
let mut listener = journal_system.start().await?;
listener.wait_role().await?;
...
rpc_server.start();
```

也就是说，master 会先启动 journal/Raft，等待角色确定，然后才启动 master RPC。leader 角色确定后，journal loader 仍可能继续处理 UFS replay，因此“Raft role 已确定”不等于“所有恢复工作已完成”。

### 3.2 JournalLoader 的 leader UFS replay

参考源码：

```text
curvine-master/src/master/journal/journal_loader.rs
curvine-master/src/master/journal/ufs_loader.rs
```

leader 角色变化时会：

```rust
self.catch_up_committed_metadata().await?;
...
let ufs_applied = self.get_ufs_applied()?;
retry_msg.replace(ApplyMsg::new_scan(ufs_applied));
```

leader replay 的 `CompleteFile`、`Rename`、`Delete` 等操作会调用 `UfsLoader`，其中 `CompleteFile` 会提交 UFS export task。

参考源码：

```text
curvine-master/src/master/journal/ufs_loader.rs
```

关键逻辑：

```rust
self.submit_export_task(&path, &mnt).await?;
```

因此 leader 在恢复 journal 时不是只读本地 RocksDB，还可能依赖 worker 完成 UFS 操作。

### 3.3 `No workers available` 的确切触发条件

参考源码：

```text
curvine-master/src/master/fs/policy/weighted_worker_policy.rs
curvine-master/src/master/fs/worker_manager.rs
```

`WeightedWorkerPolicy::choose()`：

```rust
if workers.is_empty() {
    return err_box!("No workers available");
}
```

因此本次错误含义是：

- 不是 worker 都有但容量不够；
- 不是权重为 0；
- 不是副本数大于 worker 数；
- 而是 worker 列表为空。

如果 worker 列表非空但没有满足容量、权重等条件，源码会返回 `No eligible workers for weighted selection`，与本次日志不同。

### 3.4 重试策略导致长时间卡住

参考源码：

```text
crates/common/curvine-config/src/journal_conf.rs
curvine-master/src/master/journal/journal_loader.rs
```

默认参数：

```text
max_retry_num = 1000
retry_interval_secs = 10
skip_failed_ufs_replay_after_retry = true
```

JournalLoader 逻辑是：

- leader replay 失败后进入 retry；
- 每次等待 `retry_interval_secs`；
- 未达到 `max_retry_num` 前不会跳过；
- 达到上限后，如果 `skip_failed_ufs_replay_after_retry=true`，才跳过 UFS 错误继续维持 master。

理论最大等待时间约为：

```text
1000 * 10 秒 = 10000 秒 ≈ 2 小时 46 分钟
```

本次在 retry=355 时已经因为 journal writer Raft 请求超时 abort，并未达到 1000 次。

## 4. 根因分析

### 4.1 直接根因

`cv-master-1` 成为 leader 后，其内存 worker_map 为空。leader replay 到 `CompleteFile` 时需要 worker 执行 UFS export，但无法选择任何 worker，于是持续返回：

```text
No workers available
```

### 4.2 触发条件

更可能的触发条件是滚动部署期间的 worker 注册窗口：

```text
master 重启后 worker_map 清空
worker 需要重新连接 master 并完成 Start/Running heartbeat
master 在 worker 注册完成前成为 leader
leader replay 立即依赖 worker
```

当前证据可以确认“cv-master-1 没有 worker 注册”，但仅凭现有日志不能进一步证明是以下哪一种具体情况导致 worker 没有注册：

- worker 同时滚动或处于不可用窗口；
- worker 只连接旧 leader，未及时切换到新 leader；
- master RPC 虽然启动，但 worker heartbeat 在 replay/角色切换期间失败；
- worker 地址、协议、cluster_id 或版本不匹配；
- worker 连接到的服务端点不是当前 master leader。

需要保留并对齐 worker 侧故障窗口日志，才能进一步确定注册失败的网络或配置原因。

### 4.3 放大因素

#### 放大因素一：UFS replay 与 master 恢复耦合

一个 UFS export 失败会阻塞同一批 leader replay，无法及时推进恢复水位。

#### 放大因素二：默认重试时间过长

单条 UFS 任务最多等待约 2 小时 46 分钟，线上表现为长时间无法恢复。

#### 放大因素三：entrypoint 保活

`curvine-server` abort 后，`tail -f /dev/null` 仍让容器存活。

#### 放大因素四：StatefulSet 无探针

当前 `master.yaml` 和线上 live StatefulSet 均没有 readiness、startup、liveness probe，导致 Kubernetes 无法识别 master 已退出或不可服务。

#### 放大因素五：Parallel 滚动部署

当前配置：

```yaml
podManagementPolicy: Parallel
updateStrategy:
  type: RollingUpdate
  rollingUpdate:
    maxUnavailable: 1
```

`maxUnavailable: 1` 只能限制同时不可用数量，不能保证 worker 注册完成，也不能判断 master journal replay 已完成；在无 probe 且 entrypoint 保活的情况下，Ready 判断尤其不可靠。

## 5. 影响分析

### 5.1 对 cv-master-1 的影响

- master 进程最终 abort；
- 该节点没有 master RPC 和 Raft RPC；
- 本地 journal replay 未完成；
- Pod 仍可能被误判为 Ready；
- 后续 StatefulSet 滚动控制可能基于错误状态继续推进。

### 5.2 对 Raft 集群的影响

- cv-master-1 成为 leader 后无法稳定完成 journal writer 请求；
- 其他节点可能经历 leader 切换；
- 日志中可见 `Not leader`、`Connection refused`、`deadline has elapsed`；
- 如果故障节点被错误地认为仍健康，可能延迟故障发现和恢复。

### 5.3 对 metadata 与 UFS 一致性的影响

当前代码将 metadata apply 与 leader UFS replay 分开维护 watermark，但 UFS replay 失败会阻塞 `journal_ufs_applied` 推进。

如果启用“达到重试上限后跳过 UFS replay”的应急策略：

- Curvine metadata 可能继续前进；
- 对应 UFS export 可能尚未完成；
- 必须有后续补偿任务或人工核对，否则可能出现 Curvine 与 UFS 状态不一致。

## 6. 修复方案

### 6.1 P0：先修复健康检查和 Ready 假象

参考源码已经提供 master metrics HTTP 接口：

```text
9001 /healthz
9001 /readyz
```

源码位置：

```text
curvine-master/src/master/metrics_server.rs
```

语义：

- `/healthz`：Raft role 已建立后，active 和 standby 都返回 200；
- `/readyz`：只有 active leader 返回 200，standby 返回 503；
- SafeMode、Exit、未建立 role 时返回 503。

建议在 `cv-master` StatefulSet 中增加 HTTP probes：

```yaml
readinessProbe:
  httpGet:
    path: /readyz
    port: 9001
  periodSeconds: 10
  timeoutSeconds: 2
  failureThreshold: 3

startupProbe:
  httpGet:
    path: /healthz
    port: 9001
  periodSeconds: 10
  timeoutSeconds: 2
  failureThreshold: 180

livenessProbe:
  httpGet:
    path: /healthz
    port: 9001
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 2
  failureThreshold: 6
```

说明：当前排查阶段未将该配置应用到线上。

该方案首先解决的是“master 已退出但 Pod 仍 Ready”，不能单独解决 worker 注册竞态和 UFS replay 阻塞。

### 6.2 P0：调整滚动部署顺序

建议部署流程：

```text
1. 暂停 master 并行滚动
2. 确认 worker 集群健康且可向当前 leader 注册
3. 一次只更新一台 master
4. 等待该 master 出现 Raft role
5. 等待 master 日志出现 Worker register
6. 等待 replay-scan 前进或恢复完成
7. 再更新下一台 master
```

不要使用：

```text
Pod Running
Pod READY 1/1
```

作为 master 恢复完成的唯一判断，必须同时检查：

```text
8996 Raft 端口
9001 /healthz
9001 /readyz
worker register 日志
journal replay 进度
```

### 6.3 P1：保证 worker 能在 master 重启后快速注册

需要补充验证：

- worker 的 master 地址发现机制；
- worker 是否能发现当前 leader，而不是只连接固定 master；
- worker `cluster_id` 是否匹配；
- worker 与 master 协议版本是否一致；
- worker Start/Running heartbeat 是否在 master replay 期间被处理；
- worker 滚动更新是否与 master 滚动更新同时发生；
- worker 侧是否出现连接拒绝、Not leader、超时等错误。

建议在 master 和 worker 两侧同时记录：

```text
pod name
master address
raft leader
worker register time
worker heartbeat time
worker session id
```

### 6.4 P1：解耦 metadata replay 与 UFS replay

长期推荐改造：

```text
Raft metadata replay 成功
  -> metadata applied watermark 前进
  -> master 控制面可服务
  -> UFS export 进入独立异步补偿队列
  -> UFS replay 单独暴露失败数、延迟和 backlog
```

目标是避免一条 UFS export 失败阻塞整个 master 的恢复和 Raft 稳定性。

### 6.5 P1：补充恢复进度和 worker 数指标

现有 metrics 已包含：

```text
journal_applied
journal_ufs_applied
journal_committed
worker_num
```

参考源码：

```text
curvine-master/src/master/master_metrics.rs
```

建议监控：

```text
journal_committed - journal_applied
journal_committed - journal_ufs_applied
worker_num
master healthz/readyz
Raft leader 变化次数
UFS replay error 次数
```

告警条件建议：

```text
worker_num == 0 持续超过短暂注册窗口
journal_ufs_applied 长时间不增长
Pod Ready=1 但 8995/8996/9001 无监听
master.log 出现 No workers available
journal-writer deadline has elapsed
```

### 6.6 P2：应急降低 replay 重试时间

如果线上必须避免 master 长时间卡住，可以显式配置：

```toml
max_retry_num = 6
retry_interval_secs = 10
skip_failed_ufs_replay_after_retry = true
```

这会将单条 UFS replay 的等待窗口降低到约 1 分钟。

但该方案有明确风险：

- 会跳过失败的 UFS export；
- metadata 与 UFS 可能暂时不一致；
- 必须有 UFS replay 补偿、对账和告警；
- 不建议在没有补偿机制前直接作为长期默认配置。

## 7. 当前故障处理建议

在确认 `cv-master-0` 或 `cv-master-2` 有稳定 leader、worker 已正常注册前，不建议操作 `cv-master-1` 数据目录。

允许的恢复方向：

1. 确认当前 leader 和 Raft 多数派；
2. 确认 worker 已经注册到正常 master；
3. 保持 `format_master=false`；
4. 重启或重新创建 `cv-master-1` Pod，让其复用原有 `/data/curvine/meta` 和 `/data/curvine/journal`；
5. 观察是否出现 `Worker register`；
6. 观察 `replay-scan start_index` 是否前进；
7. 观察 `/healthz`、`/readyz` 和 8995/8996/9001 端口状态。

明确禁止在未完成数据保护和一致性确认前执行：

```text
format_master=true
删除 /data/curvine/meta
删除 /data/curvine/journal
手工删除 RocksDB 文件
```

## 8. 验证清单

### 故障复现/修复前

- [ ] 记录三台 master 的 Pod start time、restartCount、container state
- [ ] 记录三台 master 的 8995、8996、9001 监听状态
- [ ] 记录当前 leader 和 standby
- [ ] 记录每台 master 的 `Worker register` 数量
- [ ] 记录 `journal_committed`、`journal_applied`、`journal_ufs_applied`
- [ ] 记录 worker Pod 的启动时间和滚动更新时间
- [ ] 保留 worker 侧 heartbeat/register 日志

### 修复后

- [ ] master 进程退出后 Pod 能被判定为 NotReady 或重启
- [ ] master 进程退出后不再因为 `tail -f /dev/null` 长时间保持 Ready
- [ ] master 重启后能在预期窗口内收到 Worker register
- [ ] leader replay 时 `worker_num` 不为 0
- [ ] `replay-scan start_index` 持续前进
- [ ] `journal_ufs_applied` 持续前进
- [ ] 8996 Raft 端口持续可用
- [ ] leader 的 `/readyz` 返回 200
- [ ] standby 的 `/healthz` 返回 200、`/readyz` 返回 503
- [ ] 滚动更新过程中始终保持 Raft 多数派
- [ ] 滚动更新完成后不存在“进程退出但 Pod Ready”的节点

## 9. 最终判断

本次 `cv-master-1` 无法恢复的直接原因是：

> 该节点成为 leader 后没有任何 worker 注册，导致 leader 在恢复 `CompleteFile` journal 时无法执行 UFS export，持续报 `No workers available`，最终 journal writer Raft 请求超时并 abort master。

长期反复出现的放大因素是：

> UFS replay 同步阻塞、worker 注册与 master 滚动部署存在竞态、entrypoint 在 master 进程退出后仍保持容器存活，以及 StatefulSet 没有有效的健康检查。

优先级最高的修复不是删除本地数据或重新 format，而是：

1. 修复 worker 注册和 master 滚动顺序；
2. 增加 `/healthz`、`/readyz` 探针，消除 Pod Ready 假象；
3. 将 UFS replay 从 master 可用性链路中解耦；
4. 在具备补偿机制后，再考虑降低失败 replay 的重试窗口。

## 10. `EntryWithAck` 是否导致 UFS replay 阻塞

### 10.1 结论先行

`new_entry_with_ack` **不会直接同步阻塞 UFS replay，也不会因为等待 ack 直接阻塞 Raft Ready**。它的主要作用是：

> 对 leader 本地提案，只有 JournalLoader 后台 apply worker 完成该 entry 后，Raft 才向提案客户端返回响应。

但是，`EntryWithAck` 与 UFS `Scan` 共用同一个 JournalLoader apply worker 和同一个 apply queue，因此存在**队列级间接耦合**：

- UFS `Scan` 卡住时，后续 `EntryWithAck` 不能被 worker 处理，提案响应会延迟；
- 后续 `EntryWithAck` 很多时，可能排在 UFS `Scan` 前面，延迟 UFS replay；
- 如果 apply queue 配置为有界且队列满，`apply_propose(false, ...)` 的入队本身也可能 await；
- 当前线上配置没有显式设置 `writer_channel_size`，源码默认值为 `0`，即无界队列，因此当前故障不是 EntryWithAck ack 等待导致的 Ready 阻塞。

### 10.2 EntryWithAck 的生产路径

源码：

```text
crates/metadata/curvine-raft/src/raft/raft_node.rs
```

leader 提交普通 journal entry 时：

```rust
let should_respond = self.is_leader() && !entry.get_data().is_empty();
let apply = self.apply_propose(entry, should_respond).await?;
```

当 `should_respond=true`：

```rust
self.storage.apply_propose(
    false,
    ApplyMsg::new_entry_with_ack(entry, tx),
).await?;
```

注意这里明确传的是 `wait=false`。

### 10.3 JournalLoader 对 EntryWithAck 的实际处理

源码：

```text
curvine-master/src/master/journal/journal_loader.rs
```

`JournalLoader::apply`：

```rust
if matches!(&msg, ApplyMsg::EntryWithAck(_)) {
    self.sender.send(msg).await?;
    return Ok(());
}
```

因此生产环境有 apply worker 时，调用链是：

```text
RaftNode::apply_propose
  -> JournalLoader::apply(wait=false, EntryWithAck)
  -> send 到 apply queue
  -> 立即返回
  -> RaftNode::apply_committed_entries 返回
```

这里没有等待业务 apply 完成，也没有等待 UFS 操作完成。

`EntryWithAck` 进入后台 worker 后，才会执行：

```rust
ApplyMsg::Entry(entry) | ApplyMsg::EntryWithAck((entry, _))
    -> self.apply0(...).await
```

apply 成功时：

```rust
Self::complete_entry_ack(msg, Ok(()));
```

失败时：

- leader：进入 retry；达到上限后按配置 skip 或 abort；
- follower：直接 fatal abort；
- 在成功或 skip 前，不会发送 EntryWithAck ack。

所以 ack 的语义是“后台 apply 已经处理完”，不是“entry 已经成功落盘到 Raft”。

### 10.4 ack 真正阻塞的是提案响应

`apply_committed_entries` 把 ack receiver 交给：

```rust
send_propose_response_after_apply(..., apply_done)
```

该函数后台等待：

```rust
apply_done.receive().await
```

收到 ack 后才向原提案客户端发送成功或失败响应。

因此影响关系是：

```text
EntryWithAck
  -> 不阻塞 apply_propose 入队
  -> 不阻塞 Raft advance/Ready（正常无界队列时）
  -> 会延迟该提案的 RPC 响应
```

如果 UFS replay 正在同一个 apply worker 中卡住，那么后续普通提案的 `apply_done` 也无法及时收到，表现为提案响应延迟。

### 10.5 它对 JournalWriter 的间接影响

leader UFS replay 成功后，`JournalLoader::set_applied` 会调用：

```rust
journal_writer.log_ufs_applied(applied.op_id, applied.term, applied.index)
```

`UfsApplied` 会被 `JournalWriter` 作为新的 Raft proposal 提交。`SenderTask::flush_pending` 使用：

```rust
client.block_on_send_propose(bytes)
```

而这个 proposal 的响应同样会经过 leader 的 `EntryWithAck` 路径。

因此可能形成如下间接耦合：

```text
UFS replay 成功
  -> 生成 UfsApplied proposal
  -> JournalWriter block_on_send_propose
  -> leader 对该 proposal 使用 EntryWithAck
  -> ack 需等待 apply worker
```

不过本次线上失败日志中的直接错误仍然是：

```text
No workers available
```

并且失败的 UFS `Scan` 在 `set_applied` 前已经返回，因此该失败 entry 不会生成新的 `UfsApplied` marker。线上 `journal-writer abort` 的直接报错是三节点 Raft 请求 60 秒 deadline 超时，不能简单归因成“EntryWithAck 等 ack 导致死锁”。更准确的结论是：

> UFS replay 卡住是主因；EntryWithAck 可能使其他 proposal 的响应进一步延迟，从而加重 journal-writer/提案超时，但不是 UFS replay 卡住的直接原因。

## 11. 选主成功后的完整回放时序

下面按实际代码顺序描述，不把 Raft 的 applied、FSM 的 applied 和 UFS applied 混为一个位置。

### 阶段 A：Raft 选举并生成 Ready

1. `RawNode` 完成选举，SoftState 变为 Leader 或 Follower。
2. `RaftNode::on_ready` 取出 Ready。
3. `finish_ready` 先持久化 entries 和 HardState。
4. 持久化完成后发送 Raft messages。
5. 处理 Ready 中的 committed entries。

关键点：Raft 层先保证 entry 和 HardState 持久化，之后才发送复制消息和处理 committed entries。

### 阶段 B：Raft 层把 committed entry 投递给 AppStorage

`apply_committed_entries` 逐个处理 committed entry：

- follower：使用 `ApplyMsg::Entry`；
- leader 且 entry 有 data：使用 `ApplyMsg::EntryWithAck`，为提案响应保存 ack receiver；
- `JournalLoader::apply` 通常只把消息放进 apply queue 后返回。

然后 `finish_ready` 继续：

```text
raw.advance(ready)
  -> 持久化新的 commit index
  -> 发送 advance 返回的 messages
  -> 处理 advance 返回的 committed entries
  -> raw.advance_apply()
```

因此在正常生产路径中：

> Raft 的 `advance_apply()` 不等待 JournalLoader 的业务 apply ack。

这与开发反馈中“Raft 只保证 entry 正确落地”基本一致，但需要注意：这里的 Raft apply cursor 更接近“已投递/已由 Raft 层推进”，不是 `JournalLoader::FsmState.applied`。

### 阶段 C：角色切换是 Ready 路径中的一个同步屏障

`finish_ready` 在处理完 committed entries 后执行：

```rust
self.storage.role_change(ss.raft_state).await?;
```

生产环境 `JournalLoader::role_change` 会发送 `ApplyMsg::RoleChange`，并等待一个单独的 ack：

```rust
self.sender.send(ApplyMsg::RoleChange((role, tx))).await?;
rx.receive().await?
```

因此 RoleChange 与 EntryWithAck 不同：

- EntryWithAck：只入队，Raft 不等待其 ack；
- RoleChange：入队后，Raft `finish_ready` 会等待 worker 返回 role-change ack。

### 阶段 D：新 leader 先做 metadata catch-up

apply worker 收到 `RoleChange(Leader)` 后执行：

```rust
self.catch_up_committed_metadata().await?;
```

该函数使用：

```rust
ApplyMsg::Scan(applied)
```

但此时传入 `is_leader=false`，所以这一步只做 metadata replay，不执行 UFS 操作：

```text
committed log
  -> metadata apply
  -> 更新 FsmState.applied
  -> 直到 metadata_applied >= committed
```

如果 metadata catch-up 失败，RoleChange ack 返回错误，`finish_ready` 返回错误，Raft node 可能停止。这是会直接影响 Ready 的路径，但它不是 UFS replay。

### 阶段 E：metadata catch-up 完成后发布 leader role

metadata catch-up 成功后，apply worker：

```rust
is_leader = true;
let ufs_applied = self.get_ufs_applied()?;
retry_msg.replace(ApplyMsg::new_scan(ufs_applied));
```

然后向 `RoleChange` ack receiver 返回成功。

`finish_ready` 收到 role-change 成功后才执行：

```rust
self.role_monitor.advance_role(&ss);
```

随后 `Master::start` 中的：

```rust
listener.wait_role().await?;
```

才会返回，master RPC/metrics 服务才继续启动。

所以实际顺序是：

```text
Raft leader 产生
  -> Ready 持久化
  -> committed entries 入队
  -> RoleChange 入队并等待
  -> follower-mode metadata catch-up 完成
  -> RoleChange ack
  -> role_monitor 发布 Leader
  -> wait_role 返回
  -> 启动 master RPC/metrics
```

### 阶段 F：leader UFS replay 在 RoleChange 之后异步执行

RoleChange ack 返回时，worker 只是把 `Scan(ufs_applied)` 放入 `retry_msg`，尚未完成整个 UFS replay。

apply worker 下一轮才会处理：

```text
Scan(ufs_applied)
  -> 读取 committed log entries
  -> 对每条 entry 执行 apply0(is_leader=true)
  -> UfsLoader 执行 mkdir/export/rename/delete
```

这说明：

> 选主成功后，UFS replay 不是 `finish_ready` 直接等待的同步步骤；它由同一个后台 apply worker 在 role 发布后继续执行。

### 阶段 G：UFS replay 成功、失败和重试

成功路径：

```text
UFS 操作成功
  -> set_applied(is_leader=true, applied, has_ufs_affecting)
  -> FsmState.ufs_applied = applied
  -> FsmState.applied = applied
  -> journal_ufs_applied metric 更新
  -> 生成 UfsApplied journal entry
```

失败路径：

```text
UFS 操作失败
  -> apply0 返回 Err
  -> 不执行 set_applied
  -> ufs_applied 不前进
  -> retry_msg 保留同一个 Scan
  -> sleep retry_interval_secs
  -> 再次处理同一批 entry
```

达到重试上限且启用 skip：

```text
apply_msg(is_leader=true, skip_ufs_error=true)
  -> 忽略 UFS 操作错误
  -> 继续 set_applied
  -> 推进 ufs_applied/applied
  -> 记录 UfsApplied marker
```

因此，当前代码中不能笼统地说“无论 UFS 成功失败，应用层都推进 applied”。准确区分如下：

| 位置/状态 | 谁负责 | UFS 失败时是否推进 |
|---|---|---|
| Raft log entry / HardState / commit | Raft storage | Raft 层可继续 advance，不等待 UFS ack |
| RaftNode 的内部 apply cursor | Raft `advance_apply()` | 不等业务 apply 完成 |
| `FsmState.applied` | JournalLoader | 正常失败时不推进；skip 时推进 |
| `FsmState.ufs_applied` | JournalLoader | 正常失败时不推进；UFS 成功或 skip 时推进 |
| `journal_ufs_applied` | JournalWriter + UfsApplied entry | 只有 set_applied 后才生成 |

## 12. 对开发反馈的准确结论

开发反馈中“回放进程不需要同步响应”可以拆成两部分：

### 正确的部分

如果“同步响应”指的是 Raft 不应该等待业务 apply 完成再推进 Ready，那么当前代码实际上已经是这样：

```text
apply_propose(false, EntryWithAck)
  -> 入队立即返回
  -> finish_ready 不等待 EntryWithAck ack
```

所以删除 ack 等待不会解决当前 `No workers available` 导致的 UFS replay 卡住。

### 需要修正的部分

如果“applied 位置无论成功失败都由 JournalLoader 推进”指的是当前实际代码行为，那么不完全成立：

- JournalLoader 的 `set_applied` 在 `apply0` 成功完成后才调用；
- UFS 错误正常返回时，`set_applied` 不执行；
- 只有配置允许 skip failed UFS replay，并实际走到 skip 分支后，才会在跳过错误后推进 watermark。

同时，Raft 层的 `advance_apply()` 与 JournalLoader 的 `FsmState.applied` 是两个不同概念。前者不等业务 ack，后者由 JournalLoader 控制，不能混称为同一个 applied 位置。

## 13. 如果要移除 EntryWithAck，需要同步修改什么

不能只把：

```rust
ApplyMsg::new_entry_with_ack(entry, tx)
```

改成：

```rust
ApplyMsg::new_entry(entry)
```

还需要明确新的提案响应语义：

1. 是否在 entry 入队成功后立即响应；
2. UFS/metadata apply 后失败如何通知客户端；
3. JournalWriter 的 `block_on_send_propose` 是否可以接受“Raft 已提交但 AppStorage 尚未 apply”的响应；
4. `UfsApplied` marker 是否仍然需要依赖应用层 watermark；
5. apply queue 满时是否仍允许 Raft Ready 因入队 await；
6. 如何监控后台 apply 失败而不丢失错误。

如果产品语义确实是“Raft commit 成功即向客户端返回，业务 replay 异步负责”，更合理的设计是显式区分：

```text
Raft commit response
AppStorage enqueue response
AppStorage apply completion/error
UFS replay completion/error
```

而不是继续使用名称上表示“应用完成确认”的 `EntryWithAck`，以免调用方误以为它是 Raft 必需的同步屏障。

## 14. 最终判断

针对“`new_entry_with_ack` 是否导致 UFS 回放阻塞”的最终判断：

1. **不是直接原因**：`EntryWithAck` 在生产环境只入队，不等待 ack；UFS replay 使用的是后续 `Scan`，不是 EntryWithAck。
2. **不直接阻塞 Raft Ready**：`apply_committed_entries` 不等待 `apply_done`，`raw.advance()` 和 `raw.advance_apply()` 可以继续；但 RoleChange ack、apply queue 满和 metadata catch-up 仍可能阻塞 Ready。
3. **会阻塞提案响应**：leader 的 proposal response 明确等待 `apply_done`，所以 UFS Scan 占用单 worker 时，其他 proposal response 会延迟。
4. **存在间接队列耦合**：EntryWithAck 和 UFS Scan 共用一个串行 apply worker；相互会影响处理顺序和响应延迟。
5. **本次线上根因仍是 UFS Scan 卡在 UfsLoader/worker 选择**：`No workers available` 导致 replay worker 重试，EntryWithAck ack 不是触发点。
6. **应用 watermark 需要区分**：当前代码中 Raft 的 commit/apply cursor 可以先推进，但 JournalLoader 的 `FsmState.applied`/`ufs_applied` 正常不会在 UFS 失败时推进；开发反馈描述的是目标语义，不是当前实现的完整事实。

## 15. master 与 worker 同时滚动更新的解决方案

### 15.1 目标

滚动更新期间必须同时满足：

```text
Raft 始终保持多数派
worker 至少有一批可用并已注册
新 leader 可以先接收 worker heartbeat
UFS replay 失败不能拖死 Raft/metadata 控制面
Pod 的 Ready 状态必须反映真实服务状态
```

不能只依赖：

```text
Pod Running
Pod READY 1/1
容器端口声明存在
```

### 15.2 当前配置为什么会产生竞态

当前配置：

```yaml
# master
podManagementPolicy: Parallel
rollingUpdate:
  maxUnavailable: 1
# worker
rollingUpdate:
  maxUnavailable: 10%
  maxSurge: 0
```

同时存在以下问题：

1. master 和 worker 可以在同一个发布窗口内一起变化；
2. worker 更新期间最多可能同时失去较大比例的 worker；
3. master 新进程的 worker_map 是空的，worker 注册不是 Raft 状态；
4. WorkerHeartbeat 在 master standby 上会被统一的 active gate 返回 `NotLeader`；
5. 新 leader 立即排队 UFS replay，没有等待 worker Running heartbeat；
6. master 和 worker 都没有有效的 Kubernetes readiness gate；
7. `publishNotReadyAddresses: true` 使 readiness 即使存在，也不会自动阻止 headless service 发布未 Ready endpoint；
8. entrypoint 在 server 进程退出后仍可通过 `tail -f /dev/null` 保持容器 Running。

### 15.3 P0：发布编排改成严格分阶段

不建议继续使用 `apply.sh all` 直接同时推动 ConfigMap、master、transfer、worker。建议发布流程拆成以下阶段：

#### 阶段 0：冻结并确认现状

```text
停止并行发布
确认当前 leader
确认 Raft 3/3 或至少 2/3 正常
确认 worker live 数量和 worker 注册时间
确认没有 master replay backlog
```

#### 阶段 1：先滚动 worker

master 保持不变，先更新 worker：

```text
旧 worker 保持服务
新 worker 启动
新 worker 完成 Start heartbeat
新 worker 完成至少一次 Running heartbeat
新 worker 出现在当前 leader 的 live worker 列表
再删除/替换旧 worker
```

Deployment 建议：

```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxUnavailable: 0
    maxSurge: 1
```

如果实际 worker 数量很大，可以使用小比例 `maxSurge`，但不建议使用当前 `maxUnavailable: 10%` 作为默认值。发布闸门不能只检查：

```bash
kubectl rollout status deploy/cv-worker
```

还必须检查当前 leader 的：

```text
worker_num{tag="live"}
Worker register 日志
worker Running heartbeat
```

只有新 worker 注册数量达到预期，并持续稳定两个 heartbeat 周期后，才能进入 master 发布阶段。

#### 阶段 2：暂停 worker 变更，再滚动 master

master 必须一次只替换一台，建议使用 StatefulSet `partition` 做人工分阶段，而不是一次修改 image 让控制器自行推进：

```text
partition=2：只更新 ordinal 2
等待 cv-master-2 完成 health/recovery/worker gate
partition=1：再更新 ordinal 1
等待完成
partition=0：最后更新 ordinal 0
等待完成
```

实际更新顺序应优先选择当前 follower，避免主动重启 leader。如果目标 Pod 当前是 leader，应先完成 leader transfer，或者等待其安全切换后再更新。

每一台 master 的完成条件至少包括：

```text
8996 Raft 端口可用
/healthz 返回 200
/readyz 返回预期状态
worker live 数量不为 0
replay-scan index 持续前进
journal_ufs_applied 持续前进
```

下一台 master 不能仅以 Pod `Ready` 作为前一台完成的依据。

#### 阶段 3：发布完成后再解冻自动扩缩容

如果 worker 有 HPA、节点池扩缩容或其它自动滚动控制器，master 发布期间应暂停或设置维护窗口，避免发布脚本和自动控制器同时改变 worker 数量。

### 15.4 P0：马上补充探针，但区分三种状态

源码已经存在：

```text
master: 9001 /healthz、/readyz
worker: 9002 /healthz
```

建议先补基础探针，解决“进程死了但 Pod Ready”的问题。

#### master 基础探针

```yaml
readinessProbe:
  httpGet:
    path: /readyz
    port: 9001
  periodSeconds: 10
  timeoutSeconds: 2
  failureThreshold: 3

livenessProbe:
  httpGet:
    path: /healthz
    port: 9001
  periodSeconds: 10
  timeoutSeconds: 2
  failureThreshold: 6
```

`startupProbe` 的失败窗口必须大于允许的合法启动恢复时间，不能使用一个过短的 30 秒窗口去杀死正在做合法 snapshot/metadata restore 的 master。

但要注意：当前 `/readyz` 只表示“当前是 active leader”，不表示“UFS replay 已完成”。因此它只能解决基础 leader readiness，不能作为最终 replay gate。

#### worker 基础探针

worker 当前 `/healthz` 只能表示 worker 进程服务状态。建议新增：

```text
worker /readyz
```

只有以下条件都满足时返回 200：

```text
worker RPC 已监听
Start heartbeat 成功
至少一次 Running heartbeat 成功
没有处于停止/退出状态
```

不能只用 TCP 8997 作为 worker readiness，因为端口监听不代表已经注册到 master。

### 15.5 P1：增加真正的 recovery/replay gate

建议在 master metrics server 增加单独的状态接口，而不是复用现有 `/readyz`：

```text
/healthz       进程/Raft role 存活
/readyz        leader metadata RPC 可接收
/recoveryz     metadata 和 UFS replay 达到服务要求
```

建议状态机：

```text
Starting
  -> RaftRoleKnown
  -> MetadataCatchUp
  -> LeaderServing
  -> WorkerRegistrationGate
  -> UfsReplay
  -> Recovered
```

如果产品要求 master 在 UFS replay 完成前不能对外提供完整服务，则 `/recoveryz` 在以下条件满足前返回 503：

```text
FsmState.applied >= committed
FsmState.ufs_applied >= required replay target
没有不可恢复的 replay error
```

如果产品允许 metadata 控制面先服务、UFS replay 异步补偿，则：

```text
/readyz 只表示 metadata leader 可服务
/recoveryz 表示 UFS replay 已追平
```

发布控制器使用 `/recoveryz`，客户端流量是否使用 `/readyz` 由产品一致性要求决定。

### 15.6 P1：解决 worker 注册闸门和启动死循环

当前 master 在 leader replay 中立即执行 UFS Scan，而 worker_map 可能还是空的。建议把 UFS replay 的启动改成：

```text
leader metadata catch-up 完成
  -> 发布 leader role
  -> 允许 WorkerHeartbeat 注册
  -> 等待至少一个 Running worker，或进入明确的 Recovering 状态
  -> 再开始 UFS replay
```

最小实现可以是：

```text
WorkerManager 增加 live_worker_count()
WorkerManager 增加 worker_registered_notify
Leader promotion 后等待 worker gate
等待期间不 abort，不把“无 worker”当 fatal
有 worker 后唤醒 UFS replay
```

必须设置超时，但超时动作不应该是 abort master：

```text
worker gate 超时
  -> recoveryz 保持 503
  -> 记录 No workers available
  -> 继续 Raft/heartbeat/metadata control plane
  -> 等 worker 注册后继续 replay
```

这样即使 worker 也在滚动，master 仍然可以保持 Raft 存活，不会因为 UFS export 直接拖死整个进程。

### 15.7 P1：WorkerHeartbeat 不应被 master active gate 完全挡住

当前 `MasterHandler::handle` 在统一入口先判断：

```text
if !master_monitor.is_active() {
    return NotLeader
}
```

这会让 standby 无法维护本地 worker 注册状态。

有两种实现路线。

#### 路线 A：最小改动，保持 heartbeat 只到 leader

保留 leader-only heartbeat，但保证 worker client：

```text
收到 NotLeader
  -> 更新 leader 地址
  -> 立即重试 Start heartbeat
  -> leader 切换后重新发送 Start
  -> Start 成功后发送 Running heartbeat
```

同时 leader 必须在 UFS replay 前先允许 heartbeat，并且 UFS replay 不能在 worker_map 为空时 fatal/无限占用唯一 apply worker。

这是改动较小、兼容当前 worker client 的方案。

#### 路线 B：更稳妥，允许 worker registration 在所有 Raft member 上建立本地状态

把以下请求从 active gate 中拆出：

```text
WorkerHeartbeat(Start)
WorkerHeartbeat(Running)
```

standby 可以维护本地内存 worker_map，但仍然禁止在 standby 执行：

```text
block allocation
metadata mutation
block report mutation
UFS job submission
```

worker 状态是临时运行态，不需要通过 Raft 复制；每个 master 都维护一份本地 worker presence。这样 leader 切换后不必等待所有 worker 再次注册。

但当前 `MasterClient` 主要通过 leader-aware `FsClient` 发送 heartbeat，若要真正实现路线 B，还需要让 worker heartbeat 能访问任一 master，或者增加独立的 peer registration RPC，不能只修改 master 的 gate。

### 15.8 P1：把 UFS replay 从唯一 apply worker 中拆出

当前结构：

```text
metadata Entry
EntryWithAck
RoleChange
UFS Scan
snapshot
```

共用一个 apply queue/worker。

长期推荐改成：

```text
Raft apply worker
  -> 负责 metadata entry、RoleChange、watermark

UFS replay executor
  -> 负责 mkdir/export/rename/delete
  -> 有独立并发度、队列、重试和超时
```

metadata entry 可以先完成并推进 metadata watermark，UFS 操作记录为：

```text
ufs_pending
ufs_succeeded
ufs_failed
ufs_retry_at
```

UFS 失败时：

```text
不阻塞 Raft Ready
不阻塞 worker heartbeat
不阻塞 metadata apply
不 abort master
```

同时暴露：

```text
ufs_replay_pending
ufs_replay_failed
ufs_replay_last_success_index
ufs_replay_oldest_age_seconds
```

### 15.9 P1：服务拆分，避免 readiness 影响 Raft peer discovery

当前 master 和 worker Service 都设置：

```yaml
publishNotReadyAddresses: true
```

这对 Raft peer DNS 可能是必要的，但它意味着 Kubernetes readiness 不会自动阻止这些 endpoint 被发现。

建议拆分服务：

```text
cv-master-peer
  headless
  publishNotReadyAddresses: true
  只用于 Raft peer/DNS

cv-master-active
  普通 Service
  publishNotReadyAddresses: false
  只选 readiness 通过的 active master

cv-worker-rpc
  普通 Service 或由 worker client 使用 leader-aware 地址
```

Raft peer 发现和业务流量发现不能共用同一个“未 Ready 仍发布”的 Service 语义。

### 15.10 推荐落地顺序

#### 第一批：不改协议即可做

```text
1. 停止 master/worker 同时滚动
2. worker maxUnavailable 改为 0，maxSurge 改为小值
3. worker 先滚动，确认 leader worker_num 稳定
4. master 使用 partition 一次更新一台
5. master 增加 /healthz、/readyz probe
6. worker 增加基础 /healthz probe
7. 发布脚本增加 worker register 和 journal replay gate
```

#### 第二批：小范围代码改造

```text
1. worker 增加 /readyz，表示已完成 Start + Running heartbeat
2. master 增加 /recoveryz
3. leader promotion 后增加 worker registration gate
4. No workers available 变成 Recovering/可重试状态，不直接 abort
5. 增加 worker_num、replay watermark 和 recovery 状态指标
```

#### 第三批：架构改造

```text
1. metadata apply 与 UFS replay 分离
2. UFS replay 独立队列和 executor
3. UFS pending/failed 补偿机制
4. worker registration 在 standby 本地预热或实现 peer registration
5. peer Service 与 active Service 拆分
```

### 15.11 验收标准

一次完整 master + worker 发布必须满足：

```text
worker rollout 期间：
  - master leader 不变化或有明确 leader
  - worker live 数量不跌破发布阈值
  - 新 worker 完成 Start + Running heartbeat

master rollout 期间：
  - 任意时刻 Raft 至少 2/3
  - 一次只更新一台 master
  - 新 master 8996 可用
  - 新 master 不出现 worker_num=0 的长时间窗口
  - replay-scan index 单调前进
  - journal_ufs_applied 单调前进
  - 无 No workers available 持续告警
  - 无进程退出但 Pod Ready 的情况
```

建议将以下条件纳入自动发布 gate：

```text
live_worker_num >= 发布前 live_worker_num 的最低比例
worker register age < heartbeat timeout
Raft quorum healthy
recoveryz == 200
journal_ufs_applied 在 SLO 内追平 committed
```

### 15.12 本次没有直接应用的内容

本次分析没有直接修改或应用以下内容：

- 没有修改线上 StatefulSet/Deployment；
- 没有重启或删除 `cv-master-1`；
- 没有修改 `format_master`；
- 没有删除 meta/journal；
- 没有直接改动 `EntryWithAck` 代码；
- 没有直接改动 worker heartbeat 协议；
- 没有把 `skip_failed_ufs_replay_after_retry` 作为长期方案打开。

推荐先落地 P0 分阶段发布和基础探针，再通过压测/故障注入验证 registration gate，最后实施 UFS replay 解耦。

## 16. 不依赖发布操作规范的 master 自恢复机制

### 16.1 核心原则

不能把系统正确性建立在以下假设上：

```text
worker 一定先滚动完成
master 一定按顺序更新
发布期间一定不会有 leader 切换
worker 一定及时重新注册
UFS 一定能立即完成 export
```

这些都应该被视为运行时正常故障，而不是 fatal error。

目标应改成：

```text
worker 暂时不存在：master 继续运行并等待
leader 暂时不存在：journal writer 保留 batch 并重试
UFS export 暂时失败：记录 pending，后台补偿
leader 变化：取消旧 replay，按新 term/index 重新调度
proposal response 延迟：不能杀进程
Raft 多数派仍在：不能因为应用层暂时失败退出
```

需要保留的真正不可恢复状态只有：

```text
本地 RocksDB 明确损坏
journal entry 无法反序列化
snapshot 校验失败且无法从 peer 恢复
metadata invariant 已被破坏
本地磁盘/文件系统明确返回不可恢复错误
```

这类错误也不应使用无条件 `process::abort()`，而应进入 `SafeMode`，停止对外 metadata 服务，但保持 metrics、healthz、Raft 诊断接口可用，等待自动恢复或明确人工处理。

### 16.2 当前必须消除的两条 abort 链

#### 链路一：UFS replay 间接触发 journal-writer abort

当前路径：

```text
leader UFS Scan
  -> No workers available
  -> JournalLoader 每 10 秒重试
  -> worker 无法处理后续任务/提案 ack 延迟
  -> JournalWriter block_on_send_propose 超时
  -> SenderTask::loop0 返回 Err
  -> SenderTask::spawn 直接 process::abort()
```

这里的 `NotLeader`、`LeaderNotReady`、`Connection refused`、`deadline elapsed` 都可能只是 leader 切换或 worker/master 滚动期间的暂时状态，不能进入 abort。

#### 链路二：JournalLoader 把可恢复 apply error 升级为 abort

当前 `abort_on_fatal_apply_error` 会在以下情况直接终止进程：

```text
leader 达到 max_retry 且不允许 skip
skip 逻辑本身失败
follower 任意 apply error
```

需要先将错误分类：

```rust
enum ApplyDisposition {
    Retry,
    SkipUfsAndAdvance,
    EnterSafeMode,
}
```

只有明确属于 metadata corruption/invariant violation 的错误才进入 `EnterSafeMode`；worker 缺失、UFS timeout、NotLeader、连接失败、leader change 都必须是 `Retry`。

### 16.3 推荐状态机

建议在 master/journal 层增加持久可观测的 recovery state：

```text
Starting
  -> FollowerMetadataReady
  -> LeaderMetadataReady
  -> WaitingForWorkers
  -> UfsReplaying
  -> DegradedUfsPending
  -> Serving
```

异常和角色变化：

```text
任意可恢复状态 -- worker missing/UFS timeout --> WaitingForWorkers/DegradedUfsPending
任意 leader 状态 -- leader lost --> FollowerMetadataReady
任意 leader 状态 -- new term --> 取消旧 replay，重新生成 replay generation
任意状态 -- metadata corruption --> SafeMode
SafeMode -- peer snapshot/人工恢复成功 --> Starting
```

状态至少包含：

```rust
struct RecoveryState {
    role: RoleState,
    term: u64,
    generation: u64,
    metadata_applied: AppliedIndex,
    ufs_applied: AppliedIndex,
    target_commit: u64,
    live_worker_count: usize,
    last_error_class: Option<RecoveryErrorClass>,
    last_error: Option<String>,
    next_retry_at: Instant,
}
```

其中 `generation` 必须随 leader term 或 leader 身份变化递增，旧 leader 的异步 UFS task 即使晚返回，也不能更新当前 leader 的 watermark。

### 16.4 Leader 选举后的正确行为

当前 leader promotion 的 metadata catch-up 可以保留，但 UFS replay 需要改成非致命后台任务：

```text
RoleChange(Leader)
  -> metadata catch-up
  -> 发布 Leader / 允许 worker heartbeat
  -> 启动 UFS replay coordinator
  -> worker 不存在时进入 WaitingForWorkers
  -> worker 回来后继续 replay
```

不能在 RoleChange 或 leader promotion 阶段同步等待 worker，也不能因为 worker 为 0 让 Raft node 退出。

推荐：

```text
WorkerManager 提供 live_worker_count()
WorkerManager 提供 worker_registered_notify()
UfsReplayCoordinator 等待 notify 或 timeout
```

worker 暂时为 0 时：

```text
healthz = 200：Raft/进程仍然活着
readyz = 按 metadata 服务策略决定
recoveryz = 503：UFS replay 尚未追平
Raft heartbeat 继续
WorkerHeartbeat 继续
UFS replay 后台等待
```

### 16.5 UFS replay 必须与 metadata/关键 Raft worker 解耦

当前 `ApplyMsg::Scan` 与 metadata apply 共用 JournalLoader 的单一 worker。最小安全改造是增加独立的：

```text
UfsReplayCoordinator
UfsReplayExecutor
UfsReplayQueue
```

目标结构：

```text
Raft/metadata apply worker
  -> committed metadata
  -> RoleChange
  -> metadata watermark
  -> 不被 UFS export 阻塞

UFS replay worker
  -> mkdir/export/rename/delete
  -> retry/backoff
  -> pending/failed 状态
  -> ufs watermark
```

UFS replay 失败时：

```text
不返回 fatal 到 RaftNode
不让 apply worker 卡住
不停止 journal-writer
不停止 worker heartbeat
不调用 process::abort()
```

UFS task 应携带：

```rust
struct UfsReplayTask {
    generation: u64,
    term: u64,
    index: u64,
    op_id: u64,
    entry: JournalEntry,
    retry_count: u64,
    next_retry_at: Instant,
}
```

task 执行前后都要检查：

```text
当前仍是同一 leader generation
当前 term 没有变化
entry 仍未被新的 ufs_applied watermark 覆盖
```

leader 变化时只取消旧 task，不删除 journal；新 leader 从持久的 `ufs_applied` 位置重新调度。

### 16.6 分离 metadata watermark 和 UFS watermark

当前 `set_applied(is_leader=true, ...)` 同时推进：

```text
state.applied
state.ufs_applied
```

这会把 metadata 和 UFS 的完成语义绑定在一起。建议拆成：

```rust
set_metadata_applied(index)
set_ufs_applied(index)
record_ufs_pending(index, error)
```

语义改成：

```text
metadata_applied：Raft committed metadata 已进入本地 FSM
ufs_applied：对应 UFS side effect 已成功完成
ufs_pending：UFS 尚未成功，但可继续补偿
```

UFS 失败时允许：

```text
metadata_applied 前进
ufs_applied 保持不变
ufs_pending 增加记录
```

如果产品要求 UFS 完成前不能提供完整服务，则用 `recoveryz=503` 控制流量，而不是停止 Raft 或 abort 进程。

### 16.7 JournalWriter 不能因为暂时无法找到 leader 而 abort

当前代码：

```rust
if let Err(e) = Self::loop0(...) {
    log::error!("... aborting master");
    std::process::abort();
}
```

应改成线程内部分类处理：

```text
flush_pending 成功
  -> batch.next()

NotLeader/LeaderNotReady/ConnectionRefused/Deadline
  -> 保留当前 batch
  -> 标记 JournalWriter=Retrying
  -> backoff 后重新 determine leader
  -> 不 abort

序列化失败/entry 构造错误
  -> 标记 JournalWriter=Failed
  -> 拒绝新的 metadata write
  -> 保持 Raft/metrics/诊断接口
  -> 进入 SafeMode 或等待人工处理
```

关键要求：

```text
flush 失败不能丢 batch
flush 失败不能推进 batch sequence
新的 write 在 writer unhealthy 时快速返回 LeaderNotReady/JournalUnavailable
不能无限阻塞 RPC executor
```

因此 JournalWriter 需要从“线程失败即结束”改成显式状态机：

```text
Healthy -> Retrying -> Healthy
Healthy -> Failed/SafeMode
```

`SenderTask` 的重试必须是可取消、带 backoff 的，不能在单独线程里无限 `block_on` 一个可能跨 leader 切换的 RPC。

### 16.8 EntryWithAck 的最小改造方向

`EntryWithAck` 不是本次 UFS 卡住的直接原因，但它把 proposal response 和单一 apply worker 绑定在一起。建议分离两种语义：

```text
RaftCommitAck：entry 已在 Raft 多数派提交
AppApplyAck：metadata 已由 AppStorage 处理
UfsReplayAck：UFS side effect 已完成
```

对普通 metadata proposal：

```text
RaftCommitAck 可以先返回
AppApply 失败通过 metrics/state/异步错误报告
```

对需要严格同步语义的 API：

```text
显式选择等待 AppApplyAck
```

不要让所有 leader proposal 默认都使用 `EntryWithAck`，否则 UFS replay 的单 worker 堵塞会延迟所有 proposal response。

### 16.9 Follower apply 失败也不能一律 abort

Follower 在滚动或磁盘抖动期间也可能遇到暂时性错误。建议：

```text
Follower metadata apply retryable error
  -> 保持 follower / SafeMode
  -> 重试本地 apply 或等待 snapshot
  -> 不参与 ready metadata 服务
  -> Raft heartbeat 继续
```

如果是本地 journal/meta 明确损坏：

```text
进入 SafeMode
通过 peer snapshot/recovery protocol 重建
重建成功后自动回到 FollowerMetadataReady
```

只有无法分类为可恢复且无法通过 snapshot 修复的错误，才需要停进程；也不建议直接 `abort`，应让 supervisor 做受控重启并保留诊断状态。

### 16.10 最小可行改造（不等 UFS 全面重构）

如果短期不能马上拆 UFS executor，至少先做以下四项：

1. 删除 `SenderTask::spawn` 对所有错误的 `process::abort()`；对 NotLeader/deadline/连接失败保留 batch 并重试。
2. `JournalLoader` 对 `No workers available`、UFS timeout、NotLeader、连接失败永不调用 `abort_on_fatal_apply_error`，进入 retry/degraded 状态。
3. worker 缺失时保持 Raft/heartbeat/metadata control plane，UFS Scan 使用独立 retry timer，不占用关键 RPC 线程。
4. 为 `RecoveryState`、`worker_num`、`journal_ufs_applied`、pending UFS 数量增加 metrics 和 `/recoveryz`。

这四项可以先消除本次故障的“必然 abort”路径，但不能完全消除单 apply worker 被 UFS replay 占用的问题；后者仍需要第 16.5 节的长期改造。

### 16.11 必须保留的 fail-closed 边界

“服务总是能够恢复”应理解为：

> 对 worker 滚动、网络抖动、leader 切换、UFS 暂时失败等可恢复故障，master 不退出并最终自动恢复。

不能承诺对以下情况盲目继续提供 metadata 服务：

```text
journal entry 损坏
RocksDB corruption
snapshot 内容校验失败
metadata invariant 破坏
本地数据和 Raft committed state 无法证明一致
```

这些情况应该：

```text
进入 SafeMode
停止 Ready metadata 服务
保留 Raft/metrics/诊断能力
尝试 peer snapshot/recovery
恢复成功后自动重新加入
```

这样既不因为短暂 worker 缺失 abort，也不会为了“永不退出”而服务错误 metadata。

### 16.12 验收标准

在故障注入下，以下场景都不允许出现 `aborting master`：

```text
全部 worker 暂时不可用 10 分钟
worker 滚动替换期间 leader 发生切换
当前 leader 被重启，两个 follower 之一成为新 leader
UFS export 连续失败
journal proposal 连续 NotLeader/deadline/ConnectionRefused
Follower metadata apply 遇到可重试 IO 错误
```

验证指标：

```text
Raft 8996 持续可用
leader 能正常切换
master 进程不退出
journal-writer batch 不丢失
worker 回来后 live_worker_num 恢复
UFS pending 数量最终下降
journal_ufs_applied 最终追平目标
recoveryz 从 503 最终恢复 200
无 process::abort
无“进程退出但容器 Running/Ready”
```

### 16.13 本阶段未实施项

本次只完成设计和文档更新，没有直接修改代码或线上资源。未实施项包括：

- `JournalLoader` 的 Retryable/Fatal 错误分类；
- `SenderTask` 的保留 batch、backoff 和重新选主；
- UFS replay 独立 executor；
- metadata/ufs watermark 拆分；
- `RecoveryState` 和 `/recoveryz`；
- worker heartbeat standby 预热或 peer registration；
- entrypoint 去除 `tail -f /dev/null`；
- 故障注入测试和线上灰度验证。
