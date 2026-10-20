# Master 自恢复最终结论与整改方案

> 本文只保留最终结论、整改方案、验收标准和边界，不重复故障时间线、完整日志上下文及排查过程。

## 1. 最终结论

### 1.1 可恢复故障不应触发进程退出

以下情况都属于运行时可恢复故障，master 不应 `process::abort()`：

- worker 暂时没有注册或 live worker 数量为 0；
- UFS export/mkdir/rename/delete 暂时失败；
- leader 切换、`NotLeader`、`LeaderNotReady`；
- Raft proposal 的连接失败、`Connection refused`、`deadline elapsed`；
- worker/master 同时滚动导致的短暂 heartbeat 中断；
- follower 的可重试 IO 错误。

当前代码存在两条错误升级路径：

1. `JournalLoader::abort_on_fatal_apply_error()` 会把部分 replay/apply error 直接升级为进程 abort；
2. `SenderTask::spawn()` 对 `block_on_send_propose()` 的任意错误直接记录 `aborting master` 并 abort。

这两条路径必须改为重试、降级或 SafeMode，不能依赖发布人员按固定顺序操作来规避。

### 1.2 UFS replay 不应拖死 Raft 和 metadata 控制面

当前 UFS replay、metadata apply、RoleChange 和部分 proposal apply 共用 JournalLoader 的串行 apply worker。

因此 UFS replay 长时间失败会间接造成：

- 后续 apply 任务无法及时处理；
- EntryWithAck 的 proposal response 延迟；
- JournalWriter proposal 超时；
- 最终触发 master abort。

`EntryWithAck` 不是 UFS replay 卡住的直接原因：生产路径中它只入队，不等待 apply ack；但它会让 proposal response 等待同一个 apply worker，因此存在间接队列耦合。

### 1.3 applied 必须拆分为不同语义

必须区分：

- `metadata_applied`：metadata 已进入本地 FSM；
- `ufs_applied`：对应 UFS side effect 已成功完成；
- `ufs_pending`：UFS side effect 尚未完成但可以补偿。

UFS 暂时失败时允许：

```text
metadata_applied 前进
ufs_applied 暂不前进
ufs_pending 增加
```

当前实现中，UFS replay 失败通常不会调用 `set_applied()`；不能把 Raft 的 `advance_apply()` 与 JournalLoader 的业务 watermark 视为同一个 applied 位置。

### 1.4 不能承诺对数据损坏继续提供服务

“服务总是能够恢复”适用于 worker 缺失、网络抖动、leader 切换、UFS 临时失败等可恢复故障。

以下情况必须 fail-closed：

- RocksDB/journal 明确损坏；
- journal entry 无法反序列化；
- snapshot 校验失败且无法从 peer 恢复；
- metadata invariant 被破坏；
- 本地状态与 Raft committed state 无法证明一致。

这类情况进入 `SafeMode`，停止 Ready metadata 服务，但保留 Raft、metrics、healthz 和恢复接口，等待自动 snapshot recovery 或人工处理；不建议无条件直接 abort。

## 2. 整改方案

### P0：消除所有可恢复故障的 abort

#### 2.1 JournalLoader 错误分类

新增可恢复错误分类：

```rust
enum ApplyDisposition {
    Retry,
    SkipUfsAndAdvance,
    EnterSafeMode,
}
```

以下错误必须归入 `Retry`：

```text
No workers available
No eligible workers
UFS timeout/connection error
NotLeader
LeaderNotReady
Connection refused
deadline elapsed
leader term changed
worker heartbeat 暂时中断
```

只有 metadata corruption、journal decode error、snapshot/invariant error 才进入 `EnterSafeMode`。

#### 2.2 JournalWriter 保留 batch 重试

将当前：

```text
flush error -> loop0 返回 Err -> abort master
```

改为：

```text
flush 成功 -> batch.next()
临时 Raft/RPC 错误 -> 保留当前 batch -> backoff -> 重新找 leader -> 重试
不可恢复序列化/数据错误 -> JournalWriter=Failed/SafeMode
```

要求：

- flush 失败不能丢 batch；
- 失败不能推进 batch sequence；
- 不得阻塞 Raft heartbeat；
- JournalWriter 不健康时新的 metadata write 快速返回 `JournalUnavailable`；
- 不能因为 `NotLeader`、超时或连接失败 abort master。

#### 2.3 增加 RecoveryState 和 recoveryz

建议状态机：

```text
Starting
 -> FollowerMetadataReady
 -> LeaderMetadataReady
 -> WaitingForWorkers
 -> UfsReplaying
 -> DegradedUfsPending
 -> Serving
```

异常处理：

```text
worker 缺失       -> WaitingForWorkers
UFS 暂时失败     -> DegradedUfsPending
leader 变化       -> 取消旧 replay，按新 term 重新调度
metadata 损坏     -> SafeMode
```

增加接口：

```text
/healthz       进程/Raft role 存活
/readyz        leader metadata 服务可用
/recoveryz     metadata 和 UFS replay 达到服务要求
```

### P1：让 worker 缺失不会阻塞 master 恢复

leader promotion 后的流程改为：

```text
metadata catch-up
  -> 发布 leader role
  -> 允许 WorkerHeartbeat
  -> worker_map 为空时进入 WaitingForWorkers
  -> worker 注册后继续 UFS replay
```

worker 为 0 时：

- Raft heartbeat 继续；
- WorkerHeartbeat 继续；
- metadata control plane 继续；
- `/healthz` 保持可用；
- `/recoveryz` 返回 503；
- UFS replay 等待 worker 注册，不退出进程。

建议增加：

```text
WorkerManager::live_worker_count()
WorkerManager::worker_registered_notify()
```

### P1：拆分 UFS replay 与关键 apply worker

长期结构：

```text
Raft/metadata apply worker
  -> metadata entry
  -> RoleChange
  -> metadata watermark

UFS replay executor
  -> mkdir/export/rename/delete
  -> 独立 queue
  -> 独立 retry/backoff/timeout
  -> pending/failed 状态
```

UFS replay 失败时：

```text
不阻塞 Raft Ready
不阻塞 WorkerHeartbeat
不阻塞 metadata apply
不 abort master
```

每个 UFS task 需要携带：

```text
term
leader generation
journal index
op_id
retry_count
next_retry_at
```

leader 变化时取消旧 generation 的 task，新 leader 从持久化的 `ufs_applied` 重新调度。

### P1：拆分 metadata 与 UFS watermark

建议替换当前 leader 侧绑定推进逻辑：

```rust
set_metadata_applied(index)
set_ufs_applied(index)
record_ufs_pending(index, error)
```

UFS 失败不回退或阻塞 metadata FSM；UFS 成功后单独推进 `ufs_applied`。

### P1：重新定义 EntryWithAck

将当前混合语义拆为：

```text
RaftCommitAck
AppApplyAck
UfsReplayAck
```

普通 metadata proposal 可以在 Raft commit 后返回，后台 apply 失败通过 recovery state、metrics 和补偿机制上报。

只有确实要求“应用完成后返回”的 API，才等待 `AppApplyAck`。

## 3. 验收标准

在以下故障注入场景下，不允许出现 `aborting master`：

```text
全部 worker 暂时不可用 10 分钟
worker 滚动替换期间 leader 切换
当前 leader 被重启
UFS export 连续失败
journal proposal 连续 NotLeader/deadline/ConnectionRefused
follower 遇到可重试 IO 错误
```

必须满足：

```text
Raft 8996 持续可用
leader 可以正常切换
master 进程不退出
journal-writer batch 不丢失
worker 恢复后 live_worker_num 恢复
ufs_pending 最终下降
journal_ufs_applied 最终追平目标
recoveryz 从 503 最终恢复 200
无 process::abort
无“进程退出但容器 Running/Ready”
```

## 4. 实施优先级

### 第一阶段：阻断 abort

1. 修改 `SenderTask`，临时 proposal/RPC 错误保留 batch 并重试；
2. 修改 `JournalLoader`，worker/UFS transient error 进入 Retry/Degraded；
3. 增加 RecoveryState、recoveryz 和关键 metrics；
4. 移除 entrypoint 中“server 退出但 tail 保持容器 Running”的行为。

### 第二阶段：恢复链路解耦

1. leader promotion 增加 WaitingForWorkers；
2. UFS replay 使用独立 executor；
3. 拆分 metadata/ufs watermark；
4. 增加 UFS pending/failed 补偿。

### 第三阶段：语义收敛

1. 拆分 RaftCommitAck、AppApplyAck、UfsReplayAck；
2. 优化 worker 在 leader 切换期间的注册；
3. 增加 SafeMode 到 peer snapshot recovery 的自动闭环。

## 5. 当前状态

本方案为代码级整改设计，当前只创建本文档，未修改代码、部署 YAML 或线上资源。
