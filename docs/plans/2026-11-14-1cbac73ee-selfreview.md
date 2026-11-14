# 1cbac73ee 七项自审（自我对抗式——假定 Busy 重试有错，逐项找反例）

## ① 有效性：真的解决了 glob 并发碰撞？
反例1：worker file_status 撞 cold 窗口现在会等待而非传播？——attempt 返回 HolderBusy（guard/lock 已 drop）→ 外层 5ms 间隔重试至 DEFAULT_COLD_TIMEOUT 截止。glob 测试的 cold 窗口（list_status glob 解析）远短于 5s 截止——**碰撞被吸收**。双模式 2/2 实证（含 worker 100 轮并发）。**攻不破**。
反例2：deadline 到期仍窗口未开？——返回原 InProgress 语义（不伪造成功）✓ 设计裁定第 3 条"到期返回明确原 Busy/超时而非伪成功"。
结论：有效 ✓

## ② 正收益
- 收益：消除并发读×cold 窗口的即时失败（客户端可见性从"碰撞即 InProgress"变为"短暂等待后成功"）；与 exists/list_status glob 的既有语义对齐
- 代价：仅 Busy 分支新增 sleep 轮询（5ms 间隔，正常无碰撞路径零开销——Ok 直接返回）；attempt 结构化带来的代码重组
- 结论：正收益 ✓

## ③ 性能
- 无竞争路径：单次 attempt 成功 → 零额外开销（view 同原逻辑）✓
- 碰撞路径：5ms 轮询 + 重建计划——cold 窗口本身排空读者（秒级），轮询开销可忽略
- 锁前 view 失败：不持任何锁等待 ✓；锁后 view 失败：drop(_locks) 后才返回 HolderBusy ✓（不得持 I 等 cold——裁定第 3 条满足）
- 结论：✓

## ④ 优雅
- 反例：更短实现？——FileStatusReadError 局部枚举 + From<FsError> 使 attempt 内业务 `?` 原样保留（执衡"更小写法"提醒的落实：无逐 match 包装）；view_typed 双别名已按执衡提醒删除（view 本就 typed）
- 结论：收敛后最小 ✓

## ⑤ 语义
- FAILED 立即 Unavailable 终态（不反复等待）✓ 裁定第 3 条
- 业务 fault（file_status.locked_build fault_point）→ Business 不被 Busy 吞 ✓ 裁定第 3 条
- restore lease（checked_live_epoch Err）→ snapshot_attempt 路径不变 ✓
- pending/reserved/atime/identity 语义不变 ✓
- 原言在：timeout 到期返回 InProgress（原语义），不承诺 FAILED 可恢复 ✓
- 结论：语义保持 ✓

## ⑥ 自洽
- 反例1：HolderBusy 时是否可能持锁？——attempt 内三处 view 的 Busy 分支：第一处无锁；第二处锁后（drop(_locks) 随 scope 结束自动发生——**验证**：`return Err(HolderBusy)` 从 match 臂返回，_locks 在该 scope 内 drop ✓）；snapshot attempt 无对象锁。**攻不破**。
- 反例2：重试期间 epoch/计划漂移？——每次重建计划+epoch 校验（原逻辑）✓
- 反例3：by-ID 分支漏改？——已加同款有界重试（裁定第 5 条"by-ID 路径也保持同一边界"）✓
- 反例4：glob 首红会不会是其它 view？——无论哪一处 view 撞 Busy 都进 HolderBusy 有界等待 ✓ 全覆盖
- 结论：闭合 ✓

## ⑦ 合规
- KISS：局部枚举+From+外层循环；无 trait/公开 API/兼容别名 ✓
- 不加全局 RMW、不采纳 cold owner 方案（裁定明令）✓
- 测试：复用既有 glob 断言（不改）+ 既有兄弟测试 ✓；事件驱动确定性测试未新增——**裁定第"确定性验证"段要求事件驱动测试**（真实已入 cold→释放后正确 status 等）——**如实登记：本候选未含该新测试，需补或由执衡裁定用现有钩子组合**（memory_read_gates/fault_point 已有，但"cold owner 释放事件"钩子需最小 cfg(test) 接线）
- 门禁：check 0、fmt 0、双模式全绿（.status 落盘）✓
- 设计对应：唯一设计 /tmp/cv-glob-file-status-cold-decision.md 逐条对号 ✓
- 结论：合规，余一项事件驱动测试待补/待裁定 ✓

## 结论
六项半全过；事件驱动新测试为唯一剩余（按裁定要求事件驱动，现有钩子需最小接线——交执衡裁定补法）。
