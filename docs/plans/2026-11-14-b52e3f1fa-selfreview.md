# b52e3f1fa 七项自审（事件驱动确定性测试补充——对 1cbac73ee 自审的"唯一剩余"闭合）

## ① 有效性：事件驱动测试真的证明了裁定要求的契约？
反例1：场景①证明"第一 view 撞窗口等待"——真实 begin_cold guard（owner 线程）+ worker 200ms 未完成断言 + 释放后正确 status ✓
反例2：场景②证明"锁后第二 view 撞 Busy 且 I 先 drop"——事件链：watch reached（lock_many 后）→ release → busy-after-locks 事件（HolderBusy 返回前）→ 同 stripe try_read_inode 探针成功（None 断言反证锁不在 reader 手中——Some=可获取=未持有）✓ **裁定第 3 条"Busy 事件在 I 释放后发出"逐字实现**。
反例3：场景③ FAILED 终态——owner guard.fail_closed()（真实终态路径）→ worker <5s 返回 Unavailable（不转 30s deadline）✓。
反例4：场景④业务错误——locked_build return_error("business in progress") 透传不被 Busy 吞 ✓；进程级 rule 用后 remove ✓。
反例5：场景⑤ deadline——owner 持窗 40s>30s worker 预算 → 原 InProgress 语义返回、elapsed≥1s 断言真实等待 ✓。
结论：五场景全覆盖裁定要求 ✓

## ② 正收益
- 收益：1cbac73ee 的"唯一剩余"闭合；五场景事件驱动（无 sleep 充到达——唯一的 200ms 是"确认未完成"的负断言窗口，非到达证明）
- 代价：两个 cfg(test) 实例观察点字段（take-once Mutex，非进程级——并发测试不互扰）+ 185 行测试
- 结论：正收益 ✓

## ③ 性能
- 生产路径：观察点在 cfg(test) 下才编译，生产零影响 ✓
- 测试耗时：场景⑤ 30s（deadline 实测的必要成本）——单例可接受
- 结论：✓

## ④ 优雅
- 复用裁定指定的 signal+release 双通道模式（与既有 wait_unwind_hook 同型）；无第二套框架 ✓
- view_typed 已按执衡提醒删除（view 本就 typed）✓
- 结论：✓

## ⑤ 语义
- 断言零改动（glob 原断言/原有测试保持）✓
- 观察点只在 cfg(test)：生产锁/view/epoch/公开 API 零变化 ✓
- busy-after-locks 事件**在 I drop 后发出**（return 前作用域）——语义与实现一致 ✓
- 结论：✓

## ⑥ 自洽
- 反例1：watch reached 后 worker 会不会先于 owner 成功？——owner 已 COLD，worker 每 attempt view Busy → HolderBusy → 外层等待 → 不会成功 ✓
- 反例2：场景② probe 的 Some 断言方向？——try_read_inode Some=获取成功=stripe 空闲=reader 未持有 ✓（曾写反，已修正方向并复核语义）
- 反例3：场景④ rule 泄漏影响后续测试？——进程级 runtime remove ✓（曾漏，fault 组首跑 scenario1 撞残留红实证后补）
- 异常路径：owner panic → ready 未发 → driver recv timeout 显式失败 ✓
- 结论：闭合 ✓

## ⑦ 合规
- 范围：测试体只写 master_filesystem/tests/ ✓；观察点=主接最小 cfg(test)（裁定允许）✓
- 断言：glob 既有断言零改动 ✓
- 无进程级全局点污染（实例字段）✓
- 门禁：check 0、fmt 0、default 4/4、fault 5/5（.status 落盘）✓
- 结论：合规 ✓

## 结论
七项全过。1cbac73ee 的唯一剩余闭合——事件驱动验证齐备。
