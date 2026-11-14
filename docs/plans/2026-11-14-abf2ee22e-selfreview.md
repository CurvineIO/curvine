# abf2ee22e 七项自审（五次复审 R1-R4 修正后的新候选）

## ① 有效性：五次复审 R1-R4 全落实？
R1/R2 by-ID+snapshot 实际 Busy 观测：by-ID hook take 独立语句（Mutex 释放后 signal——裁定第"hook take 独立语句"条）✓；snapshot hook take 移入 Busy 分支（view 成功不吞观察点）✓；by-ID 真实 cold 场景（owner ready 先于 worker spawn——**裁定"先有界等 begin_cold 成功再 spawn worker"**）✓；snapshot 直接调用（Busy→owner join 前置确认 reopen→重试成功——**"释放后成功"确定化**）✓；FAILED 直接入口 <1s Unavailable ✓。
R2 清理：CaseEnv 保存真实 conf.meta_dir ✓；cleanup drop 后 remove_dir_all 证明可清理 ✓；**doc 明确"不证明全部 Arc 销毁"**（裁定措辞要求）✓；owner recv expect 不吞 ✓；worker 有界 completion ✓；ArcStrongCount/owner_ready_marker 删除 ✓。
R3 typed 断言：FAILED=matches!(Common 前缀 JournalUnavailable:)(真实构造形式——执衡撤回假 enum 后的口径) ✓；deadline/business=matches!(InProgress)+message 校验 ✓；场景⑥受控闭包 Business(FsError::in_progress_msg) 原样透传、恰一次调用、无 Busy 事件 ✓。
R4 helper deadline 延后（get_or_insert_with+checked_add）+ sleep min(5ms, remaining) ✓；原始记录绝对路径清单已交 ✓。
结论：R1-R4 全落实 ✓

## ② 正收益
- 收益：五项审查意见全部闭合；测试事实与源码一致（pending 正向文档化——不再虚构 FileNotFound）
- 结论：✓

## ③ 性能
- 测试 0.35s 双模式；生产零变化
- 结论：✓

## ④ 优雅
- scenario3 fail 经 owner 自身 guard（无第二 begin_cold Busy 竞态）；join_owner_bounded 统一有界收尾
- 结论：✓

## ⑤ 语义
- 生产语义零变化（只动测试+hook take 结构）✓
- pending 正向断言=文档化控制流 ✓ 按裁定"不借当前修复改 pending 生产契约"
- FAILED typed=Common 前缀（真实构造）✓
- 结论：✓

## ⑥ 自洽
- 反例1：场景2 时序？——worker 先（OPEN 拿锁到 watch）→ owner begin_cold（worker 无 view，drain 即成）→ release → Busy → busy 事件（drop 后）→ probe → owner release → worker 成功 ✓ 实证
- 反例2：场景3 fail 顺序？——worker Busy 到达（busy watch）→ owner 自身 guard fail_closed → done 确认 → worker <5s Unavailable ✓ 实证
- 反例3：deadline 竞速？——owner 持窗 60s >> worker 30s deadline → InProgress 确定返回 ✓ 实证
- 结论：闭合 ✓

## ⑦ 合规
- 范围：只动测试+hook 结构（无新依赖/公开 API）✓
- 断言：glob/get_lock 原断言零改动 ✓
- 门禁：check 0、fmt 0、default 9/0 fault 10/0、restore/bulk/glob/get_lock 绿（.status 落盘）✓
- 审查对应：五次复审 R1-R4 逐条 ✓
- 结论：合规 ✓

## 结论
七项全过。新候选 abf2ee22e + 本自审分提交。
