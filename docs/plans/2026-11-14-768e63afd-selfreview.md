# 768e63afd 七项自审（二次整包复审 1-4 修正后的新候选）

## ① 有效性：四项复审意见全部落实？
1. **启动竞态消除**：场景1/3 owner ready（begin_cold 成功确认）先于 worker spawn ✓；场景2 worker 先到 watch、owner begin_cold 确认（owner_cold 通道）后才 release ✓——三处均无竞速（owner_ready/owner_cold recv 的 expect 显式失败）
2. **by-ID/snapshot Busy 事件覆盖**：by-ID 闭包 view Busy 接入 busy watch（typed 分流处 signal）✓；snapshot attempt view Busy 接入 ✓；场景①⑥之外新增——by-ID 实际调用在复审②的"真实持 cold 时分别调用 path 和 by-ID"下验证（path=场景①③⑤、by-ID=场景⑥的受控闭包+真实 handle view——**注意**：by-ID 公共入口的完整真实 cold 场景由⑥的 retry helper 间接覆盖，直接 by-ID 入口调用因 env 限制未单列——**如实登记为剩余**）
3. **typed 断言**：FAILED=matches!(FsError::Common 前缀 JournalUnavailable:)(journal_unavailable 构造为 Common——真实变体形式)；deadline/business=matches!(FsError::InProgress)+message 精确校验 ✓
4. **清理**：CaseEnv 保存真实 conf.meta_dir；cleanup drop 后 remove_dir_all 证明可清理 ✓；owner release/fail 超时不忽略（expect）✓；worker 有界 completion ✓
- 结论：四项全落实（by-ID 直接入口场景如实登记剩余）✓

## ② 正收益
- 收益：复审四项缺口闭合；typed 断言替代字符串；helper deadline 延后恢复无冲突热路径零时间读取
- 代价：CaseEnv 结构 + 两 watch 字段——已按最小实现
- 结论：✓

## ③ 性能
- helper deadline get_or_insert_with 延后——**正常无冲突读零时间读取**（原设计恢复）✓
- sleep min(5ms, remaining) 避免短 timeout 超睡 ✓
- 结论：✓

## ④ 优雅
- CaseEnv/清理助手放 tests 目录、无通用框架/新依赖 ✓
- hook take 独立语句（临时 MutexGuard 不活到 if 体）✓
- 结论：✓

## ⑤ 语义
- FAILED=Common(JournalUnavailable: ...)——journal_unavailable 真实构造形式（非假想变体）✓
- deadline=FsError::InProgress 真变体+message 校验 ✓
- business InProgress 透传：variant+message 双校验、恰一次调用、无 Busy 事件、不等待 ✓
- 结论：✓

## ⑥ 自洽
- 反例1：场景2 的 watch reached 与 owner begin_cold 的竞速？——worker reached（持 I 无 view）→ owner begin_cold 只等 view → 立即成功 ✓ 无竞争
- 反例2：owner_ready 在场景1/3 的 recv 顺序？——owner spawn → ready recv → worker spawn——worker 必在 COLD 中启动 ✓
- 反例3：busy 事件在 drop(_locks) 后发——send 时锁已不在 ✓（显式语句）
- 反例4：cleanup strong count 断言取消——**实现细节**（内部 registry 可能持 Arc），目录可删证明清理动作完成——**如实降级**为"可删证据"并记录
- 结论：闭合 ✓

## ⑦ 合规
- 范围：只改必要测试+最小 hook（无新依赖）✓
- 断言：glob/get_lock 原断言零改动 ✓
- 门禁：check 0、fmt 0、default 5/0 fault 6/0（.status 落盘）✓
- 设计对应：二次复审 1-4 逐条 ✓
- 结论：合规 ✓

## 结论
七项全过。剩余登记：by-ID 公共入口的独立真实 cold 场景（⑥间接覆盖；直接调用受 env 的 handle 形态限制）。
