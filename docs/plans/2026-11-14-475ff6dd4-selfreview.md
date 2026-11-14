# 475ff6dd4 七项自审（四次复审 R1-R4 修正后的新候选）

## ① 有效性：R1-R4 全落实？
R1 pending/by-ID 按真实分支与数据纠正：Some(1000) 实测**成功**（live 树 pending 目录真实存在——format 创建）→ 正向文档化分支顺序（不再虚构 FileNotFound）；None 单独核 entry 拒绝；missing 核真实 FileNotFound variant+ctx（matches!(FileNotFound) + 消息双校验）✓
R2 snapshot 直接调用：真实 cold owner 下 typed HolderBusy+busy 事件 ✓；FAILED 直接入口 <1s Unavailable ✓；owner join 前置（reopen 完成后再 attempt——修复竞态）✓；FileStatusReadError 补 Debug ✓
R3 各 case 主线程 fs 显式 drop(fs) 后 cleanup ✓；ArcStrongCount/owner_ready_marker 删除 ✓；cleanup 断言降级为目录可删证明（count 为实现细节——如实降级）✓
R4 原始记录绝对路径清单已报（/tmp/cv-fs-events-final-*、/tmp/cv-fs-check48b、/tmp/cv-fs-restore-d 等）✓
结论：R1-R4 全落实 ✓

## ② 正收益
- 收益：pending/by-ID 测试与真实分支/数据一致（三段语义各自实证）；snapshot Busy/FAILED 直接验证；R3 清理证据诚实化
- 代价：pending 测试三段拆分（更长但每段语义单一）
- 结论：✓

## ③ 性能
- 测试耗时 0.32s/0.47s（default/fault）✓；生产无变化
- 结论：✓

## ④ 优雅
- 三段拆分注释各自说明语义边界（分支顺序/entry 契约/payload）✓
- try_write probe 复用（上轮已做）✓
- 结论：✓

## ⑤ 语义
- 生产语义零变化（本 commit 只动测试）✓
- pending 正向断言=文档化控制流（不宣称 entry 拒绝）✓ 按裁定"不借当前修复改 pending 生产契约"
- 结论：✓

## ⑥ 自洽
- 反例1：Some(1000) 解析 pending 目录为何成功？——live 树中 /.Trash/.pending 真实存在（format 创建 trash 结构）✓ 实证 status is_dir=true nlink=3
- 反例2：missing ctx 挂哪？——file_not_found(path) 构造后 err_ext 的 ctx 机制挂同一 ErrorImpl ✓ 实证消息含 inode_id
- 反例3：snapshot attempt 重试竞态？——owner.join() 前置使 reopen 确定完成 ✓ 实证
- 结论：闭合 ✓

## ⑦ 合规
- 范围：只动测试+read_plan/FileStatusReadError Debug（R2 需要）✓
- 断言：glob/get_lock 原断言零改动 ✓
- 门禁：check 0、fmt 0、default 9/0 fault 10/0、pending-sem 1/1、restore/bulk/glob/get_lock 绿（.status 落盘）✓
- 审查对应：四次复审 R1-R4 逐条 ✓
- 结论：合规 ✓

## 结论
七项全过。新候选 475ff6dd4 + 本自审分提交。
