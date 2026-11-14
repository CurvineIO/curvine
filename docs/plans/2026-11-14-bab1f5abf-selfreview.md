# bab1f5abf 七项自审（自我对抗式——假定修复有错，逐项找反例）

## ① 有效性：真的恢复了旧语义？
反例1：叶子 dangling（/dir/file 行删边留）现在报什么？——target_edge=FileId → get_inode 缺失 → "Failed to load inode {id} from store"。测试 :172-180 期望 contains("Failed to load inode") ✓ 双模式 2/2 实证。**攻不破**。
反例2：中途 dangling（/dir/file/child）？——walk 停在 FileId（trailing=id）→ !full 前先查行 → 缺失报 "Failed to load inode"；行在（真实文件无子项）→ FileNotFound（与旧 resolve 一致：child 无 inode 可解析）。**攻不破**。
反例3：会不会误伤正常路径？——无中途 FileId 时 trailing=None，行为与旧代码完全一致（仅字段新增）。glob/exists/list 兄弟测试全绿实证。
结论：有效 ✓

## ② 正收益
- 收益：修复生产缺陷（孤儿锁行误报——dangling 文件的锁查询返回残留锁）；恢复旧 resolve 一致语义
- 代价：read_plan 加一字段+中途 FileId break（等价于旧 walk 停走语义，无新遍历）；get_lock 锁后窗口对 FileId 目标多一次 get_inode（叶子：原来也要 get_locks（一次读），现在 get_inode+get_locks 两次——**如实记录**：叶子 FileId 查询多一次行读；中途场景原来 !full 直接 FileNotFound，现在一次行读）。锁后窗口冷路径可接受；最终性能以 A/B 为准
- 结论：正收益 ✓（代价如实）

## ③ 性能
- 锁前：零新增（trailing 在既有 walk 内记录，无锁前 DB 读）✓ 执衡边界"不在锁前纯 planning 读 DB"满足
- 锁后：叶子 FileId +1 行读；中途 FileId +1 行读（原来无行读但直接错）；目录/不存在路径零新增
- 重复查询：trailing 与 target 互斥（trailing 只在 walk 未到底时非 None），同一次调用不会两次查同一 id ✓
- 结论：可控 ✓

## ④ 优雅
- 反例：有没有更短实现？——替代 A：get_lock 内完整 InodePath::resolve（执衡明令禁止：全路径二次拓扑 DFS）；替代 B：MemoryReadPlan 存全部 edge 类型（更大改动）。当前=最小（1 字段+1 break+2 检查块）。
- 与相邻代码同模式：行缺失错误消息与 resolve_inner:161-165 逐字一致（"Failed to load inode {id} from store"）✓
- 结论：✓

## ⑤ 语义
- 保留不变：root lock 请求（root_target）/pending 隐藏（check_not_pending_path）/checked_live_epoch/snapshot fallback/硬链接别名（edge name authoritative）/负路径（FileNotFound）
- mid-path 行在 → FileNotFound：与旧 resolve 一致（resolve_inner FileId hydrate 后 child 无法解析）✓
- !full 且 trailing=None（普通缺边）→ FileNotFound ✓
- 失败扩散：get_inode 错误传播（真实 store 错误不吞）✓
- 结论：语义保持 ✓

## ⑥ 自洽
- 反例1：trailing 与 target 互斥？——trailing 只在 `index+1 < len` 的 FileId 边设置并 break；target 只在最后分量设置。互斥 ✓
- 反例2：epoch/身份重验后 trailing 一致性？——trailing 来自 verified（锁后重验的同一 plan_memory_read），与 plan.matches(verified) 一起保证 ✓
- 反例3：get_inode 锁下行读与 get_locks 同窗口？——同一 verified view/fs_dir（I 锁持有中）✓ 与 get_locks 相同窗口
- 异常：get_inode store 错误传播（不吞）✓
- 结论：闭合 ✓

## ⑦ 合规
- KISS/YAGNI：1 字段+1 break+2 检查 ✓；不引入框架/缓存/重试 ✓
- 范围：只动 get_lock + read_plan（不做 file_status/exists/list 的未授权扩展——它们已有各自 inode 检查或语义不变）✓
- 断言：零改动 ✓
- 测试目录：无新增测试（既有 get_lock_path_test 覆盖）✓
- 门禁：check 0、fmt 0、双模式 2/2、兄弟测试绿（.status 落盘）✓
- 设计对应：执衡诊断文档边界逐条满足（同一窗口/无锁前读/无二次 DFS/错误契约/兄弟兼容）✓
- 结论：合规 ✓

## 结论
七项全过。glob 首跑 1 例 cold window 时序 flake（InProgress cold window panic）复跑绿——与本改动无关（改动不触碰 cold window 时序），如实记录。
