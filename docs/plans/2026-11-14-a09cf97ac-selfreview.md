# a09cf97ac 七项自审（七次复审修正后的新候选——含证据绑定更正）

## ① 有效性：七次复审两项全落实？
1. **内层 receipt 双层检查**：FAILED（:298 区域）与 snapshot（:594 区域）的 done_rx 直调改 join_owner_bounded——该 helper 先 recv_timeout expect（外层）+ receipt.expect（内层 Err 显式失败）再 join。snapshot 先确认 owner 成功完成再重试。**攻不破**。
2. **owner drop(fs2) before send**：五个 owner 线程（spawn_confirmed_owner/场景2/3/5/snapshot）在 send(receipt) 前显式 drop(fs2)——guard → clone → receipt 顺序与真实资源状态一致。**攻不破**。
结论：两项全落实 ✓

## ② 正收益
- 收益：内层超时不再被伪装正常；owner 生命周期顺序真实
- 结论：✓

## ③ 性能
- 零生产变化
- 结论：✓

## ④ 优雅
- 复用 join_owner_bounded（无新框架）
- 结论：✓

## ⑤ 语义
- **生命周期表述更正（按执衡）**：guard 先 drop，fs2 clone 随后显式 drop，**然后**才 send(receipt)——不再声称"发信时线程已析构"；join 只做现有线程收尾，不称 OS 级严格有界退出证明
- 结论：表述更正 ✓

## ⑥ 自洽
- 反例1：drop(fs2) 后 sender 还能用？——done_tx 不属于 fs2，独立变量 ✓
- 反例2：join_owner_bounded 在 snapshot 的时序？——先完成再重试 ✓
- 结论：闭合 ✓

## ⑦ 合规
- 范围：只动测试 ✓；门禁 check 0（.status 落盘）✓
- 结论：合规 ✓

## 结论
七项全过。新候选 a09cf97ac + 本自审分提交。

## 运行证据绑定（本源 post-commit，满足"运行必须在 source 提交后不变"）
| 项 | 路径 | sha 绑定文件 | status | 结果 |
|---|---|---|---|---|
| default | /tmp/cv-fs-verify-a09-default.log（argv: .argv） | .sha = a09cf97ac | 0 | 9/0（0.33s） |
| fault | /tmp/cv-fs-verify-a09-fault.log | .sha = a09cf97ac | 0 | 10/0（0.40s） |
| fmt | /tmp/cv-fs-verify-a09-fmt.log | .sha = a09cf97ac | 0 | — |
| check | /tmp/cv-fs-verify-a09-check.log | .sha = a09cf97ac | 0 | — |

**历史记录（不冒充本候选）**：
- /tmp/cv-fs-events-v15-{d,f}.log：mtime 早于 a910 commit——**如实标注未绑定**（执衡指出，不篡改）
- /tmp/cv-fs-events-v16-{d,f}.log：本轮 commit **前**的工作树（与 a09cf97ac 内容一致，但仅 commit 前记录）——与 post-commit 的 verify 记录并存
- /tmp/cv-fs-events-v14-*、-final-*、-check48b：各历史轮记录，各自绑定当时源
