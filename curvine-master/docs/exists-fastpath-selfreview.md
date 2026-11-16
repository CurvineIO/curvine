# exists 读计划复用：集成自审

集成点 `5d1e345c7`（生产）/ `dde06273a`（测试），主线 `0115c7272`。
本文只记录**已核实的源码事实与未闭合缺口**，不宣称整体性能结论。

## 1. token 捕获在读边之前
`read_plan.rs::plan_memory_read_walk` 中每次查询下一个 child edge 之前先回调
（`if let Some(node) = node { on_parent(&node.children); }` 在
`node.children.get_child_edge(...)` 之前），因此捕获的版本对应"读到该 child map 那一刻之前"的状态。
末段查询失败（负向结论）的父目录同样被捕获 —— 负向结论也受版本区间保护。

## 2. 版本失效回退
`exists_memory` 快路径条件为 `!plan.root_target && plan_versions.iter().all(|v| v.is_current())`。
任一 token 失效 → 不进入快路径 → 执行原完整第二遍 `plan_memory_read_with_components` 并保留
`plan.matches(&verified)` 比较与 `verified.target_edge` 的 inode 行检查。
快路径复用 `plan.full` / `plan.target_edge` 与第二遍 `Ok(verified.full)` / `verified.target_edge`
等价，前提正是"两次 walk 之间 child map 未变"。

## 3. epoch / restore 与锁集
- 两道 epoch：首遍 `checked_live_epoch`（失败 → `exists_snapshot`）、锁后 `checked_live_epoch`
  与 `locked_epoch != plan_epoch`。
- **epoch 变化时是 `continue` 重试循环**，不是"总是再走一次完整 walk"；重试那一轮可能直接命中快路径。
  因此真实整树 restore 用例断言 `fallbacks` 增量为 **0**、`hits` 增量为 1。
- **store 不可用是错误**（`ensure_available()?`），由外层处理，不是"退回第二遍"。
- 锁集不变：`plan.lock_requests()` 与原子在原实现一致。root 作为**祖先**时其 child map 不在
  `lock_requests()` 内，靠 token 版本区间 + epoch 保护；该保护与第二遍 walk 等价，
  但**窗口边界未经对照测试证明**。

## 4. `get_inode` 错误语义
快路径与回退路径使用同一检查与同一文案：
`if let Some(ChildEdge::FileId(id)) = …target_edge { if store.get_inode(id, None)?.is_none() { return err_box!("Failed to load inode {} from store", id); } }`。
即"活边 + 缺行"是 dangling 错误，不是 absent。

## 5. ABA / linearization
版本取自 `DirectoryChildren` 的 `SequenceState`（低位 writer 计数、高位 generation），
mutation 期间非零、完成后 generation 递增，因此"读到的版本仍 current"意味着该 child map
自捕获以来未完成任何 mutation。
**ABA 依赖**：同一父目录的 `Arc<DirectoryChildren>` 不被替换；整树 restore 会替换整棵树，
此时旧 Arc 上的版本可能仍自洽，因此 restore 场景不依赖 token，而由 epoch 检查拦下（`continue` 重试）。
未证明的情形：若存在"仅替换某目录 children Arc 而不推进 epoch"的路径，token 会失效但仍报 current。
本次核对的源码中未发现该路径，但这属于**代码审读结论，非测试证明**。

## 6. 性能实验的边界
此前的 exists 6 格对照是**旧分片锁候选 `14865b09b` vs 原型**，
`P/O > 1` 只说明较该候选改善；**不是**与全局锁 `bff4e7dc5` 的对照，
也不能由两次独立测试的比值推出全局锁指标。
`BENCH_OPS=100000` 是诊断口径，不等于正式 1000；重复观测只给出该样本范围，不构成概率结论。

## 7. benchmark 源码绑定限制
- 对照所用 O 侧 binary（`300bb456…`）的构建记录/`reflog`/源码哈希可作旁证，
  并与我交付的 `e50cd65a…` 同一 git tree、`diff` 为空，但**构建时的树快照未保存** → 只能称"可高度确信"。
- `BENCH_OPS` 噪声的三次观测（27.5% / 1.7%）只是那三次的样本范围。
- 该批 6 格数据**不得**当作正式 A/B 验收。

## 8. 保留的真实红（未删未改）
| 记录 | 事实 |
|---|---|
| `vfp10-faultlib.*` | fault 全量**并行** 803 passed / 1 failed；该 failed 为全局计数被并行其他测试污染 |
| `vfp11-fault.*` | status=101，**ENOSPC**（非断言失败） |
| `vfp11-faultlib.*` | fault 全量并行 802 passed / 2 failed；两例为 `complete_file_sharding_test` 的 fault 规则串扰 |
| `vfp12-faultlib-serial.*` | 串行 804 passed 可见于 stdout，但**缺运行时 argv/status**，不称完整原始证据 |

串行通过**不能**覆盖并行模式；未修改 fault 源码也**不能**严格排除本次改动的影响，
因此并行红按原样保留，不归因、不关闭。

## 9. 证据
| 项 | 值 |
|---|---|
| 集成源码 | 主线 `0115c7272` + `5d1e345c7` + `dde06273a` |
| default 定向 | `cargo test -p curvine-master --lib --jobs 4 -- exists_version_fastpath_test --test-threads=1` → 9 passed / 0 failed / status=0 |
| fault 定向 | 同上 + `--features fault-injection` → 9 passed / 0 failed / status=0 |
| 日志 | `/tmp/cv-main-int/{default,fault}.{stdout.log,stderr.log,status}` + `record.txt` |
