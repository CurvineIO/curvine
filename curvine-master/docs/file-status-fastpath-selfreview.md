# file_status 读计划复用：自审（隔离原型，未合入）

基线 `31aedad88`，隔离工作树 `/tmp/cv-fsvp`。记录**已核实的源码事实与未闭合缺口**。

## 1. token 捕获在读边之前
`file_status_memory_attempt` 的首遍改用 `plan_memory_read_with_versions`，它在
`node.children.get_child_edge(...)` **之前**回调 `on_parent(&node.children)`，
因此捕获的是"读到该 child map 那一刻之前"的版本；末段查询失败（负向结论）的父目录同样被捕获。

## 2. 版本失效回退
快路径条件 `!plan.root_target && plan_versions.iter().all(|v| v.is_current())`。
任一失效 → 不进入 → 执行原第二遍 `plan_memory_read` 并保留 `plan.matches(&second)` 与
`verified.*` 的后续使用。**两分支合流到单一构造点**，因此原有
`master.metadata.file_status.locked_build` 的 fault_point 与 test park **各只有一处**，两条路径都会经过
（早期版本把 fault_point 复制到快路径，导致同一 point 双注册，已由一条测试的
"reader never reached the locked build window" 暴露并修正）。

## 3. epoch / restore / 锁集 / Busy
- 两道 epoch：首遍 `checked_live_epoch`、锁后 `checked_live_epoch` + `locked_epoch != plan_epoch`。
- **epoch 变化是 `continue` 重试**，不是"再走一次完整 walk"；重试那一轮可能直接命中快路径，
  所以整树 restore 用例断言 `fallbacks` 增量可以为 0。
- **store 不可用是错误**（`ensure_available()?`）。快路径**显式补**这一次，
  因为被跳过的第二遍原本会做。
- **HolderBusy 顺序未动**：先在 Busy 时释放 inode 锁，再按原规则等待。
- 锁集与原子一致：`plan.lock_requests()` 不变。root 作为**祖先**时其 child map 不在
  `lock_requests()` 内，靠版本区间 + epoch 保护；该保护与第二遍 walk 等价，但**窗口边界未经对照测试证明**。

## 4. `get_inode` / 错误语义
快路径与回退路径共用 `build_file_status`：
- `Some(ChildEdge::Dir(node))` → `InodeDir::from_dir_node_live`（**live 属性**，测试
  `directory_attributes_changed_in_the_window_are_read_live` 证明 mtime 取的是锁窗内的新值）
- `Some(ChildEdge::FileId(id))` → `store.get_inode(id, Some(name))`；`None` → `file_not_found`
  （**不是** exists 的 "Failed to load inode"）
- `None` → `file_not_found`
- `!verified.full` → `file_not_found`
- 其余错误原样透传

## 5. ABA / linearization
版本取自 `DirectoryChildren` 的 `SequenceState`（低位 writer 计数、高位 generation），
mutation 期间非零、完成后 generation 递增 → "仍 current" 意味着该 child map 自捕获以来未完成 mutation。
**ABA 依赖**：同一父目录的 `Arc<DirectoryChildren>` 不被替换。整树 restore 会替换整棵树，
此时旧 Arc 上的版本可能仍自洽，因此该场景不靠 token 而由 epoch 检查拦下（`continue` 重试）。
未证明：若存在"仅替换某目录 children Arc 而不推进 epoch"的路径，token 会误报 current。
本次核对的源码中未发现该路径，但这是**代码审读结论，非测试证明**。

## 6. 性能实验的边界
- 对照是 **`31aedad88`（当前分片锁候选，无探针） vs 原型**，**不是** 全局锁 `bff4e7dc5`。
- 短样本（`BENCH_OPS=1000`）`same/8T` 的 p99×2.3–2.4 **在长样本（100000×8T）未重现**；
  长样本该格两轮 QPS +17.7%/+34.9%、p50 −11.3%/−20.3%、p99 −16.7%/−24.1%。
- 长样本也**只覆盖单格两次**，不能证明短测回退是偶发、更不能当作正式达标。
- `BENCH_OPS=100000` 是诊断口径，不等于正式 1000。

## 7. benchmark 源码绑定限制
- 单格过滤是**诊断树**（`/tmp/cv-diag-{O,P}`）里加的 `BENCH_ONLY_CELL`，**不在主线**。
- 两侧 harness 5 文件**逐字节一致**（含已修正的 span 分母），两侧同 `1.98` 工具链、同 argv、`status=0`。
- 主工作树 `/home/oppo/oppo/curvine` 的 `metadata_ab_bench/main.rs` **有一处非本次形态的 dirty 变更**
  （`fn selected` + 3 个环境变量），已记录在 `/tmp/cv-shared-tree-record/`
  （diff sha `4be63589…`），**未清理、未 reset、未提交**；作者未确认。

## 8. 已知缺口
1. 测试是 **7 条受控窗口内的单点交错**，不覆盖调度对抗性。
2. 整树 restore 用例只证"epoch 推进 → continue → 重试"这一条时序，**不泛化为整个恢复协议**。
3. root 作为祖先时的窗口边界未单独证明（同 exists 口径）。
4. `same/8T` 短/长样本结论不一致，**未定位**，不声称已解决。
5. 性能只对该分片锁候选比较，**未对全局锁重新验证**。

## 9. 证据
| 项 | 值 |
|---|---|
| 生产 commit | `6112a50b5` |
| 测试 commit | `6ce7045c6` |
| default 定向 | 18 passed / 0 failed / status=0 |
| fault 定向 | 20 passed / 0 failed / status=0 |
| 日志 | `/tmp/cv-fsvp-tests/{default,fault}-v3.*` |
| 长样本 | `/tmp/cv-diag-runs/`（O `71ef222e…` / P `7988aba7…`） |
