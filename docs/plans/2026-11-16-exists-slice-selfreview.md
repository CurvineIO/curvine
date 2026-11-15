# 独立七项自审 — exists 路径拆分复用切片（源码 `0799b610a`）

docs-only commit，追加于源码提交之后；无 `.rs`/`Cargo.toml` 改动。

## 1. 需求覆盖
| 要求 | 落实 |
|---|---|
| 仅复用 D3 已核的两文件语义变更 | ✓ `master_filesystem.rs` + `read_plan.rs`，+33/−4 |
| 去掉 `[D3-DIAG]` 措辞与诊断式长说明 | ✓ 注释改为正常设计说明 |
| 不改 `file_status` / 其他路径 | ✓ `git diff --stat -- curvine-master/src` = 2 files，`file_status_memory` 内 `with_components` 计数 0 |
| 解析结果仅当次调用、不跨调用缓存 | ✓ `components` 由 `exists` 局部产生并只传给本次 `exists_memory` |
| `plan_memory_read` 对所有调用者行为不变 | ✓ 签名、内部解析、错误顺序原样；仅转调新 helper |

## 2. 语义不变性（逐项复核，与执衡"不能删"清单一一对应）
锁前/锁后各自重新遍历实际目录 · `lock_requests`（stripe 排序+dedup）· 双 `checked_live_epoch`
+ 比较重试 · `plan.matches(&verified)` · `ensure_available()` · file 目标 `get_inode` dangling ·
`exists_snapshot` 冷兜底 · `ensure_metadata_current` 分支 · 全部错误/回退 —— **均未改动**
（`git diff` 只出现在规划调用与函数签名处）。

## 3. 边界与错误处理
- malformed、尾 slash、pending 路径的**判定顺序完全不变**：`exists` 中
  `trash::is_pending_path` → `path_components` → `last().is_none_or(is_empty)` → 调 `exists_memory`，
  顺序与位置一行未动（diff 只改最后一行调用处的实参）。
- 重试语义：`MEMORY_READ_ATTEMPTS = 4` 不变；每次迭代仍两次规划、两次 epoch 读。

## 4. 性能（只报证据，不做承诺）
D3 消融（O=原样 / P=本切片）：**4 种配置**（`exists` × {same, multi} × {1, 32} 线程，
每线程 100k ops），非 8 格。
- **1 线程（两种布局）：正反两个序列比较均改善**（O→P 与 P→O 同向）；
- **32 线程（两种布局）：两个序列方向不一致**（同一格符号相反）；
- 环境：D3 运行期 loadavg 见 `s1_*/s2_*.argv` 逐次记录（**load 160 属无效的 D1，不混入 D3**）；
  D3 的 harness 参数（100k ops）与正式 A/B（1k ops）不同 ⇒ **不能**把 D3 当作整体回退的
  唯一根因，**也不据此宣称目标达成**；
- 正式 metrics **按设计保留**，未删除任何指标。

## 5. 测试质量
**未新增用例**（原计划的 rename 用例已删除，理由见源码提交 `da0378a43`：
只涉同一 inode、无确定性同步落在两次 plan 之间、且注释把 `cancel_epoch` 误当 rename 计数）。
现有真实覆盖：根 `"/"`(:98)、malformed(:100)、尾 slash(:101)、pending(:102/:460)、
dangling(:111 起)、删除发布窗口(:39)、并发 create/delete(:139)、store snapshot+共享 guard(:76)、
tree reload(:782)。

**已知缺口（如实登记，不用无效用例冒充）**：没有"确定性 namespace 边变动恰好落在
`exists` 两次 plan 之间、触发 `plan.matches(&verified)` 失败并重试"的测试。
该语义目前只有生产代码保证（`MEMORY_READ_ATTEMPTS` + `matches`），**无测试直接覆盖**；
补它需要在 `lock_many` 附近加 hook（现有 `#[cfg(test)]` hook 都在 `namespace_commit_gate`，
不覆盖此点），属生产侵入，按 KISS 暂不做。

## 6. 未覆盖（如实）
- 其余 **21 个操作**未测量
- **热父场景**未测量
- 客户端/FUSE 层不受本切片影响，性能未测量
- 整体回归与完整 WAL-on A/B **未执行**（待 review 后安排）

## 7. 可回滚性
单提交 `0799b610a`，改动 3 文件（2 生产 + 1 测试），无数据格式/协议变更；
回滚即 `git revert 0799b610a`，无迁移步骤。
