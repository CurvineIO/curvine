# C3 模块收束自审（七项自我对抗式）

被审源：`88df33b35d2dc51e5fb942060733d35edc20508b`
（C3 两个部分：`36a907f3c` 布局收束 + `f442505f7`/`88df33b35` 冷窗口命名收尾；父 `e1aa12595`，C2 关闭源）
自审方式：假定自己写错了，逐项找反例。未使用子代理。

## 改动摘要（供对照）

### 部分二：冷窗口 body 命名收尾（`f442505f7` + `88df33b35`）
- `*_with_global_lock_body` → `*_cold_body`：**8 个定义 + 8 个调用 = 16 处**；
  另有 1 处测试消费者（`shared_handles_test.rs`）随之一并改，
  该处被 rustfmt 由 5 行压成 1 行（参数逐个相同）。
- 18 处 `global lock` 注释改为 `cold-window owner`（管理提到 5 处，实际文件里有 18 处，
  全部指向已删除的全局锁，故一并改；见 ⑦）。
- `git diff -M` 证明：34/34 行，过滤掉改名与注释行后**无任何实质变化**。

### 部分一：布局收束（`36a907f3c`）
```
6 files changed, 745 insertions(+), 670 deletions(-)
master_filesystem.rs          7688  (was 8295, −607)
  fs/master_filesystem/write.rs        496  (new)
  fs/master_filesystem/read_plan.rs    137  (new)
  fs/master_filesystem/write_locks.rs   48  (new)
meta/fs_dir_handle.rs          480  (was 428, +52: MetadataConfirmation)
journal/journal_loader.rs             (2 lines: import path)
```

---

## ① 有效性：改动是否真的让目标行为发生改变

C3 的目标是**布局收束**，不是行为。因此"有效性"= 职责是否真的换了位置、旧位置是否真的消失。

证据（逐条可查）：
- `grep -n "struct MetadataConfirmation" master_filesystem.rs` → **0**；`fs_dir_handle.rs` → 439 行有定义。
  旧定义已删，无 re-export（`grep "pub use.*MetadataConfirmation"` 为空）。
- `master_filesystem.rs` 内 `run_metadata_write*` / `run_recursive_metadata_write*` /
  `maybe_create_periodic_snapshot` **全部不在**，全部出现在 `write.rs`（496 行）。
- `MemoryReadPlan` / `plan_memory_read` 只在 `read_plan.rs`；
  `MEMORY_READ_ATTEMPTS` **仍在父模块**（消费者 `file_status_memory` / `file_status_snapshot` 未动）。
- `ancestor_read_target_write_requests` 只在 `write_locks.rs`，三个调用点已替换（`grep -c` = 3 调用 + 1 定义）。

反例尝试：会不会只是"复制一份、旧的还在"？
- `grep -c "fn run_metadata_write" master_filesystem.rs` = 0，物理上只有一份。
- `cargo check` 通过，重复定义会直接报错。

结论：**真的换了位置**。

## ② 正收益 vs 代价

| 收益 | 代价 |
|---|---|
| `master_filesystem.rs` −607 行；写协调、读规划、锁请求构造各有归属文件，边界可命名 | 新增 3 个模块 → 跨模块 `use super::*`，编译单元内符号来源多一层 |
| guard 与它借用的 holder 同文件，`MetadataConfirmation` 不再需要从 RPC 协调器导入 | 三处 `pub(super)` 使这些符号对 `fs` 模块（含其后代）可见——比原来"仅 `master_filesystem` 及后代"**更宽** |
| 请求构造去重 3 处（每处 9 行 → 1 行） | 去重引入一次静态方法调用（见 ③） |

反向攻击：**收益是否被"可见性变宽"抵消？**
- 原来私有 = 模块及后代可见；`pub(super)` = 父模块（`fs`）及后代可见。
- 实际消费者：父模块 `master_filesystem.rs` + `master_filesystem::tests`（既有测试）。
  `fs` 模块本身有没有新消费者？`grep -rn "run_metadata_write_exclusive\|run_metadata_write_epoch" curvine-master/src/master/fs/*.rs`
  → 只命中 `master_filesystem.rs` 与其 tests。所以**宽出来的那一层没有实际使用**，
  是可接受的代价，但没有被滥用。
- 我没有把任何符号提升到 `pub(crate)`（`grep "pub(crate) fn run_recursive\|pub(crate) struct MemoryReadPlan"` 为空）。

结论：收益成立；代价已量化且未被滥用。

## ③ 性能

**运行期**：所有搬移都是同 crate 内的位置变化，生成的 MIR 除 ④ 的一处外完全等价。

唯一真实差异是 `ancestor_read_target_write_requests`：
- 原来三处各自内联 `ids.iter().enumerate().map{}`；
- 现在是一处 `pub(super) fn` 静态调用。
- 可推演：同 crate、单 codegen unit、`pub(super)` 静态分派 → LLVM 常规内联；
  即便未内联，代价是每把锁获取前的一次函数调用 + 一次 `SmallVec` 返回（与原来相同的一次分配/构造），
  相对其后的 `lock_many`（含条带排序与加锁）可忽略。
- 若后续基准显示回归，最小补救是给该函数加 `#[inline]`；本轮**没有加**，因为设计要求"无行为/无逻辑变更"，
  而 `#[inline]` 属于性能声明，应由基准结论驱动而非先验添加。

**编译期**：模块数 +3，文件总行数基本持平（745 vs 670 净 +75 主要来自三个模块的头注释与 guard 的 doc 扩充）。

**内存/锁/IO**：零变化（无新的 Arc、无新的锁、无新的 IO）。

## ④ 优雅

- `write.rs` 头部注释明确写了"这里**不**融合成一个模式机、不共享骨架"，把设计的禁止项写在代码里，
  避免下一个人把它"重构"成策略模式。
- `read_plan.rs` 注释写明 `MEMORY_READ_ATTEMPTS` 为什么留在父模块（消费者不在本边界内）。
- `write_locks.rs` 注释写明"只共享构造，不加锁、不复制锁池的顺序/去重"。
- 与相邻代码同模式：子模块用 `use super::*;` + `impl MasterFilesystem { ... }`，
  与既有的 `master_filesystem/tests/` 模块组织一致。

- **命名收尾（部分二）**：`_with_global_lock_body` 描述的是**已经拆掉的全局锁**，与实际约束
  （冷窗口独占 owner 内执行）不符。改名后读者不必再去代码里确认"这个全局锁在哪"。
  注释同步改成 "needs the cold-window owner"，把真实约束写在调用点。
  这属于"起码的命名准确性"，不改结构、不引入概念。

反例尝试：`write_locks.rs` 只有 48 行、1 个函数，是否属于"过早抽象"？
- 3 个真实调用点、每处 9 行重复、且设计明确点名 → 不属于 YAGNI。
- 但注意：**如果只有 3 处而文件有 19 处同形，这个模块的收益是局部的**（见 ⑦ 的如实记录）。

## ⑤ 语义

逐项核（这是本改动最需要防守的一项）：

| 语义面 | 结论 | 证据 |
|---|---|---|
| `MetadataConfirmation` 的构造 | **逐字等价** | `arm(handle, writer)` 体是 `Self { handle: Some(handle), writer }`；原字面量是 `{ handle: Some(&self.fs_dir), writer: &self.journal_writer }`。无额外状态、无副作用 |
| 析构与失败隔离 | **未变** | `Drop` 实现随结构体一起搬到 `fs_dir_handle.rs`，代码逐字未改；guard 仍在 body 词法作用域内、晚于 I/L/view 声明 |
| 写协调的控制流 | **未变** | 方法体逐字节搬移；`run_metadata_write` 仍 `pub(crate)`，其内部 epoch 捕获点、fallback 边界、cold window、gate 处理全部原样 |
| `scope.wait` 前不持 view/I/L | **未变** | `wait` 所在方法体未改一个字符 |
| 读规划的加锁窗口 | **未变** | `plan_memory_read` 只被搬移；调用方仍在 guard 释放后取锁 |
| 请求构造的顺序/内容 | **未变** | 新函数产出与原来相同的序列：祖先 read、目标 write、深度递增 |
| 错误类型 | **未变** | 无 `map_err` 增删；`FsResult`/`CommonResult` 边界照旧 |
| journal 顺序 | **未变** | 搬移块不含任何 `journal_writer.append` 调用点 |

**命名收尾（部分二）的语义核查**：
- 这 8 个 `*_cold_body` 全是模块私有方法，不在任何外部契约里；改名不影响调用方语义。
- 函数签名、函数体、错误类型、加锁与 journal 顺序**一字未改**；
  `git diff -M` 过滤掉改名/注释行后为空，是可机器验证的"零行为改动"。
- 唯一非改名差异是 `shared_handles_test.rs` 一处调用被 rustfmt 压行（5 行 → 1 行），
  参数顺序与内容逐个相同。
- `ShardedOutcome` 由"模块私有"变为 `pub(super)`；
它的可见性变化不影响运行时语义，只影响编译期可见面（同 ②）。

## ⑥ 自洽（先自己攻击一遍）

| 攻击 | 结果 |
|---|---|
| 搬移后是否有重复定义？ | 无。`grep` 确认每个符号仅一处；`cargo check` 通过 |
| `pub(super)` 是否遗漏导致编译错？ | 逐步编译验证过：先报 4 个 E0624（父模块调用）、2 个 E0616（字段），逐一加 `pub(super)` 后清零 |
| 字段可见性是否放得过宽？ | 只对父模块真实读写的 7 个字段加 `pub(super)`；未加 `pub`/`pub(crate)` |
| 提取时有没有误伤相邻代码？ | **有，已修**：抽 `plan_memory_read` 时把下一个方法 `file_status_memory` 的 doc 注释一起带走了，编译期报 "expected item after doc comment"。已把 doc 还给父模块并核对了内容 |
| 三处替换是否误改其它同形代码？ | 脚本第一次按文本全局替换命中 9 处（实际文件有 19 处同形），被断言拦下；改为按三处指定行号替换并逐行断言 |
| 改名是否漏了消费者？ | **漏了一处，被编译挡下**：`shared_handles_test.rs` 直接调用 `create_cold_body`，首轮改名后 `--list` 直接 E0599 失败。改完该调用点后 `--list` 回到 725/780 |
| `--list` 用例数是否变化？ | 不变：baseline `e1aa12595` = 725 default / 780 fault；搬移后 = 725 / 780（同一 lane、同一 argv） |
| 相关测试是否回归？ | 无：`write_planning_test` 10/10 双模式、`role_epoch_test` 1/1 双模式、`recursive_lock_set_test` 20/21、`recursive_replay_lock_test` 10/10、`replay_lock_test` 15/16，均 exit 0 |

攻不动的点（坦白）：
- `master_filesystem.rs` 仍有 **7688 行**，远大于 500 行原则。C3 只是把它减了 607 行，
  **没有解决**文件过大问题——真正的拆分需要再切多轮，且每轮都要按本次的口径验证，不在本次范围。
- 我没有做性能基准（本轮不跑整体回归/A-B，按设计"全部开发完成后一次整体回归"）。
  ③ 的性能结论是可推演论证，不是实测。

## ⑦ 合规

| 规范 | 对应 |
|---|---|
| KISS / YAGNI | 只收束设计点名的四块；**没有**顺手把 19 处同形请求构造全部抽掉（见下） |
| DRY | 只抽真实重复（3 处逐字相同）；`complete` 的 location 锁、`atime` 的 best-effort 保持各自实现 |
| SOLID | 每个新模块单一职责；`write.rs` 保留各入口的独立方法，不引入模式机 |
| 文件规模 | `write.rs` 496、`read_plan.rs` 137、`write_locks.rs` 48、`fs_dir_handle.rs` 480 —— 均 < 500；`master_filesystem.rs` 7688 **超限且本次未解决**（如实记录） |
| 测试目录 | 未移动任何测试；`--list` 与测试结果均已核对 |
| 门禁 | `cargo check --lib` exit 0；`rustfmt --edition 2021` 已跑；无 `todo!()`/`unimplemented!()` 新增 |
| 设计章节 | 模块设计 §2（模块边界）、§3（可见性与真实消费者）、§4（不得改变的控制流）、§5（实施与交付） |

**命名收尾的范围偏差（如实）**：要求里说"另有 5 处注释写 needs the global lock"，
实际 `grep -i "global lock"` 命中 **18 处**，全部指向已删除的全局锁。我按"同一个编辑"一并改，
没有新增改动类别；如认为应只改 5 处请指示，回退其余 13 处是纯文本操作。

**必须上报的设计依据偏差**：设计第 1 节称"只有 `set_attr_single_path_sharded` /
`complete_file_sharded` / `touch_atime_sharded` 三处"有该请求构造。实际文件里有 **19 处**同形代码
（`mkdir_single_parent_sharded`、`create_single_parent_sharded_with_plan`、`overwrite_file_sharded`、
`delete_single_parent_sharded`、`close_file_sharded`、`symlink_single_parent_sharded`、
`delete_expired_inode_sharded`、`free_single_file_sharded`、`release_verified_sharded`、
`reopen_file_sharded`、`add_block_sharded`、`build_located_blocks`、`resize_single_file_sharded`、
`assign_worker_sharded`、`get_lock_snapshot` 等）。
我**只改了设计点名的三处**，没有扩大范围；这一偏差交由架构师裁定是否扩大或另立 followup。
这也意味着 `write_locks.rs` 当前的收益是局部的（3/19）。

---

## 自审结论

- 部分二（冷窗口命名）在 ④ 优雅与 ⑤ 语义两项下单独核查：**通过**，零行为改动可机器验证。
- ① 有效性、② 正收益、③ 性能、④ 优雅、⑤ 语义、⑥ 自洽：**通过**，其中 ⑥ 记录了两处我自己造成并已修复的问题
  （doc 注释误搬、全局替换误命中 9 处被拦下）。
- ⑦ 合规：**通过但携带两条必须上报的事项**——
  (a) `master_filesystem.rs` 仍 7688 行超限，本次未解决；
  (b) 设计称 3 处同形请求构造，实际 19 处，我只改点名 3 处。

被审源：`88df33b35d2dc51e5fb942060733d35edc20508b`（C3 两个部分的合并 tip）；
本文件为独立自审产物，单独提交。
