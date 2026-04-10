# meta-opt FUSE create/write 回归调查记录

## 背景

在 `meta-opt` worktree 中完成 metadata 内存树优化后，发现 FUSE 挂载点出现创建后写入异常：

```bash
cd /curvine-fuse
cp /tmp/1.txt .
```

用户侧表现为：

- `cp` 返回 `EIO`
- 目标文件 `1.txt` 会被创建出来
- 但文件内容为空

该问题出现在 metadata 优化之后，因此需要先确认是否为 `curvine-fuse` 本身，还是 metadata 改造后的 create 路径语义回归。

---

## 复现现象

在挂载点 `/curvine-fuse` 下执行：

```bash
cp /tmp/1.txt .
```

得到：

```text
cp: 无法创建普通文件 './1.txt': 输入/输出错误
```

但随后：

- `ls` 能看到 `1.txt`
- `cat 1.txt` 为空
- 原文件 `/tmp/1.txt` 有内容

这表明不是“创建失败”，而是“创建后没有完成预期写入”。

---

## 已确认范围

对问题区间 `0278d542..83796359` 做比对后，确认：

- **没有任何 `curvine-fuse/` 代码变更**
- 变更仅出现在 metadata 相关文件：
  - `curvine-server/src/master/meta/fs_dir.rs`
  - `curvine-server/src/master/meta/inode/inode_path.rs`
  - `curvine-server/src/master/meta/store/inode_store.rs`
  - `curvine-server/src/master/journal/journal_loader.rs`
  - 相关设计/测试文档

因此，该回归不能优先归因到 FUSE 层实现本身，必须先优先怀疑 metadata 改造引入的 create 路径语义变化。

---

## 日志证据

### 1. FUSE 侧日志

`build/dist/logs/fuse.out` 显示：

- `Create writer, path=/1.txt`
- `Close writer, path=/1.txt`
- **没有出现 write 日志**

这说明：

- FUSE create 已经走通
- writer 被创建了
- 但后续没有进入正常 write 流程
- 很快就走到了 close/release

### 2. Master 侧日志

`build/dist/logs/master.out` 显示同一时间点：

- `FileStatus succeeded=false src=/1.txt`
- `OpenFile succeeded=true src=[WCT]/1.txt`
- `AddBlock succeeded=true src=/1.txt`
- `CompleteFile succeeded=true src=/1.txt`

这说明：

- master 侧认为 create/open 流程成功
- 元数据节点被创建了
- 甚至 block/complete 相关动作也发生了
- 但用户最终看到的文件仍为空，并且 `cp` 返回了 `EIO`

### 3. 交叉结论

结合两侧日志，可以确认：

- 不是“文件不存在”
- 不是“create 请求被拒绝”
- 而是 **create 成功后，FUSE/内核没有继续进入预期的 write 路径**

---

## 当前最可疑的代码路径

### 1. `FsDir::add_last_inode()`

文件：`curvine-server/src/master/meta/fs_dir.rs`

当前 create 路径中，`add_last_inode()` 的关键顺序是：

1. 从 `InodePath` 找 parent inode
2. 用 `get_parent_entry()` 找到树上的父节点 entry
3. 先向内存树插入 child entry
4. 再从父节点重新取回 `child_entry_ref`
5. 再调用 `store.apply_add(...)` 持久化
6. 最后 `inp.append(child_ptr, child_entry_ref)`

这条链路和旧实现相比，多了对以下语义的一致性依赖：

- `get_parent_entry()` 对“部分解析路径”的定位必须绝对正确
- 刚插入树后拿到的 `child_entry_ref` 必须稳定且正确
- `append()` 后返回的 `InodePath` 必须与最终树/存储状态完全一致

### 2. `InodePath::ResolvedInode`

文件：`curvine-server/src/master/meta/inode/inode_path.rs`

改造后，`InodePath` 不再只是 inode metadata 列表，而是变成：

- `ResolvedInode { inode, entry }`

也就是：

- 元数据引用
- 树上 entry 引用

被强绑定在一起。

这在设计上更一致，但 create 场景下也更脆弱：

- 一旦 `entry` 与 `inode` 的时序、定位或返回状态不一致
- create 返回给上层的 `FileStatus/path/id/name/type/len`
- 就可能和真正持久化后的节点状态出现偏差

而这类偏差正符合当前现象：

- create 看起来成功
- 但后续 write 没有按预期发生

---

## 当前判断

到目前为止，可以确认的判断是：

1. 这不是单纯的 `curvine-fuse` 写路径实现问题
2. 这是 metadata 优化后 create 路径语义不一致的高概率回归
3. 问题最可能落在：
   - `create_file()`
   - `add_last_inode()`
   - `InodePath::get_parent_entry()`
   - `ResolvedInode` 的 entry/inode 绑定语义
   - `file_status()` 在 create 后返回值的一致性

---

## 暂时不能下死结论的点

当前还**不能**证明是哪一行代码直接导致内核不再继续下发 write。

也就是说，现在的结论边界是：

- 已经证明回归范围在 metadata 改造侧
- 已经证明 create 成功但 write 没进入正常路径
- 但尚未通过更细粒度日志把根因钉死到单个函数分支

---

## 后续排查建议

后续继续排查时，应优先验证以下一致性：

1. `create_file()` 返回后的 `InodePath` 是否完整且自洽
2. `existing_len()/len()` 在 create 场景下是否符合预期
3. `get_parent_entry()` 在“叶子不存在”的部分解析路径下是否始终指向真实父目录
4. 新 child 插入树后拿到的 `child_entry_ref` 是否与最终树状态严格一致
5. `file_status()` 返回的 `path/id/name/type/len` 是否与刚创建节点完全一致

如果这些值中有任意一个在 create 返回时不一致，就足以解释为什么用户看到的是：

- create 成功
- 文件被创建
- 但后续 write 没有正常继续

---

## 关联文档

- `PHASE1-CHANGES.md`
- `docs/inode-id-only-design-v2.md`

---

## 当前修复进度

围绕 `git clone` 在 FUSE 挂载点上的 `premature end of pack file` 回归，当前已经完成以下修复：

1. 明确 `InodePath` / `ResolvedInode` 的 request-scoped rich view 语义，约束单次请求内不重复回查同一文件实体。
2. 收紧 master 写链路，让 `open_file()`、`complete_file()`、`reopen_file()` 的返回值统一从同一份 rich inode 生成，避免 mutate 之后继续使用旧 inode 快照。
3. 清理 `fs_dir` / `master_filesystem` 中对已有 `InodePath` 文件实体的重复 `lookup` / `get_inode` 路径。
4. 在 worker 侧补齐 `write_flush` 的真实落盘动作，确保 flush 请求不只是协议层成功，而是真的把数据刷到本地块文件。
5. 把 `only_flush` 返回的 `FileBlocks` 沿客户端和 FUSE 链路向上透传，避免 flush 完成后又重新按 path 回查旧元数据。
6. 调整 FUSE reader refresh 逻辑，支持在读写并发时主动刷新 reader，并为 open-but-unlinked 文件增加基于 handle path 的兜底。

本轮为了继续逼近根因，又补了两处 FUSE 侧最小修改：

1. 撤销 `RDONLY open -> writer.complete()` 的策略，改回只做 `writer.flush()`，避免临时 pack 文件在仍有写句柄时被提前封口。
2. 增加 `new_reader_with_blocks()`，reader refresh 优先直接使用 writer 刚 flush 出来的 `FileBlocks` 构造 reader，而不是默认重新走 `path -> open()`。

---

## 当前验证结论

已经执行过的关键验证：

- `cargo test -p curvine-server --test inode_test` 通过。
- `cargo test -p curvine-fuse --lib` 通过。
- 多次重建 `curvine-fuse` release 并重启 master / worker / fuse 集群。
- 多次复现：

```bash
git clone --depth 1 https://github.com/CurvineIO/curvine.git /curvine-fuse/fuse-test/git-clone-repro
```

当前结论是：

1. `InodePath rich view` 计划本体已经完成并落地。
2. `git clone` 回归仍未彻底修复，最近一次稳定复现仍为：

```text
fatal: premature end of pack file, 2362 bytes missing
fatal: fetch-pack: invalid index-pack output
```

3. 最新日志说明问题已经不只是 master 元数据 stale return：
   - worker 最终写入的 pack block 长度会收敛到 `1435712`
   - 但 FUSE 侧在同一轮里仍然多次创建出 `len=0, blocks=0` 的 `tmp_pack_*` reader
   - master 日志中同一临时 pack 会出现多次 `CompleteFile succeeded=true`

这说明当前更像是 FUSE writer 对外暴露的 `FileBlocks` 仍然滞后，reader 刷新时拿到的不是后台 buffer writer 最新 flush 后的状态。

---

## 当前最强嫌疑点

经过本轮排查，问题已经从 metadata rich view 进一步收敛到 FUSE / client writer 生命周期链：

### 1. `FsWriterBuffer` 对外暴露的 `file_blocks`

`FuseWriter.file_blocks()` 会透传到底层 `UnifiedWriter.file_blocks()`。

但当前实现里，`FsWriterBuffer` 保存的 `file_blocks` 更像是 writer 创建时的初始快照，而不是后台 `write_future()` 在 `flush()` / `complete()` 之后的最新状态。这样会导致：

- writer 实际已经把 pack 数据写到了 worker
- 但 reader refresh 重新拿到的 `FileBlocks` 仍然是 `len=0, blocks=0`
- 进而用错误长度创建 reader，最终触发 pack 截断

### 2. 临时文件清理与并发 complete

最新日志中除了 `tmp_pack_*` 之外，还出现了：

```text
buffer writer error: ... File does not exist: /fuse-test/git-clone-repro/.git/shallow.lock
```

这表明 Git 在失败清理阶段会对 lock / temp 文件执行高频 delete，而现有 FUSE handle 生命周期里仍存在：

- writer 尚未完全收口
- 文件已被 delete / delayed delete
- 但并发 flush / complete 仍继续打到 master

这个问题会放大主故障，但当前看更像次级竞态，不是最早触发 pack 截断的第一根因。

---

## 下一步计划

当前已经开始的下一步修复方向如下：

1. 把 `FsWriterBuffer` 的 `file_blocks` 从静态快照改成共享的最新视图，让后台 buffer writer 在每次 `flush()` / `resize()` / `complete()` 后都回写最新 `FileBlocks`。
2. 让 `FsWriter` / `CacheSyncWriter` / `UnifiedWriter` 读取这份最新快照，而不是读取创建 writer 时缓存下来的旧值。
3. 在完成这条修复后，再重新验证：
   - `cargo test -p curvine-fuse --lib`
   - 重建 release 并重启
   - 重新执行 `git clone`
   - 必要时再跑 `build/tests/fuse-test.sh`

---

## 风险说明

当前代码状态属于“主计划已完成，但回归验证未完成”：

- `InodePath rich view` 语义和 master 写链一致性修复已完成。
- `git clone` 的 pack 临时文件并发场景仍未通过。
- 因此当前分支可以作为阶段性进度保存，但不能宣称该回归已经修复完毕。
