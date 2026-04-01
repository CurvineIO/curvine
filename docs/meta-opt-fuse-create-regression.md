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
