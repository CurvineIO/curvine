# inode_view id-only 设计稿 (目录专用 EntryKey + arena 版)

## 1. 文档信息

- **主题**：inode 内存树改造为 id-only + stable handle 模型
- **范围**：`curvine-server` master metadata 路径
- **目标**：保留 id-only 的低冗余设计，同时移除 `DirEntryRef` 裸指针带来的悬垂引用风险
- **核心决策**：只有目录节点进入 tree，并使用 `EntryKey + arena`；文件继续通过 `parent + name` 点查

---

## 2. 问题背景

当前 meta-opt 的主要优化方向是：

- 内存树只保留拓扑导航能力
- rich metadata 统一收敛到 `InodeView`
- 文件节点只保留 `inode id`

这条方向本身是对的，但当前实现把树上的定位结果缓存成了 `DirEntryRef`。这会带来两个问题：

1. **内存安全问题**
   - `DirEntryRef = RawPtr<DirEntry>`
   - 一旦树结构发生插入、删除、移动，旧地址可能失效
   - 后续 `as_ref()` / `as_mut()` 可能直接触发段错误

2. **create 路径语义脆弱**
   - `InodePath` 同时持有 `inode` 和 `entry`
   - 只要 tree 和 store 更新时序不一致，就会让 create 返回路径不自洽

因此，这版设计稿改成：

- **缓存稳定句柄，不缓存节点地址**
- **树节点统一存放在 arena**
- **children 里只存 name -> EntryKey**

---

## 3. 核心类型定义

### 3.1 树层 (directory-only stable handle)

```rust
pub struct EntryKey {
    slot: u32,
    generation: u32,
}

pub struct DirEntry {
    dir: Box<InodeDir>,
    children: Box<DirChildren>,
}

pub struct DirChildren {
    inner: BTreeMap<String, EntryKey>,
}

pub struct EntrySlot {
    generation: u32,
    value: Option<DirEntry>,
}

pub struct EntryArena {
    slots: Vec<EntrySlot>,
    free_list: Vec<u32>,
}
```

### 3.2 元数据层 (rich metadata)

```rust
pub enum InodeView {
    Dir(Box<InodeDir>),
    File(Box<InodeFile>),
}

pub struct InodeDir {
    id: i64,
    parent_id: i64,
    mtime: i64,
    atime: i64,
    nlink: u32,
    storage_policy: StoragePolicy,
    features: DirFeature,
}

pub struct InodeFile {
    // unchanged
}
```

### 3.3 路径解析结果

```rust
pub type InodePtr = RawPtr<InodeView>;

pub struct ResolvedDir {
    dir: InodePtr,
    entry_key: EntryKey,
}

pub struct InodePath {
    path: String,
    name: String,
    components: Vec<String>,
    resolved_dirs: Vec<ResolvedDir>,
    leaf_inode: Option<InodePtr>,
}
```

### 3.4 关键变化总结

| 项目 | 旧实现 | 新设计 |
|-----|--------|--------|
| 树节点定位 | `DirEntryRef` 裸指针 | 目录节点使用 `EntryKey` |
| children value | `Box<DirEntry>` | `EntryKey` (仅目录 child) |
| 节点存储 | 散落在 `Box` | 目录节点统一存放在 `EntryArena` |
| 失效检测 | 无 | `generation` 校验 |
| `InodePath` tree 定位 | 缓存地址 | 仅缓存目录 key |
| 文件节点 | 在 tree 中占一个 entry | 不进入 tree，按需点查 |
| 目录节点 | `Dir(Box<InodeDir>)` | `DirEntry { dir, children }` |

---

## 4. 元数据架构逻辑

### 4.1 分层架构

```text
┌──────────────────────────────────────────────────────────────┐
│                        业务层 (FsDir)                        │
│  file_status / list_status / set_attr / create / delete      │
└──────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                     路径解析层 (InodePath)                    │
│  resolve(path) -> resolved_dirs + optional leaf_inode         │
└──────────────────────────────────────────────────────────────┘
               │                                  │
               ▼                                  ▼
┌───────────────────────────┐      ┌────────────────────────────┐
│       树层 (EntryArena)    │      │      存储层 (InodeStore)   │
│  - directory topology     │      │  - RocksDB metadata        │
│  - EntryKey lookup        │      │  - get_inode(id)           │
│  - dir_name -> EntryKey   │      │  - edge(parent,name)->id   │
└───────────────────────────┘      └────────────────────────────┘
```

### 4.2 为什么必须是 EntryKey

这里的关键点不是“所有 inode 都要 EntryKey”，而是“所有驻留在 tree 里的目录节点，都要有稳定 handle”。

不能直接用 `inode id` 代替目录 tree handle，原因是：

- `inode id` 表示 metadata object
- `EntryKey` 表示目录 tree node
- tree 结构修改时，需要的是稳定定位目录节点，而不是 metadata id

也就是说：

- 目录使用 `EntryKey`
- 文件继续使用 `inode id`
- file lookup 通过 `(parent_dir_inode_id, name)` 进入 store

### 4.3 内存树 vs RocksDB 存储

**内存树 (`EntryArena`)**：

- 只保留目录拓扑
- 目录 child 关系保存为 `dir_name -> EntryKey`
- 文件节点完全不进入 tree
- dir 节点保存 `InodeDir`，避免频繁读 store

**RocksDB 存储**：

- `inode_cf`: `id -> InodeView`
- `edge_cf`: `(parent_id, name) -> child_id`
- `block_cf`: block locations

### 4.4 复杂度判断

- `EntryKey -> DirEntry`: 近似 `O(1)`
- `children.get(name)`: 只用于目录导航
- file lookup: `edge_cf(parent_id, name) -> child_id`
- 路径解析成本主要取决于：
  - 路径深度
  - 单目录下目录 child 的索引成本
  - 叶子文件的点查成本

因此，引入 `EntryKey` 的主要目的不是加速，而是：

- 保住接近原有的寻址性能
- 去掉裸指针的不安全性

---

## 5. 树层接口设计

### 5.1 EntryArena

```rust
impl EntryArena {
    pub fn insert(&mut self, entry: DirEntry) -> EntryKey;

    pub fn get(&self, key: EntryKey) -> Option<&DirEntry>;

    pub fn get_mut(&mut self, key: EntryKey) -> Option<&mut DirEntry>;

    pub fn remove(&mut self, key: EntryKey) -> Option<DirEntry>;
}
```

### 5.2 DirChildren

```rust
impl DirChildren {
    pub fn get(&self, name: &str) -> Option<EntryKey>;

    pub fn insert(&mut self, name: String, key: EntryKey) -> bool;

    pub fn remove(&mut self, name: &str) -> Option<EntryKey>;

    pub fn iter(&self) -> impl Iterator<Item = (&str, EntryKey)>;
}
```

### 5.3 FsDir

```rust
pub struct FsDir {
    root_key: EntryKey,
    arena: EntryArena,
    store: InodeStore,
}

impl FsDir {
    pub fn root_key(&self) -> EntryKey;

    pub fn entry(&self, key: EntryKey) -> CommonResult<&DirEntry>;

    pub fn entry_mut(&mut self, key: EntryKey) -> CommonResult<&mut DirEntry>;

    pub fn resolve(&self, path: &str) -> CommonResult<InodePath>;

    pub fn lookup_file(&self, parent: &InodeDir, name: &str) -> CommonResult<Option<InodeView>>;
}
```

---

## 6. 关键流程

### 6.1 create_tree() - 启动恢复树拓扑

```text
1. 从 store 读取 root inode
2. arena.insert(root_dir_entry) -> root_key
3. 以 root_key 开始 BFS/DFS
4. 遍历 edge_cf:
   - load child inode
   - if child is dir:
       arena.insert(child_dir_entry) -> child_key
       parent.children.insert(name, child_key)
   - if child is file:
       do not insert into tree
5. 返回 (last_inode_id, root_key, arena)
```

这一步的重要变化是：

- 遍历队列里保存的是 `EntryKey`
- 不是 `DirEntryRef`
- 所以不会因为 `BTreeMap` / `Vec` 结构变化导致悬垂地址

### 6.2 resolve(path) - 路径解析

```text
输入: /a/b/c.log

1. components = ["", "a", "b", "c"]
2. cur_key = root_key
3. 对中间目录逐层查找:
   - cur_entry = arena.get(cur_key)
   - next_key = cur_entry.children.get(name)
   - load dir inode into resolved_dirs
4. 最后一段:
   - if last component is directory child in tree:
       load dir inode
   - else:
       lookup_file(parent_dir_inode_id, leaf_name)
4. 返回 InodePath
```

返回结果的关键保证：

- 中间目录段使用 `entry_key`
- 叶子文件只保留 `inode`
- 文件不再持有 tree-level handle

### 6.2.1 为什么 resolve 要区分目录段和叶子文件

在目录专用 `EntryKey` 模型下，`resolve()` 不能再把每一段路径都当成 tree node 处理，而要明确区分：

- **中间目录段**：一定走内存 tree
- **叶子文件**：不在 tree 中，必须走 store 点查

原因很简单：

- tree 只缓存目录
- 文件不进入 arena
- 因此只有目录才有 `EntryKey`

#### 场景 1：目录路径 `/a/b/c`

```text
1. root_key -> "a" -> "b" -> "c"
2. 每一段都是目录
3. 每一段都能在 arena 中找到对应 DirEntry
4. 返回:
   - resolved_dirs = [/, /a, /a/b, /a/b/c]
   - leaf_inode = Some(InodeView::Dir(...))
```

#### 场景 2：文件路径 `/a/b/c.log`

```text
1. root_key -> "a" -> "b"
2. "a" 和 "b" 都是目录，可以在 arena 中定位
3. 最后一段 "c.log" 不在 tree 中
4. 用 parent_dir_inode_id + "c.log" 去 edge_cf 点查 child inode id
5. 再从 inode_cf 加载 InodeView::File
6. 返回:
   - resolved_dirs = [/, /a, /a/b]
   - leaf_inode = Some(InodeView::File(...))
```

#### 场景 3：部分解析路径 `/a/b/new.log`

```text
1. root_key -> "a" -> "b"
2. 父目录 /a/b 已经存在
3. 最后一段 "new.log" 在 edge_cf 中不存在
4. 返回:
   - resolved_dirs = [/, /a, /a/b]
   - leaf_inode = None
```

这个场景正是 `create_file()` 最依赖的输入状态：

- 父目录已定位
- 父目录有稳定 `EntryKey`
- 叶子文件尚不存在

#### 场景 4：路径中间遇到文件 `/a/file/x`

```text
1. root_key -> "a"
2. lookup_file(parent=/a, name="file") -> InodeView::File
3. 由于中间段已经是文件，不允许继续向下解析
4. resolve 立即停止，并返回错误
```

这也是为什么 `resolve()` 不能简单写成“每一段都先查 tree，再补 metadata”：

- 文件段没有 tree entry
- 一旦某一段被解析为文件，这条路径就不可能再有子段

### 6.2.2 对 create / rename / delete 的直接意义

#### create_file

- 只需要依赖 `resolved_dirs` 的最后一个目录
- 不需要也不应该为新文件分配 `EntryKey`

#### rename file

- file rename 的本质是 edge 变更：
  - `(src_parent_id, src_name)` 删除
  - `(dst_parent_id, dst_name)` 新增
- tree 不做 file 节点迁移

#### rename dir

- dir rename 才需要移动 tree node
- 被移动的是目录 `EntryKey`

#### delete file

- 只删 edge 和 inode/store 状态
- tree 无需删除 file node

#### delete dir

- 既要删 edge/store
- 也要递归删除 arena 中的目录 subtree

### 6.3 create_file(path) - 创建文件

```text
1. resolve(parent_path) -> InodePath
2. parent_inode = get_parent_dir()
3. parent_entry_key = get_parent_dir_key()
4. 构造 child InodeFile
5. store.apply_add(parent, child_name, child_inode)
6. 不向 tree 插入 file node
7. inp.leaf_inode = Some(child_inode_ptr)
```

### 6.4 delete(path) - 删除节点

```text
1. resolve(path)
2. if target is dir:
   - parent_entry_key = get_parent_dir_key()
   - parent.children.remove(name) -> child_key
   - 递归从 arena 删除 subtree
3. if target is file:
   - tree 无需删除 file node
4. store.apply_delete(...)
```

### 6.5 rename(path) - 移动目录项

```text
1. resolve(src) and resolve(dst)
2. src_parent.children.remove(src_name) -> moved_key
3. if moved target is dir:
   - dst_parent.children.insert(dst_name, moved_key)
4. if moved target is file:
   - tree 不做文件节点迁移
5. update store edge relation
```

rename 的关键点是：

- 目录移动的是 `EntryKey`
- 文件移动的是 edge 映射
- file rename 不需要 file tree entry

---

## 7. create 回归与稳定性说明

当前已知回归记录见：`docs/meta-opt-fuse-create-regression.md`

切换到 `EntryKey + arena` 后，重点改善这几个点：

1. `InodePath` 不再缓存 `DirEntryRef`
2. `get_parent_entry()` 改成 `get_parent_dir_key()`
3. 目录返回 `EntryKey`，文件只返回 `inode`
4. 删除/重命名目录后，旧 key 可以通过 `generation` 检测失效

这不能自动解决所有 create 语义问题，但至少能先把最危险的内存安全问题移除。

---

## 8. 设计原则

### 原则 1：tree handle 必须稳定

- 不允许再用 `RawPtr<DirEntry>` 作为长期缓存
- tree 层统一用 `EntryKey`

### 原则 2：name 属于 edge，不属于 inode

- child 名字只存在于 parent.children 的 key
- `InodeView` 不再承担 name 语义
 - file name 通过 `edge_cf(parent_id, name)` 管理

### 原则 3：目录 identity 与 inode identity 分离

- 目录：`inode id` + `EntryKey`
- 文件：只有 `inode id`

### 原则 4：tree 负责导航，store 负责 rich metadata

- tree 只负责目录导航
- store 仍然是 metadata truth source

### 原则 5：先安全，再优化

- 先移除段错误风险
- 再根据压测决定是否要把 `children` 从 `BTreeMap` 升级到 `HashMap` 或混合结构

---

## 9. 内存与性能预期

### 9.1 性能

- `EntryKey` 额外带来一次 arena 索引
- 这通常比字符串查找成本更低
- 对百万级总节点数影响通常很小

### 9.2 内存

- `children` value 从 `Box<DirEntry>` 变成 `EntryKey`
- `EntryArena` 只为目录节点增加 slot metadata
- 但会减少大量小对象 `Box` 分配与碎片

因此，净内存变化预计是：

- 目录数很大时有一定增加
- 文件数很大时收益更明显，因为文件不再进入 tree
- 是否值得，取决于对安全性的要求

当前结论：**值得换**

---

## 10. 实施顺序

### Phase 1：定义新类型

1. 定义 `EntryKey`
2. 定义 `EntryArena`
3. `DirChildren` value 改为 `EntryKey`
4. 删除 `DirEntryRef` 在目录 tree 中的长期语义

### Phase 2：改造树构建

1. `InodeStore::create_tree()` 返回 `(last_inode_id, root_key, arena)`
2. BFS/DFS 队列只保存目录 `EntryKey`
3. 文件 child 不进入 tree
4. 所有目录 tree child 操作改成 key lookup

### Phase 3：改造 InodePath

1. 中间目录段缓存 `entry_key`
2. 叶子文件只缓存 `leaf_inode`
3. `get_parent_entry()` 改为 `get_parent_dir_key()`

### Phase 4：改造业务层

1. `FsDir.root_entry` 改为 `root_key + arena`
2. 目录操作改为 key-based
3. 文件操作保持 `parent + name` 点查
4. 所有目录 tree lookup 都统一走 `arena`

### Phase 5：回归测试

1. create / open / complete 路径回归
2. restore / replay 路径回归
3. hardlink / rename / delete 路径回归
4. 大目录场景性能验证

---

## 11. 验收标准

- `DirEntryRef` 不再作为 tree 长期定位手段
- `DirChildren` value 为 `EntryKey`
- `FsDir` 持有 `root_key` 和 `EntryArena`
- `InodePath` 仅为目录段缓存 `entry_key`
- 文件叶子不再需要 tree-level key
- `create_tree()` 的遍历过程不再缓存裸指针
- 目录 create/delete/rename 基于 `EntryKey`
- 文件 create/delete/rename 基于 `parent + name + inode id`
- hardlink 场景下 file identity 继续由 inode id + edge 关系表达
- 恢复、创建、删除、重命名相关测试全部通过