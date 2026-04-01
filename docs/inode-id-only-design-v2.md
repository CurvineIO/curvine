# inode_view id-only 设计稿 (更新版)

## 1. 文档信息

- **主题**：inode 内存树改造为 id-only 模型
- **范围**：`curvine-server` master metadata 路径
- **当前状态**：meta-opt worktree 处于 main 分支状态，已引入 NamedEntry/NamedFile/NamedDir 结构
- **命名决策**：`DirEntry` 作为树节点 struct，`InodeView` 保持为 rich data enum (最小改动)

---

## 2. 核心类型定义

### 2.1 树层 (轻量)

```rust
/// 树节点 - 轻量，只负责拓扑导航
/// 类似文件系统的 dentry (directory entry) 概念
pub struct DirEntry {
    entry: InodeEntry,
    children: Option<Box<InodeChildren>>,
}

/// 轻量 entry enum - 只包含 id 和 kind
pub enum InodeEntry {
    File(i64),
    Dir(i64),
}

/// children 容器 - key 是名字，value 是树节点
pub struct InodeChildren {
    inner: BTreeMap<String, Box<DirEntry>>,
}
```

### 2.2 元数据层 (rich) - 保持现有含义

```rust
/// rich metadata enum - 保持现有含义，只有 Dir 和 File
/// 现有代码中 InodeView 的用法不改，最小改动
pub enum InodeView {
    Dir(Box<InodeDir>),
    File(Box<InodeFile>),
    // FileEntry variant 删除！
}

/// InodeDir - 纯 metadata，删除 children 字段
pub struct InodeDir {
    id: i64,
    parent_id: i64,
    mtime: i64,
    atime: i64,
    nlink: u32,
    storage_policy: StoragePolicy,
    features: DirFeature,
    // children 字段删除！
}

/// InodeFile - 纯 metadata，保持不变
pub struct InodeFile { ... }
```

### 2.3 指针类型

```rust
/// InodePtr - 指向 rich metadata (InodeView)，保持现有含义不变
pub type InodePtr = RawPtr<InodeView>;

/// 树节点引用 (用于导航)
pub type DirEntryRef = RawPtr<DirEntry>;
```

### 2.4 关键变化总结

| 项目 | 当前 | 新设计 |
|-----|------|-------|
| DirEntry | 不存在 | 树节点 struct (轻量，entry + children) |
| InodeEntry | 不存在 | `File(id) \| Dir(id)` enum |
| InodeView | rich data enum + FileEntry | rich data enum，删除 FileEntry variant |
| InodeDir.children | 存在 | 删除 |
| InodePtr | 指向 InodeView | 指向 InodeView (不变) |
| NamedFile/NamedDir/NamedEntry | 存在 | 删除 (名字只在 children key) |

---

## 3. 元数据架构逻辑

### 3.1 分层架构

```
┌─────────────────────────────────────────────────────────────┐
│                      业务层 (FsDir)                          │
│  file_status / list_status / set_attr / create / delete     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    路径解析层 (InodePath)                     │
│  resolve(path) → Vec<InodePtr> (指向 InodeView rich data)    │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌──────────────────────────┐    ┌──────────────────────────────┐
│     树层 (DirEntry)       │    │    存储层 (InodeStore)        │
│  - 拓扑导航               │    │  - RocksDB 持久化            │
│  - children 关系          │    │  - get_inode(id) → InodeView │
│  - entry: File(id)/Dir(id)│    │  - edges: parent→child 映射  │
│  - 轻量，内存常驻         │    │                              │
└──────────────────────────┘    └──────────────────────────────┘
                                              │
                                              ▼
                              ┌──────────────────────────────┐
                              │      RocksDB CFs             │
                              │  - inode_cf: id → InodeView  │
                              │  - edge_cf: (parent,name)→id │
                              │  - block_cf: block locations │
                              └──────────────────────────────┘
```

### 3.2 内存树 vs RocksDB 存储

**内存树 (DirEntry)**：
- 只存储拓扑关系：parent → children
- 每个节点只有 `{ entry, children }`
- entry = `File(id)` 或 `Dir(id)`，无 rich metadata
- 启动时从 RocksDB edges 构建完整拓扑

**RocksDB 存储**：
- **inode_cf**：`id → serialized(InodeView)` (rich metadata)
- **edge_cf**：`(parent_id, name) → child_id` (拓扑关系)
- **block_cf**：block locations

### 3.3 数据流向

```
启动恢复:
  RocksDB edge_cf → 遍历所有 (parent, name) → child_id
                  → 构建完整 DirEntry 树 (轻量)

路径解析:
  DirEntry.children["name"] → 找到 child DirEntry
  DirEntry.entry → 取得 id (和 kind)
  InodeStore.get_inode(id) → RocksDB inode_cf → InodeView
  InodeView → 存入 InodePath.inodes

业务操作:
  InodePath.get_last_inode() → InodePtr (指向 InodeView)
  InodePtr.as_file_ref() → &InodeFile (rich metadata)
  修改后 → InodeStore.apply_xxx() → 写入 RocksDB
```

### 3.4 关键操作流程

#### create_tree() - 启动恢复树拓扑

```
1. 加载 root: store.get_inode(ROOT_ID) → InodeView::Dir
2. 转换为树节点: DirEntry { entry: Dir(ROOT_ID), children: Some(...) }

3. BFS/DFS 遍历 edge_cf:
   for each (parent_id, child_name) → child_id:
     - 从 store 加载 child metadata 判断 kind
     - 构造 DirEntry: 
       - 文件: DirEntry { entry: File(id), children: None }
       - 目录: DirEntry { entry: Dir(id), children: Some(...) }
     - 添加到 parent 的 children map

4. 结果: 完整的 DirEntry 树，每个节点只有 id + children
```

#### resolve(path) - 路径解析

```
输入: path = "/a/b/c"
持有: root DirEntry (树根)

1. 分解路径: components = ["", "a", "b", "c"]

2. 从 root 开始逐层导航:
   cur = root
   for name in components (skip root):
     cur = cur.children.get(name)  // 从 DirEntry 树导航
     if cur == None: break (路径不存在)

     // 每一步都加载 rich data 存入 InodePath
     view = store.get_inode(cur.entry.id())
     inodes.push(InodePtr::from(view))

3. 返回 InodePath { path, components, inodes: Vec<InodePtr> }
   - inodes 全部是 rich data (InodeView)
   - 可直接用于业务操作
```

#### file_status(path) - 获取文件状态

```
1. resolve(path) → InodePath
2. inode = path.get_last_inode()  // InodePtr → InodeView
3. match inode:
     InodeView::File(f) → 构建 FileStatus from InodeFile
     InodeView::Dir(d)  → 构建 FileStatus from InodeDir
4. 返回 FileStatus
```

#### create_file(path) - 创建文件

```
1. resolve(parent_path) → InodePath (到父目录)
2. parent = path.get_last_inode().as_dir_mut()  // &mut InodeDir

3. 创建新 InodeFile (rich metadata)
   分配新 id

4. 更新内存树 (DirEntry):
   parent_entry.children.add_child(
     DirEntry { entry: File(new_id), children: None }
   )

5. 持久化到 RocksDB:
   batch.write_inode(new_file)       // inode_cf (写 InodeView)
   batch.add_child(parent.id, name, new_id)  // edge_cf
   batch.write_inode(parent)         // 更新 parent mtime
   batch.commit()
```

#### create 路径的已知风险（meta-opt 当前发现）

当前 `meta-opt` 分支已经发现一个与 create 路径强相关的回归：

- 在 `/curvine-fuse` 执行 `cp /tmp/1.txt .` 会返回 `EIO`
- 但文件节点会被成功创建出来，目录下留下空的 `/1.txt`
- `master.out` 显示 `OpenFile [WCT]/1.txt`、`AddBlock`、`CompleteFile` 都成功
- `fuse.out` 只有 writer 的创建/关闭日志，没有出现 write 日志

这说明 create 后返回给 FUSE 的文件状态/路径语义存在一致性风险，导致后续没有进入预期的 write 路径。

**重要约束**：

- 问题区间内没有 `curvine-fuse/` 代码变更
- 回归范围应优先锁定在 metadata 改造后的 create 链路
- 尤其是 `FsDir::add_last_inode()` 与 `InodePath::ResolvedInode` 对 tree entry / inode metadata 的同步语义

后续如果继续演进此设计，必须重点验证：

1. create 返回的 `InodePath` 是否完整且自洽
2. `get_parent_entry()` 在部分解析路径下是否始终指向真实父目录
3. 新 child 插入树后拿到的 `child_entry_ref` 是否与最终持久化状态严格一致
4. `file_status()` 返回的 `path/id/name/type/len` 是否与刚创建节点完全一致
5. 详细调查记录：`docs/meta-opt-fuse-create-regression.md`

#### delete(path) - 删除节点

```
1. resolve(path) → InodePath

2. 从内存树删除 (递归):
   parent_entry.children.delete(name)
   if 是目录: 递归删除所有 children 的 DirEntry

3. 持久化删除:
   batch.delete_child(parent.id, name)  // edge_cf
   batch.delete_inode(inode.id)         // inode_cf (递归)
   batch.commit()
```

### 3.5 InodeStore 接口设计

```rust
pub struct InodeStore {
    store: Arc<RocksInodeStore>,  // RocksDB 操作
    fs_stats: Arc<FileSystemStats>,
    ttl_bucket_list: Arc<TtlBucketList>,
    // 不再持有 DirEntry 树！树在 FsDir 中
}

impl InodeStore {
    /// 从 RocksDB 加载 rich metadata
    pub fn get_inode(&self, id: i64) -> CommonResult<Option<InodeView>>

    /// 恢复树拓扑 (返回轻量 DirEntry 树)
    pub fn create_tree(&self) -> CommonResult<DirEntry>

    /// 遍历目录的所有 child id (从 edge_cf)
    pub fn edges_iter(&self, parent_id: i64) -> impl Iterator<Item=(String, i64)>

    /// 写入 rich metadata (InodeView)
    pub fn apply_add(&self, parent: &InodeDir, child: &InodeView) -> CommonResult<()>
    pub fn apply_delete(&self, parent: &InodeDir, child: &InodeView) -> CommonResult<()>
    // ...
}
```

### 3.6 FsDir 持有树和 Store

```rust
pub struct FsDir {
    root: DirEntry,        // 内存树根 (轻量拓扑)
    store: InodeStore,     // RocksDB 存储
    lock: RwLock<()>,      // 保护树和 store 的一致性
}

impl FsDir {
    pub fn create_tree(&mut self) {
        self.root = self.store.create_tree()?;
    }

    pub fn resolve(&self, path: &str) -> CommonResult<InodePath> {
        InodePath::resolve(&self.root, path, &self.store)
    }
}
```

### 3.7 数据一致性

**写入顺序**：
1. 先写 RocksDB (inode + edge)
2. 再更新内存树 (DirEntry)

**恢复顺序**：
1. 从 RocksDB edge_cf 构建树拓扑 (DirEntry)
2. 按需从 inode_cf 加载 metadata (InodeView)

**锁保护**：
- FsDir 的 RwLock 保护树和 store 的原子更新
- 单个操作内：先拿锁 → 改 store → 改树 → 释放锁

---

## 4. 设计原则

### 原则 1：树节点与 inode metadata 分层
- 树节点 (DirEntry)：负责拓扑与递归
- inode metadata (InodeView)：负责 owner/group/mode/nlink/mtime/xattr 等 rich 信息

### 原则 2：名字属于 edge，不属于 node/entry
- child 的名字只存在于 `DirEntry.children` 的 key
- 不在 DirEntry 或 InodeView 中重复存储 name
- 删除 NamedFile/NamedDir/NamedEntry wrapper

### 原则 3：目录树必须可递归展开
- children 里必须存 DirEntry，不能只存 InodeEntry
- 否则目录 child 会丢失 subtree

### 原则 4：内存树上只保留最小必要信息
- DirEntry 里只保留 inode id、inode kind、children 指针

### 原则 5：tree 负责导航，store 负责元数据
- resolve/DFS/BFS/递归操作：主要依赖 DirEntry 树
- file_status/ACL/storage_policy：按 id 去 InodeStore 取 InodeView

---

## 5. 实施顺序

### Phase 1：定义新类型
1. 定义 `DirEntry` struct (entry + children)
2. 定义 `InodeEntry` enum (`File(id) | Dir(id)`)
3. 定义 `InodeChildren` struct
4. 从 `InodeView` 删除 `FileEntry` variant
5. 从 `InodeDir` 删除 children 字段
6. 删除 `NamedFile`、`NamedDir`、`NamedEntry` wrapper

### Phase 2：修改 InodeStore
1. `get_inode()` 返回 `Option<InodeView>` (不变，但无 FileEntry)
2. 重写 `create_tree()` 返回 `DirEntry` 树

### Phase 3：修改 InodePath
1. `resolve()` 使用 `DirEntry` 树导航
2. `inodes` 存储 `Vec<InodePtr>` (指向 InodeView，全 rich data)
3. 移除 FileEntry 相关逻辑

### Phase 4：修改业务层
1. `FsDir.root` 改为 `DirEntry`
2. 所有树操作用 `DirEntry`
3. 所有 metadata 操作用 `InodeView` (不变)

### Phase 5：清理与测试
1. 删除 `NamedFile`、`NamedDir`、`NamedEntry`
2. 删除 `InodeView::FileEntry` variant
3. 全量回归测试

---

## 6. 验收标准

- `DirEntry` 是 struct，包含 `entry: InodeEntry` 和 `children`
- `InodeEntry` 只有 `File(i64)` 和 `Dir(i64)` 两个 variant
- `InodeView` 只有 `Dir` 和 `File` 两个 variant，无 `FileEntry`
- `InodeDir` 无 children 字段
- `InodeChildren` value 是 `Box<DirEntry>`，key 是名字
- `InodePtr` 指向 `InodeView` (不变)
- `InodePath.inodes` 全部是 rich data (InodeView)
- `FsDir.root` 是 `DirEntry`
- `create_tree()` 构造 `DirEntry` 轻量拓扑树
- `resolve()` 用 `DirEntry` 导航，结果存 `InodeView`
- 所有测试通过