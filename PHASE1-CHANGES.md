# Phase 1 改动点整理

## 设计原则回顾

1. **InodeView** = 树节点，只存 `InodeEntry` + `children`
2. **InodeEntry** = 轻量 enum `{File(i64), Dir(i64)}`，不带 name
3. **Name** 由 edge key 表示，不在 InodeView 中
4. **Rich metadata** (InodeFile/InodeDir) 由 InodeStore 按需提供
5. **Tree 负责导航，Store 负责元数据**

---

## 一、InodeView 需要移除/改造的方法

| 方法 | 当前状态 | 设计要求 | 处理方式 |
|------|----------|----------|----------|
| `name()` | 已移除 | name 由 edge key 提供 | 调用方需要从其他来源获取 name |
| `to_file_status()` | 已移除 | 需要 rich metadata | 移到 InodeStore，或由 fs_dir 调用 store |
| `as_file_ref/mut()` | 已移除 | 需要 InodeFile | 移到 InodeStore |
| `as_dir_ref/mut()` | 已移除 | 需要 InodeDir | 移到 InodeStore |
| `mtime()` | 已移除 | 需要 metadata | 由 store 提供 |
| `update_mtime()` | 已移除 | 需要 metadata | 由 store 提供 |
| `incr_nlink()` | 已移除 | 需要 metadata | 由 store 提供 |
| `set_parent_id()` | 已移除 | 需要 metadata | 由 store 提供 |
| `is_link()` | 已移除 | 需要 InodeFile | 由 store 提供 |
| `is_file_entry()` | 已移除 | 不再需要 | InodeView 不是 enum，没有 Entry variant |
| `acl()` / `acl_mut()` | 已移除 | 需要 metadata | 由 store 提供 |
| `storage_policy()` | 已移除 | 需要 metadata | 由 store 提供 |
| `x_attr()` | 已移除 | 需要 metadata | 由 store 提供 |
| `nlink()` | 已移除 | 需要 metadata | 由 store 提供 |
| `set_attr()` | 已移除 | 需要 metadata | 由 store 提供 |
| `expiration_ms()` | 已移除 | 需要 metadata | 由 store 提供 |
| `is_expired()` | 已移除 | 需要 metadata | 由 store 提供 |
| `change_name()` | 已移除 | name 由 edge key 表示 | rename 操作修改 edge key |

## 二、InodeView 需要新增/保留的方法

| 方法 | 说明 |
|------|------|
| `id()` | ✅ 已有，返回 `entry.id()` |
| `is_dir()` | ✅ 已有，返回 `entry.is_dir()` |
| `is_file()` | ✅ 已有，返回 `entry.is_file()` |
| `entry()` | ✅ 已有，返回 `InodeEntry` |
| `get_child(name)` | ✅ 已有 |
| `get_child_ptr(name)` | ✅ 已有 |
| `add_child(name, child)` | ✅ 已有，需要 name 参数 |
| `delete_child(name)` | ✅ 已有，需要 name 参数 |
| `children_iter()` | ✅ 已有，返回 `(name, &InodeView)` |
| `child_len()` | ✅ 已有 |
| `get_child_ptr_by_glob_pattern()` | ✅ 已有 |
| `print_tree()` | ✅ 已有 |
| `new_file(id)` | ✅ 已有，只接受 id |
| `new_dir(id)` | ✅ 已有，只接受 id |

---

## 三、需要修改的文件及改动点

### 3.1 `fs_dir.rs` (55 处错误)

**核心问题**：大量方法假设 `InodeView` 携带 rich metadata

**改动点**：

| 方法 | 改动说明 |
|------|----------|
| `mkdir()` | `InodeView::new_dir(name, dir)` → `InodeView::new_dir(dir.id)`，name 作为 edge key |
| `delete()` | 无法直接访问 `file.nlink()`，需从 store 获取 InodeFile |
| `unprotected_delete()` | `File(_, file)` 模式匹配失效，需从 store 获取 |
| `rename()` | `File(name, file)` → 需要从 store 获取，name 从 InodePath 获取 |
| `create_file()` | `InodeView::new_file(name, file)` → `InodeView::new_file(file.id)` |
| `add_last_inode()` | `parent.add_child(child)` → `parent.add_child(&child_name, child)` |
| `file_status()` | `inode.to_file_status()` → `store.file_status(inode.id())` |
| `list_status()` | 遍历 children，对每个节点从 store 获取 metadata |
| `list_options()` | 同上 |
| `unprotected_set_attr()` | 需要从 store 获取 InodeFile/InodeDir 后修改 |
| `unprotected_free()` | 同上 |
| `create_hardlink()` | 需要 InodeFile 的 nlink，从 store 获取 |
| `sum_hash()` | 需要 InodeFile/InodeDir 的完整信息 |

**关键改造**：
```rust
// 旧写法
let file = inode.as_file_ref()?;

// 新写法
let file = store.get_inode_file(inode.id())?;
```

### 3.2 `inode_store.rs` (25 处错误)

**核心问题**：`create_tree()` 和 `get_inode()` 的返回值

**改动点**：

| 方法 | 改动说明 |
|------|----------|
| `create_tree()` | 返回 `InodeView` 树而非 rich inode tree |
| `get_inode()` | 返回 `NamedFile/NamedDir`（rich metadata）|
| `get_inode_file(id)` | **新增**：返回 `InodeFile` |
| `get_inode_dir(id)` | **新增**：返回 `InodeDir` |
| `file_status(id, name, path)` | **新增**：构建 FileStatus |
| `apply_add()` | 需要适配新的 children 结构 |
| `apply_delete()` | 同上 |

### 3.3 `master_filesystem.rs` (11 处错误)

**改动点**：

| 方法 | 改动说明 |
|------|----------|
| `file_status()` | 调用 `store.file_status()` |
| `get_file_inode()` | 返回 `InodeFile`，从 store 获取 |
| `get_file_blocks()` | 需要 `to_file_status()`，改为 store 方法 |

### 3.4 `journal_loader.rs` (8 处错误)

**核心问题**：Journal replay 假设 rich inode

**改动点**：

| 方法 | 改动说明 |
|------|----------|
| `apply_mkdir()` | `InodeView::new_dir(name, dir)` → `InodeView::new_dir(dir.id)` |
| `apply_create_file()` | 同上 |
| `apply_add_block()` | 需要 `as_file_mut()`，从 store 获取 |
| `apply_complete_file()` | 同上 |

**重要**：Journal 记录的是 rich metadata，replay 时需要：
1. 写入 store（持久化 rich metadata）
2. 更新树（只更新 topology）

### 3.5 `ttl_bucket.rs` / `ttl_executor.rs` (11 处错误)

**改动点**：

| 文件 | 改动说明 |
|------|----------|
| `ttl_bucket.rs` | `file_with_ttl()` 构造需要适配新 InodeView |
| `ttl_executor.rs` | `find_path_in_tree()` 遍历树需要从 children 获取 name |

### 3.6 `quota_manager.rs` (2 处错误)

**改动点**：`InodeView::File(f) => f.len` 需要从 store 获取 file length

---

## 四、InodeStore 需要新增的 API

```rust
impl InodeStore {
    /// Get InodeFile by id (rich metadata)
    pub fn get_inode_file(&self, id: i64) -> CommonResult<Option<InodeFile>>;
    
    /// Get InodeDir by id (rich metadata)
    pub fn get_inode_dir(&self, id: i64) -> CommonResult<Option<InodeDir>>;
    
    /// Build FileStatus from inode id + name + path
    pub fn file_status(&self, id: i64, name: &str, path: &str) -> FsResult<FileStatus>;
    
    /// Update mtime on inode metadata
    pub fn update_mtime(&self, id: i64, mtime: i64) -> FsResult<()>;
    
    /// Increment nlink on file metadata
    pub fn incr_nlink(&self, id: i64) -> FsResult<()>;
    
    /// Apply set_attr to inode metadata
    pub fn apply_set_attr(&self, id: i64, opts: SetAttrOpts) -> FsResult<()>;
}
```

---

## 五、策略选择

### 策略 A：增量修复（推荐）

1. 先在 `InodeStore` 添加上述新 API
2. 让 `fs_dir.rs` 等调用 store 方法
3. 逐步让编译通过
4. 每个 phase 验证测试

### 策略 B：彻底重构

1. 不添加兼容方法
2. 所有需要 metadata 的代码直接改写为 store 调用
3. 工作量大但更干净

---

## 六、预估工作量

| 文件 | 改动行数预估 | 复杂度 |
|------|-------------|--------|
| `inode_store.rs` | ~100 行 | 中 |
| `fs_dir.rs` | ~200 行 | 高 |
| `journal_loader.rs` | ~50 行 | 中 |
| `master_filesystem.rs` | ~30 行 | 低 |
| `ttl_*.rs` | ~30 行 | 低 |
| `quota_manager.rs` | ~10 行 | 低 |
| **总计** | **~420 行** | |