//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use curvine_model::{FileStatus, FileType, StoragePolicy, INTERNAL_CTIME_XATTR};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::ops::Deref;
use std::sync::{Arc, Mutex, OnceLock};

/// Cap on distinct interned strings. Owner/group names are a tiny set in
/// practice; the cap only guards against pathological workloads that churn
/// unbounded distinct values through setattr.
const MAX_INTERNED_STRINGS: usize = 4096;

fn intern_pool() -> &'static Mutex<HashMap<String, Arc<str>>> {
    static POOL: OnceLock<Mutex<HashMap<String, Arc<str>>>> = OnceLock::new();
    POOL.get_or_init(|| Mutex::new(HashMap::new()))
}

fn intern(value: &str) -> Arc<str> {
    let mut pool = intern_pool()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = pool.get(value) {
        return existing.clone();
    }
    let shared: Arc<str> = Arc::from(value);
    if pool.len() < MAX_INTERNED_STRINGS {
        pool.insert(value.to_owned(), shared.clone());
    }
    shared
}

/// Reference-counted string that serializes as a plain string.
///
/// Owner/group names are interned: most inodes share the same handful of
/// values, so one heap copy is kept instead of one per inode. File names are
/// unique, so callers use [`SharedStr::owned`] to share one allocation between
/// the dentry key and the child inode without touching the pool.
///
/// The wrapper (16 bytes) is also smaller than `String` (24 bytes), which
/// matters because it appears in every `Inode` and `CachedStatus`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SharedStr(Arc<str>);

impl SharedStr {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Wrap an owned string without consulting the intern pool.
    pub fn owned(value: String) -> Self {
        Self(Arc::from(value))
    }

    /// Test-only: whether both handles refer to the same allocation.
    #[cfg(test)]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Borrow<str> for SharedStr {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for SharedStr {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl PartialOrd for SharedStr {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SharedStr {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.as_ref().cmp(other.0.as_ref())
    }
}

impl PartialEq<str> for SharedStr {
    fn eq(&self, other: &str) -> bool {
        &*self.0 == other
    }
}

impl PartialEq<&str> for SharedStr {
    fn eq(&self, other: &&str) -> bool {
        &*self.0 == *other
    }
}

impl Default for SharedStr {
    fn default() -> Self {
        Self(intern(""))
    }
}

impl Deref for SharedStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for SharedStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for SharedStr {
    fn from(value: &str) -> Self {
        Self(intern(value))
    }
}

impl From<String> for SharedStr {
    fn from(value: String) -> Self {
        Self(intern(&value))
    }
}

impl Serialize for SharedStr {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for SharedStr {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Ok(Self(intern(&value)))
    }
}

/// The dcache copy of a backend `FileStatus`.
///
/// Only the fields the FUSE daemon actually reads are kept. Dropped on
/// purpose: `path` and `name` (the dentry already stores them), `children_num`,
/// `replicas`, `block_size` and `storage_policy` (never read here). The
/// internal ctime is inlined instead of living in `x_attr`, and user extended
/// attributes stay in a side map that is `None` for ordinary files.
///
/// The dcache is persisted as JSON lines, so old state files (which carry a
/// full `FileStatus` under `status`) still deserialize: serde ignores the
/// unknown fields and `normalize_after_restore` lifts the ctime entry out of
/// the legacy `x_attr` map.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct CachedStatus {
    pub is_dir: bool,
    pub is_complete: bool,
    pub file_type: FileType,
    pub len: i64,
    pub mtime: i64,
    pub atime: i64,
    /// 0 means "not reported by the backend"; [`CachedStatus::ctime`] then
    /// falls back to `mtime`. Stored inline instead of `Option<i64>` (16 bytes)
    /// because the field is in every inode.
    pub ctime: i64,
    pub mode: u32,
    pub nlink: u32,
    pub owner: SharedStr,
    pub group: SharedStr,
    /// Boxed: symlink targets are rare, and the box keeps the field at 8 bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<Box<SharedStr>>,
    /// User-visible extended attributes. Master-internal keys are stripped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x_attr: Option<Box<HashMap<String, Vec<u8>>>>,
}

impl CachedStatus {
    pub fn from_status(status: FileStatus) -> Self {
        let FileStatus {
            is_dir,
            is_complete,
            file_type,
            len,
            mtime,
            atime,
            mode,
            nlink,
            owner,
            group,
            target,
            mut x_attr,
            ..
        } = status;

        let ctime = if x_attr.contains_key(INTERNAL_CTIME_XATTR) {
            x_attr
                .get(INTERNAL_CTIME_XATTR)
                .and_then(|bytes| bytes.as_slice().try_into().ok())
                .map(i64::from_le_bytes)
                .unwrap_or(mtime)
        } else {
            0
        };

        x_attr.remove(INTERNAL_CTIME_XATTR);
        x_attr.retain(|key, _| !key.starts_with('\0'));

        Self {
            is_dir,
            is_complete,
            file_type,
            len,
            mtime,
            atime,
            ctime,
            mode,
            nlink,
            owner: SharedStr::from(owner),
            group: SharedStr::from(group),
            target: target.map(|target| Box::new(SharedStr::from(target))),
            x_attr: (!x_attr.is_empty()).then(|| Box::new(x_attr)),
        }
    }

    /// Rebuild a full backend status for callers outside the dcache.
    ///
    /// `path` is deliberately left empty: the dcache no longer stores a copy
    /// per inode, and path-based callers use `DirTree::get_path` instead.
    pub fn to_file_status(&self, ino: u64, name: &str) -> FileStatus {
        let mut x_attr = self.x_attr.as_deref().cloned().unwrap_or_default();
        if self.ctime != 0 {
            x_attr.insert(
                INTERNAL_CTIME_XATTR.to_owned(),
                self.ctime.to_le_bytes().to_vec(),
            );
        }

        FileStatus {
            id: ino as i64,
            path: String::new(),
            name: name.to_owned(),
            is_dir: self.is_dir,
            mtime: self.mtime,
            atime: self.atime,
            children_num: 0,
            is_complete: self.is_complete,
            len: self.len,
            replicas: 0,
            block_size: 0,
            file_type: self.file_type,
            x_attr,
            storage_policy: StoragePolicy::default(),
            mode: self.mode,
            owner: self.owner.to_string(),
            group: self.group.to_string(),
            nlink: self.nlink,
            target: self.target.as_ref().map(|target| target.to_string()),
        }
    }

    pub fn ctime(&self) -> i64 {
        if self.ctime != 0 {
            self.ctime
        } else {
            self.mtime
        }
    }

    pub fn xattr(&self, name: &str) -> Option<&[u8]> {
        self.x_attr
            .as_deref()
            .and_then(|map| map.get(name))
            .map(Vec::as_slice)
    }

    /// Merge locally-applied setattr entries into the cached copy.
    ///
    /// `add_x_attr` may carry the internal ctime (the backend reports it as an
    /// xattr); it is lifted into the inline field instead of being stored.
    pub fn apply_xattr_changes(
        &mut self,
        mut add_x_attr: HashMap<String, Vec<u8>>,
        remove_x_attr: &[String],
    ) {
        if let Some(ctime) = add_x_attr.remove(INTERNAL_CTIME_XATTR) {
            if let Ok(bytes) = ctime.as_slice().try_into() {
                self.ctime = i64::from_le_bytes(bytes);
            }
        }

        if !add_x_attr.is_empty() || !remove_x_attr.is_empty() {
            let map = self.x_attr.get_or_insert_with(Default::default);
            map.extend(add_x_attr);
            for key in remove_x_attr {
                map.remove(key);
            }
            if map.is_empty() {
                self.x_attr = None;
            }
        }
    }

    /// Migrate the ctime out of a legacy state-file `x_attr` map.
    pub fn normalize_after_restore(&mut self) {
        let Some(map) = self.x_attr.as_deref_mut() else {
            return;
        };

        if self.ctime == 0 {
            if let Some(ctime) = map
                .get(INTERNAL_CTIME_XATTR)
                .and_then(|bytes| bytes.as_slice().try_into().ok())
                .map(i64::from_le_bytes)
            {
                self.ctime = ctime;
            }
        }

        map.remove(INTERNAL_CTIME_XATTR);
        map.retain(|key, _| !key.starts_with('\0'));
        if map.is_empty() {
            self.x_attr = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(name: &str) -> FileStatus {
        FileStatus {
            id: 42,
            path: "/a/b".to_owned(),
            name: name.to_owned(),
            is_dir: false,
            mtime: 1_000,
            atime: 900,
            children_num: 0,
            is_complete: true,
            len: 123,
            replicas: 1,
            block_size: 1024,
            file_type: FileType::File,
            x_attr: HashMap::from([(
                INTERNAL_CTIME_XATTR.to_owned(),
                1_234_i64.to_le_bytes().to_vec(),
            )]),
            mode: 0o644,
            owner: "root".to_owned(),
            group: "root".to_owned(),
            nlink: 1,
            ..Default::default()
        }
    }

    #[test]
    fn from_status_lifts_ctime_out_of_x_attr() {
        let cached = CachedStatus::from_status(status("f"));
        assert_eq!(cached.ctime(), 1_234);
        assert!(cached.x_attr.is_none());
        assert_eq!(cached.owner.as_str(), "root");
    }

    #[test]
    fn to_file_status_round_trips_visible_fields() {
        let cached = CachedStatus::from_status(status("f"));
        let restored = cached.to_file_status(7, "f");

        assert_eq!(restored.id, 7);
        assert_eq!(restored.name, "f");
        assert_eq!(restored.len, 123);
        assert_eq!(restored.mode, 0o644);
        assert_eq!(restored.owner, "root");
        assert_eq!(restored.ctime(), 1_234);
        assert_eq!(restored.x_attr.len(), 1);
        // Dropped fields are not carried by the dcache copy.
        assert!(restored.path.is_empty());
        assert_eq!(restored.block_size, 0);
    }

    #[test]
    fn user_xattrs_survive_round_trip() {
        let mut st = status("f");
        st.x_attr.insert("user.k".to_owned(), b"v".to_vec());

        let cached = CachedStatus::from_status(st);
        assert_eq!(cached.xattr("user.k"), Some(&b"v"[..]));
        assert_eq!(cached.xattr(INTERNAL_CTIME_XATTR), None);

        let restored = cached.to_file_status(7, "f");
        assert_eq!(restored.x_attr.get("user.k"), Some(&b"v".to_vec()));
        assert_eq!(restored.ctime(), 1_234);
    }

    #[test]
    fn apply_xattr_changes_updates_ctime_and_side_map() {
        let mut cached = CachedStatus::from_status(status("f"));
        cached.apply_xattr_changes(
            HashMap::from([
                (
                    INTERNAL_CTIME_XATTR.to_owned(),
                    2_000_i64.to_le_bytes().to_vec(),
                ),
                ("user.k".to_owned(), b"v".to_vec()),
            ]),
            &[INTERNAL_CTIME_XATTR.to_owned()],
        );

        assert_eq!(cached.ctime(), 2_000);
        assert_eq!(cached.xattr("user.k"), Some(&b"v"[..]));

        cached.apply_xattr_changes(HashMap::new(), &["user.k".to_owned()]);
        assert!(cached.x_attr.is_none());
    }

    #[test]
    fn legacy_x_attr_ctime_is_normalized_on_restore() {
        let mut x_attr = serde_json::Map::new();
        x_attr.insert(
            INTERNAL_CTIME_XATTR.to_owned(),
            serde_json::json!([1, 0, 0, 0, 0, 0, 0, 0]),
        );
        let legacy = serde_json::json!({
            "is_dir": false,
            "file_type": "File",
            "len": 5,
            "mtime": 10,
            "x_attr": x_attr,
            "owner": "u",
            "group": "g",
            "nlink": 1,
            "path": "/legacy",
            "name": "legacy",
            "block_size": 1024
        });

        let mut cached: CachedStatus = serde_json::from_value(legacy).unwrap();
        assert_eq!(cached.ctime, 0);
        cached.normalize_after_restore();
        assert_eq!(cached.ctime(), 1);
        assert!(cached.x_attr.is_none());
        assert_eq!(cached.len, 5);
    }

    #[test]
    fn shared_str_serializes_as_plain_string() {
        let value = SharedStr::from("root");
        assert_eq!(serde_json::to_string(&value).unwrap(), "\"root\"");
        let restored: SharedStr = serde_json::from_str("\"root\"").unwrap();
        assert_eq!(restored.as_str(), "root");
    }
}
