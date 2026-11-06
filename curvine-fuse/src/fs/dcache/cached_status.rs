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
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct CachedStatus {
    pub is_dir: bool,
    pub is_complete: bool,
    pub file_type: FileType,
    pub len: i64,
    pub mtime: i64,
    pub atime: i64,
    pub ctime: i64,
    pub mode: u32,
    pub nlink: u32,
    pub owner: Box<str>,
    pub group: Box<str>,
    pub target: Option<Box<str>>,
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
            owner: owner.into_boxed_str(),
            group: group.into_boxed_str(),
            target: target.map(String::into_boxed_str),
            x_attr: (!x_attr.is_empty()).then(|| Box::new(x_attr)),
        }
    }

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
            target: self.target.as_deref().map(str::to_owned),
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
        assert_eq!(&*cached.owner, "root");
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
    fn symlink_target_round_trips_as_a_plain_string() {
        let mut st = status("link");
        st.file_type = FileType::Link;
        st.target = Some("../../dest".to_owned());

        let cached = CachedStatus::from_status(st);
        let json = serde_json::to_value(&cached).unwrap();
        assert_eq!(json["target"], serde_json::json!("../../dest"));

        let restored: CachedStatus = serde_json::from_value(json).unwrap();
        let status = restored.to_file_status(9, "link");
        assert_eq!(status.target.as_deref(), Some("../../dest"));
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
}
