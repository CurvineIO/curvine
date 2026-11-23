// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use curvine_core_error::CommonResult;
use curvine_rocksdb::DBConf;
use curvine_runtime::common::Utils;
use std::io::Write;
use std::path::{Path, PathBuf};

mod inode_store;
pub use self::inode_store::{InodeStore, OwnerKind, PurgeItem, PurgeTarget};

mod rocks_inode_store;
pub use self::rocks_inode_store::*;

#[cfg(test)]
mod client_owner_tests;

/// Durable marker left in the metadata base dir while the derived store is
/// being discarded. A crash mid-reset would otherwise leave a metadata dir that
/// startup validation refuses (the journal is still a valid RocksDB store).
pub(crate) const META_REBUILD_MARKER: &str = "metadata-rebuild-in-progress";

pub(crate) fn meta_rebuild_marker(conf: &DBConf) -> PathBuf {
    Path::new(&conf.base_dir).join(META_REBUILD_MARKER)
}

pub(crate) fn write_meta_rebuild_marker(conf: &DBConf) -> CommonResult<()> {
    let marker = meta_rebuild_marker(conf);
    let parent = marker.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;

    let tmp = parent.join(format!(".{META_REBUILD_MARKER}.tmp-{}", Utils::rand_id()));
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        file.write_all(b"metadata rebuild in progress\n")?;
        file.sync_all()?;
    }
    let linked = std::fs::hard_link(&tmp, &marker);
    let _ = std::fs::remove_file(&tmp);

    match linked {
        Ok(()) => {
            // Directory fsync is not portable (e.g. Windows rejects opening a
            // directory); the marker file itself is already synced.
            let _ = std::fs::File::open(parent).and_then(|f| f.sync_all());
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn clear_meta_rebuild_marker(conf: &DBConf) -> CommonResult<()> {
    let marker = meta_rebuild_marker(conf);
    match std::fs::remove_file(&marker) {
        Ok(()) => {
            if let Some(parent) = marker.parent() {
                let _ = std::fs::File::open(parent).and_then(|f| f.sync_all());
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
