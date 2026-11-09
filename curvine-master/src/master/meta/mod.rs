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

pub mod feature;
pub mod inode;

mod object_lock_pool;

mod namespace_commit_gate;
pub use self::namespace_commit_gate::CommitGate;

mod inode_lock_manager;
pub use self::inode_lock_manager::{
    InodeLockManager, InodeLockMode, InodeLockRequest, InodeLockSet,
};

mod block_location_lock_manager;
pub use self::block_location_lock_manager::{BlockLocationLockManager, BlockLocationLockSet};

mod fs_dir;
pub use self::fs_dir::FsDir;
pub(crate) use self::fs_dir::{
    CacheInvalidationResult, RestoredTrashEntry, TrashListing, TrashScanPage,
};

mod fs_stats;
pub use self::fs_stats::FileSystemStats;

mod inode_id;
pub use self::inode_id::InodeId;

pub mod store;

mod block_meta;
pub use block_meta::BlockMeta;

mod glob_utils;
pub use glob_utils::*;

mod lock_meta;
pub use self::lock_meta::LockMeta;

pub mod trash;

#[cfg(test)]
mod tests;
