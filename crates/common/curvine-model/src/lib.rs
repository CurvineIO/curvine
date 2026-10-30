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

pub use curvine_error::{FsError, FsResult, MAX_FILE_SIZE};

pub const UFS_INODE_ID: i64 = 0;
pub const DEFAULT_FILE_SYSTEM_MODE: u32 = 0o777;

pub trait ClientConfDefaults {
    fn replicas(&self) -> i32;
    fn block_size(&self) -> i64;
    fn storage_type(&self) -> StorageType;
    fn ttl_ms(&self) -> i64;
    fn ttl_action(&self) -> TtlAction;
    /// Base timestamp for TTL expiration; defaults to mtime (legacy behaviour).
    fn ttl_type(&self) -> TtlType {
        TtlType::Mtime
    }
    fn mode(&self) -> u32;
}

pub trait CurvinePath: Sized + std::fmt::Display {
    fn is_cv(&self) -> bool;
    fn path(&self) -> &str;
    fn full_path(&self) -> &str;
    fn from_str(path: impl AsRef<str>) -> curvine_core_error::CommonResult<Self>;
}

mod storage_info;
pub use self::storage_info::*;

mod health;
pub use self::health::*;

mod worker_address;
pub use self::worker_address::WorkerAddress;

mod heartbeat_status;
pub use self::heartbeat_status::HeartbeatStatus;

mod worker_info;
pub use self::worker_info::{TransferWorkerCapabilities, WorkerInfo};

mod worker_node_tree;
pub use self::worker_node_tree::WorkerNodeTree;

mod file_type;
pub use self::file_type::{
    is_special_file_type, FileType, FS_APPEND_FL, FS_IMMUTABLE_FL, IFLAGS_XATTR, MKNOD_RDEV_XATTR,
};

mod ttl_action;
pub use self::ttl_action::TtlAction;

mod ttl_type;
pub use self::ttl_type::TtlType;

mod client_address;
pub use self::client_address::ClientAddress;

mod client_context;
pub use self::client_context::*;

mod storage_policy;
pub use self::storage_policy::*;

mod block_info;
pub use self::block_info::*;

mod file_status;
pub use self::file_status::{FileStatus, INTERNAL_CTIME_XATTR, INTERNAL_TTL_TYPE_XATTR};

mod filesystem_info;
pub use self::filesystem_info::FilesystemInfo;

mod leader_snapshot;
pub use self::leader_snapshot::LeaderSnapshotInfo;

mod worker_command;
pub use self::worker_command::*;

mod last_block_status;
pub use self::last_block_status::LastBlockStatus;

mod worker_status;
pub use self::worker_status::WorkerStatus;

mod mount;
pub use self::mount::*;

mod posix_permission;
pub use self::posix_permission::*;

mod opts;
pub use self::opts::*;

mod file_gen;
pub use self::file_gen::{FileGen, CV_GEN_METADATA_KEY};

mod job;
pub use self::job::*;

mod transfer;
pub use self::transfer::*;

mod metrics;
pub use self::metrics::*;

mod runtime_config;
pub use self::runtime_config::*;

mod open_flags;
pub use self::open_flags::*;

mod rename_flags;
pub use self::rename_flags::*;

mod file_alloc;
pub use self::file_alloc::*;

mod file_lock;
pub use self::file_lock::*;

mod content_summary;
pub use self::content_summary::{
    ContentSummary, ContentSummarySnapshot, ContentSummarySource, DirSpaceStatus,
};

mod result;
pub use self::result::*;

mod trash_entry;
pub use self::trash_entry::*;

mod proto_utils;
pub use self::proto_utils::ProtoUtils;

mod compatibility;
pub use self::compatibility::{
    feature_enabled, CompatibilityMode, CompatibilityPolicy, CompatibilityVerdict,
};

mod display;

pub mod error {
    pub use curvine_error::*;
}

pub mod proto {
    pub use curvine_proto::*;
}

pub mod state {
    pub use super::*;
}

pub mod utils {
    pub use super::ProtoUtils;
}
