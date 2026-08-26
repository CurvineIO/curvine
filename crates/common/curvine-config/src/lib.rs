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

pub use curvine_error::{FsError, FsResult};

pub const DEFAULT_HOSTNAME: &str = "localhost";

mod db_conf;
pub use self::db_conf::DBConf;

mod raft_peer;
pub use self::raft_peer::RaftPeer;

mod journal_conf;
pub use self::journal_conf::JournalConf;

mod master_conf;
pub use self::master_conf::MasterConf;

mod compatibility_conf;
pub use self::compatibility_conf::CompatibilityConf;

mod worker_conf;
pub use self::worker_conf::{WorkerConf, WorkerDataDir};

mod cluster_conf;
pub use self::cluster_conf::ClusterConf;

mod cli_conf;
pub use self::cli_conf::CliConf;

mod client_conf;
pub use self::client_conf::{ClientConf, ClientConfCliOverrides};

mod fuse_conf;
pub use self::fuse_conf::FuseConf;

mod job_conf;
pub use self::job_conf::JobConf;

mod transfer_conf;
pub use self::transfer_conf::*;

mod ufs_conf;
pub use self::ufs_conf::{UfsConf, UfsConfBuilder};

/// Unified layered config-loading pipeline (file → env → cli).
pub mod pipeline;
pub use pipeline::{ConfigLoader, DiscoveredPath};

/// Unified validation framework (per-section validate + unknown-key audit).
pub mod validation;

impl curvine_model::ClientConfDefaults for ClientConf {
    fn replicas(&self) -> i32 {
        self.replicas
    }

    fn block_size(&self) -> i64 {
        self.block_size
    }

    fn storage_type(&self) -> curvine_model::StorageType {
        self.storage_type
    }

    fn ttl_ms(&self) -> i64 {
        self.ttl_ms
    }

    fn ttl_action(&self) -> curvine_model::TtlAction {
        self.ttl_action
    }

    fn mode(&self) -> u32 {
        self.get_mode()
    }
}

pub mod conf {
    pub use super::*;
}

pub mod fs {
    pub use curvine_fs_api::*;
}

pub mod state {
    pub use curvine_model::*;
}
