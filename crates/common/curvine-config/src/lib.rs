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

mod dir_space_conf;
pub use self::dir_space_conf::{
    DirSpaceConf, DIR_SPACE_ENABLED_RUNTIME_KEY, DIR_SPACE_TRACKED_PATHS_RUNTIME_KEY,
};

mod master_conf;
pub use self::master_conf::{ChildrenContainer, MasterConf};

mod mds_conf;
pub use self::mds_conf::{KvBackendType, MdsConf};

mod compatibility_conf;
pub use self::compatibility_conf::CompatibilityConf;

mod discovery_conf;
pub use self::discovery_conf::DiscoveryConf;

mod worker_conf;
pub use self::worker_conf::{WorkerConf, WorkerDataDir};

mod observability_conf;
pub use self::observability_conf::*;

mod web_conf;
pub use self::web_conf::*;

mod profiling_conf;
pub use self::profiling_conf::ProfilingConf;

mod cluster_conf;
pub use self::cluster_conf::{ClusterConf, S3GatewayConf};

mod cli_conf;
pub use self::cli_conf::CliConf;

mod client_conf;
pub use self::client_conf::{ClientConf, ClientConfCliOverrides};

mod fuse_conf;
pub use self::fuse_conf::{FuseConf, FuseConfCliOverrides};

mod job_conf;
pub use self::job_conf::JobConf;

mod transfer_conf;
pub use self::transfer_conf::*;

mod ufs_conf;
pub use self::ufs_conf::{UfsConf, UfsConfBuilder};

/// Unified layered config-loading pipeline (file → env → cli).
pub mod pipeline;
pub use pipeline::{ConfigLoader, ConfigSource, ConfigSourceMap, DiscoveredPath};

/// Unified validation framework (per-section validate + unknown-key audit).
pub mod validation;

/// Process-scoped effective configuration view (overlay + provenance).
pub mod config_view;
pub use config_view::{effective_config_view, ConfigView, RuntimeEntryView, ViewProfile};

/// Hot-reload extension points (no watcher/RPC wired yet — see module docs).
pub mod reload;

/// Process-local runtime config registry (admin push, non-persistent overrides).
pub mod runtime_registry;
pub use runtime_registry::{
    RuntimeConfigEntryView, RuntimeConfigRegistry, RuntimeConfigRegistryBuilder,
    RuntimeConfigUpdateResult,
};

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

    fn ttl_type(&self) -> curvine_model::TtlType {
        self.ttl_type
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
