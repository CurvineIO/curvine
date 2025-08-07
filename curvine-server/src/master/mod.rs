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

use crate::master::fs::WorkerManager;
use crate::master::journal::JournalLoader;
use crate::master::meta::FsDir;
use curvine_common::raft::storage::RocksLogStorage;
use curvine_common::raft::RaftJournal;
use orpc::sync::ArcRwLock;

pub mod meta;

mod load;
pub use self::load::*;

mod master_server;
pub use self::master_server::*;

mod master_handler;
pub use self::master_handler::*;

pub mod fs;

pub mod journal;

mod master_monitor;
pub use self::master_monitor::*;

mod master_metrics;
pub use self::master_metrics::*;

mod router_handler;
pub use self::router_handler::*;

mod rpc_context;
pub use rpc_context::RpcContext;

pub mod mount;

pub type MetaRaftJournal = RaftJournal<RocksLogStorage, JournalLoader>;
pub type SyncFsDir = ArcRwLock<FsDir>;
pub type SyncWorkerManager = ArcRwLock<WorkerManager>;
pub use mount::MountManager;
