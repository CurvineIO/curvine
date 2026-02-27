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

use raft::eraftpb;

mod raft_node;
pub use self::raft_node::RaftNode;

mod raft_client;
pub use self::raft_client::RaftClient;

mod raft_code;
pub use self::raft_code::RaftCode;

mod raft_server;
pub use self::raft_server::RaftServer;

pub mod storage;

mod raft_journal;
pub use self::raft_journal::RaftJournal;

mod raft_group;
pub use self::raft_group::RaftGroup;

// Re-export RaftPeer from conf module for backward compatibility
pub use crate::conf::{NodeId, RaftPeer};

mod raft_error;
pub use self::raft_error::RaftError;

mod role_monitor;
pub use self::role_monitor::*;

pub mod snapshot;

mod raft_utils;
pub use self::raft_utils::RaftUtils;

pub type RaftResult<T> = Result<T, RaftError>;

pub type LibRaftResult<T> = raft::Result<T>;

pub type LibRaftMessage = eraftpb::Message;

// The default leader id means that the current cluster has no leader generated.
pub const DEFAULT_LEADER_ID: u64 = 0;

pub const LOG_START_INDEX: u64 = 0;
