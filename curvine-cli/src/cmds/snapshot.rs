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

use crate::util::*;
use clap::{Parser, Subcommand};
use curvine_client_core::file::FsClient;
use curvine_core_error::CommonResult;
use std::sync::Arc;

#[derive(Parser, Debug)]
pub struct SnapshotCommand {
    #[command(subcommand)]
    action: SnapshotAction,
}

#[derive(Subcommand, Debug)]
pub enum SnapshotAction {
    /// Ask the active master to create a metadata snapshot now
    ///
    /// The checkpoint runs on the master and cannot be cancelled. On a large
    /// namespace it can exceed `client.rpc_timeout_ms` (120s by default) even
    /// though the snapshot succeeds; the retry then reports `a leader snapshot
    /// is already in progress`. Raise `client.rpc_timeout_ms` in the
    /// configuration file passed with `--conf` before running this on a large
    /// namespace.
    ///
    /// A call inside `snapshot_min_interval` is rejected; `--force` runs the
    /// checkpoint anyway, stalling metadata writes for its duration.
    #[command(name = "create")]
    Create {
        /// Create the checkpoint even if the last one is inside snapshot_min_interval
        #[arg(long)]
        force: bool,
    },
}

impl SnapshotCommand {
    pub async fn execute(&self, client: Arc<FsClient>) -> CommonResult<()> {
        match self.action {
            SnapshotAction::Create { force } => {
                let info = handle_rpc_result(client.create_leader_snapshot(force)).await;
                println!("snapshot dir: {}", info.dir);
                println!("checkpoint cost: {} ms", info.cost_ms);
                println!("journal truncation and checkpoint retention are applied asynchronously");
                Ok(())
            }
        }
    }
}
