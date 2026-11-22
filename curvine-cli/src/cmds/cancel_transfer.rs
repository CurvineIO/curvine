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

use clap::Parser;
use curvine_core_error::CommonResult;
use curvine_model::TransferState;
use curvine_transfer_client::TransferClient;

use crate::util::*;

#[derive(Parser, Debug)]
pub struct CancelTransferCommand {
    job_id: String,
}

impl CancelTransferCommand {
    pub async fn execute_transfer_only(&self, transfer_client: TransferClient) -> CommonResult<()> {
        if self.job_id.trim().is_empty() {
            eprintln!("Error: Job ID cannot be empty");
            std::process::exit(1);
        }

        println!("\nCancelling transfer job");
        println!("┌─────────────────────────────────");
        println!("│ Job ID: {}", self.job_id);
        let response = handle_rpc_result(transfer_client.cancel(&self.job_id, None)).await;
        print_transfer_cancel_result(response.state);
        println!("└─────────────────────────────────");
        Ok(())
    }
}

fn print_transfer_cancel_result(state: i32) {
    let state = TransferState::from(state);
    match state {
        TransferState::Canceling | TransferState::Canceled => {
            println!("│ ✅ Job cancel request accepted");
            println!("│ State: {:?}", state);
        }
        TransferState::Completed | TransferState::Failed | TransferState::PartialSuccess => {
            println!("│ ℹ️ Job is already terminal");
            println!("│ State: {:?}", state);
        }
        _ => {
            println!("│ ✅ Job cancel request accepted");
            println!("│ State: {:?}", state);
        }
    }
}
