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
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::Path;
use orpc::common::ByteUnit;
use orpc::CommonResult;

#[derive(Debug)]
pub enum FreeCommand {
    Free { path: String, recursive: bool },
}

impl FreeCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            FreeCommand::Free { path, recursive } => {
                if path.trim().is_empty() {
                    eprintln!("Error: Path cannot be empty");
                    std::process::exit(1);
                }

                let path = Path::from_str(path)?;
                let res = handle_rpc_result(client.free(&path, *recursive)).await;

                println!(
                    "inodes: {}, space: {}",
                    res.inodes,
                    ByteUnit::byte_to_string(res.bytes as u64),
                );
                Ok(())
            }
        }
    }
}
