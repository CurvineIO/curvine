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
use curvine_common::state::FileBlocks;
use orpc::common::ByteUnit;
use orpc::CommonResult;

#[derive(Debug)]
pub enum BlocksCommand {
    Blocks { path: String, format: String },
}

impl BlocksCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            BlocksCommand::Blocks { path, format } => {
                if path.trim().is_empty() {
                    eprintln!("Error: Path cannot be empty");
                    std::process::exit(1);
                }

                let path = Path::from_str(path)?;
                let file_blocks =
                    handle_rpc_result(client.fs_client().get_block_locations(&path)).await;

                match format.as_str() {
                    "json" => Self::print_json(&file_blocks),
                    _ => Self::print_table(&file_blocks),
                }

                Ok(())
            }
        }
    }

    fn print_table(file_blocks: &FileBlocks) {
        println!("\n📄 File Information:");
        println!("  Path: {}", file_blocks.status.path);
        println!(
            "  Size: {} ({})",
            ByteUnit::byte_to_string(file_blocks.status.len as u64),
            file_blocks.status.len
        );
        println!("  Complete: {}", file_blocks.status.is_complete);
        println!("  Blocks: {}", file_blocks.block_locs.len());

        if file_blocks.block_locs.is_empty() {
            println!("\n⚠️  No blocks found for this file");
            return;
        }

        println!("\n📦 Block Locations:");
        let separator = format!("{:-<80}", "");
        println!("{}", separator);
        println!(
            "{:<6} {:<15} {:<12} {:<12} {:<6} Workers",
            "Block", "Block ID", "Size", "Storage", "Count"
        );
        println!("{}", separator);

        for (idx, block) in file_blocks.block_locs.iter().enumerate() {
            let size_str = ByteUnit::byte_to_string(block.block.len as u64);
            let storage_type = format!("{:?}", block.block.storage_type);
            let worker_count = block.locs.len();

            let workers: Vec<String> = block
                .locs
                .iter()
                .map(|addr| format!("{}:{}", addr.hostname, addr.rpc_port))
                .collect();

            let first_worker = workers.first().map(|w| w.as_str()).unwrap_or("");
            println!(
                "{:<6} {:<15} {:<12} {:<12} {:<6} {}",
                idx + 1,
                block.block.id,
                size_str,
                storage_type,
                worker_count,
                first_worker
            );

            if workers.len() > 1 {
                for worker in &workers[1..] {
                    println!(
                        "{:<6} {:<15} {:<12} {:<12} {:<6} {}",
                        "", "", "", "", "", worker
                    );
                }
            }

            if idx < file_blocks.block_locs.len() - 1 && workers.len() > 1 {
                println!();
            }
        }

        println!("{}", separator);
    }

    fn print_json(file_blocks: &FileBlocks) {
        match serde_json::to_string_pretty(file_blocks) {
            Ok(json) => println!("{}", json),
            Err(e) => {
                eprintln!("Error serializing to JSON: {}", e);
                std::process::exit(1);
            }
        }
    }
}
