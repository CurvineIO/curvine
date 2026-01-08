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
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::state::MasterInfo;
use num_bigint::ToBigInt;
use orpc::CommonResult;

#[derive(Parser, Debug)]
pub struct ReportCommand {
    #[clap(subcommand)]
    pub action: Option<ReportSubCommand>,
}

#[derive(Subcommand, Debug)]
pub enum ReportSubCommand {
    Json,
    All {
        #[clap(long, default_value = "true")]
        show_workers: bool,
    },
    Capacity {
        #[clap(value_name = "WORKER_ADDRESS")]
        worker_address: Option<String>,
    },
    Used,
    Available,
}

impl ReportCommand {
    pub async fn execute(&self, fs: UnifiedFileSystem) -> CommonResult<()> {
        let rep = handle_rpc_result(fs.get_master_info()).await;
        let report = CurvineReport { info: rep };
        match &self.action {
            Some(action) => match action {
                ReportSubCommand::Json => {
                    println!("{}", report.to_json());
                }
                ReportSubCommand::All { show_workers } => {
                    println!("{}", report.simple(*show_workers));
                }
                ReportSubCommand::Capacity { worker_address } => {
                    if let Some(addr) = worker_address {
                        println!("{}", report.capacity_worker(addr));
                    } else {
                        println!("{}", report.capacity_cluster());
                    }
                }
                ReportSubCommand::Used => {
                    println!("{}", report.used());
                }
                ReportSubCommand::Available => {
                    println!("{}", report.available());
                }
            },
            None => {
                println!("{}", report.simple(true));
            }
        }
        Ok(())
    }
}

struct CurvineReport {
    info: MasterInfo,
}

impl CurvineReport {
    // Serialize the MasterInfo to JSON
    pub fn to_json(&self) -> String {
        match serde_json::to_string_pretty(&self.info) {
            Ok(json) => json,
            Err(e) => format!("Error serializing to JSON: {}", e),
        }
    }

    pub fn simple(&self, show_workers: bool) -> String {
        let mut builder = String::new();
        builder.push_str(&format!(
            "{:>20}: {}\n",
            "active_master", self.info.active_master
        ));

        builder.push_str(&format!("{:>20}: ", "journal_nodes"));
        for i in 0..self.info.journal_nodes_count() {
            if i == 0 {
                builder.push_str(&format!("{}\n", self.info.get_journal_nodes(i).unwrap()));
            } else {
                builder.push_str(&format!(
                    "{}{}\n",
                    " ".repeat(22),
                    self.info.get_journal_nodes(i).unwrap()
                ));
            }
        }
        if self.info.journal_nodes_count() == 0 {
            builder.push('\n');
        }

        builder.push_str(&format!(
            "{:>20}: {}\n",
            "capacity",
            bytes_to_string(&self.info.capacity.to_bigint().unwrap())
        ));

        let available = format!(
            "{:>20}: {} ({:.2}%)\n",
            "available",
            bytes_to_string(&self.info.available.to_bigint().unwrap()),
            Self::get_percent(self.info.available, self.info.capacity)
        );
        builder.push_str(&available);

        let used = format!(
            "{:>20}: {} ({:.2}%)\n",
            "fs_used",
            bytes_to_string(&self.info.fs_used.to_bigint().unwrap()),
            Self::get_percent(self.info.fs_used, self.info.capacity)
        );
        builder.push_str(&used);

        builder.push_str(&format!(
            "{:>20}: {}\n",
            "non_fs_used",
            bytes_to_string(&self.info.non_fs_used.to_bigint().unwrap()),
        ));
        builder.push_str(&format!(
            "{:>20}: {}\n",
            "live_worker_num",
            self.info.live_workers.len()
        ));
        builder.push_str(&format!(
            "{:>20}: {}\n",
            "lost_worker_num",
            self.info.lost_workers.len()
        ));
        builder.push_str(&format!(
            "{:>20}: {}\n",
            "inode_dir_num", self.info.inode_dir_num
        ));
        builder.push_str(&format!(
            "{:>20}: {}\n",
            "inode_file_num", self.info.inode_file_num
        ));
        builder.push_str(&format!("{:>20}: {}\n", "block_num", self.info.block_num));

        if !show_workers {
            return builder;
        }

        // Output worker details
        builder.push_str(&format!("{:>20}: ", "live_worker_list"));
        for i in 0..self.info.live_workers.len() {
            if let Some(worker) = self.info.get_live_worker(i) {
                let str = format!(
                    "{}:{},{}/{} ({:.2}%)",
                    worker.address.hostname,
                    worker.address.rpc_port,
                    bytes_to_string(&worker.available.to_bigint().unwrap()),
                    bytes_to_string(&worker.capacity.to_bigint().unwrap()),
                    Self::get_percent(worker.available, worker.capacity)
                );
                if i == 0 {
                    builder.push_str(&format!("{}\n", str));
                } else {
                    builder.push_str(&format!("{}{}\n", " ".repeat(22), str));
                }
            }
        }

        if self.info.live_workers.is_empty() {
            builder.push('\n');
        }

        // Output lost worker details
        builder.push_str(&format!("{:>20}: ", "lost_worker_list"));
        for i in 0..self.info.lost_workers.len() {
            if let Some(worker) = self.info.get_lost_worker(i) {
                let str = format!("{}:{}", worker.address.hostname, worker.address.rpc_port,);

                if i == 0 {
                    builder.push_str(&format!("{}\n", str));
                } else {
                    builder.push_str(&format!("{}{}\n", " ".repeat(22), str));
                }
            }
        }

        builder
    }

    pub fn capacity_cluster(&self) -> String {
        let mut builder = String::new();

        // Cluster level summary only
        builder.push_str("=== Cluster Capacity ===\n");
        builder.push_str(&format!(
            "Total Capacity: {}\n",
            bytes_to_string(&self.info.capacity.to_bigint().unwrap())
        ));
        builder.push_str(&format!(
            "Total Available: {} ({:.2}%)\n",
            bytes_to_string(&self.info.available.to_bigint().unwrap()),
            Self::get_percent(self.info.available, self.info.capacity)
        ));
        builder.push_str(&format!(
            "Total fs-used: {} ({:.2}%)\n",
            bytes_to_string(&self.info.fs_used.to_bigint().unwrap()),
            Self::get_percent(self.info.fs_used, self.info.capacity)
        ));
        builder.push_str(&format!(
            "Total Non-FS Used: {}\n",
            bytes_to_string(&self.info.non_fs_used.to_bigint().unwrap())
        ));

        builder
    }

    pub fn capacity_worker(&self, worker_address: &str) -> String {
        let mut builder = String::new();

        // Find the worker by IP address (only match IP, ignore port)
        let worker = self
            .info
            .live_workers
            .iter()
            .find(|w| w.address.ip_addr == worker_address);

        let worker = match worker {
            Some(w) => w,
            None => {
                return format!("Worker not found: {}", worker_address);
            }
        };

        // Worker level summary
        builder.push_str(&format!(
            "=== Worker {}:{} ===\n",
            worker.address.hostname, worker.address.rpc_port
        ));
        builder.push_str(&format!(
            "Capacity: {}\n",
            bytes_to_string(&worker.capacity.to_bigint().unwrap())
        ));
        builder.push_str(&format!(
            "Available: {} ({:.2}%)\n",
            bytes_to_string(&worker.available.to_bigint().unwrap()),
            Self::get_percent(worker.available, worker.capacity)
        ));
        builder.push_str(&format!(
            "Fs-used: {} ({:.2}%)\n",
            bytes_to_string(&worker.fs_used.to_bigint().unwrap()),
            Self::get_percent(worker.fs_used, worker.capacity)
        ));
        builder.push_str(&format!(
            "Non-FS Used: {}\n",
            bytes_to_string(&worker.non_fs_used.to_bigint().unwrap())
        ));
        builder.push('\n');

        // Storage level details
        builder.push_str("=== Storages ===\n");
        if worker.storage_map.is_empty() {
            builder.push_str("  No storages found\n");
        } else {
            let mut storages: Vec<_> = worker.storage_map.values().collect();
            storages.sort_by_key(|s| s.dir_id);
            for storage in storages {
                builder.push_str(&format!(
                    "  [{}]：  {}:\n",
                    storage.storage_type.as_str_name(),
                    storage.dir_path
                ));
                builder.push_str(&format!(
                    "    Capacity: {}\n",
                    bytes_to_string(&storage.capacity.to_bigint().unwrap())
                ));
                builder.push_str(&format!(
                    "    Available: {} ({:.2}%)\n",
                    bytes_to_string(&storage.available.to_bigint().unwrap()),
                    Self::get_percent(storage.available, storage.capacity)
                ));
                builder.push_str(&format!(
                    "    Fs-used: {} ({:.2}%)\n",
                    bytes_to_string(&storage.fs_used.to_bigint().unwrap()),
                    Self::get_percent(storage.fs_used, storage.capacity)
                ));
                builder.push_str(&format!(
                    "    Non-FS Used: {}\n",
                    bytes_to_string(&storage.non_fs_used.to_bigint().unwrap())
                ));
                if storage.failed {
                    builder.push_str("    Status: FAILED\n");
                }
                builder.push('\n');
            }
        }

        builder
    }

    pub fn used(&self) -> String {
        let mut builder = String::new();
        for i in 0..self.info.live_workers.len() {
            if let Some(worker) = self.info.get_live_worker(i) {
                let str = format!(
                    "{}:{}  {}",
                    worker.address.hostname,
                    worker.address.rpc_port,
                    bytes_to_string(&worker.fs_used.to_bigint().unwrap()),
                );
                builder.push_str(&format!("{}\n", str));
            }
        }

        builder
    }

    pub fn available(&self) -> String {
        let mut builder = String::new();
        for i in 0..self.info.live_workers.len() {
            if let Some(worker) = self.info.get_live_worker(i) {
                let str = format!(
                    "{}:{}  {}",
                    worker.address.hostname,
                    worker.address.rpc_port,
                    bytes_to_string(&worker.available.to_bigint().unwrap()),
                );
                builder.push_str(&format!("{}\n", str));
            }
        }

        builder
    }

    // Helper method to calculate percentage
    fn get_percent(numerator: i64, denominator: i64) -> f64 {
        if denominator == 0 {
            return 0.0;
        }
        (numerator as f64 / denominator as f64) * 100.0
    }
}
