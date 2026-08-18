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
use curvine_core_error::CommonResult;
use curvine_model::{FilesystemInfo, WorkerInfo};
use curvine_proto::ComponentInfoProto;
use curvine_unified_fs::UnifiedFileSystem;
use serde::Serialize;

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
    /// Print Fluid CacheRuntime ReportSummary JSON
    FluidSummary,
    Used,
    Available,
}

impl ReportCommand {
    pub async fn execute(&self, fs: UnifiedFileSystem) -> CommonResult<()> {
        let rep = handle_rpc_result(fs.get_filesystem_info()).await;
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
                ReportSubCommand::FluidSummary => {
                    println!("{}", report.fluid_summary());
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
    info: FilesystemInfo,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FluidReportSummary {
    cached: String,
    cached_percentage: String,
    cache_capacity: String,
    cache_hit_ratio: String,
    file_num: String,
    ufs_total: String,
}

impl CurvineReport {
    // Serialize the FilesystemInfo to JSON
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
            bytes_to_string(self.info.capacity)
        ));

        let available = format!(
            "{:>20}: {} ({:.2}%)\n",
            "available",
            bytes_to_string(self.info.available),
            Self::get_percent(self.info.available, self.info.capacity)
        );
        builder.push_str(&available);

        let used = format!(
            "{:>20}: {} ({:.2}%)\n",
            "fs_used",
            bytes_to_string(self.info.fs_used),
            Self::get_percent(self.info.fs_used, self.info.capacity)
        );
        builder.push_str(&used);

        builder.push_str(&format!(
            "{:>20}: {}\n",
            "non_fs_used",
            bytes_to_string(self.info.non_fs_used),
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
                    "{}:{},{}/{} ({:.2}%){}",
                    worker.address.hostname,
                    worker.address.rpc_port,
                    bytes_to_string(worker.available),
                    bytes_to_string(worker.capacity),
                    Self::get_percent(worker.available, worker.capacity),
                    Self::worker_detail_suffix(worker),
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
                let str = format!(
                    "{}:{}{}",
                    worker.address.hostname,
                    worker.address.rpc_port,
                    Self::worker_detail_suffix(worker),
                );

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
            bytes_to_string(self.info.capacity)
        ));
        builder.push_str(&format!(
            "Total Available: {} ({:.2}%)\n",
            bytes_to_string(self.info.available),
            Self::get_percent(self.info.available, self.info.capacity)
        ));
        builder.push_str(&format!(
            "Total fs-used: {} ({:.2}%)\n",
            bytes_to_string(self.info.fs_used),
            Self::get_percent(self.info.fs_used, self.info.capacity)
        ));
        builder.push_str(&format!(
            "Total Non-FS Used: {}\n",
            bytes_to_string(self.info.non_fs_used)
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
        builder.push_str(&format!("Capacity: {}\n", bytes_to_string(worker.capacity)));
        builder.push_str(&format!(
            "Available: {} ({:.2}%)\n",
            bytes_to_string(worker.available),
            Self::get_percent(worker.available, worker.capacity)
        ));
        builder.push_str(&format!(
            "Fs-used: {} ({:.2}%)\n",
            bytes_to_string(worker.fs_used),
            Self::get_percent(worker.fs_used, worker.capacity)
        ));
        builder.push_str(&format!(
            "Non-FS Used: {}\n",
            bytes_to_string(worker.non_fs_used)
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
                    bytes_to_string(storage.capacity)
                ));
                builder.push_str(&format!(
                    "    Available: {} ({:.2}%)\n",
                    bytes_to_string(storage.available),
                    Self::get_percent(storage.available, storage.capacity)
                ));
                builder.push_str(&format!(
                    "    Fs-used: {} ({:.2}%)\n",
                    bytes_to_string(storage.fs_used),
                    Self::get_percent(storage.fs_used, storage.capacity)
                ));
                builder.push_str(&format!(
                    "    Non-FS Used: {}\n",
                    bytes_to_string(storage.non_fs_used)
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
                    bytes_to_string(worker.fs_used),
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
                    bytes_to_string(worker.available),
                );
                builder.push_str(&format!("{}\n", str));
            }
        }

        builder
    }

    pub fn fluid_summary(&self) -> String {
        // Fluid defines cachedPercentage against UFS total. Curvine does not
        // keep UFS total as cheap master metadata yet, so avoid deriving it
        // from cache capacity.
        let summary = FluidReportSummary {
            cached: fluid_bytes_to_string(self.info.fs_used),
            cached_percentage: "0".to_string(),
            cache_capacity: fluid_bytes_to_string(self.info.capacity),
            cache_hit_ratio: "0".to_string(),
            file_num: self.info.inode_file_num.to_string(),
            ufs_total: "0".to_string(),
        };

        serde_json::to_string(&summary).expect("Fluid report summary serialization should not fail")
    }

    // Helper method to calculate percentage
    fn get_percent(numerator: i64, denominator: i64) -> f64 {
        if denominator == 0 {
            return 0.0;
        }
        (numerator as f64 / denominator as f64) * 100.0
    }

    fn worker_detail_suffix(worker: &WorkerInfo) -> String {
        let mut details = vec![];
        // Prefer the structured component info reported on the reserved 1000+
        // range; fall back to the legacy display string for older workers and
        // for partially-populated component info that renders nothing.
        match &worker.component_info {
            Some(info) => {
                let display = Self::component_info_display(info);
                if !display.is_empty() {
                    details.push(display);
                } else if !worker.software_version.is_empty() {
                    details.push(format!("version={}", worker.software_version));
                }
            }
            None => {
                if !worker.software_version.is_empty() {
                    details.push(format!("version={}", worker.software_version));
                }
            }
        }
        if worker.startup_time_ms > 0 {
            details.push(format!(
                "startup_time={}",
                Self::format_epoch_ms(worker.startup_time_ms)
            ));
        }

        if details.is_empty() {
            String::new()
        } else {
            format!(", {}", details.join(", "))
        }
    }

    /// Render the structured worker version metadata as a compact string.
    fn component_info_display(info: &ComponentInfoProto) -> String {
        let mut parts = vec![];
        if let Some(component) = info.component.as_deref().filter(|s| !s.is_empty()) {
            parts.push(format!("component={}", component));
        }
        if let Some(version) = info.release_version.as_deref().filter(|s| !s.is_empty()) {
            parts.push(format!("version={}", version));
        }
        if let Some(commit) = info.git_commit.as_deref().filter(|s| !s.is_empty()) {
            parts.push(format!("commit={}", commit));
        }
        if let Some(protocol) = info.protocol_version {
            parts.push(format!("protocol={}", protocol));
        }

        parts.join(", ")
    }

    fn format_epoch_ms(timestamp_ms: u64) -> String {
        let Ok(timestamp_ms) = i64::try_from(timestamp_ms) else {
            return "-".to_string();
        };
        let Some(datetime) = chrono::DateTime::from_timestamp_millis(timestamp_ms) else {
            return "-".to_string();
        };

        datetime
            .with_timezone(&chrono::Local)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string()
    }
}

fn fluid_bytes_to_string(size: i64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    const TIB: f64 = GIB * 1024.0;
    const PIB: f64 = TIB * 1024.0;

    let size = size.max(0) as f64;
    let (value, unit) = if size >= PIB {
        (size / PIB, "PiB")
    } else if size >= TIB {
        (size / TIB, "TiB")
    } else if size >= GIB {
        (size / GIB, "GiB")
    } else if size >= MIB {
        (size / MIB, "MiB")
    } else if size >= KIB {
        (size / KIB, "KiB")
    } else {
        (size, "B")
    };

    format!("{value:.2}{unit}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::WorkerAddress;

    #[test]
    fn simple_report_renders_worker_report_fields() {
        let startup_time_ms = 123_456;
        let worker = WorkerInfo {
            address: WorkerAddress {
                worker_id: 7,
                hostname: "worker-host".to_string(),
                ip_addr: "127.0.0.1".to_string(),
                rpc_port: 1234,
                web_port: 5678,
            },
            software_version: "0.1.0-test".to_string(),
            startup_time_ms,
            capacity: 1024,
            available: 512,
            ..Default::default()
        };
        let report = CurvineReport {
            info: FilesystemInfo {
                live_workers: vec![worker],
                ..Default::default()
            },
        };

        let output = report.simple(true);

        assert!(output.contains("worker-host:1234"));
        assert!(output.contains("version=0.1.0-test"));
        assert!(output.contains(&format!(
            "startup_time={}",
            CurvineReport::format_epoch_ms(startup_time_ms)
        )));
    }

    #[test]
    fn simple_report_renders_structured_worker_version() {
        let startup_time_ms = 123_456;
        let worker = WorkerInfo {
            address: WorkerAddress {
                worker_id: 7,
                hostname: "worker-host".to_string(),
                ip_addr: "127.0.0.1".to_string(),
                rpc_port: 1234,
                web_port: 5678,
            },
            software_version: "0.1.0-test".to_string(),
            startup_time_ms,
            component_info: Some(curvine_proto::ComponentInfoProto {
                component: Some("worker".to_string()),
                release_version: Some("0.4.0-alpha".to_string()),
                git_commit: Some("24c8487".to_string()),
                protocol_version: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        };
        let report = CurvineReport {
            info: FilesystemInfo {
                live_workers: vec![worker],
                ..Default::default()
            },
        };

        let output = report.simple(true);

        // Structured component info is preferred over the legacy display string.
        assert!(output.contains("component=worker"));
        assert!(output.contains("version=0.4.0-alpha"));
        assert!(output.contains("commit=24c8487"));
        assert!(output.contains("protocol=1"));
        assert!(!output.contains("version=0.1.0-test"));
    }

    #[test]
    fn simple_report_falls_back_to_software_version_for_empty_component_info() {
        // A partially-populated/new-but-empty component_info renders nothing,
        // so the display must fall back to the legacy software_version string
        // instead of hiding version details.
        let worker = WorkerInfo {
            address: WorkerAddress {
                worker_id: 8,
                hostname: "worker-host".to_string(),
                ip_addr: "127.0.0.1".to_string(),
                rpc_port: 1234,
                web_port: 5678,
            },
            software_version: "0.2.0-test".to_string(),
            component_info: Some(curvine_proto::ComponentInfoProto::default()),
            ..Default::default()
        };
        let report = CurvineReport {
            info: FilesystemInfo {
                live_workers: vec![worker],
                ..Default::default()
            },
        };

        let output = report.simple(true);

        assert!(output.contains("worker-host:1234"));
        assert!(output.contains("version=0.2.0-test"));
    }
}
