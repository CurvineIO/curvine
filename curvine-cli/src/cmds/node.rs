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
use clap::Parser;
use curvine_client_core::file::FsClient;
use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_model::WorkerInfo;
use curvine_runtime::common::ByteUnit;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(arg_required_else_help = true)]
pub struct NodeCommand {
    /// list all nodes
    #[arg(long, short = 'l')]
    list: bool,

    /// show version distribution across all workers
    #[arg(long)]
    versions: bool,

    /// add decommission node
    #[arg(long, visible_alias = "add-dcm")]
    add_decommission: bool,

    /// remove decommission node
    #[arg(long, visible_alias = "remove-dcm")]
    remove_decommission: bool,

    /// node list (hostname, ip, or host:port). Repeat the argument or separate with commas.
    #[arg(
        required_if_eq("add_decommission", "true"),
        required_if_eq("remove_decommission", "true")
    )]
    nodes: Vec<String>,
}

impl NodeCommand {
    fn print_worker(worker: &WorkerInfo) {
        let address = format!("{}:{}", worker.address.hostname, worker.address.rpc_port);
        println!(
            "{:<25} {:<15} {:<15} {:<15}",
            address,
            format!("{:?}", worker.status),
            ByteUnit::byte_to_string(worker.capacity as u64),
            ByteUnit::byte_to_string(worker.available as u64)
        );
    }

    // Handle listing all worker nodes
    async fn handle_list(&self, client: Arc<FsClient>) -> CommonResult<()> {
        let info = handle_rpc_result(client.get_filesystem_info()).await;

        println!("Worker Nodes:");
        println!(
            "{:<25} {:<15} {:<15} {:<15}",
            "Address", "Status", "Capacity", "Available"
        );
        println!("{}", "-".repeat(70));

        for worker in info
            .live_workers
            .iter()
            .chain(info.blacklist_workers.iter())
            .chain(info.decommission_workers.iter())
            .chain(info.lost_workers.iter())
        {
            Self::print_worker(worker);
        }

        Ok(())
    }

    /// Selectors from repeated arguments and comma-separated values.
    fn node_selectors(&self) -> Vec<String> {
        self.nodes
            .iter()
            .flat_map(|node| node.split(','))
            .map(|node| node.trim().to_string())
            .filter(|node| !node.is_empty())
            .collect()
    }

    fn print_decommission_results(successful: &[String], failed: &[String], operation_type: &str) {
        if successful.is_empty() && failed.is_empty() {
            println!("No worker was {} decommission list", operation_type);
            return;
        }

        if !successful.is_empty() {
            println!("Successfully {} workers:", operation_type);
            for addr in successful {
                println!("  {}", addr);
            }
            println!("Total: {} worker(s)", successful.len());
        }

        if !failed.is_empty() {
            println!("Failed to {} workers:", operation_type);
            for addr in failed {
                println!("  {}", addr);
            }
            println!("Total: {} worker(s)", failed.len());
        }
    }

    async fn set_decommission_status(
        &self,
        client: Arc<FsClient>,
        status: &str,
        operation_type: &str,
    ) -> CommonResult<()> {
        let mut successful = Vec::new();
        let mut failed = Vec::new();
        for node in self.node_selectors() {
            match client.set_worker_status(&node, status).await {
                Ok((worker, _)) => successful.push(format!(
                    "{}:{}",
                    worker.address.hostname, worker.address.rpc_port
                )),
                Err(err) => failed.push(format!("{} ({})", node, err)),
            }
        }
        Self::print_decommission_results(&successful, &failed, operation_type);
        Ok(())
    }

    async fn handle_add_decommission(&self, client: Arc<FsClient>) -> CommonResult<()> {
        self.set_decommission_status(client, "decommission", "added to")
            .await
    }

    async fn handle_remove_decommission(&self, client: Arc<FsClient>) -> CommonResult<()> {
        self.set_decommission_status(client, "remove_decommission", "removed from")
            .await
    }

    // Handle showing version distribution across workers
    async fn handle_versions(&self, client: Arc<FsClient>) -> CommonResult<()> {
        let info = handle_rpc_result(client.get_filesystem_info()).await;
        let mut workers = info.live_workers;
        workers.extend(info.blacklist_workers);
        workers.extend(info.decommission_workers);

        let version_counts = Self::count_worker_versions(&workers);

        println!("Version Distribution:");
        println!("{}", "-".repeat(40));

        let mut total: u32 = 0;
        for (version, count) in &version_counts.versions {
            println!("  {:<20}: {} worker(s)", version, count);
            total += *count;
        }

        if version_counts.legacy_count > 0 {
            println!(
                "  {:<20}: {} worker(s)",
                "legacy (no version)", version_counts.legacy_count
            );
            total += version_counts.legacy_count;
        }

        println!("{}", "-".repeat(40));
        println!("  {:<20}: {} worker(s)", "total", total);

        Ok(())
    }

    /// Count worker release-version distribution.
    ///
    /// Prefers the structured `component_info.release_version` reported on
    /// heartbeat (T7+); falls back to the legacy `software_version` display
    /// string. Workers with no useful version data count as `legacy_count`.
    fn count_worker_versions(workers: &[WorkerInfo]) -> VersionCounts {
        let mut version_counts: BTreeMap<String, u32> = BTreeMap::new();
        let mut legacy_count: u32 = 0;

        for worker in workers {
            // Prefer structured component_info.release_version (T7+). When
            // component_info is present but release_version is missing or
            // empty (partially-upgraded / malformed payloads), fall back to
            // the legacy software_version display string.
            let release_version = worker
                .component_info
                .as_ref()
                .and_then(|info| info.release_version.as_deref())
                .filter(|version| !version.is_empty());

            if let Some(version) = release_version {
                *version_counts.entry(version.to_string()).or_insert(0) += 1;
            } else {
                let sv = worker.software_version.as_str();
                if sv.is_empty() || sv == "unknown" {
                    legacy_count += 1;
                } else {
                    *version_counts
                        .entry(format!("legacy ({})", sv))
                        .or_insert(0) += 1;
                }
            }
        }

        VersionCounts {
            versions: version_counts,
            legacy_count,
        }
    }

    pub async fn execute(&self, client: Arc<FsClient>, _conf: ClusterConf) -> CommonResult<()> {
        if self.versions {
            return self.handle_versions(client).await;
        }

        if self.list {
            return self.handle_list(client).await;
        }

        if self.add_decommission {
            return self.handle_add_decommission(client).await;
        }

        if self.remove_decommission {
            return self.handle_remove_decommission(client).await;
        }

        Ok(())
    }
}

/// Release-version distribution across live workers, as counted by
/// [`NodeCommand::count_worker_versions`].
struct VersionCounts {
    /// Map of display version -> worker count. BTreeMap keeps output stable.
    versions: BTreeMap<String, u32>,
    /// Workers with no usable version data (no structured release_version and
    /// no usable legacy software_version).
    legacy_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_proto::ComponentInfoProto;

    fn worker_with_component_info(version: &str) -> WorkerInfo {
        WorkerInfo {
            software_version: "0.0.0-old".to_string(),
            component_info: Some(ComponentInfoProto {
                component: Some("worker".to_string()),
                release_version: Some(version.to_string()),
                protocol_version: Some(1),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn worker_with_legacy_software_version(version: &str) -> WorkerInfo {
        WorkerInfo {
            software_version: version.to_string(),
            ..Default::default()
        }
    }

    fn worker_with_component_info_proto(
        info: ComponentInfoProto,
        software_version: &str,
    ) -> WorkerInfo {
        WorkerInfo {
            software_version: software_version.to_string(),
            component_info: Some(info),
            ..Default::default()
        }
    }

    #[test]
    fn count_worker_versions_prefers_structured_component_info() {
        let workers = vec![
            worker_with_component_info("0.4.0-alpha"),
            worker_with_component_info("0.4.0-alpha"),
            worker_with_component_info("0.4.0-beta"),
        ];

        let counts = NodeCommand::count_worker_versions(&workers);

        assert_eq!(counts.versions.get("0.4.0-alpha"), Some(&2));
        assert_eq!(counts.versions.get("0.4.0-beta"), Some(&1));
        assert_eq!(counts.legacy_count, 0);
    }

    #[test]
    fn count_worker_versions_falls_back_to_legacy_software_version() {
        let workers = vec![
            worker_with_legacy_software_version("0.3.0-test"),
            worker_with_legacy_software_version("0.3.0-test"),
            worker_with_legacy_software_version("unknown"),
        ];

        let counts = NodeCommand::count_worker_versions(&workers);

        assert_eq!(counts.versions.get("legacy (0.3.0-test)"), Some(&2));
        assert_eq!(counts.legacy_count, 1);
    }

    #[test]
    fn count_worker_versions_falls_back_when_component_info_lacks_release_version() {
        // component_info present but without a usable release_version must
        // fall back to the legacy software_version display string instead of
        // counting the worker as legacy.
        let workers = vec![
            worker_with_component_info_proto(ComponentInfoProto::default(), "0.2.0-test"),
            worker_with_component_info_proto(
                ComponentInfoProto {
                    component: Some("worker".to_string()),
                    release_version: Some(String::new()),
                    ..Default::default()
                },
                "0.2.0-test",
            ),
        ];

        let counts = NodeCommand::count_worker_versions(&workers);

        assert_eq!(counts.versions.get("legacy (0.2.0-test)"), Some(&2));
        assert_eq!(counts.legacy_count, 0);
    }

    #[test]
    fn count_worker_versions_counts_legacy_when_no_version_data_at_all() {
        // component_info present but release_version unusable, and no legacy
        // software_version either -> legacy.
        let workers = vec![worker_with_component_info_proto(
            ComponentInfoProto {
                component: Some("worker".to_string()),
                release_version: Some(String::new()),
                ..Default::default()
            },
            "",
        )];

        let counts = NodeCommand::count_worker_versions(&workers);

        assert!(counts.versions.is_empty());
        assert_eq!(counts.legacy_count, 1);
    }

    #[test]
    fn add_decommission_accepts_node_without_double_dash() {
        let cmd =
            NodeCommand::try_parse_from(["node", "--add-decommission", "10.144.228.84"]).unwrap();
        assert!(cmd.add_decommission);
        assert_eq!(cmd.nodes, vec!["10.144.228.84".to_string()]);
    }

    #[test]
    fn node_selectors_split_commas_and_repeated_args() {
        let cmd = NodeCommand::try_parse_from([
            "node",
            "--add-decommission",
            "10.144.228.84,10.144.228.85",
            "host-a",
        ])
        .unwrap();
        assert_eq!(
            cmd.node_selectors(),
            vec![
                "10.144.228.84".to_string(),
                "10.144.228.85".to_string(),
                "host-a".to_string(),
            ]
        );
    }
}
