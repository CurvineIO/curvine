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
use curvine_core_error::{err_box, CommonResult};
use curvine_model::WorkerInfo;
use curvine_runtime::common::ByteUnit;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(arg_required_else_help = true)]
pub struct NodeCommand {
    #[command(subcommand)]
    command: Option<NodeSubCommand>,

    /// Alias for `node list`, kept so existing scripts keep working.
    #[arg(long, short = 'l', hide = true)]
    list: bool,

    /// Alias for `node versions`, kept so existing scripts keep working.
    #[arg(long, hide = true)]
    versions: bool,

    /// Alias for `node decommission --add/--remove`.
    #[arg(long, visible_alias = "add-dcm", hide = true)]
    add_decommission: bool,

    #[arg(long, visible_alias = "remove-dcm", hide = true)]
    remove_decommission: bool,

    /// Alias for `node decommission --add/--remove`.
    #[arg(hide = true)]
    nodes: Vec<String>,
}

#[derive(clap::Subcommand, Debug)]
pub enum NodeSubCommand {
    /// List nodes with their health state
    List {
        /// healthy: every directory usable; unhealthy: at least one is not.
        #[arg(long, value_enum, default_value_t = HealthFilter::All)]
        health: HealthFilter,

        /// Machine-readable output
        #[arg(long)]
        json: bool,
    },

    /// Show the release version distribution
    Versions,

    /// Check node health: every unhealthy node, or one node in detail
    Check {
        /// hostname, ip, or host:port. Omit to survey the whole cluster.
        node: Option<String>,

        /// Machine-readable output
        #[arg(long)]
        json: bool,
    },

    /// Manage the decommission list
    Decommission {
        #[arg(long, visible_alias = "add-dcm")]
        add: bool,

        #[arg(long, visible_alias = "remove-dcm")]
        remove: bool,

        /// node list (hostname, ip, or host:port). Repeat or separate with commas.
        #[arg(required = true)]
        nodes: Vec<String>,
    },
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthFilter {
    All,
    Healthy,
    Unhealthy,
}

impl NodeCommand {
    async fn handle_list(
        &self,
        client: Arc<FsClient>,
        health: HealthFilter,
        json: bool,
    ) -> CommonResult<()> {
        let info = handle_rpc_result(client.get_filesystem_info()).await;

        let mut workers: Vec<(String, &WorkerInfo)> = info
            .live_workers
            .iter()
            .chain(info.blacklist_workers.iter())
            .chain(info.decommission_workers.iter())
            .chain(info.lost_workers.iter())
            .map(|worker| (format!("{:?}", worker.status), worker))
            .collect();

        // Unhealthy nodes first: the list exists to surface problems.
        workers.sort_by_key(|(_, worker)| {
            (!Self::unhealthy_dirs(worker).is_empty(), worker.rpc_addr())
        });

        if json {
            let payload = serde_json::json!({
                "workers": workers
                    .iter()
                    .map(|(status, worker)| serde_json::json!({
                        "worker_id": worker.worker_id(),
                        "address": worker.rpc_addr(),
                        "status": status,
                        "health": Self::health_label(worker),
                        "accepting_allocations": worker.accepting_allocations(),
                        "schedulable": worker.schedulable(),
                        "capacity": worker.capacity,
                        "available": worker.available,
                    }))
                    .collect::<Vec<_>>(),
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&payload).unwrap_or_default()
            );
            return Ok(());
        }

        println!(
            "{:<25} {:<15} {:<12} {:<12} {:<12} Fault",
            "Address", "Status", "Health", "Capacity", "Available"
        );
        println!("{}", "-".repeat(96));
        for (status, worker) in workers {
            if !Self::keep(worker, health) {
                continue;
            }
            let fault = Self::unhealthy_dirs(worker)
                .first()
                .map(|dir| format!("{}({})", dir.fault_kind, dir.dir_path))
                .unwrap_or_else(|| "-".to_string());
            println!(
                "{:<25} {:<15} {:<12} {:<12} {:<12} {}",
                worker.rpc_addr(),
                status,
                Self::health_label(worker),
                ByteUnit::byte_to_string(worker.capacity.max(0) as u64),
                ByteUnit::byte_to_string(worker.available.max(0) as u64),
                fault,
            );
        }
        Ok(())
    }

    fn keep(worker: &WorkerInfo, health: HealthFilter) -> bool {
        match health {
            HealthFilter::All => true,
            HealthFilter::Healthy => worker.accepting_allocations(),
            HealthFilter::Unhealthy => !worker.accepting_allocations(),
        }
    }

    /// Selectors from repeated arguments and comma-separated values.
    fn node_selectors(nodes: &[String]) -> Vec<String> {
        nodes
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
        selectors: Vec<String>,
    ) -> CommonResult<()> {
        let mut successful = Vec::new();
        let mut failed = Vec::new();
        for node in Self::node_selectors(&selectors) {
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
        // Legacy flags: `cv node -l`, `cv node --versions`, `cv node --add-dcm ...`.
        if self.add_decommission || self.remove_decommission {
            return self.handle_decommission_alias(client).await;
        }
        if self.list {
            return self.handle_list(client, HealthFilter::All, false).await;
        }
        if self.versions {
            return self.handle_versions(client).await;
        }

        match &self.command {
            None | Some(NodeSubCommand::List { .. }) => {
                let (health, json) = match &self.command {
                    Some(NodeSubCommand::List { health, json }) => (*health, *json),
                    _ => (HealthFilter::All, false),
                };
                self.handle_list(client, health, json).await
            }
            Some(NodeSubCommand::Versions) => self.handle_versions(client).await,
            Some(NodeSubCommand::Check { node, json }) => {
                self.handle_check(client, node.as_deref(), *json).await
            }
            Some(NodeSubCommand::Decommission { add, remove, nodes }) => {
                let status = match (add, remove) {
                    (true, false) => ("decommission", "added to"),
                    (false, true) => ("remove_decommission", "removed from"),
                    _ => return err_box!("Specify either --add or --remove"),
                };
                self.set_decommission_status(client, status.0, status.1, nodes.clone())
                    .await
            }
        }
    }

    async fn handle_decommission_alias(&self, client: Arc<FsClient>) -> CommonResult<()> {
        let (status, operation) = if self.add_decommission {
            ("decommission", "added to")
        } else {
            ("remove_decommission", "removed from")
        };
        self.set_decommission_status(client, status, operation, self.nodes.clone())
            .await
    }

    /// `cv node check`: survey unhealthy nodes, or explain one node in detail.
    async fn handle_check(
        &self,
        client: Arc<FsClient>,
        selector: Option<&str>,
        json: bool,
    ) -> CommonResult<()> {
        let info = handle_rpc_result(client.get_filesystem_info()).await;
        let workers = Self::all_workers(&info);

        let selected: Vec<&WorkerInfo> = match selector {
            Some(node) => {
                let node = node.trim();
                let found = workers
                    .iter()
                    .find(|worker| Self::matches(worker, node))
                    .ok_or_else(|| curvine_core_error::err_msg!("worker not found: {}", node))?;
                vec![found]
            }
            None => workers
                .iter()
                .copied()
                .filter(|worker| !worker.accepting_allocations())
                .collect(),
        };

        if json {
            let payload = serde_json::json!({
                "workers": selected
                    .iter()
                    .map(|worker| serde_json::json!({
                        "worker_id": worker.worker_id(),
                        "address": worker.rpc_addr(),
                        "status": format!("{:?}", worker.status),
                        "accepting_allocations": worker.accepting_allocations(),
                        "schedulable": worker.schedulable(),
                        "storage": Self::unhealthy_dirs(worker)
                            .iter()
                            .map(|dir| serde_json::json!({
                                "dir_id": dir.dir_id,
                                "path": dir.dir_path,
                                "state": dir.health_state.as_str_name(),
                                "fault": dir.fault_kind.as_str_name(),
                                "since_ms": dir.unhealthy_since_ms,
                            }))
                            .collect::<Vec<_>>(),
                    }))
                    .collect::<Vec<_>>(),
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&payload).unwrap_or_default()
            );
            return Ok(());
        }

        if selected.is_empty() {
            println!("All workers are healthy.");
            return Ok(());
        }

        for worker in selected {
            println!(
                "{}  {}  worker_id={}  health={}  accepting={}  schedulable={}",
                worker.address.hostname,
                worker.rpc_addr(),
                worker.worker_id(),
                Self::health_label(worker),
                worker.accepting_allocations(),
                worker.schedulable(),
            );
            for dir in Self::unhealthy_dirs(worker) {
                println!(
                    "  dir {} {} state={} fault={} unhealthy_for={}",
                    dir.dir_id,
                    dir.dir_path,
                    dir.health_state.as_str_name(),
                    dir.fault_kind.as_str_name(),
                    Self::elapsed(dir.unhealthy_since_ms),
                );
            }
        }
        Ok(())
    }

    fn all_workers(info: &curvine_model::FilesystemInfo) -> Vec<&WorkerInfo> {
        info.live_workers
            .iter()
            .chain(info.blacklist_workers.iter())
            .chain(info.decommission_workers.iter())
            .chain(info.lost_workers.iter())
            .collect()
    }

    fn unhealthy_dirs(worker: &WorkerInfo) -> Vec<&curvine_model::StorageInfo> {
        worker
            .storage_map
            .values()
            .filter(|dir| !dir.is_usable())
            .collect()
    }

    /// `unhealthy` means the node cannot take new writes for at least one
    /// directory; it is a fact, not a routing decision.
    fn health_label(worker: &WorkerInfo) -> &'static str {
        if worker.accepting_allocations() {
            "healthy"
        } else {
            "unhealthy"
        }
    }

    fn matches(worker: &WorkerInfo, selector: &str) -> bool {
        worker.address.hostname == selector
            || worker.address.ip_addr == selector
            || worker.rpc_addr() == selector
            || worker.worker_id().to_string() == selector
    }

    fn elapsed(since_ms: i64) -> String {
        if since_ms <= 0 {
            return "-".to_string();
        }
        let now = curvine_runtime::common::LocalTime::mills() as i64;
        let secs = ((now - since_ms) / 1000).max(0);
        match secs {
            s if s < 60 => format!("{}s", s),
            s if s < 3600 => format!("{}m", s / 60),
            s => format!("{}h", s / 3600),
        }
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
    fn legacy_decommission_flags_still_parse() {
        let cmd =
            NodeCommand::try_parse_from(["node", "--add-decommission", "10.144.228.84"]).unwrap();
        assert!(cmd.add_decommission);
        assert_eq!(cmd.nodes, vec!["10.144.228.84".to_string()]);
    }

    #[test]
    fn node_selectors_split_commas_and_repeated_args() {
        let nodes = vec![
            "10.144.228.84,10.144.228.85".to_string(),
            "host-a".to_string(),
        ];
        assert_eq!(
            NodeCommand::node_selectors(&nodes),
            vec![
                "10.144.228.84".to_string(),
                "10.144.228.85".to_string(),
                "host-a".to_string(),
            ]
        );
    }

    #[test]
    fn check_subcommand_parses_with_and_without_a_node() {
        let all = NodeCommand::try_parse_from(["node", "check"]).unwrap();
        match all.command {
            Some(NodeSubCommand::Check { node, json }) => {
                assert!(node.is_none());
                assert!(!json);
            }
            other => panic!("unexpected command: {:?}", other),
        }

        let one = NodeCommand::try_parse_from(["node", "check", "h135:8997", "--json"]).unwrap();
        match one.command {
            Some(NodeSubCommand::Check { node, json }) => {
                assert_eq!(node.as_deref(), Some("h135:8997"));
                assert!(json);
            }
            other => panic!("unexpected command: {:?}", other),
        }
    }

    #[test]
    fn list_subcommand_parses_health_filter() {
        let cmd = NodeCommand::try_parse_from(["node", "list", "--health", "unhealthy"]).unwrap();
        match cmd.command {
            Some(NodeSubCommand::List { health, json }) => {
                assert_eq!(health, HealthFilter::Unhealthy);
                assert!(!json);
            }
            other => panic!("unexpected command: {:?}", other),
        }
    }
}
