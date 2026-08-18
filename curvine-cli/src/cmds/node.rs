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
use curvine_runtime::common::ByteUnit;
use reqwest::Client;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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
    #[arg(long)]
    add_decommission: bool,

    /// remove decommission node
    #[arg(long)]
    remove_decommission: bool,

    /// node list with port (format: hostname:port), comma separated for multiple nodes
    #[arg(
        last = true,
        required_if_eq("add_decommission", "true"),
        required_if_eq("remove_decommission", "true")
    )]
    nodes: Vec<String>,
}

impl NodeCommand {
    // Send HTTP GET request to master server
    async fn http_get(
        &self,
        client: Arc<FsClient>,
        conf: &ClusterConf,
        path: &str,
    ) -> CommonResult<String> {
        // Get master address and web port
        let filesystem_info = client.get_filesystem_info().await?;
        let master_parts: Vec<&str> = filesystem_info.active_master.split(':').collect();
        let master_host = master_parts[0];
        let web_port = conf.master.web_port;

        // Build complete URL
        let url = format!("http://{}:{}{}", master_host, web_port, path);

        // Create HTTP client and send request
        let http_client = match Client::builder().timeout(Duration::from_secs(30)).build() {
            Ok(client) => client,
            Err(e) => return err_box!("Failed to create HTTP client: {}", e),
        };

        let response = match http_client.get(&url).send().await {
            Ok(resp) => resp,
            Err(e) => return err_box!("Failed to send HTTP request: {}", e),
        };

        let status = response.status();
        if !status.is_success() {
            return err_box!("HTTP request failed with status: {}", status);
        }

        let body = match response.text().await {
            Ok(text) => text,
            Err(e) => return err_box!("Failed to read response body: {}", e),
        };

        Ok(body)
    }

    // Handle listing all worker nodes
    async fn handle_list(&self, client: Arc<FsClient>, conf: &ClusterConf) -> CommonResult<()> {
        // Get worker list
        let url = "/api/workers";
        let response = handle_rpc_result(self.http_get(client.clone(), conf, url)).await;

        // Parse JSON response
        let workers_map: HashMap<String, Vec<serde_json::Value>> = serde_json::from_str(&response)?;

        // Get live_workers and lost_workers
        let live_workers = workers_map.get("live_workers").cloned().unwrap_or_default();
        let lost_workers = workers_map.get("lost_workers").cloned().unwrap_or_default();

        // Print worker information
        println!("Worker Nodes:");
        println!(
            "{:<25} {:<15} {:<15} {:<15}",
            "Address", "Status", "Capacity", "Available"
        );
        println!("{}", "-".repeat(70));

        // Process live_workers
        for worker in live_workers {
            let hostname = worker["address"]["hostname"].as_str().unwrap_or("Unknown");
            let rpc_port = worker["address"]["rpc_port"].as_u64().unwrap_or(0);
            let address = format!("{hostname}:{rpc_port}");
            let status = worker["status"].as_str().unwrap_or("Unknown");
            let capacity = worker["capacity"].as_i64().unwrap_or(0);
            let available = worker["available"].as_i64().unwrap_or(0);

            println!(
                "{:<25} {:<15} {:<15} {:<15}",
                address,
                status,
                ByteUnit::byte_to_string(capacity as u64),
                ByteUnit::byte_to_string(available as u64)
            );
        }

        // Process lost_workers
        for worker in lost_workers {
            let hostname = worker["address"]["hostname"].as_str().unwrap_or("Unknown");
            let rpc_port = worker["address"]["rpc_port"].as_u64().unwrap_or(0);
            let address = format!("{hostname}:{rpc_port}");
            let status = worker["status"].as_str().unwrap_or("Unknown");
            let capacity = worker["capacity"].as_i64().unwrap_or(0);
            let available = worker["available"].as_i64().unwrap_or(0);

            println!(
                "{:<25} {:<15} {:<15} {:<15}",
                address,
                status,
                ByteUnit::byte_to_string(capacity as u64),
                ByteUnit::byte_to_string(available as u64)
            );
        }

        Ok(())
    }

    // Extract hostnames from node addresses (hostname:port format)
    fn extract_hostnames(&self, nodes: &[String]) -> Vec<String> {
        nodes
            .iter()
            .map(|node| {
                // Split by ':' and take the first part (hostname)
                node.split(':').next().unwrap_or(node).to_string()
            })
            .collect()
    }

    // Process decommission operation results
    fn process_decommission_results(
        &self,
        result: &[String],
        requested_nodes: &[String],
        operation_type: &str,
    ) {
        if result.is_empty() {
            println!("No worker was {} decommission list", operation_type);
            return;
        }

        // Create a map of requested nodes for quick lookup
        let mut requested_map = HashMap::new();
        for node in requested_nodes {
            let hostname = node.split(':').next().unwrap_or(node);
            requested_map.insert(hostname.to_string(), node.clone());
        }

        // Track successful and failed operations
        let mut successful = Vec::new();
        let mut failed = Vec::new();

        // Process successful operations
        for worker in result {
            // Extract worker_id and hostname:port from worker info
            let parts: Vec<&str> = worker.split(',').collect();
            if parts.len() >= 2 {
                let addr_part = parts[1].to_string();
                successful.push(addr_part);
            }
        }

        // Identify failed operations
        for (hostname, full_node) in &requested_map {
            let found = result.iter().any(|worker| {
                let parts: Vec<&str> = worker.split(',').collect();
                if parts.len() >= 2 {
                    let worker_hostname = parts[1].split(':').next().unwrap_or("");
                    worker_hostname == hostname
                } else {
                    false
                }
            });

            if !found {
                failed.push(full_node.clone());
            }
        }

        // Print successful operations
        if !successful.is_empty() {
            println!("Successfully {} workers:", operation_type);
            for addr in &successful {
                println!("  {}", addr);
            }
            println!("Total: {} worker(s)", successful.len());
        }

        // Print failed operations
        if !failed.is_empty() {
            println!("Failed to {} workers:", operation_type);
            for addr in &failed {
                println!("  {}", addr);
            }
            println!("Total: {} worker(s)", failed.len());
        }
    }

    // Handle adding workers to decommission list
    async fn handle_add_decommission(
        &self,
        client: Arc<FsClient>,
        conf: &ClusterConf,
    ) -> CommonResult<()> {
        // Extract hostname from hostname:port format
        let worker_hostnames = self.extract_hostnames(&self.nodes);
        let workers = worker_hostnames.join(",");

        // Call add-dcm API
        let url = format!("/add-dcm?workers={}", workers);
        let response = handle_rpc_result(self.http_get(client.clone(), conf, &url)).await;

        // Parse response
        let result: Vec<String> = serde_json::from_str(&response)?;

        // Process and display results
        self.process_decommission_results(&result, &self.nodes, "added to");

        Ok(())
    }

    // Handle removing workers from decommission list
    async fn handle_remove_decommission(
        &self,
        client: Arc<FsClient>,
        conf: &ClusterConf,
    ) -> CommonResult<()> {
        // Extract hostname from hostname:port format
        let worker_hostnames = self.extract_hostnames(&self.nodes);
        let workers = worker_hostnames.join(",");

        // Call remove-dcm API
        let url = format!("/remove-dcm?workers={}", workers);
        let response = handle_rpc_result(self.http_get(client.clone(), conf, &url)).await;

        // Parse response
        let result: Vec<String> = serde_json::from_str(&response)?;

        // Process and display results
        self.process_decommission_results(&result, &self.nodes, "removed from");

        Ok(())
    }

    // Handle showing version distribution across workers
    async fn handle_versions(&self, client: Arc<FsClient>, conf: &ClusterConf) -> CommonResult<()> {
        let url = "/api/workers";
        let response = handle_rpc_result(self.http_get(client.clone(), conf, url)).await;

        // Parse JSON response
        let workers_map: HashMap<String, Vec<serde_json::Value>> = serde_json::from_str(&response)?;

        let live_workers = workers_map.get("live_workers").cloned().unwrap_or_default();

        let version_counts = Self::count_worker_versions(&live_workers);

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

    /// Count worker release-version distribution from the `/api/workers` JSON
    /// payload.
    ///
    /// Prefers the structured `component_info.release_version` reported on
    /// heartbeat (T7+); falls back to the legacy `software_version` display
    /// string. Workers with no useful version data count as `legacy_count`.
    fn count_worker_versions(workers: &[serde_json::Value]) -> VersionCounts {
        // Count version distribution: release_version (from component_info) -> count
        let mut version_counts: BTreeMap<String, u32> = BTreeMap::new();
        let mut legacy_count: u32 = 0;

        for worker in workers {
            // Prefer structured component_info
            if let Some(component_info) = worker["component_info"].as_object() {
                if let Some(version) = component_info
                    .get("release_version")
                    .and_then(|v| v.as_str())
                {
                    if !version.is_empty() {
                        *version_counts.entry(version.to_string()).or_insert(0) += 1;
                        continue;
                    }
                }
                // component_info exists but empty -> treat as legacy
                legacy_count += 1;
            } else {
                // Fall back to legacy software_version
                let sv = worker["software_version"].as_str().unwrap_or("unknown");
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

    pub async fn execute(&self, client: Arc<FsClient>, conf: ClusterConf) -> CommonResult<()> {
        if self.versions {
            return self.handle_versions(client, &conf).await;
        }

        if self.list {
            return self.handle_list(client, &conf).await;
        }

        if self.add_decommission {
            return self.handle_add_decommission(client, &conf).await;
        }

        if self.remove_decommission {
            return self.handle_remove_decommission(client, &conf).await;
        }

        Ok(())
    }
}

/// Release-version distribution across live workers, as counted by
/// [`NodeCommand::count_worker_versions`].
struct VersionCounts {
    /// Map of display version -> worker count. BTreeMap keeps output stable.
    versions: BTreeMap<String, u32>,
    /// Workers with no usable version data (empty component_info or missing
    /// release version).
    legacy_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker_with_component_info(version: &str) -> serde_json::Value {
        serde_json::json!({
            "address": {"hostname": "worker-host", "rpc_port": 1234},
            "status": "Live",
            "software_version": "0.0.0-old",
            "component_info": {
                "component": "worker",
                "release_version": version,
                "protocol_version": 1
            }
        })
    }

    fn worker_with_legacy_software_version(version: &str) -> serde_json::Value {
        serde_json::json!({
            "address": {"hostname": "worker-host", "rpc_port": 1234},
            "status": "Live",
            "software_version": version
        })
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
    fn count_worker_versions_treats_empty_component_info_as_legacy() {
        // component_info present but without a release_version must not
        // override the legacy software_version fallback; it counts as legacy.
        let workers = vec![serde_json::json!({
            "address": {"hostname": "worker-host", "rpc_port": 1234},
            "status": "Live",
            "software_version": "0.2.0-test",
            "component_info": {}
        })];

        let counts = NodeCommand::count_worker_versions(&workers);

        assert!(counts.versions.is_empty());
        assert_eq!(counts.legacy_count, 1);
    }
}
