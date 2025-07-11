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
use curvine_client::file::FsClient;
use curvine_common::conf::ClusterConf;
use orpc::{err_box, CommonResult, common::ByteUnit};
use std::collections::HashMap;
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(arg_required_else_help = true)]
pub struct NodeCommand {
    /// list all nodes
    #[arg(long, short = 'l')]
    list: bool,

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
        let master_info = client.get_master_info().await?;
        let master_parts: Vec<&str> = master_info.active_master.split(':').collect();
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
        println!("{:<25} {:<15} {:<15} {:<15}", "Address", "Status", "Capacity", "Available");
        println!("{}", "-".repeat(70));
        
        // Process live_workers
        for worker in live_workers {
            let hostname = worker["address"]["hostname"].as_str().unwrap_or("Unknown");
            let rpc_port = worker["address"]["rpc_port"].as_u64().unwrap_or(0);
            let address = format!("{hostname}:{rpc_port}");
            let status = worker["status"].as_str().unwrap_or("Unknown");
            let capacity = worker["capacity"].as_i64().unwrap_or(0);
            let available = worker["available"].as_i64().unwrap_or(0);
            
            println!("{:<25} {:<15} {:<15} {:<15}", 
                address, 
                status, 
                ByteUnit::byte_to_string(capacity as u64),
                ByteUnit::byte_to_string(available as u64));
        }
        
        // Process lost_workers
        for worker in lost_workers {
            let hostname = worker["address"]["hostname"].as_str().unwrap_or("Unknown");
            let rpc_port = worker["address"]["rpc_port"].as_u64().unwrap_or(0);
            let address = format!("{hostname}:{rpc_port}");
            let status = worker["status"].as_str().unwrap_or("Unknown");
            let capacity = worker["capacity"].as_i64().unwrap_or(0);
            let available = worker["available"].as_i64().unwrap_or(0);
            
            println!("{:<25} {:<15} {:<15} {:<15}", 
                address, 
                status, 
                ByteUnit::byte_to_string(capacity as u64),
                ByteUnit::byte_to_string(available as u64));
        }

        Ok(())
    }

    // Extract hostnames from node addresses (hostname:port format)
    fn extract_hostnames(&self, nodes: &[String]) -> Vec<String> {
        nodes.iter()
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
        operation_type: &str
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
        conf: &ClusterConf
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
        conf: &ClusterConf
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

    pub async fn execute(&self, client: Arc<FsClient>, conf: ClusterConf) -> CommonResult<()> {
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
