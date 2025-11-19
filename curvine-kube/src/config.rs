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

use crate::error::KubeError;
use curvine_common::conf::ClusterConf;
use k8s_openapi::api::core::v1::ResourceRequirements;
use std::collections::HashMap;
use std::path::Path;

const CURVINE_HOME: &str = "/app/curvine";

#[derive(Debug, Clone)]
pub struct KubernetesConfig {
    pub cluster_id: String,
    pub namespace: String,
    pub master: MasterConfig,
    pub worker: WorkerConfig,
    pub service: ServiceConfig,
    pub storage: Option<StorageConfig>,
    pub image_pull_policy: String,
    pub image_pull_secrets: Vec<String>,
    pub cluster_domain: String,
}

#[derive(Debug, Clone)]
pub struct MasterConfig {
    pub replicas: u32,
    pub image: String,
    pub resources: Option<ResourceRequirements>,
    pub node_selector: Option<HashMap<String, String>>,
    pub affinity: Option<k8s_openapi::api::core::v1::Affinity>,
    pub pod_template: Option<String>,
    pub graceful_shutdown: bool,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub tolerations: Vec<k8s_openapi::api::core::v1::Toleration>,
    pub service_account: Option<String>,
    pub env_vars: HashMap<String, String>,
    pub dns_policy: Option<String>,
    pub priority_class: Option<String>,
}

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub replicas: u32,
    pub image: String,
    pub resources: Option<ResourceRequirements>,
    pub node_selector: Option<HashMap<String, String>>,
    pub anti_affinity: bool,
    pub pod_template: Option<String>,
    pub storage_class: Option<String>,
    pub graceful_shutdown: bool,
    pub host_network: bool,
    pub init_container: bool,
    pub host_path_storage: Option<HashMap<String, String>>, // path -> hostPath mapping
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub tolerations: Vec<k8s_openapi::api::core::v1::Toleration>,
    pub service_account: Option<String>,
    pub env_vars: HashMap<String, String>,
    pub dns_policy: Option<String>,
    pub priority_class: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub service_type: ServiceType,
    pub annotations: HashMap<String, String>,
    pub session_affinity: Option<String>,
    pub external_ips: Vec<String>,
    pub load_balancer_source_ranges: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceType {
    ClusterIP,
    NodePort,
    LoadBalancer,
}

impl ServiceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ServiceType::ClusterIP => "ClusterIP",
            ServiceType::NodePort => "NodePort",
            ServiceType::LoadBalancer => "LoadBalancer",
        }
    }
}

impl std::str::FromStr for ServiceType {
    type Err = KubeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ClusterIP" => Ok(ServiceType::ClusterIP),
            "NodePort" => Ok(ServiceType::NodePort),
            "LoadBalancer" => Ok(ServiceType::LoadBalancer),
            _ => Err(KubeError::ConfigError(format!(
                "Invalid service type: {}",
                s
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub storage_class: String,
    pub master_storage_class: Option<String>,
    pub worker_storage_class: Option<String>,
    pub master_size: Option<String>,
    pub worker_size: Option<String>,
}

pub struct KubernetesConfigBuilder {
    cluster_conf: ClusterConf,
    kube_config: KubernetesConfig,
}

impl KubernetesConfigBuilder {
    pub fn new(cluster_conf: ClusterConf, kube_config: KubernetesConfig) -> Self {
        Self {
            cluster_conf,
            kube_config,
        }
    }

    pub fn build_cluster_side_config(&self) -> Result<String, KubeError> {
        let mut cluster_side_conf = self.cluster_conf.clone();

        let master_addrs: Vec<_> = (0..self.kube_config.master.replicas)
            .map(|i| {
                format!(
                    "{}-master-{}.{}-master.{}.svc.{}",
                    self.kube_config.cluster_id,
                    i,
                    self.kube_config.cluster_id,
                    self.kube_config.namespace,
                    self.kube_config.cluster_domain
                )
            })
            .collect();

        use curvine_common::raft::RaftPeer;
        cluster_side_conf.journal.journal_addrs = master_addrs
            .iter()
            .enumerate()
            .map(|(i, hostname)| RaftPeer {
                id: (i + 1) as u64,
                hostname: hostname.clone(),
                port: cluster_side_conf.journal.rpc_port,
            })
            .collect();

        use orpc::io::net::InetAddr;
        cluster_side_conf.client.master_addrs = master_addrs
            .iter()
            .map(|hostname| InetAddr::new(hostname.clone(), cluster_side_conf.master.rpc_port))
            .collect();

        cluster_side_conf.master.meta_dir = Self::resolve_path(&self.cluster_conf.master.meta_dir);
        cluster_side_conf.journal.journal_dir =
            Self::resolve_path(&self.cluster_conf.journal.journal_dir);

        toml::to_string(&cluster_side_conf).map_err(|e| KubeError::ConfigError(e.to_string()))
    }

    fn resolve_path(path: &str) -> String {
        if Path::new(path).is_absolute() {
            path.to_string()
        } else {
            format!("{}/{}", CURVINE_HOME, path)
        }
    }
}

impl KubernetesConfig {
    pub fn validate(&self) -> Result<(), KubeError> {
        if !is_valid_k8s_name(&self.cluster_id) {
            return Err(KubeError::ConfigError(format!(
                "Invalid cluster_id: {}",
                self.cluster_id
            )));
        }

        if self.cluster_id.len() > 45 {
            return Err(KubeError::ConfigError(format!(
                "cluster_id too long (max 45 chars): {}",
                self.cluster_id
            )));
        }

        if self.master.replicas == 0 {
            return Err(KubeError::ConfigError(
                "master.replicas must be > 0".to_string(),
            ));
        }

        if self.master.replicas % 2 == 0 {
            return Err(KubeError::ConfigError(
                "master.replicas should be odd for Raft (recommended: 3, 5, 7)".to_string(),
            ));
        }

        if self.worker.replicas == 0 {
            return Err(KubeError::ConfigError(
                "worker.replicas must be > 0".to_string(),
            ));
        }

        let valid_policies = ["Always", "IfNotPresent", "Never"];
        if !valid_policies.contains(&self.image_pull_policy.as_str()) {
            return Err(KubeError::ConfigError(format!(
                "Invalid image_pull_policy: {}",
                self.image_pull_policy
            )));
        }

        Ok(())
    }
}

pub(crate) fn is_valid_k8s_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 253 {
        return false;
    }

    if !name.chars().next().unwrap_or(' ').is_alphanumeric() {
        return false;
    }
    if !name.chars().last().unwrap_or(' ').is_alphanumeric() {
        return false;
    }

    name.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}
