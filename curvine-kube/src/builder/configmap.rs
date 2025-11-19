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

use crate::config::KubernetesConfigBuilder;
use crate::error::KubeError;
use curvine_common::conf::ClusterConf;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use std::collections::{BTreeMap, HashMap};

pub struct ConfigMapBuilder {
    cluster_conf: ClusterConf,
    cluster_id: String,
    namespace: String,
    master_replicas: u32,
}

impl ConfigMapBuilder {
    pub fn new(
        cluster_conf: ClusterConf,
        cluster_id: String,
        namespace: String,
        master_replicas: u32,
    ) -> Self {
        Self {
            cluster_conf,
            cluster_id,
            namespace,
            master_replicas,
        }
    }

    pub fn build(&self) -> Result<ConfigMap, KubeError> {
        let cluster_side_config = self.build_cluster_side_config()?;

        let metadata = ObjectMeta {
            name: Some(format!("{}-config", self.cluster_id)),
            namespace: Some(self.namespace.clone()),
            labels: Some(self.get_labels()),
            ..Default::default()
        };

        let mut data = std::collections::BTreeMap::new();
        data.insert("curvine-cluster.toml".to_string(), cluster_side_config);

        let config_map = ConfigMap {
            metadata,
            data: Some(data),
            ..Default::default()
        };

        Ok(config_map)
    }

    fn build_cluster_side_config(&self) -> Result<String, KubeError> {
        use crate::config::{
            KubernetesConfig, MasterConfig, ServiceConfig, ServiceType, WorkerConfig,
        };

        let kube_config = KubernetesConfig {
            cluster_id: self.cluster_id.clone(),
            namespace: self.namespace.clone(),
            master: MasterConfig {
                replicas: self.master_replicas,
                image: String::new(),
                resources: None,
                node_selector: None,
                affinity: None,
                pod_template: None,
                graceful_shutdown: false,
                labels: HashMap::new(),
                annotations: HashMap::new(),
                tolerations: Vec::new(),
                service_account: None,
                env_vars: HashMap::new(),
                dns_policy: None,
                priority_class: None,
            },
            worker: WorkerConfig {
                replicas: 1,
                image: String::new(),
                resources: None,
                node_selector: None,
                anti_affinity: false,
                pod_template: None,
                storage_class: None,
                graceful_shutdown: false,
                host_network: false,
                init_container: false,
                host_path_storage: None,
                labels: HashMap::new(),
                annotations: HashMap::new(),
                tolerations: Vec::new(),
                service_account: None,
                env_vars: HashMap::new(),
                dns_policy: None,
                priority_class: None,
            },
            service: ServiceConfig {
                service_type: ServiceType::ClusterIP,
                annotations: HashMap::new(),
                session_affinity: None,
                external_ips: Vec::new(),
                load_balancer_source_ranges: Vec::new(),
            },
            storage: None,
            image_pull_policy: "IfNotPresent".to_string(),
            image_pull_secrets: Vec::new(),
            cluster_domain: "cluster.local".to_string(),
        };

        let config_builder = KubernetesConfigBuilder::new(self.cluster_conf.clone(), kube_config);
        config_builder.build_cluster_side_config()
    }

    pub fn get_labels(&self) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert("app".to_string(), self.cluster_id.clone());
        labels.insert("component".to_string(), "config".to_string());
        labels.insert("type".to_string(), "curvine-native-kubernetes".to_string());
        labels
    }
}
