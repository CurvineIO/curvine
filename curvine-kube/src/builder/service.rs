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

use crate::config::ServiceType;
use crate::error::KubeError;
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::core::v1::ServicePort;
use k8s_openapi::api::core::v1::ServiceSpec;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use std::collections::{BTreeMap, HashMap};

const MASTER_SERVICE_PORTS: &[(&str, i32)] = &[
    ("rpc", 8995),
    ("journal", 8996),
    ("web", 9000),
    ("web1", 9001),
];

const WORKER_SERVICE_PORT: (&str, i32) = ("worker", 8997);

pub struct ServiceBuilder {
    cluster_id: String,
    namespace: String,
    service_config: ServiceType,
    annotations: HashMap<String, String>,
    session_affinity: Option<String>,
    external_ips: Vec<String>,
    load_balancer_source_ranges: Vec<String>,
}

impl ServiceBuilder {
    pub fn new(
        cluster_id: String,
        namespace: String,
        service_config: ServiceType,
        annotations: HashMap<String, String>,
    ) -> Self {
        Self {
            cluster_id,
            namespace,
            service_config,
            annotations,
            session_affinity: None,
            external_ips: Vec::new(),
            load_balancer_source_ranges: Vec::new(),
        }
    }

    pub fn with_config(
        cluster_id: String,
        namespace: String,
        service_config: ServiceType,
        annotations: HashMap<String, String>,
        session_affinity: Option<String>,
        external_ips: Vec<String>,
        load_balancer_source_ranges: Vec<String>,
    ) -> Self {
        Self {
            cluster_id,
            namespace,
            service_config,
            annotations,
            session_affinity,
            external_ips,
            load_balancer_source_ranges,
        }
    }

    pub fn build(&self) -> Result<Service, KubeError> {
        self.build_with_owner(None)
    }

    pub fn build_with_owner(&self, owner_uid: Option<String>) -> Result<Service, KubeError> {
        let mut ports = Vec::new();

        for (name, port) in MASTER_SERVICE_PORTS {
            ports.push(self.create_service_port(name, *port));
        }

        ports.push(self.create_service_port(WORKER_SERVICE_PORT.0, WORKER_SERVICE_PORT.1));

        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), self.cluster_id.clone());
        selector.insert("component".to_string(), "master".to_string());

        let annotations = if self.annotations.is_empty() {
            None
        } else {
            let mut btree_annotations = std::collections::BTreeMap::new();
            for (k, v) in &self.annotations {
                btree_annotations.insert(k.clone(), v.clone());
            }
            Some(btree_annotations)
        };

        let mut metadata = ObjectMeta {
            name: Some(format!("{}-master", self.cluster_id)),
            namespace: Some(self.namespace.clone()),
            labels: Some(self.get_labels()),
            annotations,
            ..Default::default()
        };

        // Add owner reference if provided
        if let Some(uid) = owner_uid {
            metadata.owner_references = Some(vec![
                k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
                    api_version: "v1".to_string(),
                    kind: "ConfigMap".to_string(),
                    name: format!("{}-config", self.cluster_id),
                    uid,
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                },
            ]);
        }

        let service = Service {
            metadata,
            spec: Some(ServiceSpec {
                type_: Some(self.service_config.as_str().to_string()),
                ports: Some(ports),
                selector: Some(selector),
                cluster_ip: if matches!(self.service_config, ServiceType::ClusterIP) {
                    None
                } else {
                    Some("None".to_string())
                },
                session_affinity: self.session_affinity.clone(),
                external_ips: if self.external_ips.is_empty() {
                    None
                } else {
                    Some(self.external_ips.clone())
                },
                load_balancer_source_ranges: if self.load_balancer_source_ranges.is_empty() {
                    None
                } else {
                    Some(self.load_balancer_source_ranges.clone())
                },
                ..Default::default()
            }),
            ..Default::default()
        };

        Ok(service)
    }

    fn create_service_port(&self, name: &str, port: i32) -> ServicePort {
        ServicePort {
            name: Some(name.to_string()),
            port,
            target_port: Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(port)),
            protocol: Some("TCP".to_string()),
            ..Default::default()
        }
    }

    pub fn get_labels(&self) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert("app".to_string(), self.cluster_id.clone());
        labels.insert("component".to_string(), "master".to_string());
        labels.insert("type".to_string(), "curvine-native-kubernetes".to_string());
        labels
    }
}
