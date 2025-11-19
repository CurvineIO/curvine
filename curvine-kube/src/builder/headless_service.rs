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
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::core::v1::ServicePort;
use k8s_openapi::api::core::v1::ServiceSpec;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use std::collections::BTreeMap;

const MASTER_SERVICE_PORTS: &[(&str, i32)] = &[
    ("rpc", 8995),
    ("journal", 8996),
    ("web", 9000),
    ("web1", 9001),
];

pub struct HeadlessServiceBuilder {
    cluster_id: String,
    namespace: String,
}

impl HeadlessServiceBuilder {
    pub fn new(cluster_id: String, namespace: String) -> Self {
        Self {
            cluster_id,
            namespace,
        }
    }

    pub fn build(&self) -> Result<Service, KubeError> {
        self.build_with_owner(None)
    }

    pub fn build_with_owner(&self, owner_uid: Option<String>) -> Result<Service, KubeError> {
        let ports = MASTER_SERVICE_PORTS
            .iter()
            .map(|(name, port)| self.create_service_port(name, *port))
            .collect();

        let mut selector = BTreeMap::new();
        selector.insert("app".to_string(), self.cluster_id.clone());
        selector.insert("component".to_string(), "master".to_string());

        let mut metadata = ObjectMeta {
            name: Some(format!("{}-master-headless", self.cluster_id)),
            namespace: Some(self.namespace.clone()),
            labels: Some(self.get_labels()),
            ..Default::default()
        };

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
                cluster_ip: Some("None".to_string()),
                type_: Some("ClusterIP".to_string()),
                ports: Some(ports),
                selector: Some(selector),
                publish_not_ready_addresses: Some(true),
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
        labels.insert("service-type".to_string(), "headless".to_string());
        labels
    }
}
