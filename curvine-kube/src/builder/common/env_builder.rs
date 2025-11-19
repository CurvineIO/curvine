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

//! Environment variable builder for Curvine components
//!
//! This module provides a unified way to build environment variables
//! for different Curvine components, eliminating code duplication.

use crate::constants::{APP_HOME, CURVINE_CONF_FILE, CURVINE_HOME, ORPC_BIND_HOSTNAME};
use k8s_openapi::api::core::v1::{EnvVar, EnvVarSource, ObjectFieldSelector};
use std::collections::HashMap;

pub struct EnvironmentBuilder {
    component: &'static str,
    cluster_id: String,
    namespace: String,
    cluster_domain: String,
    custom_vars: HashMap<String, String>,
}

impl EnvironmentBuilder {
    pub fn new(
        component: &'static str,
        cluster_id: String,
        namespace: String,
        cluster_domain: String,
    ) -> Self {
        Self {
            component,
            cluster_id,
            namespace,
            cluster_domain,
            custom_vars: HashMap::new(),
        }
    }

    pub fn with_custom_vars(mut self, vars: &HashMap<String, String>) -> Self {
        self.custom_vars.extend(vars.clone());
        self
    }

    pub fn build(self) -> Vec<EnvVar> {
        let mut env_vars = Vec::new();
        env_vars.extend(self.build_base_env_vars());
        env_vars.extend(self.build_k8s_env_vars());
        env_vars.extend(self.build_component_env_vars());
        env_vars.extend(self.build_custom_env_vars());
        env_vars
    }

    fn build_base_env_vars(&self) -> Vec<EnvVar> {
        vec![
            EnvVar {
                name: "APP_HOME".to_string(),
                value: Some(APP_HOME.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "CURVINE_HOME".to_string(),
                value: Some(CURVINE_HOME.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "CURVINE_CONF_FILE".to_string(),
                value: Some(CURVINE_CONF_FILE.to_string()),
                ..Default::default()
            },
            EnvVar {
                name: "ORPC_BIND_HOSTNAME".to_string(),
                value: Some(ORPC_BIND_HOSTNAME.to_string()),
                ..Default::default()
            },
        ]
    }

    fn build_k8s_env_vars(&self) -> Vec<EnvVar> {
        vec![
            EnvVar {
                name: "POD_IP".to_string(),
                value_from: Some(EnvVarSource {
                    field_ref: Some(ObjectFieldSelector {
                        field_path: "status.podIP".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            EnvVar {
                name: "POD_NAMESPACE".to_string(),
                value_from: Some(EnvVarSource {
                    field_ref: Some(ObjectFieldSelector {
                        field_path: "metadata.namespace".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            EnvVar {
                name: "POD_CLUSTER_DOMAIN".to_string(),
                value: Some(self.cluster_domain.clone()),
                ..Default::default()
            },
        ]
    }

    fn build_component_env_vars(&self) -> Vec<EnvVar> {
        let mut env_vars = Vec::new();

        match self.component {
            "master" => {
                env_vars.push(EnvVar {
                    name: "POD_NAME".to_string(),
                    value_from: Some(EnvVarSource {
                        field_ref: Some(ObjectFieldSelector {
                            field_path: "metadata.name".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                });

                let master_hostname_fqdn = format!(
                    "$(POD_NAME).{}-master.{}.svc.{}",
                    self.cluster_id, self.namespace, self.cluster_domain
                );
                env_vars.push(EnvVar {
                    name: "CURVINE_MASTER_HOSTNAME".to_string(),
                    value: Some(master_hostname_fqdn),
                    ..Default::default()
                });
            }
            "worker" => {
                env_vars.push(EnvVar {
                    name: "POD_NAME".to_string(),
                    value_from: Some(EnvVarSource {
                        field_ref: Some(ObjectFieldSelector {
                            field_path: "metadata.name".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                });

                // Master hostname for worker to connect
                let master_hostname = format!(
                    "{}-master-0.{}-master.{}.svc.{}",
                    self.cluster_id, self.cluster_id, self.namespace, self.cluster_domain
                );
                env_vars.push(EnvVar {
                    name: "CURVINE_MASTER_HOSTNAME".to_string(),
                    value: Some(master_hostname),
                    ..Default::default()
                });

                // Worker hostname: Use FQDN for StatefulSet (stable across pod restarts)
                // Format: {pod-name}.{service-name}.{namespace}.svc.{cluster-domain}
                // Example: test-worker-0.test-worker.default.svc.cluster.local
                let worker_hostname_fqdn = format!(
                    "$(POD_NAME).{}-worker.{}.svc.{}",
                    self.cluster_id, self.namespace, self.cluster_domain
                );
                env_vars.push(EnvVar {
                    name: "CURVINE_WORKER_HOSTNAME".to_string(),
                    value: Some(worker_hostname_fqdn),
                    ..Default::default()
                });
            }
            _ => {}
        }

        env_vars
    }

    fn build_custom_env_vars(&self) -> Vec<EnvVar> {
        self.custom_vars
            .iter()
            .map(|(key, value)| EnvVar {
                name: key.clone(),
                value: Some(value.clone()),
                ..Default::default()
            })
            .collect()
    }
}
