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

use crate::builder::{
    pod_merger::merge_pod_with_template, traits::PodBuilder, EnvironmentBuilder, LifecycleBuilder,
};
use crate::config::KubernetesConfig;
use crate::constants::*;
use crate::error::Result;
use crate::pod_template_utils::{format_bytes, load_pod_from_template_file};
use curvine_common::conf::{ClusterConf, WorkerDataDir};
use curvine_common::state::StorageType;
use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{Container, PersistentVolumeClaim, PodSpec, PodTemplateSpec};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use std::collections::BTreeMap;

pub struct WorkerBuilder {
    cluster_id: String,
    namespace: String,
    config: KubernetesConfig,
    cluster_conf: ClusterConf,
}

impl PodBuilder for WorkerBuilder {
    fn component_name(&self) -> &'static str {
        "worker"
    }

    fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    fn build_base_pod(&self) -> Result<k8s_openapi::api::core::v1::Pod> {
        WorkerBuilder::build_base_pod_impl(self)
    }

    fn build_volumes(&self) -> Result<Vec<k8s_openapi::api::core::v1::Volume>> {
        WorkerBuilder::build_volumes_impl(self)
    }

    fn build_volume_mounts(&self) -> Result<Vec<k8s_openapi::api::core::v1::VolumeMount>> {
        WorkerBuilder::build_volume_mounts_impl(self)
    }

    fn pod_template_path(&self) -> Option<&str> {
        self.config.worker.pod_template.as_deref()
    }

    fn main_container_name(&self) -> &'static str {
        "cv-worker"
    }
}

impl WorkerBuilder {
    pub fn new(
        cluster_id: String,
        namespace: String,
        config: KubernetesConfig,
        cluster_conf: ClusterConf,
    ) -> Self {
        Self {
            cluster_id,
            namespace,
            config,
            cluster_conf,
        }
    }

    pub fn build(&self) -> Result<StatefulSet> {
        self.build_with_owner(None)
    }

    pub fn build_with_owner(&self, owner_uid: Option<String>) -> Result<StatefulSet> {
        let template_pod = if let Some(ref template_file) = self.config.worker.pod_template {
            Some(load_pod_from_template_file(
                template_file,
                CONTAINER_NAME_WORKER,
            )?)
        } else {
            None
        };

        let builder_pod = self.build_base_pod_impl()?;

        let builder_volumes = self.build_volumes_impl()?;
        let builder_mounts = self.build_volume_mounts_impl()?;

        // Merge with template
        let builder_labels = self.get_labels();
        let final_pod = merge_pod_with_template(
            template_pod,
            builder_pod,
            builder_volumes,
            builder_mounts,
            builder_labels,
        )?;

        // Build volumeClaimTemplates for StatefulSet
        let volume_claim_templates = self.build_volume_claim_templates()?;

        let mut metadata = ObjectMeta {
            name: Some(format!("{}-worker", self.cluster_id)),
            namespace: Some(self.namespace.clone()),
            labels: Some(self.get_labels()),
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

        // Build StatefulSet
        let statefulset = StatefulSet {
            metadata,
            spec: Some(StatefulSetSpec {
                replicas: Some(self.config.worker.replicas as i32),
                service_name: Some(format!("{}-worker", self.cluster_id)),
                selector: LabelSelector {
                    match_labels: Some(self.get_selector_labels()),
                    ..Default::default()
                },
                template: PodTemplateSpec {
                    metadata: Some(final_pod.metadata.clone()),
                    spec: final_pod.spec,
                },
                volume_claim_templates,
                pod_management_policy: Some("Parallel".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        Ok(statefulset)
    }

    pub fn build_base_pod_impl(&self) -> Result<k8s_openapi::api::core::v1::Pod> {
        // Use unified environment builder
        let env_vars = EnvironmentBuilder::new(
            "worker",
            self.cluster_id.clone(),
            self.namespace.clone(),
            self.config.cluster_domain.clone(),
        )
        .with_custom_vars(&self.config.worker.env_vars)
        .build();

        // Build container with environment variables
        let container = Container {
            name: CONTAINER_NAME_WORKER.to_string(),
            image: Some(self.config.worker.image.clone()),
            image_pull_policy: Some(self.config.image_pull_policy.clone()),
            args: Some(vec![COMPONENT_WORKER.to_string()]),
            env: Some(env_vars),
            working_dir: Some(APP_HOME.to_string()),
            ports: Some(vec![
                k8s_openapi::api::core::v1::ContainerPort {
                    container_port: WORKER_RPC_PORT,
                    name: Some(PORT_NAME_RPC.to_string()),
                    ..Default::default()
                },
                k8s_openapi::api::core::v1::ContainerPort {
                    container_port: WORKER_WEB_PORT,
                    name: Some(PORT_NAME_WEB.to_string()),
                    ..Default::default()
                },
            ]),
            resources: self.config.worker.resources.clone(),
            security_context: Some(k8s_openapi::api::core::v1::SecurityContext {
                privileged: Some(SECURITY_PRIVILEGED),
                ..Default::default()
            }),
            lifecycle: LifecycleBuilder::build_default_graceful_shutdown(
                "worker",
                self.config.worker.graceful_shutdown,
            ),
            ..Default::default()
        };

        // Merge selector labels with user labels
        let mut all_labels = self.get_selector_labels();
        for (k, v) in &self.config.worker.labels {
            all_labels.insert(k.clone(), v.clone());
        }

        // Build annotations
        let annotations = if !self.config.worker.annotations.is_empty() {
            Some(self.config.worker.annotations.clone().into_iter().collect())
        } else {
            None
        };

        // Determine DNS policy
        let dns_policy = if let Some(ref policy) = self.config.worker.dns_policy {
            Some(policy.clone())
        } else if self.config.worker.host_network {
            Some(DNS_POLICY_CLUSTER_FIRST_WITH_HOST_NET.to_string())
        } else {
            None
        };

        let pod = k8s_openapi::api::core::v1::Pod {
            metadata: ObjectMeta {
                labels: Some(all_labels),
                annotations,
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![container],
                restart_policy: Some(RESTART_POLICY_ALWAYS.to_string()),
                host_network: Some(self.config.worker.host_network),
                dns_policy,
                init_containers: self.build_init_containers(),
                node_selector: self.config.worker.node_selector.as_ref().map(|hm| {
                    let mut btm = BTreeMap::new();
                    for (k, v) in hm {
                        btm.insert(k.clone(), v.clone());
                    }
                    btm
                }),
                affinity: self.build_affinity(),
                service_account_name: self.config.worker.service_account.clone(),
                tolerations: if !self.config.worker.tolerations.is_empty() {
                    Some(self.config.worker.tolerations.clone())
                } else {
                    None
                },
                priority_class_name: self.config.worker.priority_class.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };

        Ok(pod)
    }

    pub fn parse_data_dirs(&self) -> Result<Vec<(usize, WorkerDataDir)>> {
        let mut data_dirs = Vec::new();

        for (index, data_dir_str) in self.cluster_conf.worker.data_dir.iter().enumerate() {
            let data_dir = WorkerDataDir::from_str(data_dir_str).map_err(|e| {
                crate::error::KubeError::ConfigError(format!(
                    "Invalid data_dir format '{}': {}",
                    data_dir_str, e
                ))
            })?;

            data_dirs.push((index, data_dir));
        }

        Ok(data_dirs)
    }

    pub fn build_volume_claim_templates(&self) -> Result<Option<Vec<PersistentVolumeClaim>>> {
        let data_dirs = self.parse_data_dirs()?;
        let mut templates = Vec::new();

        for (index, data_dir) in data_dirs {
            match data_dir.storage_type {
                StorageType::Mem => {
                    continue;
                }
                StorageType::Ssd | StorageType::Hdd | StorageType::Disk | StorageType::Ufs => {
                    let volume_name = format!("{}{}", VOLUME_NAME_DATA_DIR_PREFIX, index);

                    let storage_size = if let Some(ref storage_config) = self.config.storage {
                        storage_config
                            .worker_size
                            .clone()
                            .unwrap_or_else(|| "20Gi".to_string())
                    } else {
                        "20Gi".to_string()
                    };

                    let mut resources = BTreeMap::new();
                    resources.insert("storage".to_string(), Quantity(storage_size));

                    let pvc_template = PersistentVolumeClaim {
                        metadata: ObjectMeta {
                            name: Some(volume_name.clone()),
                            ..Default::default()
                        },
                        spec: Some(k8s_openapi::api::core::v1::PersistentVolumeClaimSpec {
                            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                            resources: Some(
                                k8s_openapi::api::core::v1::VolumeResourceRequirements {
                                    requests: Some(resources),
                                    ..Default::default()
                                },
                            ),
                            storage_class_name: self.config.storage.as_ref().and_then(|s| {
                                s.worker_storage_class.clone().or_else(|| {
                                    if s.storage_class.is_empty() {
                                        None
                                    } else {
                                        Some(s.storage_class.clone())
                                    }
                                })
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    };

                    templates.push(pvc_template);
                }
            }
        }

        if templates.is_empty() {
            Ok(None)
        } else {
            Ok(Some(templates))
        }
    }

    pub fn build_volumes_impl(&self) -> Result<Vec<k8s_openapi::api::core::v1::Volume>> {
        let mut volumes = Vec::new();

        // ConfigMap Volume (required)
        volumes.push(k8s_openapi::api::core::v1::Volume {
            name: VOLUME_NAME_CONFIG.to_string(),
            config_map: Some(k8s_openapi::api::core::v1::ConfigMapVolumeSource {
                name: format!("{}{}", self.cluster_id, SERVICE_SUFFIX_CONFIG),
                default_mode: Some(CONFIG_FILE_MODE),
                items: Some(vec![k8s_openapi::api::core::v1::KeyToPath {
                    key: CONFIG_FILE_NAME.to_string(),
                    path: CONFIG_FILE_NAME.to_string(),
                    mode: Some(CONFIG_FILE_MODE),
                }]),
                optional: Some(false),
            }),
            ..Default::default()
        });

        // Data directory Volumes
        let data_dirs = self.parse_data_dirs()?;
        for (index, data_dir) in data_dirs {
            let volume_name = format!("{}{}", VOLUME_NAME_DATA_DIR_PREFIX, index);

            // Strategy 1: Check if hostPath is configured for this path
            if let Some(ref host_paths) = self.config.worker.host_path_storage {
                if let Some(host_path) = host_paths.get(&data_dir.path) {
                    // Use hostPath storage (for development/testing)
                    volumes.push(k8s_openapi::api::core::v1::Volume {
                        name: volume_name,
                        host_path: Some(k8s_openapi::api::core::v1::HostPathVolumeSource {
                            path: host_path.clone(),
                            type_: Some(VOLUME_TYPE_DIRECTORY_OR_CREATE.to_string()),
                        }),
                        ..Default::default()
                    });
                    continue;
                }
            }

            // Strategy 2: Use appropriate volume type based on storage type
            match data_dir.storage_type {
                StorageType::Mem => {
                    // Memory storage: emptyDir with Memory medium
                    // This is ephemeral but fast, suitable for caching
                    let size_limit = if data_dir.capacity > 0 {
                        Some(k8s_openapi::apimachinery::pkg::api::resource::Quantity(
                            format_bytes(data_dir.capacity),
                        ))
                    } else {
                        None
                    };

                    volumes.push(k8s_openapi::api::core::v1::Volume {
                        name: volume_name,
                        empty_dir: Some(k8s_openapi::api::core::v1::EmptyDirVolumeSource {
                            medium: Some(VOLUME_MEDIUM_MEMORY.to_string()),
                            size_limit,
                        }),
                        ..Default::default()
                    });
                }
                StorageType::Ssd | StorageType::Hdd | StorageType::Disk | StorageType::Ufs => {
                    continue;
                }
            }
        }

        Ok(volumes)
    }

    pub fn build_volume_mounts_impl(&self) -> Result<Vec<k8s_openapi::api::core::v1::VolumeMount>> {
        let mut mounts = Vec::new();

        // ConfigMap VolumeMount (using subPath to avoid overwriting entire directory)
        mounts.push(k8s_openapi::api::core::v1::VolumeMount {
            name: VOLUME_NAME_CONFIG.to_string(),
            mount_path: CURVINE_CONF_FILE.to_string(),
            sub_path: Some(CONFIG_FILE_NAME.to_string()),
            read_only: Some(true),
            ..Default::default()
        });

        // Data directory VolumeMounts
        let data_dirs = self.parse_data_dirs()?;
        for (index, data_dir) in data_dirs {
            let volume_name = format!("{}{}", VOLUME_NAME_DATA_DIR_PREFIX, index);

            mounts.push(k8s_openapi::api::core::v1::VolumeMount {
                name: volume_name,
                mount_path: data_dir.path.clone(),
                read_only: Some(false),
                ..Default::default()
            });
        }

        Ok(mounts)
    }

    // Note: Deployment doesn't support volumeClaimTemplates
    // For persistent storage in Deployment, use static PVCs or hostPath

    /// Get labels for the pod (implements PodBuilder trait)
    pub fn get_labels(&self) -> BTreeMap<String, String> {
        <Self as PodBuilder>::get_labels(self)
    }

    /// Get selector labels (implements PodBuilder trait)
    pub fn get_selector_labels(&self) -> BTreeMap<String, String> {
        <Self as PodBuilder>::get_selector_labels(self)
    }

    // merge_with_template is now in pod_merger module

    /// Build PodAntiAffinity for Worker nodes
    fn build_affinity(&self) -> Option<k8s_openapi::api::core::v1::Affinity> {
        if !self.config.worker.anti_affinity {
            return None;
        }

        Some(k8s_openapi::api::core::v1::Affinity {
            pod_anti_affinity: Some(k8s_openapi::api::core::v1::PodAntiAffinity {
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    k8s_openapi::api::core::v1::WeightedPodAffinityTerm {
                        weight: 100,
                        pod_affinity_term: k8s_openapi::api::core::v1::PodAffinityTerm {
                            label_selector: Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                                match_expressions: Some(vec![
                                    k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement {
                                        key: LABEL_APP.to_string(),
                                        operator: "In".to_string(),
                                        values: Some(vec![self.cluster_id.clone()]),
                                    },
                                    k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement {
                                        key: LABEL_COMPONENT.to_string(),
                                        operator: "In".to_string(),
                                        values: Some(vec![COMPONENT_WORKER.to_string()]),
                                    },
                                ]),
                                ..Default::default()
                            }),
                            topology_key: TOPOLOGY_KEY_HOSTNAME.to_string(),
                            ..Default::default()
                        },
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    /// Build init containers for startup dependencies
    fn build_init_containers(&self) -> Option<Vec<Container>> {
        if !self.config.worker.init_container {
            return None;
        }

        // InitContainer to wait for Master to be ready
        Some(vec![Container {
            name: "wait-for-master".to_string(),
            image: Some(INIT_CONTAINER_IMAGE.to_string()),
            command: Some(vec!["sh".to_string(), "-c".to_string()]),
            args: Some(vec![format!(
                "until nc -z {}{}{}.{}.svc.{} {}; do echo waiting for master; sleep 2; done",
                self.cluster_id,
                SERVICE_SUFFIX_MASTER,
                SERVICE_SUFFIX_HEADLESS,
                self.namespace,
                self.config.cluster_domain,
                MASTER_RPC_PORT
            )]),
            ..Default::default()
        }])
    }
}
