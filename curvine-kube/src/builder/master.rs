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
use crate::pod_template_utils::load_pod_from_template_file;
use curvine_common::conf::ClusterConf;
use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use k8s_openapi::api::core::v1::{Container, PodSpec, PodTemplateSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use std::collections::BTreeMap;
use std::path::Path;

const CURVINE_HOME: &str = "/app/curvine";

pub struct MasterBuilder {
    cluster_id: String,
    namespace: String,
    config: KubernetesConfig,
    cluster_conf: ClusterConf,
    is_update_mode: bool,
}

impl PodBuilder for MasterBuilder {
    fn component_name(&self) -> &'static str {
        "master"
    }

    fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    fn build_base_pod(&self) -> Result<k8s_openapi::api::core::v1::Pod> {
        MasterBuilder::build_base_pod_impl(self)
    }

    fn build_volumes(&self) -> Result<Vec<k8s_openapi::api::core::v1::Volume>> {
        MasterBuilder::build_volumes_impl(self)
    }

    fn build_volume_mounts(&self) -> Result<Vec<k8s_openapi::api::core::v1::VolumeMount>> {
        MasterBuilder::build_volume_mounts_impl(self)
    }

    fn pod_template_path(&self) -> Option<&str> {
        self.config.master.pod_template.as_deref()
    }

    fn main_container_name(&self) -> &'static str {
        "cv-master"
    }
}

impl MasterBuilder {
    pub fn new(
        cluster_id: String,
        namespace: String,
        config: KubernetesConfig,
        cluster_conf: ClusterConf,
        is_update_mode: bool,
    ) -> Self {
        Self {
            cluster_id,
            namespace,
            config,
            cluster_conf,
            is_update_mode,
        }
    }

    pub fn build(&self) -> Result<StatefulSet> {
        self.build_with_owner(None)
    }

    pub fn build_with_owner(&self, owner_uid: Option<String>) -> Result<StatefulSet> {
        let template_pod = if let Some(ref template_file) = self.config.master.pod_template {
            Some(load_pod_from_template_file(
                template_file,
                CONTAINER_NAME_MASTER,
            )?)
        } else {
            None
        };

        let builder_pod = self.build_base_pod_impl()?;

        let builder_volumes = self.build_volumes_impl()?;
        let builder_mounts = self.build_volume_mounts_impl()?;

        let builder_labels = self.get_labels();
        let final_pod = merge_pod_with_template(
            template_pod,
            builder_pod,
            builder_volumes,
            builder_mounts,
            builder_labels,
        )?;

        let mut metadata = ObjectMeta {
            name: Some(format!("{}-master", self.cluster_id)),
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

        let stateful_set = StatefulSet {
            metadata,
            spec: Some(StatefulSetSpec {
                replicas: if self.is_update_mode {
                    None
                } else {
                    Some(self.config.master.replicas as i32)
                },
                service_name: Some(format!("{}-master", self.cluster_id)),
                selector: LabelSelector {
                    match_labels: Some(self.get_selector_labels()),
                    ..Default::default()
                },
                template: PodTemplateSpec {
                    metadata: Some(final_pod.metadata.clone()),
                    spec: final_pod.spec,
                },
                volume_claim_templates: Some(self.build_volume_claim_templates()?),
                pod_management_policy: Some("Parallel".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        Ok(stateful_set)
    }

    pub fn build_base_pod_impl(&self) -> Result<k8s_openapi::api::core::v1::Pod> {
        let env_vars = EnvironmentBuilder::new(
            "master",
            self.cluster_id.clone(),
            self.namespace.clone(),
            self.config.cluster_domain.clone(),
        )
        .with_custom_vars(&self.config.master.env_vars)
        .build();

        let container = Container {
            name: CONTAINER_NAME_MASTER.to_string(),
            image: Some(self.config.master.image.clone()),
            image_pull_policy: Some(self.config.image_pull_policy.clone()),
            args: Some(vec![COMPONENT_MASTER.to_string()]),
            env: Some(env_vars),
            working_dir: Some(APP_HOME.to_string()),
            ports: Some(vec![
                k8s_openapi::api::core::v1::ContainerPort {
                    container_port: MASTER_RPC_PORT,
                    name: Some(PORT_NAME_RPC.to_string()),
                    ..Default::default()
                },
                k8s_openapi::api::core::v1::ContainerPort {
                    container_port: MASTER_JOURNAL_PORT,
                    name: Some(PORT_NAME_JOURNAL.to_string()),
                    ..Default::default()
                },
                k8s_openapi::api::core::v1::ContainerPort {
                    container_port: MASTER_WEB_PORT,
                    name: Some(PORT_NAME_WEB.to_string()),
                    ..Default::default()
                },
                k8s_openapi::api::core::v1::ContainerPort {
                    container_port: MASTER_WEB1_PORT,
                    name: Some(PORT_NAME_WEB1.to_string()),
                    ..Default::default()
                },
            ]),
            readiness_probe: None,
            liveness_probe: Some(k8s_openapi::api::core::v1::Probe {
                tcp_socket: Some(k8s_openapi::api::core::v1::TCPSocketAction {
                    port: k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(
                        MASTER_RPC_PORT,
                    ),
                    ..Default::default()
                }),
                initial_delay_seconds: Some(LIVENESS_INITIAL_DELAY),
                period_seconds: Some(LIVENESS_PERIOD),
                timeout_seconds: Some(LIVENESS_TIMEOUT),
                failure_threshold: Some(LIVENESS_FAILURE_THRESHOLD),
                ..Default::default()
            }),
            resources: self.config.master.resources.clone(),
            lifecycle: LifecycleBuilder::build_default_graceful_shutdown(
                "master",
                self.config.master.graceful_shutdown,
            ),
            ..Default::default()
        };

        let mut all_labels = self.get_selector_labels();
        for (k, v) in &self.config.master.labels {
            all_labels.insert(k.clone(), v.clone());
        }

        let annotations = if !self.config.master.annotations.is_empty() {
            Some(self.config.master.annotations.clone().into_iter().collect())
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
                node_selector: self.config.master.node_selector.as_ref().map(|hm| {
                    let mut btm = BTreeMap::new();
                    for (k, v) in hm {
                        btm.insert(k.clone(), v.clone());
                    }
                    btm
                }),
                affinity: self.config.master.affinity.clone(),
                restart_policy: Some(RESTART_POLICY_ALWAYS.to_string()),
                service_account_name: self.config.master.service_account.clone(),
                tolerations: if !self.config.master.tolerations.is_empty() {
                    Some(self.config.master.tolerations.clone())
                } else {
                    None
                },
                dns_policy: self.config.master.dns_policy.clone(),
                priority_class_name: self.config.master.priority_class.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };

        Ok(pod)
    }

    pub fn build_volumes_impl(&self) -> Result<Vec<k8s_openapi::api::core::v1::Volume>> {
        let mut volumes = Vec::new();

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

        Ok(volumes)
    }

    pub fn build_volume_mounts_impl(&self) -> Result<Vec<k8s_openapi::api::core::v1::VolumeMount>> {
        let mut mounts = Vec::new();

        mounts.push(k8s_openapi::api::core::v1::VolumeMount {
            name: VOLUME_NAME_CONFIG.to_string(),
            mount_path: CURVINE_CONF_FILE.to_string(),
            sub_path: Some(CONFIG_FILE_NAME.to_string()),
            read_only: Some(true),
            ..Default::default()
        });

        let meta_dir = Self::resolve_path(&self.cluster_conf.master.meta_dir);
        mounts.push(k8s_openapi::api::core::v1::VolumeMount {
            name: VOLUME_NAME_META_DATA.to_string(),
            mount_path: meta_dir,
            read_only: Some(false),
            ..Default::default()
        });

        let journal_dir = Self::resolve_path(&self.cluster_conf.journal.journal_dir);
        mounts.push(k8s_openapi::api::core::v1::VolumeMount {
            name: VOLUME_NAME_JOURNAL_DATA.to_string(),
            mount_path: journal_dir,
            read_only: Some(false),
            ..Default::default()
        });

        Ok(mounts)
    }

    fn resolve_path(path: &str) -> String {
        if Path::new(path).is_absolute() {
            path.to_string()
        } else {
            format!("{}/{}", CURVINE_HOME, path)
        }
    }

    pub fn build_volume_claim_templates(
        &self,
    ) -> Result<Vec<k8s_openapi::api::core::v1::PersistentVolumeClaim>> {
        let mut templates = Vec::new();

        let storage_size = self
            .config
            .storage
            .as_ref()
            .and_then(|s| s.master_size.clone())
            .unwrap_or_else(|| DEFAULT_STORAGE_SIZE.to_string());

        let storage_class = self.config.storage.as_ref().and_then(|s| {
            s.master_storage_class.clone().or_else(|| {
                if s.storage_class.is_empty() {
                    None
                } else {
                    Some(s.storage_class.clone())
                }
            })
        });

        templates.push(PersistentVolumeClaim {
            metadata: ObjectMeta {
                name: Some(VOLUME_NAME_META_DATA.to_string()),
                labels: Some(self.get_labels()),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::core::v1::PersistentVolumeClaimSpec {
                access_modes: Some(vec![DEFAULT_ACCESS_MODE.to_string()]),
                storage_class_name: storage_class.clone(),
                resources: Some(k8s_openapi::api::core::v1::VolumeResourceRequirements {
                    requests: Some({
                        let mut reqs = std::collections::BTreeMap::new();
                        reqs.insert(
                            "storage".to_string(),
                            k8s_openapi::apimachinery::pkg::api::resource::Quantity(
                                storage_size.clone(),
                            ),
                        );
                        reqs
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            status: None,
        });

        templates.push(PersistentVolumeClaim {
            metadata: ObjectMeta {
                name: Some(VOLUME_NAME_JOURNAL_DATA.to_string()),
                labels: Some(self.get_labels()),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::core::v1::PersistentVolumeClaimSpec {
                access_modes: Some(vec![DEFAULT_ACCESS_MODE.to_string()]),
                storage_class_name: storage_class,
                resources: Some(k8s_openapi::api::core::v1::VolumeResourceRequirements {
                    requests: Some({
                        let mut reqs = std::collections::BTreeMap::new();
                        reqs.insert(
                            "storage".to_string(),
                            k8s_openapi::apimachinery::pkg::api::resource::Quantity(storage_size),
                        );
                        reqs
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            status: None,
        });

        Ok(templates)
    }

    pub fn get_labels(&self) -> BTreeMap<String, String> {
        <Self as PodBuilder>::get_labels(self)
    }

    pub fn get_selector_labels(&self) -> BTreeMap<String, String> {
        <Self as PodBuilder>::get_selector_labels(self)
    }
}
