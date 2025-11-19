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
    ConfigMapBuilder, HeadlessServiceBuilder, MasterBuilder, ServiceBuilder, WorkerBuilder,
};
use crate::client::{CurvineKubeClient, CurvineKubeClientImpl};
use crate::config::KubernetesConfig;
use crate::error::KubeError;
use crate::validator::KubernetesValidator;
use curvine_common::conf::ClusterConf;
use std::time::Duration;
use tokio::time::sleep;

pub struct CurvineClusterDescriptor {
    client: Box<dyn CurvineKubeClient>,
    namespace: String,
}

impl CurvineClusterDescriptor {
    pub async fn new(namespace: String) -> Result<Self, KubeError> {
        let client = CurvineKubeClientImpl::new(namespace.clone()).await?;
        Ok(Self {
            client: Box::new(client),
            namespace,
        })
    }

    pub async fn new_with_config(
        namespace: String,
        kubeconfig_path: Option<String>,
        context: Option<String>,
    ) -> Result<Self, KubeError> {
        let client =
            CurvineKubeClientImpl::new_with_config(namespace.clone(), kubeconfig_path, context)
                .await?;
        Ok(Self {
            client: Box::new(client),
            namespace,
        })
    }

    pub async fn deploy_cluster(
        &self,
        cluster_conf: &ClusterConf,
        kube_config: &KubernetesConfig,
    ) -> Result<(), KubeError> {
        use crate::validator::KubernetesValidator;
        let validator = KubernetesValidator::new(self.client.get_client());
        validator
            .validate_deployment(cluster_conf, kube_config)
            .await?;

        let cluster_exists = self
            .client
            .get_statefulset(&format!("{}-master", kube_config.cluster_id))
            .await
            .is_ok();

        if cluster_exists {
            return Err(KubeError::ValidationError(format!(
                "\n Cluster already exists\n\
                \n  Cluster ID: '{}'\n\
                Namespace: '{}'\n\
                \n To modify the cluster, use the 'update' command:\n\
                cv k8s update -c {} [options]\n\
                \n Examples:\n\
                - Scale workers: cv k8s update -c {} --worker-replicas 5\n\
                - Upgrade image: cv k8s update -c {} --master-image docker.io/curvine:v1.0",
                kube_config.cluster_id,
                kube_config.namespace,
                kube_config.cluster_id,
                kube_config.cluster_id,
                kube_config.cluster_id
            )));
        }

        println!("Creating new cluster resources...");
        self.apply_cluster_internal(cluster_conf, kube_config, true)
            .await
    }

    pub async fn update_cluster(
        &self,
        cluster_conf: &ClusterConf,
        kube_config: &KubernetesConfig,
    ) -> Result<(), KubeError> {
        use crate::validator::KubernetesValidator;
        let validator = KubernetesValidator::new(self.client.get_client());
        validator.validate_update(cluster_conf, kube_config).await?;

        let cluster_exists = self
            .client
            .get_statefulset(&format!("{}-master", kube_config.cluster_id))
            .await
            .is_ok();

        if !cluster_exists {
            return Err(KubeError::ValidationError(format!(
                "\n Cluster not found\n\
                \n  Cluster ID: '{}'\n\
                Namespace: '{}'\n\
                \n To create a new cluster, use the 'deploy' command:\n\
                cv k8s deploy -c {} [options]\n\
                \n Example:\n\
                cv k8s deploy -c {} --master-replicas 1 --worker-replicas 1",
                kube_config.cluster_id,
                kube_config.namespace,
                kube_config.cluster_id,
                kube_config.cluster_id
            )));
        }

        println!("Updating existing cluster resources...");
        self.apply_cluster_internal(cluster_conf, kube_config, false)
            .await
    }

    async fn apply_cluster_internal(
        &self,
        cluster_conf: &ClusterConf,
        kube_config: &KubernetesConfig,
        is_first_deployment: bool,
    ) -> Result<(), KubeError> {
        let is_update_mode = !is_first_deployment;

        if let Some(storage_config) = &kube_config.storage {
            let validator = KubernetesValidator::new(self.client.get_client());

            let master_sc = storage_config.master_storage_class.as_ref().or({
                if !storage_config.storage_class.is_empty() {
                    Some(&storage_config.storage_class)
                } else {
                    None
                }
            });

            let worker_sc = storage_config.worker_storage_class.as_ref().or({
                if !storage_config.storage_class.is_empty() {
                    Some(&storage_config.storage_class)
                } else {
                    None
                }
            });

            if master_sc.is_none() || worker_sc.is_none() {
                if let Ok(Some(default_sc)) = validator.get_default_storage_class().await {
                    if master_sc.is_none() && worker_sc.is_none() {
                        println!("ℹ️  Using cluster default StorageClass: '{}'", default_sc);
                        println!("   (Master and Worker will use this StorageClass)");
                    } else if master_sc.is_none() {
                        println!(
                            "ℹ️  Master using cluster default StorageClass: '{}'",
                            default_sc
                        );
                    } else if worker_sc.is_none() {
                        println!(
                            "ℹ️  Worker using cluster default StorageClass: '{}'",
                            default_sc
                        );
                    }
                }
            }
        }

        let actual_master_replicas = if is_update_mode {
            self.client
                .get_statefulset(&format!("{}-master", kube_config.cluster_id))
                .await
                .ok()
                .and_then(|ss| ss.spec)
                .and_then(|spec| spec.replicas)
                .map(|r| r as u32)
                .unwrap_or(kube_config.master.replicas)
        } else {
            kube_config.master.replicas
        };

        let configmap_builder = ConfigMapBuilder::new(
            cluster_conf.clone(),
            kube_config.cluster_id.clone(),
            kube_config.namespace.clone(),
            actual_master_replicas,
        );
        let configmap = configmap_builder.build()?;

        self.client.apply_configmap(&configmap).await?;
        println!("✓ ConfigMap applied");

        let applied_configmap = self
            .client
            .get_configmap(&format!("{}-config", kube_config.cluster_id))
            .await?;

        let configmap_uid = applied_configmap
            .metadata
            .uid
            .ok_or_else(|| KubeError::ValidationError("ConfigMap UID not found".to_string()))?;

        let master_builder = MasterBuilder::new(
            kube_config.cluster_id.clone(),
            kube_config.namespace.clone(),
            kube_config.clone(),
            cluster_conf.clone(),
            is_update_mode,
        );
        let master_statefulset = master_builder.build_with_owner(Some(configmap_uid.clone()))?;

        self.client.apply_statefulset(&master_statefulset).await?;
        println!("✓ Master StatefulSet applied");

        let worker_builder = WorkerBuilder::new(
            kube_config.cluster_id.clone(),
            kube_config.namespace.clone(),
            kube_config.clone(),
            cluster_conf.clone(),
        );
        let worker_statefulset = worker_builder.build_with_owner(Some(configmap_uid.clone()))?;

        let headless_service_builder = HeadlessServiceBuilder::new(
            kube_config.cluster_id.clone(),
            kube_config.namespace.clone(),
        );
        let headless_service =
            headless_service_builder.build_with_owner(Some(configmap_uid.clone()))?;

        let service_builder = ServiceBuilder::with_config(
            kube_config.cluster_id.clone(),
            kube_config.namespace.clone(),
            kube_config.service.service_type,
            kube_config.service.annotations.clone(),
            kube_config.service.session_affinity.clone(),
            kube_config.service.external_ips.clone(),
            kube_config.service.load_balancer_source_ranges.clone(),
        );
        let service = service_builder.build_with_owner(Some(configmap_uid.clone()))?;

        self.client.apply_statefulset(&worker_statefulset).await?;
        println!("✓ Worker StatefulSet applied");

        self.client.apply_service(&headless_service).await?;
        println!("✓ Headless Service applied");

        self.client.apply_service(&service).await?;
        println!("✓ Service applied");

        if is_first_deployment {
            println!("\nWaiting for cluster to be ready...");
            self.wait_for_cluster_ready(&kube_config.cluster_id).await?;
            println!("✓ Cluster is ready!");
        } else {
            println!("\n✓ Cluster resources updated successfully.");
            println!(
                "  Use 'kubectl get pods -l app={}' to check pod status.",
                kube_config.cluster_id
            );
        }

        Ok(())
    }

    async fn wait_for_cluster_ready(&self, cluster_id: &str) -> Result<(), KubeError> {
        const MAX_WAIT_SECONDS: u64 = 300;
        const CHECK_INTERVAL_SECONDS: u64 = 5;

        let mut waited = 0;

        while waited < MAX_WAIT_SECONDS {
            if let Ok(ss) = self
                .client
                .get_statefulset(&format!("{}-master", cluster_id))
                .await
            {
                if let Some(ref status) = ss.status {
                    if let Some(ready_replicas) = status.ready_replicas {
                        if let Some(replicas) = ss.spec.as_ref().and_then(|s| s.replicas) {
                            if ready_replicas == replicas {
                                if let Ok(worker_statefulset) = self
                                    .client
                                    .get_statefulset(&format!("{}-worker", cluster_id))
                                    .await
                                {
                                    if let Some(ref status) = worker_statefulset.status {
                                        if let Some(ready_replicas) = status.ready_replicas {
                                            if let Some(replicas) = worker_statefulset
                                                .spec
                                                .as_ref()
                                                .and_then(|s| s.replicas)
                                            {
                                                if ready_replicas == replicas {
                                                    return Ok(());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            sleep(Duration::from_secs(CHECK_INTERVAL_SECONDS)).await;
            waited += CHECK_INTERVAL_SECONDS;
        }

        Err(KubeError::Timeout(format!(
            "Cluster {} did not become ready within {} seconds",
            cluster_id, MAX_WAIT_SECONDS
        )))
    }

    pub async fn get_cluster_status(&self, cluster_id: &str) -> Result<ClusterStatus, KubeError> {
        let mut status = ClusterStatus {
            cluster_id: cluster_id.to_string(),
            master: None,
            worker: None,
            service: None,
            configmap: None,
        };

        match self
            .client
            .get_statefulset(&format!("{}-master", cluster_id))
            .await
        {
            Ok(ss) => {
                let name = ss.metadata.name.clone().unwrap_or_default();
                let replicas = ss.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0) as u32;
                let ready_replicas = ss
                    .status
                    .as_ref()
                    .and_then(|s| s.ready_replicas)
                    .unwrap_or(0) as u32;
                status.master = Some(StatefulSetStatus {
                    name,
                    replicas,
                    ready_replicas,
                });
            }
            Err(KubeError::NotFound { .. }) => {}
            Err(e) => return Err(e),
        }

        match self
            .client
            .get_deployment(&format!("{}-worker", cluster_id))
            .await
        {
            Ok(deployment) => {
                let name = deployment.metadata.name.clone().unwrap_or_default();
                let replicas = deployment
                    .spec
                    .as_ref()
                    .and_then(|s| s.replicas)
                    .unwrap_or(0) as u32;
                let ready_replicas = deployment
                    .status
                    .as_ref()
                    .and_then(|s| s.ready_replicas)
                    .unwrap_or(0) as u32;
                status.worker = Some(StatefulSetStatus {
                    name,
                    replicas,
                    ready_replicas,
                });
            }
            Err(KubeError::NotFound { .. }) => {}
            Err(e) => return Err(e),
        }

        match self
            .client
            .get_service(&format!("{}-master", cluster_id))
            .await
        {
            Ok(svc) => {
                let name = svc.metadata.name.clone().unwrap_or_default();
                let cluster_ip = svc.spec.as_ref().and_then(|s| s.cluster_ip.clone());
                status.service = Some(ServiceStatus { name, cluster_ip });
            }
            Err(KubeError::NotFound { .. }) => {}
            Err(e) => return Err(e),
        }

        match self
            .client
            .get_configmap(&format!("{}-config", cluster_id))
            .await
        {
            Ok(cm) => {
                let name = cm.metadata.name.clone().unwrap_or_default();
                status.configmap = Some(ConfigMapStatus { name });
            }
            Err(KubeError::NotFound { .. }) => {}
            Err(e) => return Err(e),
        }

        Ok(status)
    }

    pub async fn delete_cluster(
        &self,
        cluster_id: &str,
        delete_pvcs: bool,
    ) -> Result<(), KubeError> {
        self.client
            .stop_and_cleanup_cluster(cluster_id, delete_pvcs)
            .await
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// List all Curvine clusters in the namespace by querying ConfigMaps with the curvine label
    pub async fn list_clusters(&self) -> Result<Vec<ClusterInfo>, KubeError> {
        let label_selector = "type=curvine-native-kubernetes";
        let configmaps = self.client.list_configmaps(label_selector).await?;

        let mut clusters = Vec::new();
        for cm in configmaps {
            if let Some(name) = cm.metadata.name {
                if let Some(cluster_id) = name.strip_suffix("-config") {
                    let cluster_id = cluster_id.to_string();

                    let master_ss = self
                        .client
                        .get_statefulset(&format!("{}-master", cluster_id))
                        .await
                        .ok();

                    let worker_ss = self
                        .client
                        .get_statefulset(&format!("{}-worker", cluster_id))
                        .await
                        .ok();

                    let master_replicas = master_ss
                        .as_ref()
                        .and_then(|ss| ss.spec.as_ref())
                        .and_then(|spec| spec.replicas)
                        .unwrap_or(0);

                    let master_ready = master_ss
                        .as_ref()
                        .and_then(|ss| ss.status.as_ref())
                        .and_then(|status| status.ready_replicas)
                        .unwrap_or(0);

                    let worker_replicas = worker_ss
                        .as_ref()
                        .and_then(|ss| ss.spec.as_ref())
                        .and_then(|spec| spec.replicas)
                        .unwrap_or(0);

                    let worker_ready = worker_ss
                        .as_ref()
                        .and_then(|ss| ss.status.as_ref())
                        .and_then(|status| status.ready_replicas)
                        .unwrap_or(0);

                    clusters.push(ClusterInfo {
                        cluster_id,
                        namespace: self.namespace.clone(),
                        master_replicas: master_replicas as u32,
                        master_ready: master_ready as u32,
                        worker_replicas: worker_replicas as u32,
                        worker_ready: worker_ready as u32,
                    });
                }
            }
        }

        Ok(clusters)
    }
}

#[derive(Debug, Clone)]
pub struct ClusterInfo {
    pub cluster_id: String,
    pub namespace: String,
    pub master_replicas: u32,
    pub master_ready: u32,
    pub worker_replicas: u32,
    pub worker_ready: u32,
}

#[derive(Debug, Clone)]
pub struct ClusterStatus {
    pub cluster_id: String,
    pub master: Option<StatefulSetStatus>,
    pub worker: Option<StatefulSetStatus>,
    pub service: Option<ServiceStatus>,
    pub configmap: Option<ConfigMapStatus>,
}

#[derive(Debug, Clone)]
pub struct StatefulSetStatus {
    pub name: String,
    pub replicas: u32,
    pub ready_replicas: u32,
}

#[derive(Debug, Clone)]
pub struct ServiceStatus {
    pub name: String,
    pub cluster_ip: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ConfigMapStatus {
    pub name: String,
}
