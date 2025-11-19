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
use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolumeClaim, Pod, Service};
use kube::{Api, Client};
use std::collections::HashMap;

#[async_trait::async_trait]
pub trait CurvineKubeClient: Send + Sync {
    async fn create_master_statefulset(&self, statefulset: &StatefulSet) -> Result<(), KubeError>;

    async fn create_deployment(&self, deployment: &Deployment) -> Result<(), KubeError>;

    async fn create_service(&self, service: &Service) -> Result<(), KubeError>;

    async fn create_configmap(&self, configmap: &ConfigMap) -> Result<(), KubeError>;

    async fn apply_configmap(&self, configmap: &ConfigMap) -> Result<(), KubeError>;

    async fn apply_service(&self, service: &Service) -> Result<(), KubeError>;

    async fn apply_statefulset(&self, statefulset: &StatefulSet) -> Result<(), KubeError>;

    async fn apply_deployment(&self, deployment: &Deployment) -> Result<(), KubeError>;

    async fn get_statefulset(&self, name: &str) -> Result<StatefulSet, KubeError>;

    async fn get_deployment(&self, name: &str) -> Result<Deployment, KubeError>;

    async fn get_service(&self, name: &str) -> Result<Service, KubeError>;

    async fn get_configmap(&self, name: &str) -> Result<ConfigMap, KubeError>;

    async fn list_configmaps(&self, label_selector: &str) -> Result<Vec<ConfigMap>, KubeError>;

    fn get_client(&self) -> Client;

    async fn get_pods_with_labels(
        &self,
        labels: &HashMap<String, String>,
    ) -> Result<Vec<Pod>, KubeError>;

    async fn delete_statefulset(&self, name: &str) -> Result<(), KubeError>;

    async fn delete_deployment(&self, name: &str) -> Result<(), KubeError>;

    async fn delete_service(&self, name: &str) -> Result<(), KubeError>;

    async fn delete_configmap(&self, name: &str) -> Result<(), KubeError>;

    async fn delete_pod(&self, name: &str) -> Result<(), KubeError>;

    async fn delete_pvcs_for_cluster(&self, cluster_id: &str) -> Result<(), KubeError>;

    async fn stop_and_cleanup_cluster(
        &self,
        cluster_id: &str,
        delete_pvcs: bool,
    ) -> Result<(), KubeError>;
}

pub struct CurvineKubeClientImpl {
    client: Client,
    namespace: String,
}

impl CurvineKubeClientImpl {
    pub async fn new(namespace: String) -> Result<Self, KubeError> {
        let client = Client::try_default().await.map_err(|e| {
            KubeError::KubeError(format!("Failed to create Kubernetes client: {}", e))
        })?;

        Ok(Self { client, namespace })
    }

    pub fn get_client(&self) -> Client {
        self.client.clone()
    }

    pub async fn new_with_config(
        namespace: String,
        kubeconfig_path: Option<String>,
        context: Option<String>,
    ) -> Result<Self, KubeError> {
        use kube::config::{KubeConfigOptions, Kubeconfig};

        let kubeconfig = if let Some(path) = kubeconfig_path {
            Kubeconfig::read_from(path)
                .map_err(|e| KubeError::KubeError(format!("Failed to load kubeconfig: {}", e)))?
        } else {
            Kubeconfig::read()
                .map_err(|e| KubeError::KubeError(format!("Failed to load kubeconfig: {}", e)))?
        };

        let config_options = KubeConfigOptions {
            context,
            cluster: None,
            user: None,
        };

        let config = kube::Config::from_custom_kubeconfig(kubeconfig, &config_options)
            .await
            .map_err(|e| {
                KubeError::KubeError(format!("Failed to create Kubernetes config: {}", e))
            })?;

        let client = Client::try_from(config).map_err(|e| {
            KubeError::KubeError(format!("Failed to create Kubernetes client: {}", e))
        })?;

        Ok(Self { client, namespace })
    }
}

#[async_trait::async_trait]
impl CurvineKubeClient for CurvineKubeClientImpl {
    async fn create_master_statefulset(&self, statefulset: &StatefulSet) -> Result<(), KubeError> {
        let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), &self.namespace);
        let pp = kube::api::PostParams::default();

        api.create(&pp, statefulset).await?;
        Ok(())
    }

    async fn create_deployment(&self, deployment: &Deployment) -> Result<(), KubeError> {
        let api: Api<Deployment> = Api::namespaced(self.client.clone(), &self.namespace);
        let pp = kube::api::PostParams::default();

        api.create(&pp, deployment).await?;
        Ok(())
    }

    async fn create_service(&self, service: &Service) -> Result<(), KubeError> {
        let api: Api<Service> = Api::namespaced(self.client.clone(), &self.namespace);
        let pp = kube::api::PostParams::default();

        api.create(&pp, service).await?;
        Ok(())
    }

    async fn create_configmap(&self, configmap: &ConfigMap) -> Result<(), KubeError> {
        let api: Api<ConfigMap> = Api::namespaced(self.client.clone(), &self.namespace);
        let pp = kube::api::PostParams::default();

        api.create(&pp, configmap).await?;
        Ok(())
    }

    async fn apply_configmap(&self, configmap: &ConfigMap) -> Result<(), KubeError> {
        let api: Api<ConfigMap> = Api::namespaced(self.client.clone(), &self.namespace);
        let name = configmap
            .metadata
            .name
            .as_ref()
            .ok_or_else(|| KubeError::ConfigError("ConfigMap name is required".to_string()))?;

        match api.get(name).await {
            Ok(_) => {
                let patch_params = kube::api::PatchParams::apply("curvine-cli").force();
                let patch = serde_json::to_value(configmap).map_err(|e| {
                    KubeError::KubeError(format!("Failed to serialize ConfigMap: {}", e))
                })?;
                api.patch(name, &patch_params, &kube::api::Patch::Apply(patch))
                    .await?;
            }
            Err(kube::Error::Api(ae)) if ae.code == 404 => {
                let pp = kube::api::PostParams::default();
                api.create(&pp, configmap).await?;
            }
            Err(e) => return Err(KubeError::KubeError(e.to_string())),
        }
        Ok(())
    }

    async fn apply_service(&self, service: &Service) -> Result<(), KubeError> {
        let api: Api<Service> = Api::namespaced(self.client.clone(), &self.namespace);
        let name = service
            .metadata
            .name
            .as_ref()
            .ok_or_else(|| KubeError::ConfigError("Service name is required".to_string()))?;

        match api.get(name).await {
            Ok(existing) => {
                let mut service_to_patch = service.clone();
                if let (Some(existing_spec), Some(ref mut new_spec)) =
                    (&existing.spec, &mut service_to_patch.spec)
                {
                    new_spec.cluster_ip = existing_spec.cluster_ip.clone();
                    new_spec.cluster_ips = existing_spec.cluster_ips.clone();
                }

                let patch_params = kube::api::PatchParams::apply("curvine-cli").force();
                let patch = serde_json::to_value(&service_to_patch).map_err(|e| {
                    KubeError::KubeError(format!("Failed to serialize Service: {}", e))
                })?;
                api.patch(name, &patch_params, &kube::api::Patch::Apply(patch))
                    .await?;
            }
            Err(kube::Error::Api(ae)) if ae.code == 404 => {
                let pp = kube::api::PostParams::default();
                api.create(&pp, service).await?;
            }
            Err(e) => return Err(KubeError::KubeError(e.to_string())),
        }
        Ok(())
    }

    async fn apply_statefulset(&self, statefulset: &StatefulSet) -> Result<(), KubeError> {
        let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), &self.namespace);
        let name =
            statefulset.metadata.name.as_ref().ok_or_else(|| {
                KubeError::ConfigError("StatefulSet name is required".to_string())
            })?;

        match api.get(name).await {
            Ok(existing) => {
                if let (Some(existing_spec), Some(new_spec)) = (&existing.spec, &statefulset.spec) {
                    let existing_pvcs = existing_spec.volume_claim_templates.as_ref();
                    let new_pvcs = new_spec.volume_claim_templates.as_ref();

                    if let (Some(existing), Some(new)) = (existing_pvcs, new_pvcs) {
                        if existing.len() != new.len() {
                            return Err(KubeError::ConfigError(
                                "StatefulSet volumeClaimTemplates count cannot be changed. Please delete and recreate the StatefulSet.".to_string()
                            ));
                        }

                        for (idx, (ex_pvc, new_pvc)) in existing.iter().zip(new.iter()).enumerate()
                        {
                            let ex_name = ex_pvc.metadata.name.as_deref();
                            let new_name = new_pvc.metadata.name.as_deref();

                            if ex_name != new_name {
                                return Err(KubeError::ConfigError(
                                    format!("StatefulSet volumeClaimTemplate[{}] name cannot be changed. Please delete and recreate the StatefulSet.", idx)
                                ));
                            }

                            if let (Some(ex_spec), Some(new_spec)) = (&ex_pvc.spec, &new_pvc.spec) {
                                if ex_spec.storage_class_name != new_spec.storage_class_name {
                                    return Err(KubeError::ConfigError(
                                        format!("StatefulSet volumeClaimTemplate[{}] storageClassName cannot be changed. Please delete and recreate the StatefulSet.", idx)
                                    ));
                                }

                                if ex_spec.access_modes != new_spec.access_modes {
                                    return Err(KubeError::ConfigError(
                                        format!("StatefulSet volumeClaimTemplate[{}] accessModes cannot be changed. Please delete and recreate the StatefulSet.", idx)
                                    ));
                                }

                                if let (Some(ex_res), Some(new_res)) =
                                    (&ex_spec.resources, &new_spec.resources)
                                {
                                    if let (Some(ex_req), Some(new_req)) =
                                        (&ex_res.requests, &new_res.requests)
                                    {
                                        let ex_storage = ex_req.get("storage");
                                        let new_storage = new_req.get("storage");
                                        if ex_storage != new_storage {
                                            return Err(KubeError::ConfigError(
                                                format!("StatefulSet volumeClaimTemplate[{}] storage size cannot be changed. Please delete and recreate the StatefulSet.", idx)
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                let patch_params = kube::api::PatchParams::apply("curvine-cli").force();
                let patch = serde_json::to_value(statefulset).map_err(|e| {
                    KubeError::KubeError(format!("Failed to serialize StatefulSet: {}", e))
                })?;
                api.patch(name, &patch_params, &kube::api::Patch::Apply(patch))
                    .await?;
            }
            Err(kube::Error::Api(ae)) if ae.code == 404 => {
                let pp = kube::api::PostParams::default();
                api.create(&pp, statefulset).await?;
            }
            Err(e) => return Err(KubeError::KubeError(e.to_string())),
        }
        Ok(())
    }

    async fn apply_deployment(&self, deployment: &Deployment) -> Result<(), KubeError> {
        let api: Api<Deployment> = Api::namespaced(self.client.clone(), &self.namespace);
        let name = deployment
            .metadata
            .name
            .as_ref()
            .ok_or_else(|| KubeError::ConfigError("Deployment name is required".to_string()))?;

        match api.get(name).await {
            Ok(_) => {
                let patch_params = kube::api::PatchParams::apply("curvine-cli").force();
                let patch = serde_json::to_value(deployment).map_err(|e| {
                    KubeError::KubeError(format!("Failed to serialize Deployment: {}", e))
                })?;
                api.patch(name, &patch_params, &kube::api::Patch::Apply(patch))
                    .await?;
            }
            Err(kube::Error::Api(ae)) if ae.code == 404 => {
                let pp = kube::api::PostParams::default();
                api.create(&pp, deployment).await?;
            }
            Err(e) => return Err(KubeError::KubeError(e.to_string())),
        }
        Ok(())
    }

    async fn get_statefulset(&self, name: &str) -> Result<StatefulSet, KubeError> {
        let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), &self.namespace);
        api.get(name).await.map_err(|e| {
            if let kube::Error::Api(ae) = e {
                if ae.code == 404 {
                    KubeError::not_found("StatefulSet", name, &self.namespace)
                } else {
                    KubeError::KubeError(ae.message)
                }
            } else {
                KubeError::KubeError(e.to_string())
            }
        })
    }

    async fn get_deployment(&self, name: &str) -> Result<Deployment, KubeError> {
        let api: Api<Deployment> = Api::namespaced(self.client.clone(), &self.namespace);
        api.get(name).await.map_err(|e| {
            if let kube::Error::Api(ae) = e {
                if ae.code == 404 {
                    KubeError::not_found("Deployment", name, &self.namespace)
                } else {
                    KubeError::KubeError(ae.message)
                }
            } else {
                KubeError::KubeError(e.to_string())
            }
        })
    }

    async fn get_service(&self, name: &str) -> Result<Service, KubeError> {
        let api: Api<Service> = Api::namespaced(self.client.clone(), &self.namespace);
        api.get(name).await.map_err(|e| {
            if let kube::Error::Api(ae) = e {
                if ae.code == 404 {
                    KubeError::not_found("Service", name, &self.namespace)
                } else {
                    KubeError::KubeError(ae.message)
                }
            } else {
                KubeError::KubeError(e.to_string())
            }
        })
    }

    async fn get_configmap(&self, name: &str) -> Result<ConfigMap, KubeError> {
        let api: Api<ConfigMap> = Api::namespaced(self.client.clone(), &self.namespace);
        api.get(name).await.map_err(|e| {
            if let kube::Error::Api(ae) = e {
                if ae.code == 404 {
                    KubeError::not_found("ConfigMap", name, &self.namespace)
                } else {
                    KubeError::KubeError(ae.message)
                }
            } else {
                KubeError::KubeError(e.to_string())
            }
        })
    }

    async fn list_configmaps(&self, label_selector: &str) -> Result<Vec<ConfigMap>, KubeError> {
        let api: Api<ConfigMap> = Api::namespaced(self.client.clone(), &self.namespace);
        let list_params = kube::api::ListParams::default().labels(label_selector);

        api.list(&list_params)
            .await
            .map(|list| list.items)
            .map_err(|e| KubeError::KubeError(e.to_string()))
    }

    fn get_client(&self) -> Client {
        self.client.clone()
    }

    async fn get_pods_with_labels(
        &self,
        labels: &HashMap<String, String>,
    ) -> Result<Vec<Pod>, KubeError> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let label_selector = labels
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",");

        let lp = kube::api::ListParams::default().labels(&label_selector);

        let pods = api.list(&lp).await?;
        Ok(pods.items)
    }

    async fn delete_statefulset(&self, name: &str) -> Result<(), KubeError> {
        let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), &self.namespace);
        let dp = kube::api::DeleteParams::default();

        api.delete(name, &dp).await?;
        Ok(())
    }

    async fn delete_deployment(&self, name: &str) -> Result<(), KubeError> {
        let api: Api<Deployment> = Api::namespaced(self.client.clone(), &self.namespace);
        let dp = kube::api::DeleteParams::default();

        api.delete(name, &dp).await?;
        Ok(())
    }

    async fn delete_service(&self, name: &str) -> Result<(), KubeError> {
        let api: Api<Service> = Api::namespaced(self.client.clone(), &self.namespace);
        let dp = kube::api::DeleteParams::default();

        api.delete(name, &dp).await?;
        Ok(())
    }

    async fn delete_configmap(&self, name: &str) -> Result<(), KubeError> {
        let api: Api<ConfigMap> = Api::namespaced(self.client.clone(), &self.namespace);
        let dp = kube::api::DeleteParams::default();

        api.delete(name, &dp).await?;
        Ok(())
    }

    async fn delete_pod(&self, name: &str) -> Result<(), KubeError> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let dp = kube::api::DeleteParams::default();

        api.delete(name, &dp).await?;
        Ok(())
    }

    async fn delete_pvcs_for_cluster(&self, cluster_id: &str) -> Result<(), KubeError> {
        let api: Api<PersistentVolumeClaim> = Api::namespaced(self.client.clone(), &self.namespace);
        let dp = kube::api::DeleteParams::default();

        let lp = kube::api::ListParams::default().labels(&format!("app={}", cluster_id));

        let pvcs = api.list(&lp).await?;

        for pvc in pvcs.items {
            if let Some(name) = pvc.metadata.name.as_ref() {
                let _ = api.delete(name, &dp).await;
            }
        }

        Ok(())
    }

    async fn stop_and_cleanup_cluster(
        &self,
        cluster_id: &str,
        delete_pvcs: bool,
    ) -> Result<(), KubeError> {
        let configmap_name = format!("{}-config", cluster_id);

        if self.get_configmap(&configmap_name).await.is_err() {
            let master_ss_name = format!("{}-master", cluster_id);
            let worker_ss_name = format!("{}-worker", cluster_id);
            let master_exists = self.get_statefulset(&master_ss_name).await.is_ok();
            let worker_exists = self.get_statefulset(&worker_ss_name).await.is_ok();

            if !master_exists && !worker_exists {
                return Err(KubeError::not_found("Cluster", cluster_id, &self.namespace));
            }

            let _ = self.delete_statefulset(&master_ss_name).await;
            let _ = self.delete_statefulset(&worker_ss_name).await;
            let _ = self.delete_service(&format!("{}-master", cluster_id)).await;
            let _ = self
                .delete_service(&format!("{}-master-headless", cluster_id))
                .await;
            let _ = self.delete_configmap(&configmap_name).await;
        } else {
            self.delete_configmap(&configmap_name).await?;
        }

        if delete_pvcs {
            let _ = self.delete_pvcs_for_cluster(cluster_id).await;
        }

        Ok(())
    }
}
