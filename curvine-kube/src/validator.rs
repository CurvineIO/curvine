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

use crate::config::KubernetesConfig;
use crate::error::KubeError;
use curvine_common::conf::ClusterConf;
use curvine_common::state::StorageType;
use kube::Client;

pub struct KubernetesValidator {
    client: Client,
}

impl KubernetesValidator {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn validate_deployment(
        &self,
        cluster_conf: &ClusterConf,
        kube_config: &KubernetesConfig,
    ) -> Result<(), KubeError> {
        self.validate_worker_mem_size(cluster_conf)?;
        self.validate_master_pvcs(kube_config).await?;
        if let Some(storage_config) = &kube_config.storage {
            if let Some(ref master_sc) = storage_config.master_storage_class {
                if !master_sc.is_empty() {
                    self.validate_storage_class(master_sc).await?;
                }
            } else if !storage_config.storage_class.is_empty() {
                self.validate_storage_class(&storage_config.storage_class)
                    .await?;
            }

            if let Some(ref worker_sc) = storage_config.worker_storage_class {
                if !worker_sc.is_empty()
                    && storage_config.master_storage_class.as_ref() != Some(worker_sc)
                {
                    self.validate_storage_class(worker_sc).await?;
                }
            }
        }

        Ok(())
    }

    pub async fn validate_update(
        &self,
        _cluster_conf: &ClusterConf,
        kube_config: &KubernetesConfig,
    ) -> Result<(), KubeError> {
        let valid_policies = ["Always", "IfNotPresent", "Never"];
        if !valid_policies.contains(&kube_config.image_pull_policy.as_str()) {
            return Err(KubeError::ConfigError(format!(
                "Invalid image_pull_policy: {}",
                kube_config.image_pull_policy
            )));
        }

        if kube_config.worker.replicas == 0 {
            return Err(KubeError::ConfigError(
                "worker.replicas must be > 0".to_string(),
            ));
        }

        self.validate_master_pvcs(kube_config).await?;
        if let Some(storage_config) = &kube_config.storage {
            self.validate_storage_class(&storage_config.storage_class)
                .await?;
        }

        Ok(())
    }

    fn validate_worker_mem_size(&self, cluster_conf: &ClusterConf) -> Result<(), KubeError> {
        use curvine_common::conf::WorkerDataDir;
        let block_size = if cluster_conf.client.block_size > 0 {
            cluster_conf.client.block_size as u64
        } else {
            use orpc::common::ByteUnit;
            ByteUnit::from_str(&cluster_conf.client.block_size_str)
                .map_err(|e| KubeError::ConfigError(format!("Invalid block_size_str: {}", e)))?
                .as_byte()
        };

        for data_dir_str in cluster_conf.worker.data_dir.iter() {
            let data_dir = WorkerDataDir::from_str(data_dir_str)
                .map_err(|e| KubeError::ConfigError(format!("Invalid data_dir format: {}", e)))?;

            if data_dir.storage_type == StorageType::Mem
                && data_dir.capacity > 0
                && data_dir.capacity < block_size
            {
                return Err(KubeError::ValidationError(format!(
                    "Worker data_dir MEM size ({} bytes, ~{}MB) is less than worker block size ({} bytes, ~{}MB). \
                    This can cause performance issues. \
                    \n\nRecommendations:\n\
                    - Increase MEM size to at least {}MB: [MEM:{}]\n\
                    - Or use disk storage: [DISK]/path/to/data\n\
                    - Or remove the MEM size limit: [MEM]/path/to/data",
                    data_dir.capacity,
                    data_dir.capacity / (1024 * 1024),
                    block_size,
                    block_size / (1024 * 1024),
                    (block_size / (1024 * 1024)) + 1,
                    (block_size / (1024 * 1024)) + 1
                )));
            }
        }

        Ok(())
    }

    async fn validate_master_pvcs(&self, _kube_config: &KubernetesConfig) -> Result<(), KubeError> {
        // Storage class validation is now done in validate_deployment
        // to support separate master and worker storage classes
        Ok(())
    }

    async fn validate_storage_class(&self, storage_class_name: &str) -> Result<(), KubeError> {
        use k8s_openapi::api::storage::v1::StorageClass;
        use kube::api::Api;

        let api: Api<StorageClass> = Api::all(self.client.clone());

        match api.get(storage_class_name).await {
            Ok(_) => Ok(()),
            Err(kube::error::Error::Api(ae)) if ae.code == 404 => {
                let available_classes = self.list_storage_classes().await.unwrap_or_default();
                Err(KubeError::ValidationError(format!(
                    "\n StorageClass not found\n\
                    \n  Requested: '{}'\n\
\n Available StorageClasses:\n{}\n\
\n Use one of the above StorageClasses or create a new one before deploying.",
                    storage_class_name,
                    if available_classes.is_empty() {
                        "  (none found)".to_string()
                    } else {
                        available_classes
                            .into_iter()
                            .map(|s| format!("  - {}", s))
                            .collect::<Vec<_>>()
                            .join("\n")
                    }
                )))
            }
            Err(e) => Err(KubeError::KubeError(format!(
                "Failed to check StorageClass '{}': {}",
                storage_class_name, e
            ))),
        }
    }

    async fn list_storage_classes(&self) -> Result<Vec<String>, KubeError> {
        use k8s_openapi::api::storage::v1::StorageClass;
        use kube::api::Api;

        let api: Api<StorageClass> = Api::all(self.client.clone());

        match api.list(&Default::default()).await {
            Ok(list) => {
                let names = list
                    .items
                    .iter()
                    .filter_map(|sc| sc.metadata.name.clone())
                    .collect();
                Ok(names)
            }
            Err(e) => Err(KubeError::KubeError(format!(
                "Failed to list StorageClasses: {}",
                e
            ))),
        }
    }

    pub async fn get_default_storage_class(&self) -> Result<Option<String>, KubeError> {
        use k8s_openapi::api::storage::v1::StorageClass;
        use kube::api::Api;

        let api: Api<StorageClass> = Api::all(self.client.clone());

        match api.list(&Default::default()).await {
            Ok(list) => {
                // Find the default storage class (marked with annotation)
                for sc in list.items {
                    if let Some(annotations) = &sc.metadata.annotations {
                        // Check both standard annotations for default storage class
                        if annotations.get("storageclass.kubernetes.io/is-default-class")
                            == Some(&"true".to_string())
                            || annotations.get("storageclass.beta.kubernetes.io/is-default-class")
                                == Some(&"true".to_string())
                        {
                            return Ok(sc.metadata.name);
                        }
                    }
                }
                Ok(None)
            }
            Err(e) => Err(KubeError::KubeError(format!(
                "Failed to list StorageClasses: {}",
                e
            ))),
        }
    }
}
