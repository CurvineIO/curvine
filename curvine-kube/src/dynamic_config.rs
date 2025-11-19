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

use crate::{KubernetesConfig, ServiceType, StorageConfig};
use k8s_openapi::api::core::v1::ResourceRequirements;
use std::collections::{BTreeMap, HashMap};

pub fn apply_to_kube_config(configs: &HashMap<String, String>, kube_config: &mut KubernetesConfig) {
    if let Some(image) = configs.get("kubernetes.container.image") {
        kube_config.master.image = image.clone();
        kube_config.worker.image = image.clone();
    }

    if let Some(image) = configs.get("kubernetes.master.image") {
        kube_config.master.image = image.clone();
    }

    if let Some(image) = configs.get("kubernetes.worker.image") {
        kube_config.worker.image = image.clone();
    }

    if let Some(policy) = configs.get("kubernetes.image.pull-policy") {
        kube_config.image_pull_policy = policy.clone();
    }

    if let Some(domain) = configs.get("kubernetes.cluster.domain") {
        kube_config.cluster_domain = domain.clone();
    }

    if let Some(replicas_str) = configs.get("kubernetes.master.replicas") {
        if let Ok(replicas) = replicas_str.parse::<u32>() {
            kube_config.master.replicas = replicas;
        }
    }

    if let Some(replicas_str) = configs.get("kubernetes.worker.replicas") {
        if let Ok(replicas) = replicas_str.parse::<u32>() {
            kube_config.worker.replicas = replicas;
        }
    }

    if let Some(storage_class) = configs.get("kubernetes.storage.class") {
        if kube_config.storage.is_none() {
            kube_config.storage = Some(StorageConfig {
                storage_class: storage_class.clone(),
                master_storage_class: None,
                worker_storage_class: None,
                master_size: None,
                worker_size: None,
            });
        } else if let Some(storage) = &mut kube_config.storage {
            storage.storage_class = storage_class.clone();
        }
    }

    if let Some(size) = configs.get("kubernetes.storage.master-size") {
        if kube_config.storage.is_none() {
            kube_config.storage = Some(StorageConfig {
                storage_class: "standard".to_string(),
                master_storage_class: None,
                worker_storage_class: None,
                master_size: Some(size.clone()),
                worker_size: None,
            });
        } else if let Some(storage) = &mut kube_config.storage {
            storage.master_size = Some(size.clone());
        }
    }

    if let Some(size) = configs.get("kubernetes.storage.worker-size") {
        if kube_config.storage.is_none() {
            kube_config.storage = Some(StorageConfig {
                storage_class: "standard".to_string(),
                master_storage_class: None,
                worker_storage_class: None,
                master_size: None,
                worker_size: Some(size.clone()),
            });
        } else if let Some(storage) = &mut kube_config.storage {
            storage.worker_size = Some(size.clone());
        }
    }

    if let Some(size) = configs.get("kubernetes.storage.size") {
        if kube_config.storage.is_none() {
            kube_config.storage = Some(StorageConfig {
                storage_class: "standard".to_string(),
                master_storage_class: None,
                worker_storage_class: None,
                master_size: Some(size.clone()),
                worker_size: Some(size.clone()),
            });
        } else if let Some(storage) = &mut kube_config.storage {
            if storage.master_size.is_none() {
                storage.master_size = Some(size.clone());
            }
            if storage.worker_size.is_none() {
                storage.worker_size = Some(size.clone());
            }
        }
    }

    if let Some(storage_class) = configs.get("kubernetes.worker.storage-class") {
        kube_config.worker.storage_class = Some(storage_class.clone());
    }

    if let Some(template) = configs.get("kubernetes.master.pod-template") {
        kube_config.master.pod_template = Some(template.clone());
    }

    if let Some(template) = configs.get("kubernetes.worker.pod-template") {
        kube_config.worker.pod_template = Some(template.clone());
    }

    if let Some(service_type) = configs.get("kubernetes.service.type") {
        if let Ok(st) = service_type.parse::<ServiceType>() {
            kube_config.service.service_type = st;
        }
    }

    if kube_config.master.resources.is_none() {
        kube_config.master.resources = Some(ResourceRequirements {
            requests: Some({
                let mut map = BTreeMap::new();
                map.insert(
                    "cpu".to_string(),
                    k8s_openapi::apimachinery::pkg::api::resource::Quantity("1000m".to_string()),
                );
                map.insert(
                    "memory".to_string(),
                    k8s_openapi::apimachinery::pkg::api::resource::Quantity("2Gi".to_string()),
                );
                map
            }),
            limits: Some({
                let mut map = BTreeMap::new();
                map.insert(
                    "cpu".to_string(),
                    k8s_openapi::apimachinery::pkg::api::resource::Quantity("1000m".to_string()),
                );
                map.insert(
                    "memory".to_string(),
                    k8s_openapi::apimachinery::pkg::api::resource::Quantity("2Gi".to_string()),
                );
                map
            }),
            ..Default::default()
        });
    }

    if let Some(cpu_str) = configs.get("kubernetes.master.cpu") {
        if let Ok(cpu_float) = cpu_str.parse::<f64>() {
            let cpu_milli = (cpu_float * 1000.0) as i32;
            let cpu_quantity =
                k8s_openapi::apimachinery::pkg::api::resource::Quantity(format!("{}m", cpu_milli));

            if let Some(resources) = &mut kube_config.master.resources {
                let requests = resources.requests.get_or_insert_with(BTreeMap::new);
                requests.insert("cpu".to_string(), cpu_quantity.clone());

                let limits = resources.limits.get_or_insert_with(BTreeMap::new);
                limits.insert("cpu".to_string(), cpu_quantity);
            }
        }
    }

    if let Some(mem_str) = configs.get("kubernetes.master.memory") {
        let mem_quantity = k8s_openapi::apimachinery::pkg::api::resource::Quantity(mem_str.clone());

        if let Some(resources) = &mut kube_config.master.resources {
            let requests = resources.requests.get_or_insert_with(BTreeMap::new);
            requests.insert("memory".to_string(), mem_quantity.clone());

            let limits = resources.limits.get_or_insert_with(BTreeMap::new);
            limits.insert("memory".to_string(), mem_quantity);
        }
    }

    if kube_config.worker.resources.is_none() {
        kube_config.worker.resources = Some(ResourceRequirements {
            requests: Some({
                let mut map = BTreeMap::new();
                map.insert(
                    "cpu".to_string(),
                    k8s_openapi::apimachinery::pkg::api::resource::Quantity("500m".to_string()),
                );
                map.insert(
                    "memory".to_string(),
                    k8s_openapi::apimachinery::pkg::api::resource::Quantity("1Gi".to_string()),
                );
                map
            }),
            limits: Some({
                let mut map = BTreeMap::new();
                map.insert(
                    "cpu".to_string(),
                    k8s_openapi::apimachinery::pkg::api::resource::Quantity("500m".to_string()),
                );
                map.insert(
                    "memory".to_string(),
                    k8s_openapi::apimachinery::pkg::api::resource::Quantity("1Gi".to_string()),
                );
                map
            }),
            ..Default::default()
        });
    }

    if let Some(cpu_str) = configs.get("kubernetes.worker.cpu") {
        if let Ok(cpu_float) = cpu_str.parse::<f64>() {
            let cpu_milli = (cpu_float * 1000.0) as i32;
            let cpu_quantity =
                k8s_openapi::apimachinery::pkg::api::resource::Quantity(format!("{}m", cpu_milli));

            if let Some(resources) = &mut kube_config.worker.resources {
                let requests = resources.requests.get_or_insert_with(BTreeMap::new);
                requests.insert("cpu".to_string(), cpu_quantity.clone());

                let limits = resources.limits.get_or_insert_with(BTreeMap::new);
                limits.insert("cpu".to_string(), cpu_quantity);
            }
        }
    }

    if let Some(mem_str) = configs.get("kubernetes.worker.memory") {
        let mem_quantity = k8s_openapi::apimachinery::pkg::api::resource::Quantity(mem_str.clone());

        if let Some(resources) = &mut kube_config.worker.resources {
            let requests = resources.requests.get_or_insert_with(BTreeMap::new);
            requests.insert("memory".to_string(), mem_quantity.clone());

            let limits = resources.limits.get_or_insert_with(BTreeMap::new);
            limits.insert("memory".to_string(), mem_quantity);
        }
    }

    if let Some(selector_str) = configs.get("kubernetes.master.node-selector") {
        let selectors = parse_key_value_pairs(selector_str);
        if !selectors.is_empty() {
            kube_config.master.node_selector = Some(selectors);
        }
    }

    if let Some(selector_str) = configs.get("kubernetes.worker.node-selector") {
        let selectors = parse_key_value_pairs(selector_str);
        if !selectors.is_empty() {
            kube_config.worker.node_selector = Some(selectors);
        }
    }

    if let Some(labels_str) = configs.get("kubernetes.master.labels") {
        let labels = parse_key_value_pairs(labels_str);
        kube_config.master.labels.extend(labels);
    }

    if let Some(labels_str) = configs.get("kubernetes.worker.labels") {
        let labels = parse_key_value_pairs(labels_str);
        kube_config.worker.labels.extend(labels);
    }

    if let Some(annotations_str) = configs.get("kubernetes.master.annotations") {
        let annotations = parse_key_value_pairs(annotations_str);
        kube_config.master.annotations.extend(annotations);
    }

    if let Some(annotations_str) = configs.get("kubernetes.worker.annotations") {
        let annotations = parse_key_value_pairs(annotations_str);
        kube_config.worker.annotations.extend(annotations);
    }

    if let Some(sa) = configs.get("kubernetes.master.service-account") {
        kube_config.master.service_account = Some(sa.clone());
    }

    if let Some(sa) = configs.get("kubernetes.worker.service-account") {
        kube_config.worker.service_account = Some(sa.clone());
    }

    for (key, value) in configs {
        if key.starts_with("kubernetes.master.env.") {
            let env_name = key.strip_prefix("kubernetes.master.env.").unwrap();
            kube_config
                .master
                .env_vars
                .insert(env_name.to_string(), value.clone());
        }
        if key.starts_with("kubernetes.worker.env.") {
            let env_name = key.strip_prefix("kubernetes.worker.env.").unwrap();
            kube_config
                .worker
                .env_vars
                .insert(env_name.to_string(), value.clone());
        }
    }

    if let Some(dns_policy) = configs.get("kubernetes.pod.dns-policy") {
        kube_config.master.dns_policy = Some(dns_policy.clone());
        kube_config.worker.dns_policy = Some(dns_policy.clone());
    }

    if let Some(priority) = configs.get("kubernetes.pod.priority-class") {
        kube_config.master.priority_class = Some(priority.clone());
        kube_config.worker.priority_class = Some(priority.clone());
    }

    if let Some(priority) = configs.get("kubernetes.master.priority-class") {
        kube_config.master.priority_class = Some(priority.clone());
    }

    if let Some(priority) = configs.get("kubernetes.worker.priority-class") {
        kube_config.worker.priority_class = Some(priority.clone());
    }

    if let Some(annotations_str) = configs.get("kubernetes.service.annotations") {
        let annotations = parse_key_value_pairs(annotations_str);
        kube_config.service.annotations.extend(annotations);
    }

    if let Some(external_ips_str) = configs.get("kubernetes.service.external-ips") {
        let ips: Vec<String> = external_ips_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        kube_config.service.external_ips = ips;
    }
}

fn parse_key_value_pairs(input: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in input.split(',') {
        let parts: Vec<&str> = pair.splitn(2, '=').collect();
        if parts.len() == 2 {
            map.insert(parts[0].trim().to_string(), parts[1].trim().to_string());
        }
    }
    map
}
