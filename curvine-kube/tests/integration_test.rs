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

#[cfg(test)]
mod tests {
    use curvine_common::conf::ClusterConf;
    use curvine_kube::*;
    use std::collections::HashMap;

    fn create_test_cluster_conf() -> ClusterConf {
        // Create a minimal test configuration
        ClusterConf::default()
    }

    fn create_test_kube_config() -> KubernetesConfig {
        KubernetesConfig {
            cluster_id: "test-cluster".to_string(),
            namespace: "default".to_string(),
            master: MasterConfig {
                replicas: 3,
                image: "docker.io/curvine:latest".to_string(),
                resources: None,
                node_selector: None,
                affinity: None,
                pod_template: None,
                graceful_shutdown: true,
                labels: HashMap::new(),
                annotations: HashMap::new(),
                tolerations: Vec::new(),
                service_account: None,
                env_vars: HashMap::new(),
                dns_policy: None,
                priority_class: None,
            },
            worker: WorkerConfig {
                replicas: 3,
                image: "docker.io/curvine:latest".to_string(),
                resources: None,
                node_selector: None,
                anti_affinity: false,
                pod_template: None,
                storage_class: None,
                graceful_shutdown: true,
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
            storage: Some(StorageConfig {
                storage_class: "standard".to_string(),
                master_storage_class: None,
                worker_storage_class: None,
                master_size: Some("10Gi".to_string()),
                worker_size: Some("10Gi".to_string()),
            }),
            image_pull_policy: "IfNotPresent".to_string(),
            image_pull_secrets: Vec::new(),
            cluster_domain: "cluster.local".to_string(),
        }
    }

    #[tokio::test]
    #[ignore] // Requires Kubernetes cluster
    async fn test_config_validation() {
        let config = create_test_kube_config();
        assert!(config.validate().is_ok());

        // Test invalid cluster_id
        let mut invalid_config = config.clone();
        invalid_config.cluster_id = "invalid-cluster-id-with-uppercase".to_string();
        assert!(invalid_config.validate().is_err());

        // Test even master replicas
        let mut invalid_config = config.clone();
        invalid_config.master.replicas = 2;
        assert!(invalid_config.validate().is_err());
    }

    #[tokio::test]
    #[ignore] // Requires Kubernetes cluster
    async fn test_descriptor_creation() {
        let descriptor = CurvineClusterDescriptor::new("default".to_string())
            .await
            .expect("Failed to create descriptor");

        // Descriptor should be created successfully
        assert!(descriptor.namespace() == "default");
    }

    #[tokio::test]
    #[ignore] // Requires Kubernetes cluster
    async fn test_builder_configmap() {
        let cluster_conf = create_test_cluster_conf();
        let builder = ConfigMapBuilder::new(
            cluster_conf,
            "test-cluster".to_string(),
            "default".to_string(),
            3,
        );

        let configmap = builder.build().expect("Failed to build ConfigMap");
        assert_eq!(
            configmap.metadata.name.as_ref().unwrap(),
            "test-cluster-config"
        );
    }

    #[tokio::test]
    #[ignore] // Requires Kubernetes cluster
    async fn test_builder_master() {
        let cluster_conf = create_test_cluster_conf();
        let kube_config = create_test_kube_config();

        let builder = MasterBuilder::new(
            "test-cluster".to_string(),
            "default".to_string(),
            kube_config,
            cluster_conf,
            false, // is_update_mode = false for tests
        );

        let statefulset = builder.build().expect("Failed to build Master StatefulSet");
        assert_eq!(
            statefulset.metadata.name.as_ref().unwrap(),
            "test-cluster-master"
        );
        assert_eq!(statefulset.spec.as_ref().unwrap().replicas, Some(3));
    }

    #[tokio::test]
    #[ignore] // Requires Kubernetes cluster
    async fn test_builder_worker() {
        let cluster_conf = create_test_cluster_conf();
        let kube_config = create_test_kube_config();

        let builder = WorkerBuilder::new(
            "test-cluster".to_string(),
            "default".to_string(),
            kube_config,
            cluster_conf,
        );

        let deployment = builder.build().expect("Failed to build Worker Deployment");
        assert_eq!(
            deployment.metadata.name.as_ref().unwrap(),
            "test-cluster-worker"
        );
        assert_eq!(deployment.spec.as_ref().unwrap().replicas, Some(3));
    }

    #[tokio::test]
    #[ignore] // Requires Kubernetes cluster
    async fn test_builder_service() {
        let builder = ServiceBuilder::new(
            "test-cluster".to_string(),
            "default".to_string(),
            ServiceType::ClusterIP,
            HashMap::new(),
        );

        let service = builder.build().expect("Failed to build Service");
        assert_eq!(
            service.metadata.name.as_ref().unwrap(),
            "test-cluster-master"
        );
    }
}
