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

use curvine_common::conf::ClusterConf;
use curvine_kube::*;
use std::collections::HashMap;

mod test_utils {
    use super::*;

    pub fn create_test_cluster_conf() -> ClusterConf {
        let mut conf = ClusterConf::default();
        conf.master.meta_dir = "testing/meta".to_string();
        conf.journal.journal_dir = "testing/journal".to_string();
        conf.worker.data_dir = vec![
            "[MEM:10GB]/data/mem".to_string(),
            "[SSD:100GB]/data/ssd".to_string(),
            "[HDD:500GB]/data/hdd".to_string(),
            "[DISK]testing/data".to_string(),
        ];
        conf
    }

    pub fn create_test_kubernetes_config() -> KubernetesConfig {
        KubernetesConfig {
            cluster_id: "test".to_string(),
            namespace: "default".to_string(),
            master: MasterConfig {
                replicas: 3,
                image: "curvine:latest".to_string(),
                resources: None,
                node_selector: None,
                affinity: None,
                pod_template: None,
                graceful_shutdown: false,
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
                image: "curvine:latest".to_string(),
                resources: None,
                node_selector: None,
                anti_affinity: false,
                pod_template: None,
                storage_class: None,
                graceful_shutdown: false,
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
                storage_class: "default".to_string(),
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
}

// ============================================================================
// Tests for Path Resolution (relative path + CURVINE_HOME)
// ============================================================================

#[test]
fn test_relative_path_resolution() {
    // Test that relative paths are correctly resolved with CURVINE_HOME prefix
    // This is tested indirectly through ConfigMapBuilder
    let conf = test_utils::create_test_cluster_conf();
    let config = test_utils::create_test_kubernetes_config();

    // Relative paths in config
    assert!(!conf.master.meta_dir.starts_with('/'));
    assert!(!conf.journal.journal_dir.starts_with('/'));

    // When building ConfigMap, paths should be resolved
    let config_builder = KubernetesConfigBuilder::new(conf.clone(), config.clone());
    let result = config_builder.build_cluster_side_config();

    assert!(result.is_ok());
    let config_str = result.unwrap();

    // ConfigMap should contain resolved paths with CURVINE_HOME prefix
    assert!(config_str.contains("/app/curvine/testing/meta"));
    assert!(config_str.contains("/app/curvine/testing/journal"));
}

#[test]
fn test_absolute_path_preserved() {
    // Test that absolute paths are preserved as-is
    let mut conf = ClusterConf::default();
    conf.master.meta_dir = "/opt/curvine/data/meta".to_string();
    conf.journal.journal_dir = "/opt/curvine/data/journal".to_string();
    conf.worker.data_dir = vec!["[SSD]/data/ssd".to_string()];

    let config = test_utils::create_test_kubernetes_config();

    let config_builder = KubernetesConfigBuilder::new(conf.clone(), config.clone());
    let result = config_builder.build_cluster_side_config();

    assert!(result.is_ok());
    let config_str = result.unwrap();

    // Absolute paths should be preserved
    assert!(config_str.contains("/opt/curvine/data/meta"));
    assert!(config_str.contains("/opt/curvine/data/journal"));
}

// ============================================================================
// Tests for Worker Storage Types
// ============================================================================

#[test]
fn test_worker_storage_types_parsing() {
    // Test parsing of different storage types
    let conf = test_utils::create_test_cluster_conf();

    // Should have 4 data directories
    assert_eq!(conf.worker.data_dir.len(), 4);

    // Verify each storage type can be parsed
    for data_dir_str in &conf.worker.data_dir {
        use curvine_common::conf::WorkerDataDir;
        let result = WorkerDataDir::from_str(data_dir_str);
        assert!(result.is_ok(), "Failed to parse: {}", data_dir_str);
    }
}

#[test]
fn test_worker_mem_storage() {
    // Test MEM storage type parsing
    use curvine_common::conf::WorkerDataDir;
    use curvine_common::state::StorageType;

    let data_dir = WorkerDataDir::from_str("[MEM:10GB]/data/mem").unwrap();
    assert_eq!(data_dir.storage_type, StorageType::Mem);
    assert_eq!(data_dir.capacity, 10 * 1024 * 1024 * 1024); // 10GB in bytes
}

#[test]
fn test_worker_ssd_storage() {
    // Test SSD storage type parsing
    use curvine_common::conf::WorkerDataDir;
    use curvine_common::state::StorageType;

    let data_dir = WorkerDataDir::from_str("[SSD:100GB]/data/ssd").unwrap();
    assert_eq!(data_dir.storage_type, StorageType::Ssd);
    assert_eq!(data_dir.capacity, 100 * 1024 * 1024 * 1024); // 100GB in bytes
}

#[test]
fn test_worker_hdd_storage() {
    // Test HDD storage type parsing
    use curvine_common::conf::WorkerDataDir;
    use curvine_common::state::StorageType;

    let data_dir = WorkerDataDir::from_str("[HDD:500GB]/data/hdd").unwrap();
    assert_eq!(data_dir.storage_type, StorageType::Hdd);
    assert_eq!(data_dir.capacity, 500 * 1024 * 1024 * 1024); // 500GB in bytes
}

#[test]
fn test_worker_disk_storage_no_slash() {
    // Test DISK storage type without leading slash (as in default config)
    use curvine_common::conf::WorkerDataDir;
    use curvine_common::state::StorageType;

    let data_dir = WorkerDataDir::from_str("[DISK]testing/data").unwrap();
    assert_eq!(data_dir.storage_type, StorageType::Disk);
}

#[test]
fn test_worker_storage_without_capacity() {
    // Test storage type without explicit capacity
    use curvine_common::conf::WorkerDataDir;
    use curvine_common::state::StorageType;

    let data_dir = WorkerDataDir::from_str("[SSD]/data/ssd").unwrap();
    assert_eq!(data_dir.storage_type, StorageType::Ssd);
    assert_eq!(data_dir.capacity, 0); // No capacity specified
}

#[test]
fn test_worker_default_storage_type() {
    // Test default storage type (DISK) when no type specified
    use curvine_common::conf::WorkerDataDir;
    use curvine_common::state::StorageType;

    let data_dir = WorkerDataDir::from_str("/data/default").unwrap();
    assert_eq!(data_dir.storage_type, StorageType::Disk);
}

// ============================================================================
// Tests for Service Port Configuration
// ============================================================================

#[test]
fn test_master_service_ports() {
    // Test that Master service ports are correctly configured
    let _conf = test_utils::create_test_cluster_conf();
    let config = test_utils::create_test_kubernetes_config();

    let builder = ServiceBuilder::new(
        "test".to_string(),
        "default".to_string(),
        config.service.service_type,
        config.service.annotations.clone(),
    );

    let result = builder.build();
    assert!(result.is_ok());

    let service = result.unwrap();

    // Should have ports defined
    assert!(service.spec.is_some());
    if let Some(spec) = service.spec {
        assert!(spec.ports.is_some());
        let ports = spec.ports.unwrap();

        // Should have at least 5 ports (rpc, journal, web, web1, worker)
        assert!(ports.len() >= 5);

        // Check specific ports exist
        let port_names: Vec<_> = ports
            .iter()
            .filter_map(|p| p.name.as_ref())
            .map(|s| s.as_str())
            .collect();

        assert!(port_names.contains(&"rpc"));
        assert!(port_names.contains(&"journal"));
        assert!(port_names.contains(&"web"));
        assert!(port_names.contains(&"web1"));
        assert!(port_names.contains(&"worker"));
    }
}

#[test]
fn test_headless_service_ports() {
    // Test that Headless service has correct ports (excludes worker port)
    let builder = HeadlessServiceBuilder::new("test".to_string(), "default".to_string());

    let result = builder.build();
    assert!(result.is_ok());

    let service = result.unwrap();

    // Should be headless (clusterIP: None)
    assert!(service.spec.is_some());
    if let Some(spec) = service.spec {
        assert_eq!(spec.cluster_ip, Some("None".to_string()));

        // Should have ports
        assert!(spec.ports.is_some());
        let ports = spec.ports.unwrap();

        // Should have 4 ports (rpc, journal, web, web1) - no worker port
        assert_eq!(ports.len(), 4);

        let port_names: Vec<_> = ports
            .iter()
            .filter_map(|p| p.name.as_ref())
            .map(|s| s.as_str())
            .collect();

        assert!(port_names.contains(&"rpc"));
        assert!(port_names.contains(&"journal"));
        assert!(port_names.contains(&"web"));
        assert!(port_names.contains(&"web1"));
        assert!(!port_names.contains(&"worker")); // Should not have worker port
    }
}

// ============================================================================
// Tests for Configuration Validation
// ============================================================================

#[test]
fn test_kubernetes_config_validate_valid() {
    // Test validation of valid configuration
    let config = test_utils::create_test_kubernetes_config();
    let result = config.validate();
    assert!(result.is_ok());
}

#[test]
fn test_kubernetes_config_validate_invalid_cluster_id() {
    // Test validation fails for invalid cluster ID
    let mut config = test_utils::create_test_kubernetes_config();
    config.cluster_id = "Test-Invalid".to_string(); // uppercase not allowed

    let result = config.validate();
    assert!(result.is_err());
}

#[test]
fn test_kubernetes_config_validate_even_master_replicas() {
    // Test validation fails for even number of Master replicas
    let mut config = test_utils::create_test_kubernetes_config();
    config.master.replicas = 2; // even number not allowed

    let result = config.validate();
    assert!(result.is_err());
}

#[test]
fn test_kubernetes_config_validate_zero_master_replicas() {
    // Test validation fails for zero Master replicas
    let mut config = test_utils::create_test_kubernetes_config();
    config.master.replicas = 0;

    let result = config.validate();
    assert!(result.is_err());
}

#[test]
fn test_kubernetes_config_validate_zero_worker_replicas() {
    // Test validation fails for zero Worker replicas
    let mut config = test_utils::create_test_kubernetes_config();
    config.worker.replicas = 0;

    let result = config.validate();
    assert!(result.is_err());
}

// ============================================================================
// Tests for Error Types
// ============================================================================

#[test]
fn test_validation_error_display() {
    // Test ValidationError is properly displayed
    let err = KubeError::ValidationError("PVC not found".to_string());
    assert!(err.to_string().contains("PVC not found"));
}

#[test]
fn test_validation_error_from_string() {
    // Test creating ValidationError from string
    let msg = "Required resource missing";
    let err = KubeError::ValidationError(msg.to_string());

    match err {
        KubeError::ValidationError(s) => assert_eq!(s, msg),
        _ => panic!("Expected ValidationError"),
    }
}

// ============================================================================
// Tests for ConfigMap Builder
// ============================================================================

#[test]
fn test_configmap_builder_build() {
    // Test ConfigMap building
    let conf = test_utils::create_test_cluster_conf();
    let _config = test_utils::create_test_kubernetes_config();

    let builder = ConfigMapBuilder::new(conf.clone(), "test".to_string(), "default".to_string(), 3);

    let result = builder.build();
    assert!(result.is_ok());

    let configmap = result.unwrap();

    // Check metadata
    assert_eq!(configmap.metadata.name, Some("test-config".to_string()));
    assert_eq!(configmap.metadata.namespace, Some("default".to_string()));

    // Check data
    assert!(configmap.data.is_some());
    let data = configmap.data.unwrap();
    assert!(data.contains_key("curvine-cluster.toml"));
}

#[test]
fn test_configmap_builder_labels() {
    // Test ConfigMap labels
    let conf = test_utils::create_test_cluster_conf();

    let builder = ConfigMapBuilder::new(conf.clone(), "test".to_string(), "default".to_string(), 3);

    let labels = builder.get_labels();
    assert_eq!(labels.get("app"), Some(&"test".to_string()));
    assert_eq!(labels.get("component"), Some(&"config".to_string()));
    assert_eq!(
        labels.get("type"),
        Some(&"curvine-native-kubernetes".to_string())
    );
}

// ============================================================================
// Tests for Master Path Resolution
// ============================================================================

#[test]
fn test_master_relative_path_mount() {
    // Test that Master relative paths are resolved in volume mounts
    let mut conf = ClusterConf::default();
    conf.master.meta_dir = "testing/meta".to_string();
    conf.journal.journal_dir = "testing/journal".to_string();
    conf.worker.data_dir = vec!["[DISK]/data".to_string()];

    let config = test_utils::create_test_kubernetes_config();

    let builder = MasterBuilder::new(
        "test".to_string(),
        "default".to_string(),
        config.clone(),
        conf.clone(),
        false, // is_update_mode = false for tests
    );

    let result = builder.build_volume_mounts_impl();
    assert!(result.is_ok());

    let mounts = result.unwrap();

    // Find meta_dir and journal_dir mounts
    let meta_mount = mounts.iter().find(|m| m.name == "meta-data");
    let journal_mount = mounts.iter().find(|m| m.name == "journal-data");

    assert!(meta_mount.is_some());
    assert!(journal_mount.is_some());

    // Paths should be resolved with CURVINE_HOME prefix
    assert_eq!(meta_mount.unwrap().mount_path, "/app/curvine/testing/meta");
    assert_eq!(
        journal_mount.unwrap().mount_path,
        "/app/curvine/testing/journal"
    );
}

#[test]
fn test_master_absolute_path_mount() {
    // Test that Master absolute paths are preserved
    let mut conf = ClusterConf::default();
    conf.master.meta_dir = "/opt/curvine/data/meta".to_string();
    conf.journal.journal_dir = "/opt/curvine/data/journal".to_string();
    conf.worker.data_dir = vec!["[DISK]/data".to_string()];

    let config = test_utils::create_test_kubernetes_config();

    let builder = MasterBuilder::new(
        "test".to_string(),
        "default".to_string(),
        config.clone(),
        conf.clone(),
        false, // is_update_mode = false for tests
    );

    let result = builder.build_volume_mounts_impl();
    assert!(result.is_ok());

    let mounts = result.unwrap();

    // Find meta_dir and journal_dir mounts
    let meta_mount = mounts.iter().find(|m| m.name == "meta-data");
    let journal_mount = mounts.iter().find(|m| m.name == "journal-data");

    // Absolute paths should be preserved
    assert_eq!(meta_mount.unwrap().mount_path, "/opt/curvine/data/meta");
    assert_eq!(
        journal_mount.unwrap().mount_path,
        "/opt/curvine/data/journal"
    );
}

// ============================================================================
// Tests for Worker Storage Volume Creation
// ============================================================================

#[test]
fn test_worker_mem_volume_creation() {
    // Test that MEM storage creates emptyDir with Memory medium
    let conf = test_utils::create_test_cluster_conf();
    let config = test_utils::create_test_kubernetes_config();

    let builder = WorkerBuilder::new(
        "test".to_string(),
        "default".to_string(),
        config.clone(),
        conf.clone(),
    );

    let result = builder.build_volumes_impl();
    assert!(result.is_ok());

    let volumes = result.unwrap();

    // Should have volumes for: configmap, mem, ssd, hdd, disk
    assert!(volumes.len() >= 5);

    // Find memory volume
    let mem_volume = volumes.iter().find(|v| v.name == "data-dir-0");
    assert!(mem_volume.is_some());

    let vol = mem_volume.unwrap();
    assert!(vol.empty_dir.is_some());

    let empty_dir = vol.empty_dir.as_ref().unwrap();
    assert_eq!(empty_dir.medium, Some("Memory".to_string()));
    assert!(empty_dir.size_limit.is_some()); // Should have size limit
}

#[test]
fn test_worker_persistent_storage_volume_creation() {
    // Test that persistent storage (SSD/HDD/DISK) creates PVC references
    let conf = test_utils::create_test_cluster_conf();
    let config = test_utils::create_test_kubernetes_config();

    let builder = WorkerBuilder::new(
        "test".to_string(),
        "default".to_string(),
        config.clone(),
        conf.clone(),
    );

    let result = builder.build_volumes_impl();
    assert!(result.is_ok());

    let volumes = result.unwrap();

    // Find SSD volume (should be data-dir-1)
    let ssd_volume = volumes.iter().find(|v| v.name == "data-dir-1");
    assert!(ssd_volume.is_some());

    let vol = ssd_volume.unwrap();
    assert!(vol.persistent_volume_claim.is_some());

    let pvc = vol.persistent_volume_claim.as_ref().unwrap();
    // PVC name format: {cluster_id}-worker-{storage_type}-{worker_idx}
    // Since we have 3 worker replicas (0, 1, 2), we check for one of them
    assert!(pvc.claim_name.starts_with("test-worker-ssd-"));
    assert!(
        pvc.claim_name.ends_with("-0")
            || pvc.claim_name.ends_with("-1")
            || pvc.claim_name.ends_with("-2")
    );
}

// ============================================================================
// Tests for UFS Storage Support
// ============================================================================

#[test]
fn test_worker_ufs_storage() {
    // Test UFS storage type parsing and handling
    use curvine_common::conf::WorkerDataDir;
    use curvine_common::state::StorageType;

    let data_dir = WorkerDataDir::from_str("[UFS:200GB]/data/ufs").unwrap();
    assert_eq!(data_dir.storage_type, StorageType::Ufs);
    assert_eq!(data_dir.capacity, 200 * 1024 * 1024 * 1024); // 200GB in bytes
}

#[test]
fn test_worker_ufs_volume_creation() {
    // Test that UFS storage creates PVC references like other persistent storage
    let mut conf = ClusterConf::default();
    conf.master.meta_dir = "testing/meta".to_string();
    conf.journal.journal_dir = "testing/journal".to_string();
    conf.worker.data_dir = vec!["[UFS:200GB]/data/ufs".to_string()];

    let config = test_utils::create_test_kubernetes_config();

    let builder = WorkerBuilder::new(
        "test".to_string(),
        "default".to_string(),
        config.clone(),
        conf.clone(),
    );

    let result = builder.build_volumes_impl();
    assert!(result.is_ok());

    let volumes = result.unwrap();

    // Find UFS volume
    let ufs_volume = volumes.iter().find(|v| v.name == "data-dir-0");
    assert!(ufs_volume.is_some());

    let vol = ufs_volume.unwrap();
    assert!(vol.persistent_volume_claim.is_some());

    let pvc = vol.persistent_volume_claim.as_ref().unwrap();
    assert_eq!(pvc.claim_name, "test-worker-ufs-0");
}
