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

/// Application paths
pub const APP_HOME: &str = "/app";
pub const CURVINE_HOME: &str = "/app/curvine";
pub const CURVINE_CONF_FILE: &str = "/app/curvine/conf/curvine-cluster.toml";

/// Network configuration
pub const ORPC_BIND_HOSTNAME: &str = "0.0.0.0";
pub const POD_CLUSTER_DOMAIN: &str = "cluster.local";

/// Master ports
pub const MASTER_RPC_PORT: i32 = 8995;
pub const MASTER_JOURNAL_PORT: i32 = 8996;
pub const MASTER_WEB_PORT: i32 = 9000;
pub const MASTER_WEB1_PORT: i32 = 9001;

/// Worker ports
pub const WORKER_RPC_PORT: i32 = 8997;
pub const WORKER_WEB_PORT: i32 = 9001;

/// Health check configuration
pub const LIVENESS_INITIAL_DELAY: i32 = 15;
pub const LIVENESS_PERIOD: i32 = 300;
pub const LIVENESS_TIMEOUT: i32 = 60;
pub const LIVENESS_FAILURE_THRESHOLD: i32 = 5;

/// Graceful shutdown
pub const GRACEFUL_SHUTDOWN_DELAY: u32 = 10;

/// Default resource settings
pub const DEFAULT_STORAGE_SIZE: &str = "10Gi";
pub const DEFAULT_ACCESS_MODE: &str = "ReadWriteOnce";

/// Rolling update settings
pub const MAX_UNAVAILABLE: &str = "25%";
pub const MAX_SURGE: &str = "25%";

/// Init container settings
pub const INIT_CONTAINER_IMAGE: &str = "busybox:latest";

/// Resource labels
pub const LABEL_APP: &str = "app";
pub const LABEL_COMPONENT: &str = "component";
pub const LABEL_TYPE: &str = "type";
pub const LABEL_TYPE_VALUE: &str = "curvine-native-kubernetes";

/// Components
pub const COMPONENT_MASTER: &str = "master";
pub const COMPONENT_WORKER: &str = "worker";

/// Container names
pub const CONTAINER_NAME_MASTER: &str = "cv-master";
pub const CONTAINER_NAME_WORKER: &str = "cv-worker";

/// Restart policy
pub const RESTART_POLICY_ALWAYS: &str = "Always";

/// StatefulSet pod management policy
pub const POD_MANAGEMENT_POLICY_PARALLEL: &str = "Parallel";

/// Deployment strategy
pub const STRATEGY_TYPE_ROLLING_UPDATE: &str = "RollingUpdate";

/// Service names and suffixes
pub const SERVICE_SUFFIX_MASTER: &str = "-master";
pub const SERVICE_SUFFIX_WORKER: &str = "-worker";
pub const SERVICE_SUFFIX_HEADLESS: &str = "-headless";
pub const SERVICE_SUFFIX_CONFIG: &str = "-config";

/// Volume and VolumeMount names
pub const VOLUME_NAME_CONFIG: &str = "curvine-conf";
pub const VOLUME_NAME_META_DATA: &str = "meta-data";
pub const VOLUME_NAME_JOURNAL_DATA: &str = "journal-data";
pub const VOLUME_NAME_DATA_DIR_PREFIX: &str = "data-dir-";

/// ConfigMap configuration
pub const CONFIG_FILE_NAME: &str = "curvine-cluster.toml";
pub const CONFIG_FILE_MODE: i32 = 0o644;

/// Security context
pub const SECURITY_PRIVILEGED: bool = true;

/// Volume types and medium
pub const VOLUME_MEDIUM_MEMORY: &str = "Memory";
pub const VOLUME_TYPE_DIRECTORY_OR_CREATE: &str = "DirectoryOrCreate";

/// DNS policies
pub const DNS_POLICY_CLUSTER_FIRST_WITH_HOST_NET: &str = "ClusterFirstWithHostNet";

/// Port names
pub const PORT_NAME_RPC: &str = "rpc";
pub const PORT_NAME_JOURNAL: &str = "journal";
pub const PORT_NAME_WEB: &str = "web";
pub const PORT_NAME_WEB1: &str = "web1";

/// Affinity topology key
pub const TOPOLOGY_KEY_HOSTNAME: &str = "kubernetes.io/hostname";
