//! Kubernetes deployment commands

use clap::{Parser, Subcommand};
use curvine_common::conf::ClusterConf;
use curvine_kube::{
    CurvineClusterDescriptor, KubernetesConfig, MasterConfig, ServiceConfig, ServiceType,
    StorageConfig, WorkerConfig,
};
use orpc::CommonResult;
use std::collections::HashMap;

#[derive(Parser, Debug)]
#[command(name = "k8s", about = "Kubernetes deployment commands")]
pub struct K8sCommand {
    #[command(subcommand)]
    command: K8sSubCommand,
}

#[derive(Subcommand, Debug)]
pub enum K8sSubCommand {
    /// Deploy a new Curvine cluster to Kubernetes (creates all resources)
    Deploy(DeployCommand),

    /// Update an existing Curvine cluster (modifies replicas, images, etc.)
    Update(UpdateCommand),

    /// List all Curvine clusters
    List(ListCommand),

    /// Show cluster status
    Status(StatusCommand),

    /// Delete a cluster
    Delete(DeleteCommand),
}

#[derive(Parser, Debug, Clone)]
pub struct DeployCommand {
    /// Cluster ID (must be a valid Kubernetes name)
    /// If not provided, will use client.kubernetes.cluster_id from config file
    #[arg(long, short = 'c')]
    pub cluster_id: Option<String>,

    /// Kubernetes namespace
    #[arg(long, short = 'n', default_value = "default")]
    pub namespace: String,

    /// Path to kubeconfig file
    /// If not specified, uses default kubeconfig resolution (KUBECONFIG env or ~/.kube/config)
    #[arg(long)]
    pub kubeconfig: Option<String>,

    /// Kubernetes context to use
    /// If not specified, uses current context from kubeconfig
    #[arg(long)]
    pub context: Option<String>,

    /// Master image
    #[arg(long, default_value = "docker.io/curvine:latest")]
    pub master_image: String,

    /// Worker image
    #[arg(long, default_value = "docker.io/curvine:latest")]
    pub worker_image: String,

    /// Master replicas (must be odd for Raft)
    #[arg(long, default_value = "3")]
    pub master_replicas: u32,

    /// Worker replicas
    #[arg(long, default_value = "3")]
    pub worker_replicas: u32,

    /// Master Pod template file path (YAML)
    #[arg(long)]
    pub master_pod_template: Option<String>,

    /// Worker Pod template file path (YAML)
    #[arg(long)]
    pub worker_pod_template: Option<String>,

    /// Storage class for PVCs (both master and worker, unless master-storage-class or worker-storage-class is specified)
    #[arg(long)]
    pub storage_class: Option<String>,

    /// Master storage class (overrides storage-class for master)
    #[arg(long)]
    pub master_storage_class: Option<String>,

    /// Worker storage class (overrides storage-class for worker)
    #[arg(long)]
    pub worker_storage_class: Option<String>,

    /// Master storage size (e.g., "100Gi")
    #[arg(long)]
    pub master_storage_size: Option<String>,

    /// Worker storage size (e.g., "100Gi")
    #[arg(long)]
    pub worker_storage_size: Option<String>,

    /// Service type (ClusterIP, NodePort, LoadBalancer)
    #[arg(long, default_value = "ClusterIP")]
    pub service_type: String,

    /// Image pull policy
    #[arg(long, default_value = "IfNotPresent")]
    pub image_pull_policy: String,

    /// Dynamic configuration properties to override any settings (-D key=value)
    ///
    /// Basic: kubernetes.namespace, kubernetes.cluster-id, kubernetes.config.file, kubernetes.context
    /// Images: kubernetes.container.image, kubernetes.master.image, kubernetes.worker.image, kubernetes.image.pull-policy
    /// Replicas: kubernetes.master.replicas, kubernetes.worker.replicas
    /// Resources: kubernetes.master.cpu, kubernetes.master.memory, kubernetes.worker.cpu, kubernetes.worker.memory
    /// Node Scheduling: kubernetes.master.node-selector, kubernetes.worker.node-selector (format: key1=val1,key2=val2)
    /// Labels: kubernetes.master.labels, kubernetes.worker.labels (format: key1=val1,key2=val2)
    /// Annotations: kubernetes.master.annotations, kubernetes.worker.annotations, kubernetes.service.annotations
    /// Security: kubernetes.master.service-account, kubernetes.worker.service-account
    /// Environment: kubernetes.master.env.VAR_NAME, kubernetes.worker.env.VAR_NAME (dynamic env vars)
    /// DNS & Priority: kubernetes.pod.dns-policy, kubernetes.pod.priority-class
    /// Storage: kubernetes.storage.class, kubernetes.storage.size
    /// Service: kubernetes.service.type, kubernetes.service.external-ips (comma-separated)
    /// Templates: kubernetes.master.pod-template, kubernetes.worker.pod-template (for complex config like tolerations)
    ///
    /// Example: -Dkubernetes.master.cpu=2.0 -Dkubernetes.master.labels=app=curvine,tier=master
    #[arg(short = 'D', value_name = "KEY=VALUE")]
    pub properties: Vec<String>,
}

#[derive(Parser, Debug, Clone)]
pub struct UpdateCommand {
    #[arg(long, short = 'c')]
    pub cluster_id: Option<String>,

    #[arg(long, short = 'n', default_value = "default")]
    pub namespace: String,

    #[arg(long)]
    pub kubeconfig: Option<String>,

    #[arg(long)]
    pub context: Option<String>,

    /// Path to Curvine cluster configuration file (curvine-cluster.toml)
    /// If flag is used without value, reads from CURVINE_CONF_FILE environment variable
    /// If path is specified, uses that path to update ConfigMap
    /// If flag is not used, ConfigMap is not updated
    #[arg(long, num_args = 0..=1, default_missing_value = "${CURVINE_CONF_FILE}", value_name = "PATH")]
    pub config_file: Option<String>,

    #[arg(long)]
    pub master_image: Option<String>,

    #[arg(long)]
    pub worker_image: Option<String>,

    #[arg(long)]
    pub master_replicas: Option<u32>,

    #[arg(long)]
    pub worker_replicas: Option<u32>,

    #[arg(long)]
    pub master_pod_template: Option<String>,

    #[arg(long)]
    pub worker_pod_template: Option<String>,

    #[arg(long)]
    pub service_type: Option<String>,

    #[arg(long)]
    pub image_pull_policy: Option<String>,

    #[arg(short = 'D', value_name = "KEY=VALUE")]
    pub properties: Vec<String>,
}

#[derive(Parser, Debug)]
pub struct ListCommand {
    #[arg(long, short = 'n')]
    pub namespace: Option<String>,
}

#[derive(Parser, Debug)]
pub struct StatusCommand {
    /// Cluster ID
    #[arg(required = true)]
    pub cluster_id: String,

    /// Kubernetes namespace
    #[arg(long, short = 'n', default_value = "default")]
    pub namespace: String,
}

#[derive(Parser, Debug)]
pub struct DeleteCommand {
    /// Cluster ID
    #[arg(required = true)]
    pub cluster_id: String,

    /// Kubernetes namespace
    #[arg(long, short = 'n', default_value = "default")]
    pub namespace: String,

    /// Delete PVCs (persistent volumes)
    #[arg(long)]
    pub delete_pvcs: bool,
}

impl K8sCommand {
    pub async fn execute(&self, cluster_conf: ClusterConf) -> CommonResult<()> {
        match &self.command {
            K8sSubCommand::Deploy(cmd) => cmd.execute(cluster_conf).await,
            K8sSubCommand::Update(cmd) => cmd.execute(cluster_conf).await,
            K8sSubCommand::List(cmd) => cmd.execute().await,
            K8sSubCommand::Status(cmd) => cmd.execute().await,
            K8sSubCommand::Delete(cmd) => cmd.execute().await,
        }
    }
}

impl DeployCommand {
    async fn execute(&self, cluster_conf: ClusterConf) -> CommonResult<()> {
        // Parse dynamic configurations
        let dynamic_configs = if !self.properties.is_empty() {
            Some(
                parse_dynamic_configs(&self.properties)
                    .map_err(|e| format!("Failed to parse dynamic configs: {}", e))?,
            )
        } else {
            None
        };

        // Note: Dynamic configs are now applied directly to KubernetesConfig
        // via curvine_kube::dynamic_config::apply_to_kube_config() later in this function.
        // We no longer modify the deploy command here.
        let cmd = self;

        // Get Kubernetes configuration from ClientConf.kubernetes or use command line arguments
        // Priority: command line arguments > ClientConf.kubernetes > defaults
        let kube_conf = cluster_conf.client.kubernetes.as_ref();

        // Resolve cluster_id: command line > config file > error
        let cluster_id = cmd.cluster_id.clone()
            .or_else(|| kube_conf.and_then(|k| k.cluster_id.clone()))
            .ok_or_else(|| "cluster_id is required (use --cluster-id or set client.kubernetes.cluster_id in config)".to_string())?;

        // Resolve namespace: command line > config file > default
        let namespace = if !cmd.namespace.is_empty() && cmd.namespace != "default" {
            cmd.namespace.clone()
        } else {
            kube_conf
                .map(|k| k.namespace.clone())
                .unwrap_or_else(|| cmd.namespace.clone())
        };

        // Resolve service type: command line > config file > default
        let service_type_str = if cmd.service_type != "ClusterIP" {
            cmd.service_type.clone()
        } else {
            kube_conf
                .map(|k| k.service.service_type.clone())
                .unwrap_or_else(|| cmd.service_type.clone())
        };
        let service_type = service_type_str
            .parse::<ServiceType>()
            .map_err(|e| format!("Invalid service type: {}", e))?;

        // Resolve master config: command line > config file > defaults
        let master_replicas = if cmd.master_replicas != 3 {
            cmd.master_replicas
        } else {
            kube_conf
                .map(|k| k.master.replicas)
                .unwrap_or(cmd.master_replicas)
        };

        let master_image = if cmd.master_image != "docker.io/curvine:latest" {
            cmd.master_image.clone()
        } else {
            kube_conf
                .map(|k| k.master.image.clone())
                .unwrap_or_else(|| cmd.master_image.clone())
        };

        let master_pod_template = cmd
            .master_pod_template
            .clone()
            .or_else(|| kube_conf.and_then(|k| k.master.pod_template.clone()));

        let master_node_selector = kube_conf.and_then(|k| k.master.node_selector.clone());

        // Resolve worker config: command line > config file > defaults
        let worker_replicas = if cmd.worker_replicas != 3 {
            cmd.worker_replicas
        } else {
            kube_conf
                .map(|k| k.worker.replicas)
                .unwrap_or(cmd.worker_replicas)
        };

        let worker_image = if cmd.worker_image != "docker.io/curvine:latest" {
            cmd.worker_image.clone()
        } else {
            kube_conf
                .map(|k| k.worker.image.clone())
                .unwrap_or_else(|| cmd.worker_image.clone())
        };

        let worker_pod_template = cmd
            .worker_pod_template
            .clone()
            .or_else(|| kube_conf.and_then(|k| k.worker.pod_template.clone()));

        let worker_node_selector = kube_conf.and_then(|k| k.worker.node_selector.clone());

        // Resolve storage config: command line > config file
        let storage_class = cmd
            .storage_class
            .clone()
            .or_else(|| kube_conf.and_then(|k| k.worker.storage_class.clone()))
            .or_else(|| {
                kube_conf.and_then(|k| k.storage.as_ref().map(|s| s.storage_class.clone()))
            });

        let master_storage_size = cmd.master_storage_size.clone().or_else(|| {
            kube_conf.and_then(|k| k.storage.as_ref().and_then(|s| s.master_size.clone()))
        });

        let worker_storage_size = cmd.worker_storage_size.clone().or_else(|| {
            kube_conf.and_then(|k| k.storage.as_ref().and_then(|s| s.worker_size.clone()))
        });

        // Resolve image pull policy: command line > config file > default
        let image_pull_policy = if cmd.image_pull_policy != "IfNotPresent" {
            cmd.image_pull_policy.clone()
        } else {
            kube_conf
                .map(|k| k.image_pull_policy.clone())
                .unwrap_or_else(|| cmd.image_pull_policy.clone())
        };

        // Build Kubernetes configuration
        let mut kube_config = KubernetesConfig {
            cluster_id,
            namespace: namespace.clone(),
            master: MasterConfig {
                replicas: master_replicas,
                image: master_image,
                resources: None,
                node_selector: master_node_selector,
                affinity: None,
                pod_template: master_pod_template,
                graceful_shutdown: kube_conf
                    .map(|k| k.master.graceful_shutdown)
                    .unwrap_or(true),
                labels: HashMap::new(),
                annotations: HashMap::new(),
                tolerations: Vec::new(),
                service_account: None,
                env_vars: HashMap::new(),
                dns_policy: None,
                priority_class: None,
            },
            worker: WorkerConfig {
                replicas: worker_replicas,
                image: worker_image,
                resources: None,
                node_selector: worker_node_selector,
                anti_affinity: false,
                pod_template: worker_pod_template,
                storage_class: storage_class.clone(),
                graceful_shutdown: kube_conf
                    .map(|k| k.worker.graceful_shutdown)
                    .unwrap_or(true),
                host_network: kube_conf.map(|k| k.worker.host_network).unwrap_or(false),
                init_container: kube_conf.map(|k| k.worker.init_container).unwrap_or(false),
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
                service_type,
                annotations: kube_conf
                    .map(|k| k.service.annotations.clone())
                    .unwrap_or_default(),
                session_affinity: kube_conf.and_then(|k| k.service.session_affinity.clone()),
                external_ips: kube_conf
                    .map(|k| k.service.external_ips.clone())
                    .unwrap_or_default(),
                load_balancer_source_ranges: Vec::new(),
            },
            storage: storage_class
                .as_ref()
                .or(cmd.master_storage_class.as_ref())
                .or(cmd.worker_storage_class.as_ref())
                .or(master_storage_size.as_ref())
                .or(worker_storage_size.as_ref())
                .map(|_| {
                    let fallback_sc = cmd
                        .master_storage_class
                        .as_ref()
                        .or(cmd.worker_storage_class.as_ref())
                        .cloned()
                        .unwrap_or_default();
                    StorageConfig {
                        storage_class: storage_class.clone().unwrap_or(fallback_sc),
                        master_storage_class: cmd.master_storage_class.clone(),
                        worker_storage_class: cmd.worker_storage_class.clone(),
                        master_size: master_storage_size.clone(),
                        worker_size: worker_storage_size.clone(),
                    }
                }),
            image_pull_policy,
            image_pull_secrets: kube_conf
                .map(|k| k.image_pull_secrets.clone())
                .unwrap_or_default(),
            cluster_domain: "cluster.local".to_string(),
        };

        // Apply advanced dynamic configurations directly to kube_config
        if let Some(ref configs) = dynamic_configs {
            curvine_kube::dynamic_config::apply_to_kube_config(configs, &mut kube_config);
        }

        // Create cluster descriptor with kubeconfig options
        let descriptor = CurvineClusterDescriptor::new_with_config(
            namespace,
            cmd.kubeconfig.clone(),
            cmd.context.clone(),
        )
        .await
        .map_err(|e| format!("Failed to create cluster descriptor: {}", e))?;

        descriptor
            .deploy_cluster(&cluster_conf, &kube_config)
            .await
            .map_err(|e| {
                // Clean up error formatting - remove duplicate prefixes
                let error_msg = e.to_string();
                if error_msg.starts_with("Failed to deploy cluster: ") {
                    error_msg.to_string()
                } else {
                    format!("Deployment failed: {}", error_msg)
                }
            })?;

        println!("Cluster {} deployed successfully!", kube_config.cluster_id);
        Ok(())
    }
}

impl UpdateCommand {
    async fn execute(&self, mut cluster_conf: ClusterConf) -> CommonResult<()> {
        if let Some(ref config_file_path) = self.config_file {
            // Handle ${CURVINE_CONF_FILE} placeholder
            let actual_path = if config_file_path == "${CURVINE_CONF_FILE}" {
                std::env::var("CURVINE_CONF_FILE").map_err(|_| {
                    "❌ CURVINE_CONF_FILE environment variable not set\n\n  Usage:\n  export CURVINE_CONF_FILE=/path/to/your/curvine-cluster.toml\n  cv k8s update -c test --config-file".to_string()
                })?
            } else {
                config_file_path.clone()
            };

            // Validate file exists
            if !std::path::Path::new(&actual_path).exists() {
                return Err(format!("❌ Configuration file not found: {}", actual_path).into());
            }

            // Load the new configuration
            cluster_conf = ClusterConf::from(&actual_path)
                .map_err(|e| format!("Failed to load configuration from {}: {}", actual_path, e))?;

            println!("✓ Loaded new configuration from: {}", actual_path);
            println!("  Note: Dynamic parameters (master addresses, journal addresses) will be regenerated from cluster state");
        }

        // Check if master_replicas is being updated (not supported)
        if self.master_replicas.is_some() {
            return Err(
                "❌ Master replicas cannot be updated\n\n  Reason: Master nodes form a Raft cluster and changing replica count requires manual cluster reconfiguration.\n\n💡 Supported updates:\n- Worker replicas: --worker-replicas <count>\n- Master image: --master-image <image>\n- Worker image: --worker-image <image>\n- Image pull policy: --image-pull-policy <policy>\n- Service type: --service-type <type>\n- Pod templates and resources via -D\n\n📝 Example: cv k8s update -c test --worker-replicas 5".to_string().into()
            );
        }

        // Parse dynamic configurations
        let dynamic_configs = if !self.properties.is_empty() {
            Some(
                parse_dynamic_configs(&self.properties)
                    .map_err(|e| format!("Failed to parse dynamic configs: {}", e))?,
            )
        } else {
            None
        };

        // Note: Dynamic configs are now applied directly to KubernetesConfig
        // via curvine_kube::dynamic_config::apply_to_kube_config() later in this function.
        // We no longer modify the update command here.

        // Check if master_replicas was set via CLI
        if self.master_replicas.is_some() {
            return Err(
                "❌ Master replicas cannot be updated (via -D kubernetes.master.replicas)\n\n  Reason: Master nodes form a Raft cluster and changing replica count requires manual cluster reconfiguration.\n\n💡 Supported updates:\n- Worker replicas: --worker-replicas or -Dkubernetes.worker.replicas\n- Master image: --master-image or -Dkubernetes.master.image\n- Worker image: --worker-image or -Dkubernetes.worker.image\n\n📝 Example: cv k8s update -c test -Dkubernetes.worker.replicas=5".to_string().into()
            );
        }

        // Use self for configuration resolution
        let cmd = self;

        // Get Kubernetes configuration from ClientConf.kubernetes or use command line arguments
        let kube_conf = cluster_conf.client.kubernetes.as_ref();

        // Resolve cluster_id: command line > config file > error
        let cluster_id = cmd.cluster_id.clone()
            .or_else(|| kube_conf.and_then(|k| k.cluster_id.clone()))
            .ok_or_else(|| "cluster_id is required (use --cluster-id or set client.kubernetes.cluster_id in config)".to_string())?;

        // Resolve namespace: command line > config file > default
        let namespace = if !cmd.namespace.is_empty() && cmd.namespace != "default" {
            cmd.namespace.clone()
        } else {
            kube_conf
                .map(|k| k.namespace.clone())
                .unwrap_or_else(|| cmd.namespace.clone())
        };

        // Resolve service type: only use if explicitly provided
        let service_type_str = cmd
            .service_type
            .clone()
            .or_else(|| kube_conf.map(|k| k.service.service_type.clone()));
        let service_type = if let Some(st) = service_type_str {
            st.parse::<ServiceType>()
                .map_err(|e| format!("Invalid service type: {}", e))?
        } else {
            ServiceType::ClusterIP
        };

        let master_replicas = kube_conf.map(|k| k.master.replicas).unwrap_or(1) as i32;

        let master_image = cmd
            .master_image
            .clone()
            .or_else(|| kube_conf.map(|k| k.master.image.clone()));

        let master_pod_template = cmd
            .master_pod_template
            .clone()
            .or_else(|| kube_conf.and_then(|k| k.master.pod_template.clone()));

        let master_node_selector = kube_conf.and_then(|k| k.master.node_selector.clone());

        // Resolve worker config: only update if explicitly provided
        let worker_replicas = cmd
            .worker_replicas
            .or_else(|| kube_conf.map(|k| k.worker.replicas));

        let worker_image = cmd
            .worker_image
            .clone()
            .or_else(|| kube_conf.map(|k| k.worker.image.clone()));

        let worker_pod_template = cmd
            .worker_pod_template
            .clone()
            .or_else(|| kube_conf.and_then(|k| k.worker.pod_template.clone()));

        let worker_node_selector = kube_conf.and_then(|k| k.worker.node_selector.clone());

        // Note: storage_class and storage_size are not updatable
        // They only affect new PVCs, and existing PVCs cannot be modified in K8s
        // Use the existing storage config from cluster
        let storage_config = kube_conf.and_then(|k| {
            k.storage.as_ref().map(|s| StorageConfig {
                storage_class: s.storage_class.clone(),
                master_storage_class: s.master_storage_class.clone(),
                worker_storage_class: s.worker_storage_class.clone(),
                master_size: s.master_size.clone(),
                worker_size: s.worker_size.clone(),
            })
        });

        // Resolve image pull policy: only update if explicitly provided
        let image_pull_policy = cmd
            .image_pull_policy
            .clone()
            .or_else(|| kube_conf.map(|k| k.image_pull_policy.clone()));

        // Build Kubernetes configuration with optional fields
        let mut kube_config = KubernetesConfig {
            cluster_id,
            namespace: namespace.clone(),
            master: MasterConfig {
                replicas: master_replicas as u32,
                image: master_image.unwrap_or_else(|| "docker.io/curvine:latest".to_string()),
                resources: None,
                node_selector: master_node_selector,
                affinity: None,
                pod_template: master_pod_template,
                graceful_shutdown: kube_conf
                    .map(|k| k.master.graceful_shutdown)
                    .unwrap_or(true),
                labels: HashMap::new(),
                annotations: HashMap::new(),
                tolerations: Vec::new(),
                service_account: None,
                env_vars: HashMap::new(),
                dns_policy: None,
                priority_class: None,
            },
            worker: WorkerConfig {
                replicas: worker_replicas.unwrap_or(3),
                image: worker_image.unwrap_or_else(|| "docker.io/curvine:latest".to_string()),
                resources: None,
                node_selector: worker_node_selector,
                anti_affinity: false,
                pod_template: worker_pod_template,
                storage_class: storage_config.as_ref().map(|s| s.storage_class.clone()),
                graceful_shutdown: kube_conf
                    .map(|k| k.worker.graceful_shutdown)
                    .unwrap_or(true),
                host_network: kube_conf.map(|k| k.worker.host_network).unwrap_or(false),
                init_container: kube_conf.map(|k| k.worker.init_container).unwrap_or(false),
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
                service_type,
                annotations: kube_conf
                    .map(|k| k.service.annotations.clone())
                    .unwrap_or_default(),
                session_affinity: kube_conf.and_then(|k| k.service.session_affinity.clone()),
                external_ips: kube_conf
                    .map(|k| k.service.external_ips.clone())
                    .unwrap_or_default(),
                load_balancer_source_ranges: Vec::new(),
            },
            storage: storage_config,
            image_pull_policy: image_pull_policy.unwrap_or_else(|| "IfNotPresent".to_string()),
            image_pull_secrets: kube_conf
                .map(|k| k.image_pull_secrets.clone())
                .unwrap_or_default(),
            cluster_domain: "cluster.local".to_string(),
        };

        // Apply advanced dynamic configurations directly to kube_config
        if let Some(ref configs) = dynamic_configs {
            curvine_kube::dynamic_config::apply_to_kube_config(configs, &mut kube_config);
        }

        // Create cluster descriptor with kubeconfig options
        let descriptor = CurvineClusterDescriptor::new_with_config(
            namespace,
            cmd.kubeconfig.clone(),
            cmd.context.clone(),
        )
        .await
        .map_err(|e| format!("Failed to create cluster descriptor: {}", e))?;

        // Update cluster (fails if cluster doesn't exist)
        descriptor
            .update_cluster(&cluster_conf, &kube_config)
            .await
            .map_err(|e| {
                let error_msg = e.to_string();
                if error_msg.starts_with("Failed to update cluster: ") {
                    error_msg.to_string()
                } else {
                    format!("Cluster update failed: {}", error_msg)
                }
            })?;

        println!("Cluster {} updated successfully!", kube_config.cluster_id);
        Ok(())
    }
}

impl StatusCommand {
    async fn execute(&self) -> CommonResult<()> {
        let descriptor = CurvineClusterDescriptor::new(self.namespace.clone())
            .await
            .map_err(|e| format!("Failed to create cluster descriptor: {}", e))?;

        let status = descriptor
            .get_cluster_status(&self.cluster_id)
            .await
            .map_err(|e| format!("Failed to get cluster status: {}", e))?;

        println!("Cluster Status: {}", status.cluster_id);
        println!("---");

        if let Some(ref master) = status.master {
            println!("Master StatefulSet: {}", master.name);
            println!("  Replicas: {}/{}", master.ready_replicas, master.replicas);
        } else {
            println!("Master StatefulSet: Not found");
        }

        if let Some(ref worker) = status.worker {
            println!("Worker StatefulSet: {}", worker.name);
            println!("  Replicas: {}/{}", worker.ready_replicas, worker.replicas);
        } else {
            println!("Worker StatefulSet: Not found");
        }

        if let Some(ref service) = status.service {
            println!("Service: {}", service.name);
            if let Some(ref cluster_ip) = service.cluster_ip {
                println!("  Cluster IP: {}", cluster_ip);
            }
        } else {
            println!("Service: Not found");
        }

        if let Some(ref configmap) = status.configmap {
            println!("ConfigMap: {}", configmap.name);
        } else {
            println!("ConfigMap: Not found");
        }

        Ok(())
    }
}

impl DeleteCommand {
    async fn execute(&self) -> CommonResult<()> {
        let descriptor = CurvineClusterDescriptor::new(self.namespace.clone())
            .await
            .map_err(|e| format!("Failed to create cluster descriptor: {}", e))?;

        descriptor
            .delete_cluster(&self.cluster_id, self.delete_pvcs)
            .await
            .map_err(|e| format!("Failed to delete cluster: {}", e))?;

        println!("Cluster {} deleted successfully!", self.cluster_id);
        Ok(())
    }
}

impl ListCommand {
    async fn execute(&self) -> CommonResult<()> {
        use curvine_kube::CurvineClusterDescriptor;
        let namespaces: Vec<String> = if let Some(ref ns) = self.namespace {
            vec![ns.clone()]
        } else {
            vec!["default".to_string(), "curvine".to_string()]
        };

        let mut all_clusters = Vec::new();

        for namespace in &namespaces {
            let descriptor = match CurvineClusterDescriptor::new(namespace.clone()).await {
                Ok(d) => d,
                Err(_) => continue,
            };

            match descriptor.list_clusters().await {
                Ok(clusters) => all_clusters.extend(clusters),
                Err(_) => continue,
            }
        }

        if all_clusters.is_empty() {
            println!(
                "No Curvine clusters found in namespace(s): {}",
                namespaces.join(", ")
            );
            return Ok(());
        }

        println!("Curvine Clusters:");
        println!(
            "{:<20} {:<15} {:<20} {:<20}",
            "CLUSTER", "NAMESPACE", "MASTER (READY/TOTAL)", "WORKER (READY/TOTAL)"
        );
        println!("{:-<75}", "");

        for cluster in all_clusters {
            println!(
                "{:<20} {:<15} {:<20} {:<20}",
                cluster.cluster_id,
                cluster.namespace,
                format!("{}/{}", cluster.master_ready, cluster.master_replicas),
                format!("{}/{}", cluster.worker_ready, cluster.worker_replicas),
            );
        }

        Ok(())
    }
}

/// Parse dynamic configuration properties from -D key=value format
fn parse_dynamic_configs(configs: &[String]) -> Result<HashMap<String, String>, String> {
    let mut map = HashMap::new();

    for config in configs {
        let parts: Vec<&str> = config.splitn(2, '=').collect();
        if parts.len() != 2 {
            return Err(format!(
                "Invalid config format: '{}'. Expected 'key=value'",
                config
            ));
        }

        let key = parts[0].trim();
        let value = parts[1].trim();

        if key.is_empty() {
            return Err(format!("Empty key in config: '{}'", config));
        }

        map.insert(key.to_string(), value.to_string());
    }

    Ok(map)
}
