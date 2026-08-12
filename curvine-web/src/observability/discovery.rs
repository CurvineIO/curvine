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

use std::time::Duration;

use curvine_config::ClusterConf;
use curvine_model::{MasterInfo, WorkerInfo};
use curvine_unified_fs::UnifiedFileSystem;
use tokio::time::timeout;

use crate::util::addr::{
    fuse_metrics_instance, fuse_metrics_scrape_url, is_ip_literal, master_host_short_name,
    master_metrics_scrape_url, worker_metrics_display_hostname, worker_metrics_instance,
    worker_metrics_scrape_url,
};

use super::model::{MetricTarget, ACTIVE_MASTER_INSTANCE};

const MASTER_INFO_TIMEOUT_SECS: u64 = 5;

pub struct MasterDiscovery {
    pub target: MetricTarget,
    pub error: Option<String>,
}

pub struct WorkerDiscovery {
    pub target: MetricTarget,
    pub warning: Option<String>,
}

pub struct DiscoveryResult {
    pub master: MasterDiscovery,
    pub workers: Vec<WorkerDiscovery>,
    pub fuses: Vec<MetricTarget>,
    pub transfers: Vec<MetricTarget>,
    /// Populated when `get_master_info` succeeds; used to backfill master gauge
    /// metrics when the master `/metrics` HTTP scrape is unavailable.
    pub master_info: Option<MasterInfo>,
}

pub fn build_master_metrics_target(conf: &ClusterConf, active_master: &str) -> MetricTarget {
    let trimmed = active_master.trim();
    let url = master_metrics_scrape_url(conf, trimmed);
    let active_master_rpc = if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    };
    MetricTarget::master(ACTIVE_MASTER_INSTANCE.to_string(), url, active_master_rpc)
}

pub fn build_worker_metrics_target(
    worker: &WorkerInfo,
    worker_state: &str,
    conf: &ClusterConf,
) -> WorkerDiscovery {
    let worker_id = worker.worker_id();
    let instance = worker_metrics_instance(&worker.address, conf);
    let hostname = worker_metrics_display_hostname(&worker.address);
    let mut warning = None;
    if hostname.is_empty() {
        warning = Some(format!(
            "worker {worker_id} has no routable hostname or IP for metrics scrape"
        ));
    } else if crate::util::addr::is_loopback_hostname(&hostname) {
        warning = Some(format!(
            "worker {worker_id} metrics endpoint uses loopback host {hostname}"
        ));
    }
    let url = if hostname.is_empty() {
        format!("http://invalid-worker-{worker_id}/metrics")
    } else {
        worker_metrics_scrape_url(&worker.address, conf)
    };
    WorkerDiscovery {
        target: MetricTarget::worker(instance, url, worker_id, worker_state),
        warning,
    }
}

fn discover_workers(
    workers: &[WorkerInfo],
    worker_state: &str,
    conf: &ClusterConf,
) -> Vec<WorkerDiscovery> {
    workers
        .iter()
        .map(|worker| build_worker_metrics_target(worker, worker_state, conf))
        .collect()
}

fn discover_workers_from_master_info(
    info: &MasterInfo,
    conf: &ClusterConf,
) -> Vec<WorkerDiscovery> {
    let mut workers = Vec::new();
    workers.extend(discover_workers(&info.live_workers, "live", conf));
    workers.extend(discover_workers(&info.blacklist_workers, "blacklist", conf));
    workers.extend(discover_workers(
        &info.decommission_workers,
        "decommission",
        conf,
    ));
    workers.extend(discover_workers(&info.lost_workers, "lost", conf));
    workers
}

pub fn discover_fuse_targets(conf: &ClusterConf) -> Vec<MetricTarget> {
    let fuse_conf = &conf.web.observability.fuse;
    if !fuse_conf.enabled {
        return Vec::new();
    }
    let endpoints: Vec<String> = if fuse_conf.targets.is_empty() {
        vec![format!("127.0.0.1:{}", conf.fuse.web_port)]
    } else {
        fuse_conf.targets.clone()
    };
    endpoints
        .into_iter()
        .map(|endpoint| {
            let instance = fuse_metrics_instance(&endpoint);
            let url = fuse_metrics_scrape_url(&endpoint);
            MetricTarget::fuse(instance, url)
        })
        .collect()
}

pub fn discover_transfer_targets(conf: &ClusterConf) -> Vec<MetricTarget> {
    if !conf.transfer.enabled {
        return Vec::new();
    }
    let configured = conf.transfer.hostname.trim();
    let hostname = if configured.is_empty() {
        "127.0.0.1".to_string()
    } else if is_ip_literal(configured) {
        configured.to_string()
    } else {
        master_host_short_name(configured).to_string()
    };
    let instance = format!("{hostname}:{}", conf.transfer.web_port);
    let url = format!("http://{instance}/metrics");
    vec![MetricTarget::transfer(instance, url)]
}

pub async fn discover_targets(
    conf: &ClusterConf,
    unified_fs: &UnifiedFileSystem,
) -> DiscoveryResult {
    match timeout(
        Duration::from_secs(MASTER_INFO_TIMEOUT_SECS),
        unified_fs.get_master_info(),
    )
    .await
    {
        Ok(Ok(info)) => DiscoveryResult {
            master: MasterDiscovery {
                target: build_master_metrics_target(conf, info.active_master.trim()),
                error: None,
            },
            workers: discover_workers_from_master_info(&info, conf),
            fuses: discover_fuse_targets(conf),
            transfers: discover_transfer_targets(conf),
            master_info: Some(info),
        },
        Ok(Err(err)) => DiscoveryResult {
            master: MasterDiscovery {
                target: build_master_metrics_target(conf, ""),
                error: Some(err.to_string()),
            },
            workers: Vec::new(),
            fuses: discover_fuse_targets(conf),
            transfers: discover_transfer_targets(conf),
            master_info: None,
        },
        Err(_) => DiscoveryResult {
            master: MasterDiscovery {
                target: build_master_metrics_target(conf, ""),
                error: Some(format!(
                    "master info request timed out after {MASTER_INFO_TIMEOUT_SECS}s"
                )),
            },
            workers: Vec::new(),
            fuses: discover_fuse_targets(conf),
            transfers: discover_transfer_targets(conf),
            master_info: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use curvine_model::WorkerAddress;
    use curvine_net::net::InetAddr;

    use super::*;
    use crate::util::addr::{fuse_metrics_scrape_url, master_metrics_scrape_url};

    fn test_conf() -> ClusterConf {
        let mut conf = ClusterConf::default();
        conf.master.hostname = "localhost".to_string();
        conf.master.rpc_port = 8995;
        conf.worker.web_port = 9001;
        conf.client.master_addrs = vec![InetAddr::new("cv-master-0", 8995)];
        conf
    }

    fn sample_worker(
        worker_id: u32,
        hostname: &str,
        ip_addr: &str,
        rpc_port: u32,
        web_port: u32,
    ) -> WorkerInfo {
        WorkerInfo::new(
            WorkerAddress {
                worker_id,
                hostname: hostname.to_string(),
                ip_addr: ip_addr.to_string(),
                rpc_port,
                web_port,
            },
            WorkerInfo::default_weight(),
        )
    }

    #[test]
    fn builds_master_target_for_host_port_mapped_cluster() {
        let mut conf = ClusterConf::default();
        conf.master.rpc_port = 8995;
        conf.master.web_port = 9000;
        conf.client.master_addrs = vec![
            InetAddr::new("cv-master-1", 18995),
            InetAddr::new("cv-master-2", 28995),
            InetAddr::new("cv-master-3", 38995),
        ];
        conf.client.master_web_addrs = vec![
            InetAddr::new("cv-master-1", 19000),
            InetAddr::new("cv-master-2", 29000),
            InetAddr::new("cv-master-3", 39000),
        ];
        let target = build_master_metrics_target(&conf, "cv-master-2:8995");
        assert_eq!(target.component, "master");
        assert_eq!(target.instance, ACTIVE_MASTER_INSTANCE);
        assert_eq!(
            target.url,
            master_metrics_scrape_url(&conf, "cv-master-2:8995")
        );
        assert_eq!(target.id, "master:active-master");
        assert_eq!(target.active_master.as_deref(), Some("cv-master-2:8995"));
    }

    #[test]
    fn builds_master_target_from_configured_master_when_active_is_loopback() {
        let conf = test_conf();
        let target = build_master_metrics_target(&conf, "127.0.0.1:8995");
        assert_eq!(target.component, "master");
        assert_eq!(target.instance, ACTIVE_MASTER_INSTANCE);
        assert_eq!(target.url, "http://cv-master-0:9010/metrics");
        assert_eq!(target.id, "master:active-master");
    }

    #[test]
    fn builds_master_target_from_active_master_in_ha() {
        let mut conf = test_conf();
        conf.master.hostname = "cv-master-0".to_string();
        let target = build_master_metrics_target(&conf, "cv-master-1:8995");
        assert_eq!(target.instance, ACTIVE_MASTER_INSTANCE);
        assert_eq!(target.url, "http://cv-master-1:9010/metrics");
    }

    #[test]
    fn worker_target_uses_web_port_not_rpc_port() {
        let conf = test_conf();
        let worker = sample_worker(1, "cv-worker-0", "10.0.0.1", 8996, 9008);
        let discovery = build_worker_metrics_target(&worker, "live", &conf);
        assert_eq!(discovery.target.instance, "cv-worker-0:9008");
        assert_eq!(
            discovery.target.url,
            crate::util::addr::worker_metrics_scrape_url(&worker.address, &conf)
        );
        assert!(!discovery.target.url.contains(":8996"));
    }

    #[test]
    fn worker_target_prefers_reported_web_port_over_conf() {
        let conf = test_conf();
        let worker = sample_worker(2, "w2", "10.0.0.2", 8996, 9010);
        let discovery = build_worker_metrics_target(&worker, "live", &conf);
        assert_eq!(discovery.target.instance, "w2:9010");
    }

    #[test]
    fn worker_target_falls_back_to_conf_web_port_when_reported_zero() {
        let conf = test_conf();
        let worker = sample_worker(3, "w3", "10.0.0.3", 8996, 0);
        let discovery = build_worker_metrics_target(&worker, "live", &conf);
        assert_eq!(discovery.target.instance, "w3:9001");
    }

    #[test]
    fn worker_target_falls_back_to_ip_when_hostname_loopback() {
        let conf = test_conf();
        let worker = sample_worker(4, "localhost", "10.0.0.4", 8996, 9001);
        let discovery = build_worker_metrics_target(&worker, "live", &conf);
        assert_eq!(discovery.target.instance, "10.0.0.4:9001");
    }

    #[test]
    fn worker_target_preserves_worker_state() {
        let conf = test_conf();
        let worker = sample_worker(5, "w5", "10.0.0.5", 8996, 9001);
        for state in ["live", "blacklist", "decommission", "lost"] {
            let discovery = build_worker_metrics_target(&worker, state, &conf);
            assert_eq!(discovery.target.worker_state.as_deref(), Some(state));
            assert_eq!(discovery.target.worker_id, Some(5));
        }
    }

    #[test]
    fn invalid_worker_does_not_abort_discovery_batch() {
        let conf = test_conf();
        let valid = sample_worker(6, "w6", "10.0.0.6", 8996, 9001);
        let invalid = sample_worker(7, "", "", 8996, 9001);
        let workers: Vec<_> = [valid, invalid]
            .iter()
            .map(|worker| build_worker_metrics_target(worker, "live", &conf))
            .collect();
        assert_eq!(workers.len(), 2);
        assert_eq!(workers[0].target.instance, "w6:9001");
        assert!(workers[1].warning.is_some());
        assert_eq!(workers[1].target.worker_id, Some(7));
    }

    #[test]
    fn discovers_all_worker_states_from_master_info() {
        let conf = test_conf();
        let info = MasterInfo {
            live_workers: vec![sample_worker(1, "w1", "10.0.0.1", 8996, 9001)],
            blacklist_workers: vec![sample_worker(2, "w2", "10.0.0.2", 8996, 9001)],
            decommission_workers: vec![sample_worker(3, "w3", "10.0.0.3", 8996, 9001)],
            lost_workers: vec![sample_worker(4, "w4", "10.0.0.4", 8996, 9001)],
            ..Default::default()
        };
        let workers = discover_workers_from_master_info(&info, &conf);
        assert_eq!(workers.len(), 4);
        let states: Vec<_> = workers
            .iter()
            .filter_map(|worker| worker.target.worker_state.clone())
            .collect();
        assert!(states.contains(&"live".to_string()));
        assert!(states.contains(&"blacklist".to_string()));
        assert!(states.contains(&"decommission".to_string()));
        assert!(states.contains(&"lost".to_string()));
    }

    #[test]
    fn discovers_no_fuse_targets_when_disabled_by_default() {
        let conf = test_conf();
        assert!(!conf.web.observability.fuse.enabled);
        assert!(discover_fuse_targets(&conf).is_empty());
    }

    #[test]
    fn discovers_fuse_targets_from_default_local_endpoint() {
        let mut conf = test_conf();
        conf.web.observability.fuse.enabled = true;
        let targets = discover_fuse_targets(&conf);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].component, "fuse");
        assert_eq!(
            targets[0].instance,
            format!("127.0.0.1:{}", conf.fuse.web_port)
        );
        assert_eq!(
            targets[0].url,
            fuse_metrics_scrape_url(&format!("127.0.0.1:{}", conf.fuse.web_port))
        );
    }

    #[test]
    fn discovers_configured_fuse_targets() {
        let mut conf = test_conf();
        conf.web.observability.fuse.enabled = true;
        conf.web.observability.fuse.targets = vec!["10.0.0.9:9002".to_string()];
        let targets = discover_fuse_targets(&conf);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].instance, "10.0.0.9:9002");
    }

    #[test]
    fn discovers_no_transfer_target_when_disabled_by_default() {
        let conf = test_conf();
        assert!(!conf.transfer.enabled);
        assert!(discover_transfer_targets(&conf).is_empty());
    }

    #[test]
    fn discovers_transfer_target_from_config() {
        let mut conf = test_conf();
        conf.transfer.enabled = true;
        conf.transfer.hostname = "cv-transfer-0".to_string();
        conf.transfer.web_port = 9011;
        let targets = discover_transfer_targets(&conf);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].component, "transfer");
        assert_eq!(targets[0].instance, "cv-transfer-0:9011");
        assert_eq!(targets[0].url, "http://cv-transfer-0:9011/metrics");
        assert_eq!(targets[0].id, "transfer:cv-transfer-0:9011");
    }

    #[test]
    fn discovers_transfer_target_preserves_ip_literal_host() {
        let mut conf = test_conf();
        conf.transfer.enabled = true;
        conf.transfer.hostname = "10.0.0.9".to_string();
        conf.transfer.web_port = 9011;
        let targets = discover_transfer_targets(&conf);
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].instance, "10.0.0.9:9011");
        assert_eq!(targets[0].url, "http://10.0.0.9:9011/metrics");
    }
}
