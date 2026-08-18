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

use std::env;
use std::net::{IpAddr, ToSocketAddrs};
use std::process::Command;

use curvine_config::ClusterConf;
use curvine_model::WorkerAddress;
use curvine_net::net::{InetAddr, NetUtils};

pub fn is_loopback_hostname(hostname: &str) -> bool {
    matches!(
        hostname.trim().to_ascii_lowercase().as_str(),
        "localhost" | "127.0.0.1" | "::1" | "0.0.0.0"
    )
}

pub fn configured_master_addrs(conf: &ClusterConf) -> Vec<InetAddr> {
    if conf.client.master_addrs.is_empty() {
        conf.journal
            .journal_addrs
            .iter()
            .map(|peer| InetAddr::new(peer.hostname.clone(), conf.master.rpc_port))
            .collect()
    } else {
        conf.client.master_addrs.clone()
    }
}

/// UI-oriented hostname resolution for Overview/Workers display.
pub fn resolve_display_hostname(conf: &ClusterConf, active_master: &str) -> String {
    if !is_loopback_hostname(&conf.master.hostname) {
        return conf.master.hostname.clone();
    }
    if let Ok(addr) = InetAddr::from_str(active_master) {
        if !is_loopback_hostname(&addr.hostname) {
            return addr.hostname;
        }
    }
    let local = NetUtils::local_hostname();
    if !is_loopback_hostname(&local) {
        return local;
    }
    if let Ok(host) = env::var("HOSTNAME") {
        let host = host.trim().to_string();
        if !host.is_empty() && !is_loopback_hostname(&host) {
            return host;
        }
    }
    for peer in &conf.journal.journal_addrs {
        if !is_loopback_hostname(&peer.hostname) {
            return peer.hostname.clone();
        }
    }
    for addr in configured_master_addrs(conf) {
        if !is_loopback_hostname(&addr.hostname) {
            return addr.hostname.clone();
        }
    }
    conf.master.hostname.clone()
}

pub fn display_inet_addr_with_hint(addr: &InetAddr, hint_hostname: &str) -> String {
    let hostname = if !is_loopback_hostname(&addr.hostname) {
        addr.hostname.clone()
    } else if !is_loopback_hostname(hint_hostname) {
        hint_hostname.to_string()
    } else {
        NetUtils::local_hostname()
    };
    format!("{hostname}:{}", addr.port)
}

pub fn display_master_endpoint(conf: &ClusterConf, active_master: &str) -> String {
    display_inet_addr_with_hint(
        &conf.master_addr(),
        &resolve_display_hostname(conf, active_master),
    )
}

pub fn display_addr_text_for_cluster(
    addr: &str,
    conf: &ClusterConf,
    active_master: &str,
) -> String {
    let hint = resolve_display_hostname(conf, active_master);
    InetAddr::from_str(addr)
        .map(|parsed| display_inet_addr_with_hint(&parsed, &hint))
        .unwrap_or_else(|_| addr.to_string())
}

pub fn is_ip_literal(value: &str) -> bool {
    value.parse::<IpAddr>().is_ok()
}

/// Stable host identity across DNS suffix drift (e.g. `master-2` vs `master-2.svc.cluster.local`).
pub fn master_host_short_name(hostname: &str) -> &str {
    hostname.split('.').next().unwrap_or(hostname)
}

/// Host label for master metrics scrape URLs and instance labels.
/// DNS names are shortened to the first label; IP literals are kept intact.
fn master_metrics_host_label(hostname: &str) -> &str {
    if is_ip_literal(hostname) {
        hostname
    } else {
        master_host_short_name(hostname)
    }
}

/// Whether two host strings refer to the same cluster node for master/journal mapping.
pub fn hosts_match_for_cluster_mapping(left: &str, right: &str) -> bool {
    let left = left.trim();
    let right = right.trim();
    if left == right {
        return true;
    }
    // IP literals must match exactly; `10.41.54.240` and `10.41.54.239` both shorten to `10`.
    if is_ip_literal(left) || is_ip_literal(right) {
        return false;
    }
    master_host_short_name(left) == master_host_short_name(right)
}

pub fn configured_master_for_active(conf: &ClusterConf, active_master: &str) -> Option<InetAddr> {
    let active = active_master.trim();
    if active.is_empty() {
        return configured_master_addrs(conf).into_iter().next();
    }
    let Ok(active_addr) = InetAddr::from_str(active) else {
        return None;
    };
    configured_master_addrs(conf)
        .into_iter()
        .find(|configured| {
            configured.to_string() == active
                || hosts_match_for_cluster_mapping(&configured.hostname, &active_addr.hostname)
        })
}

pub fn configured_master_web_addrs(conf: &ClusterConf) -> &[InetAddr] {
    &conf.client.master_web_addrs
}

pub fn configured_master_web_for_active(
    conf: &ClusterConf,
    active_master: &str,
) -> Option<InetAddr> {
    if conf.client.master_web_addrs.is_empty() {
        return None;
    }
    let active = active_master.trim();
    if active.is_empty() {
        return conf.client.master_web_addrs.first().cloned();
    }
    let Ok(active_addr) = InetAddr::from_str(active) else {
        return None;
    };
    if let Some(matched) = conf.client.master_web_addrs.iter().find(|configured| {
        configured.to_string() == active
            || hosts_match_for_cluster_mapping(&configured.hostname, &active_addr.hostname)
    }) {
        return Some(matched.clone());
    }
    // Align by index with `master_addrs` when scrape host differs (LB / NodePort / host publish).
    let master_addrs = configured_master_addrs(conf);
    let idx = master_addrs.iter().position(|configured| {
        configured.to_string() == active
            || hosts_match_for_cluster_mapping(&configured.hostname, &active_addr.hostname)
    })?;
    conf.client.master_web_addrs.get(idx).cloned()
}

/// Web/metrics port for the active master from explicit `client.master_web_addrs`,
/// otherwise `master.web_port`.
pub fn master_scrape_web_port(conf: &ClusterConf, active_master: &str) -> u16 {
    configured_master_web_for_active(conf, active_master)
        .map(|addr| addr.port)
        .unwrap_or(conf.master.web_port)
}

fn master_metrics_display_hostname(conf: &ClusterConf, active_master: &str) -> String {
    let active = active_master.trim();
    if let Ok(addr) = InetAddr::from_str(active) {
        if !active.is_empty() && !is_loopback_hostname(&addr.hostname) {
            return master_metrics_host_label(&addr.hostname).to_string();
        }
    }
    if let Some(configured) = configured_master_for_active(conf, active) {
        return master_metrics_host_label(&configured.hostname).to_string();
    }
    for addr in configured_master_addrs(conf) {
        if !is_loopback_hostname(&addr.hostname) {
            return master_metrics_host_label(&addr.hostname).to_string();
        }
    }
    if let Ok(addr) = InetAddr::from_str(active) {
        if !active.is_empty() {
            return addr.hostname;
        }
    }
    if !conf.master.hostname.is_empty() {
        return conf.master.hostname.clone();
    }
    configured_master_addrs(conf)
        .into_iter()
        .next()
        .map(|addr| addr.hostname)
        .unwrap_or_else(|| conf.master.hostname.clone())
}

fn is_loopback_ip(value: &str) -> bool {
    value
        .parse::<IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

pub fn resolve_display_ip(hostname: &str, fallback: &str) -> String {
    let fallback = fallback.trim();
    if !fallback.is_empty() && is_ip_literal(fallback) {
        return fallback.to_string();
    }
    let hostname = hostname.trim();
    if hostname.is_empty() {
        return fallback.to_string();
    }
    (hostname, 0)
        .to_socket_addrs()
        .ok()
        .and_then(|mut addrs| {
            addrs
                .find(|addr| addr.ip().is_ipv4())
                .or_else(|| addrs.next())
        })
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|| fallback.to_string())
}

fn local_non_loopback_ip() -> Option<String> {
    let host = NetUtils::local_hostname();
    if is_loopback_hostname(&host) {
        return None;
    }
    let ip = resolve_display_ip(&host, "");
    if is_loopback_ip(&ip) {
        None
    } else {
        Some(ip)
    }
}

/// Routable scrape host for master `/metrics` from the `curvine-web` process.
///
/// When `client.master_web_addrs` matches the active master, uses the configured
/// scrape hostname (LB / NodePort / host-published address). Otherwise falls back to
/// the active or configured RPC master hostname. When that name resolves to loopback
/// (common in docker demo `/etc/hosts`) but explicit web addrs are set, prefer this
/// machine's routable IP so scrapes hit docker port mappings.
pub fn resolve_master_scrape_hostname(conf: &ClusterConf, active_master: &str) -> String {
    if let Some(endpoint) = configured_master_web_for_active(conf, active_master) {
        let configured_host = endpoint.hostname.trim();
        if !configured_host.is_empty() && !is_loopback_hostname(configured_host) {
            return configured_host.to_string();
        }
        let resolved = resolve_display_ip(configured_host, "");
        if is_loopback_ip(&resolved) {
            if let Some(local_ip) = local_non_loopback_ip() {
                return local_ip;
            }
        }
        if !configured_host.is_empty() {
            return configured_host.to_string();
        }
    }
    let logical = master_metrics_display_hostname(conf, active_master);
    let resolved = resolve_display_ip(&logical, "");
    if is_loopback_ip(&resolved) && configured_master_web_for_active(conf, active_master).is_some()
    {
        if let Some(local_ip) = local_non_loopback_ip() {
            return local_ip;
        }
    }
    logical
}

/// Logical `instance` label for master metrics (scrape hostname + metrics port).
pub fn master_metrics_instance(conf: &ClusterConf, active_master: &str) -> String {
    if let Some(endpoint) = configured_master_web_for_active(conf, active_master) {
        return format!("{}:{}", endpoint.hostname, endpoint.port);
    }
    format!(
        "{}:{}",
        master_metrics_display_hostname(conf, active_master),
        conf.master.web_port
    )
}

/// HTTP scrape URL for master `/metrics`.
pub fn master_metrics_scrape_url(conf: &ClusterConf, active_master: &str) -> String {
    format!(
        "http://{}:{}/metrics",
        resolve_master_scrape_hostname(conf, active_master),
        master_scrape_web_port(conf, active_master)
    )
}

/// HTTP URL for master overview / lightweight runtime probes.
pub fn master_overview_scrape_url(conf: &ClusterConf, master_addr: &str) -> String {
    format!(
        "http://{}:{}/api/overview",
        resolve_master_scrape_hostname(conf, master_addr),
        master_scrape_web_port(conf, master_addr)
    )
}

/// HTTP URL for master `/api/v1/master/runtime` (version + start_time).
pub fn master_runtime_scrape_url(conf: &ClusterConf, master_addr: &str) -> String {
    format!(
        "http://{}:{}/api/v1/master/runtime",
        resolve_master_scrape_hostname(conf, master_addr),
        master_scrape_web_port(conf, master_addr)
    )
}

fn docker_container_ip(hostname: &str) -> Option<String> {
    let name = hostname.trim();
    if name.is_empty() {
        return None;
    }
    let output = Command::new("docker")
        .args([
            "inspect",
            "-f",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            name,
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let ip = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if is_ip_literal(&ip) && !is_loopback_ip(&ip) {
        Some(ip)
    } else {
        None
    }
}

/// Scrape-oriented hostname for worker `/metrics`.
///
/// Prefer the worker-reported routable IP when present. In docker/k8s demos the
/// logical hostname may be pinned in the host `/etc/hosts` to a stale container IP
/// after restarts, while the worker heartbeat still carries the current address.
pub fn resolve_worker_scrape_hostname(addr: &WorkerAddress) -> String {
    let reported_ip = addr.ip_addr.trim();
    if !reported_ip.is_empty() && is_ip_literal(reported_ip) && !is_loopback_ip(reported_ip) {
        return reported_ip.to_string();
    }
    let hostname = addr.hostname.trim();
    if !hostname.is_empty() && !is_loopback_hostname(hostname) {
        if let Some(container_ip) = docker_container_ip(hostname) {
            let resolved = resolve_display_ip(hostname, reported_ip);
            if is_ip_literal(&resolved)
                && resolved != container_ip
                && !is_loopback_ip(&container_ip)
            {
                return container_ip;
            }
        }
        return hostname.to_string();
    }
    let ip = resolve_display_ip(hostname, reported_ip);
    if !ip.is_empty() && !is_loopback_ip(&ip) {
        return ip;
    }
    if !hostname.is_empty() {
        return hostname.to_string();
    }
    ip
}

/// UI `instance` label host for worker metrics (logical hostname when available).
pub fn worker_metrics_display_hostname(addr: &WorkerAddress) -> String {
    let hostname = addr.hostname.trim();
    if !hostname.is_empty() && !is_loopback_hostname(hostname) {
        return hostname.to_string();
    }
    let reported_ip = addr.ip_addr.trim();
    if !reported_ip.is_empty() && is_ip_literal(reported_ip) && !is_loopback_ip(reported_ip) {
        return reported_ip.to_string();
    }
    let ip = resolve_display_ip(hostname, reported_ip);
    if !ip.is_empty() && !is_loopback_ip(&ip) {
        return ip;
    }
    if !hostname.is_empty() {
        return hostname.to_string();
    }
    reported_ip.to_string()
}

/// Worker metrics port: worker-reported `web_port` wins over `conf.worker.web_port`.
pub fn worker_web_port(addr: &WorkerAddress, conf: &ClusterConf) -> u16 {
    if addr.web_port > 0 {
        addr.web_port as u16
    } else {
        conf.worker.web_port
    }
}

/// Metrics scrape instance for worker: logical host + worker web port.
pub fn worker_metrics_instance(addr: &WorkerAddress, conf: &ClusterConf) -> String {
    format!(
        "{}:{}",
        worker_metrics_display_hostname(addr),
        worker_web_port(addr, conf)
    )
}

/// HTTP scrape URL for worker `/metrics`.
pub fn worker_metrics_scrape_url(addr: &WorkerAddress, conf: &ClusterConf) -> String {
    format!(
        "http://{}:{}/metrics",
        resolve_worker_scrape_hostname(addr),
        worker_web_port(addr, conf)
    )
}

/// Instance label for a FUSE metrics endpoint (`host:port`).
pub fn fuse_metrics_instance(endpoint: &str) -> String {
    let trimmed = endpoint.trim();
    if let Some(rest) = trimmed.strip_prefix("http://") {
        let without_path = rest.split('/').next().unwrap_or(rest);
        return without_path.to_string();
    }
    if let Some(rest) = trimmed.strip_prefix("https://") {
        let without_path = rest.split('/').next().unwrap_or(rest);
        return without_path.to_string();
    }
    trimmed.to_string()
}

/// HTTP scrape URL for FUSE `/metrics`.
pub fn fuse_metrics_scrape_url(endpoint: &str) -> String {
    let trimmed = endpoint.trim();
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        if trimmed.ends_with("/metrics") {
            return trimmed.to_string();
        }
        return format!("{}/metrics", trimmed.trim_end_matches('/'));
    }
    format!("http://{}/metrics", trimmed.trim_start_matches('/'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_net::net::InetAddr;

    fn test_conf() -> ClusterConf {
        let mut conf = ClusterConf::default();
        conf.master.hostname = "localhost".to_string();
        conf.master.web_port = 9000;
        conf.master.rpc_port = 8995;
        conf.client.master_addrs = vec![InetAddr::new("cv-master-0", 8995)];
        conf
    }

    #[test]
    fn master_metrics_uses_master_web_port_not_rpc_port() {
        let mut conf = ClusterConf::default();
        conf.master.hostname = "cv-master-0".to_string();
        let instance = master_metrics_instance(&conf, "cv-master-1:8995");
        assert_eq!(instance, "cv-master-1:9010");
    }

    #[test]
    fn hosts_match_for_cluster_mapping_distinguishes_ip_literals() {
        assert!(!hosts_match_for_cluster_mapping(
            "10.41.54.240",
            "10.41.54.239"
        ));
        assert!(hosts_match_for_cluster_mapping(
            "10.41.54.240",
            "10.41.54.240"
        ));
        assert!(hosts_match_for_cluster_mapping(
            "cv-curvine-master-1.cv-curvine-master.default.svc.cluster.local",
            "cv-curvine-master-1.cv-curvine-master.default.svc.common1ack.oppo.local"
        ));
    }

    #[test]
    fn master_scrape_hostname_preserves_ip_literal_active_master() {
        let mut conf = ClusterConf::default();
        conf.master.web_port = 9010;
        conf.client.master_addrs = vec![
            InetAddr::new("10.41.54.240", 8995),
            InetAddr::new("10.41.54.239", 8995),
            InetAddr::new("10.41.54.242", 8995),
        ];
        let host = resolve_master_scrape_hostname(&conf, "10.41.54.240:8995");
        assert_eq!(host, "10.41.54.240");
        assert_eq!(
            master_metrics_scrape_url(&conf, "10.41.54.240:8995"),
            "http://10.41.54.240:9010/metrics"
        );
    }

    #[test]
    fn master_scrape_hostname_prefers_active_master_over_conf_hostname() {
        let mut conf = ClusterConf::default();
        conf.master.hostname = "cv-master-0".to_string();
        conf.client.master_addrs = vec![
            InetAddr::new("cv-master-0", 8995),
            InetAddr::new("cv-master-1", 8995),
        ];
        let host = resolve_master_scrape_hostname(&conf, "cv-master-1:8995");
        assert_eq!(host, "cv-master-1");
    }

    #[test]
    fn master_scrape_hostname_falls_back_to_configured_master_when_active_is_loopback() {
        let conf = test_conf();
        let host = resolve_master_scrape_hostname(&conf, "127.0.0.1:8995");
        assert_eq!(host, "cv-master-0");
    }

    #[test]
    fn master_scrape_hostname_falls_back_to_configured_master_when_active_missing() {
        let conf = test_conf();
        let host = resolve_master_scrape_hostname(&conf, "");
        assert_eq!(host, "cv-master-0");
    }

    #[test]
    fn resolve_display_hostname_uses_configured_master_hostname_when_not_loopback() {
        let mut conf = ClusterConf::default();
        conf.master.hostname = "cv-master-0".to_string();
        let host = resolve_display_hostname(&conf, "127.0.0.1:8995");
        assert_eq!(host, "cv-master-0");
    }

    #[test]
    fn resolve_display_hostname_uses_active_master_host_when_routable() {
        let conf = test_conf();
        let host = resolve_display_hostname(&conf, "real-master.example.com:8995");
        assert_eq!(host, "real-master.example.com");
    }

    #[test]
    fn worker_metrics_uses_web_port_not_rpc_port() {
        let mut conf = ClusterConf::default();
        conf.worker.web_port = 9001;
        let addr = WorkerAddress {
            worker_id: 1,
            hostname: "cv-worker-0".to_string(),
            ip_addr: "10.0.0.1".to_string(),
            rpc_port: 8996,
            web_port: 9008,
        };
        assert_eq!(worker_web_port(&addr, &conf), 9008);
        assert_eq!(worker_metrics_instance(&addr, &conf), "cv-worker-0:9008");
    }

    #[test]
    fn worker_web_port_prefers_worker_reported_port_over_conf() {
        let mut conf = ClusterConf::default();
        conf.worker.web_port = 9001;
        let addr = WorkerAddress {
            worker_id: 2,
            hostname: "w2".to_string(),
            ip_addr: "10.0.0.2".to_string(),
            rpc_port: 8996,
            web_port: 9010,
        };
        assert_eq!(worker_web_port(&addr, &conf), 9010);
    }

    #[test]
    fn worker_web_port_falls_back_to_conf_when_reported_zero() {
        let mut conf = ClusterConf::default();
        conf.worker.web_port = 9001;
        let addr = WorkerAddress {
            worker_id: 3,
            hostname: "w3".to_string(),
            ip_addr: "10.0.0.3".to_string(),
            rpc_port: 8996,
            web_port: 0,
        };
        assert_eq!(worker_web_port(&addr, &conf), 9001);
        assert_eq!(worker_metrics_instance(&addr, &conf), "w3:9001");
    }

    fn host_mapped_master_conf() -> ClusterConf {
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
        conf
    }

    #[test]
    fn master_metrics_uses_explicit_master_web_addrs() {
        let conf = host_mapped_master_conf();
        assert_eq!(
            master_metrics_instance(&conf, "cv-master-2:8995"),
            "cv-master-2:29000"
        );
        let url = master_metrics_scrape_url(&conf, "cv-master-2:8995");
        assert_eq!(url, "http://cv-master-2:29000/metrics");
        assert_eq!(
            master_runtime_scrape_url(&conf, "cv-master-2:8995"),
            "http://cv-master-2:29000/api/v1/master/runtime"
        );
        assert_eq!(
            master_overview_scrape_url(&conf, "cv-master-2:8995"),
            "http://cv-master-2:29000/api/overview"
        );
        assert!(!url.contains(":9000/metrics"));
        assert!(!url.contains("127.0.0.1"));
    }

    #[test]
    fn master_metrics_uses_configured_web_host_for_lb_mapping() {
        let mut conf = ClusterConf::default();
        conf.client.master_addrs = vec![InetAddr::new("cv-master-0", 8995)];
        conf.client.master_web_addrs = vec![InetAddr::new("host-machine", 19010)];
        assert_eq!(
            master_metrics_instance(&conf, "cv-master-0:8995"),
            "host-machine:19010"
        );
        assert_eq!(
            master_metrics_scrape_url(&conf, "cv-master-0:8995"),
            "http://host-machine:19010/metrics"
        );
    }

    #[test]
    fn master_metrics_matches_dns_suffix_drift_via_short_hostname() {
        let mut conf = host_mapped_master_conf();
        conf.client.master_addrs = vec![InetAddr::new("cv-master-2.svc.cluster.local", 28995)];
        conf.client.master_web_addrs = vec![InetAddr::new("cv-master-2.svc.cluster.local", 29000)];
        assert_eq!(master_scrape_web_port(&conf, "cv-master-2:8995"), 29000);
        let url = master_metrics_scrape_url(&conf, "cv-master-2:8995");
        assert!(url.contains(":29000/metrics"));
        assert!(!url.contains("127.0.0.1"));
    }

    #[test]
    fn master_scrape_web_port_falls_back_to_conf_when_web_addrs_missing() {
        let mut conf = host_mapped_master_conf();
        conf.client.master_web_addrs.clear();
        conf.client.master_addrs = vec![InetAddr::new("bad-master", 8000)];
        conf.master.web_port = 9000;
        assert_eq!(master_scrape_web_port(&conf, "bad-master:8995"), 9000);
    }

    #[test]
    fn worker_scrape_hostname_falls_back_to_ip_when_hostname_loopback() {
        let addr = WorkerAddress {
            worker_id: 4,
            hostname: "localhost".to_string(),
            ip_addr: "10.0.0.4".to_string(),
            rpc_port: 8996,
            web_port: 9001,
        };
        assert_eq!(resolve_worker_scrape_hostname(&addr), "10.0.0.4");
        assert_eq!(
            worker_metrics_instance(&addr, &ClusterConf::default()),
            "10.0.0.4:9001"
        );
    }

    #[test]
    fn worker_scrape_url_prefers_reported_ip_over_hostname() {
        let mut conf = ClusterConf::default();
        conf.worker.web_port = 9001;
        let addr = WorkerAddress {
            worker_id: 8,
            hostname: "curvine-demo-worker-1".to_string(),
            ip_addr: "172.18.0.7".to_string(),
            rpc_port: 8996,
            web_port: 9001,
        };
        assert_eq!(
            worker_metrics_instance(&addr, &conf),
            "curvine-demo-worker-1:9001"
        );
        assert_eq!(
            worker_metrics_scrape_url(&addr, &conf),
            "http://172.18.0.7:9001/metrics"
        );
    }
}
