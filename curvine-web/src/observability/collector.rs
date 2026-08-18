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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::stream::{self, StreamExt};
use log::warn;
use tokio::sync::Semaphore;
use tokio::time::sleep;

use super::builtin_provider::BuiltinProvider;
use super::discovery::{discover_targets, DiscoveryResult};
use super::model::{MetricSample, MetricTarget, TargetHealth, ACTIVE_MASTER_INSTANCE};
use super::provider::TargetRegistry;
use super::scraper::MetricsScraper;
use curvine_model::FilesystemInfo;

pub async fn run_collector(provider: Arc<BuiltinProvider>) {
    loop {
        if provider.collector_running() {
            collect_once(provider.clone()).await;
        }
        sleep(Duration::from_millis(provider.scrape_interval_ms())).await;
    }
}

pub async fn run_discovery(provider: Arc<BuiltinProvider>) {
    loop {
        if provider.discovery_running() {
            discover_once(&provider).await;
        }
        sleep(Duration::from_millis(provider.scrape_interval_ms())).await;
    }
}

struct PreparedDiscovery {
    scrape_targets: Vec<MetricTarget>,
    master_warning: Option<String>,
    worker_warnings: std::collections::HashMap<String, String>,
    discovery: DiscoveryResult,
}

async fn prepare_discovery(provider: &BuiltinProvider, mutate_tsdb: bool) -> PreparedDiscovery {
    let discovery = discover_targets(provider.conf(), provider.unified_fs()).await;
    let master_warning = discovery.master.error.clone();
    let discovery_failed = master_warning.is_some();
    if let Some(ref err) = master_warning {
        warn!("observability master discovery failed: {err}");
    }

    let mut scrape_targets = vec![discovery.master.target.clone()];
    let mut worker_warnings = std::collections::HashMap::new();
    for worker in &discovery.workers {
        if let Some(ref warning) = worker.warning {
            worker_warnings.insert(worker.target.id.clone(), warning.clone());
        }
        scrape_targets.push(worker.target.clone());
    }
    scrape_targets.extend(discovery.fuses.iter().cloned());
    scrape_targets.extend(discovery.transfers.iter().cloned());

    if discovery_failed {
        merge_stale_worker_targets(&mut scrape_targets, &provider.targets());
        apply_stale_worker_warnings(&mut worker_warnings, &scrape_targets, &master_warning);
    }

    if !discovery_failed && mutate_tsdb {
        let discovered_ids: Vec<String> = scrape_targets
            .iter()
            .map(|target| target.id.clone())
            .collect();
        provider.retain_targets(&discovered_ids);
        provider
            .tsdb()
            .purge_legacy_master_instances(ACTIVE_MASTER_INSTANCE);
    } else if !discovery_failed {
        let discovered_ids: Vec<String> = scrape_targets
            .iter()
            .map(|target| target.id.clone())
            .collect();
        provider.retain_targets(&discovered_ids);
    }

    PreparedDiscovery {
        scrape_targets,
        master_warning,
        worker_warnings,
        discovery,
    }
}

pub async fn discover_once(provider: &BuiltinProvider) {
    let timestamp_ms = now_ms();
    provider.set_last_discovery_ms(timestamp_ms);
    let prepared = prepare_discovery(provider, false).await;
    for target in prepared.scrape_targets {
        let last_warning = if target.component == "master" {
            prepared.master_warning.clone().unwrap_or_default()
        } else {
            prepared
                .worker_warnings
                .get(&target.id)
                .cloned()
                .unwrap_or_default()
        };
        let previous = provider.get_target(&target.id);
        provider.update_target(TargetHealth {
            id: target.id,
            component: target.component,
            instance: target.instance,
            url: target.url,
            worker_id: target.worker_id,
            worker_state: target.worker_state,
            active_master: target.active_master,
            up: previous.as_ref().map(|health| health.up).unwrap_or(false),
            last_scrape_ms: previous.as_ref().and_then(|health| health.last_scrape_ms),
            last_success_ms: previous.as_ref().and_then(|health| health.last_success_ms),
            last_scrape_duration_ms: previous
                .as_ref()
                .and_then(|health| health.last_scrape_duration_ms),
            last_error: previous
                .as_ref()
                .map(|health| health.last_error.clone())
                .unwrap_or_default(),
            last_warning: if last_warning.is_empty() {
                "Prometheus mode; scrape health from Prometheus".to_string()
            } else {
                last_warning
            },
            sample_count: previous
                .as_ref()
                .map(|health| health.sample_count)
                .unwrap_or(0),
            parse_error_count: previous
                .as_ref()
                .map(|health| health.parse_error_count)
                .unwrap_or(0),
        });
    }
}

pub async fn collect_once(provider: Arc<BuiltinProvider>) {
    // Discovery, target mutation and TSDB insertion form one collection
    // transaction. Serialize manual resumes, mode switches and the background
    // loop so one target set is never scraped twice concurrently.
    let _collect_guard = provider.collect_lock().lock().await;
    let timestamp_ms = now_ms();
    provider.set_last_discovery_ms(timestamp_ms);

    let prepared = prepare_discovery(&provider, true).await;
    let master_warning = prepared.master_warning.clone();
    let worker_warnings = prepared.worker_warnings.clone();
    let scrape_targets = prepared.scrape_targets;
    let discovery = prepared.discovery;

    let max_concurrent_scrapes = provider.max_concurrent_scrapes();
    let semaphore = Arc::new(Semaphore::new(max_concurrent_scrapes));
    let scraper = provider.scraper().clone();
    stream::iter(scrape_targets.into_iter().map(|target| {
        let provider = provider.clone();
        let semaphore = semaphore.clone();
        let scraper = scraper.clone();
        let master_warning = master_warning.clone();
        let worker_warnings = worker_warnings.clone();
        async move {
            let _permit = semaphore.acquire().await.expect("semaphore closed");
            scrape_one_target(
                &provider,
                &scraper,
                target,
                timestamp_ms,
                master_warning,
                worker_warnings,
            )
            .await;
        }
    }))
    .buffer_unordered(max_concurrent_scrapes)
    .collect::<Vec<_>>()
    .await;

    maybe_ingest_master_info_fallback(&provider, &discovery, timestamp_ms).await;

    provider.set_last_collect_ms(timestamp_ms);
}

async fn scrape_one_target(
    provider: &BuiltinProvider,
    scraper: &MetricsScraper,
    target: MetricTarget,
    timestamp_ms: i64,
    master_warning: Option<String>,
    worker_warnings: std::collections::HashMap<String, String>,
) {
    let mut result = scraper.scrape(&target).await;
    let sample_count = result.samples.len();
    let scrape_succeeded = result.error.is_none() && sample_count > 0;
    if scrape_succeeded {
        inject_target_labels(&mut result.samples, &target);
        provider.tsdb().insert(timestamp_ms, result.samples);
    }

    let scrape_error = result.error.unwrap_or_default();
    let last_warning = if target.component == "master" {
        master_warning.unwrap_or_default()
    } else {
        worker_warnings.get(&target.id).cloned().unwrap_or_default()
    };
    let previous_success = provider
        .get_target(&target.id)
        .and_then(|health| health.last_success_ms);
    let last_success_ms = if scrape_succeeded {
        Some(timestamp_ms)
    } else {
        previous_success
    };

    provider.update_target(TargetHealth {
        id: target.id,
        component: target.component,
        instance: target.instance,
        url: target.url,
        worker_id: target.worker_id,
        worker_state: target.worker_state,
        active_master: target.active_master,
        up: scrape_succeeded,
        last_scrape_ms: Some(timestamp_ms),
        last_success_ms,
        last_scrape_duration_ms: Some(result.duration_ms),
        last_error: scrape_error,
        last_warning,
        sample_count,
        parse_error_count: result.parse_error_count,
    });
}

const MASTER_INFO_FALLBACK_NOTE: &str =
    "master /metrics scrape failed; ingesting gauge metrics from GetMasterInfo";

fn master_info_fallback_samples(info: &FilesystemInfo) -> Vec<MetricSample> {
    let mut samples = vec![
        gauge_sample("capacity", info.capacity),
        gauge_sample("available", info.available),
        gauge_sample("fs_used", info.fs_used),
        gauge_sample("inode_dir_num", info.inode_dir_num),
        gauge_sample("inode_file_num", info.inode_file_num),
        gauge_sample("num_blocks", info.block_num),
    ];
    for (tag, count) in [
        ("live", info.live_workers.len()),
        ("blacklist", info.blacklist_workers.len()),
        ("decommission", info.decommission_workers.len()),
        ("lost", info.lost_workers.len()),
    ] {
        let mut labels = BTreeMap::new();
        labels.insert("tag".to_string(), tag.to_string());
        samples.push(MetricSample {
            name: "worker_num".to_string(),
            labels,
            value: count as f64,
        });
    }
    samples
}

fn gauge_sample(name: &str, value: i64) -> MetricSample {
    MetricSample {
        name: name.to_string(),
        labels: BTreeMap::new(),
        value: value as f64,
    }
}

async fn maybe_ingest_master_info_fallback(
    provider: &BuiltinProvider,
    discovery: &DiscoveryResult,
    timestamp_ms: i64,
) {
    let Some(info) = &discovery.master_info else {
        return;
    };
    let target = &discovery.master.target;
    let http_scrape_ok = provider
        .get_target(&target.id)
        .map(|health| health.up)
        .unwrap_or(false);
    if http_scrape_ok {
        return;
    }

    let mut samples = master_info_fallback_samples(info);
    if samples.is_empty() {
        return;
    }
    inject_target_labels(&mut samples, target);
    provider.tsdb().insert(timestamp_ms, samples);

    if let Some(mut health) = provider.get_target(&target.id) {
        if !health.last_warning.contains("GetMasterInfo") {
            if !health.last_warning.is_empty() {
                health.last_warning.push_str("; ");
            }
            health.last_warning.push_str(MASTER_INFO_FALLBACK_NOTE);
        }
        provider.update_target(health);
    }
}

fn metric_target_from_health(health: &TargetHealth) -> MetricTarget {
    if health.component == "worker" {
        MetricTarget::worker(
            health.instance.clone(),
            health.url.clone(),
            health.worker_id.unwrap_or_default(),
            health.worker_state.as_deref().unwrap_or("live"),
        )
    } else if health.component == "transfer" {
        MetricTarget::transfer(health.instance.clone(), health.url.clone())
    } else {
        MetricTarget::master(
            health.instance.clone(),
            health.url.clone(),
            health.active_master.clone(),
        )
    }
}

fn merge_stale_worker_targets(
    scrape_targets: &mut Vec<MetricTarget>,
    previous_targets: &[TargetHealth],
) {
    let existing: std::collections::HashSet<_> = scrape_targets
        .iter()
        .map(|target| target.id.clone())
        .collect();
    for previous in previous_targets {
        if previous.component == "worker" && !existing.contains(&previous.id) {
            scrape_targets.push(metric_target_from_health(previous));
        }
    }
}

fn apply_stale_worker_warnings(
    worker_warnings: &mut std::collections::HashMap<String, String>,
    scrape_targets: &[MetricTarget],
    master_warning: &Option<String>,
) {
    let reason = master_warning
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or("master info unavailable");
    let stale_warning = format!("worker discovery stale: {reason}");
    for target in scrape_targets {
        if target.component != "worker" {
            continue;
        }
        worker_warnings
            .entry(target.id.clone())
            .and_modify(|warning| {
                if warning.is_empty() {
                    *warning = stale_warning.clone();
                }
            })
            .or_insert(stale_warning.clone());
    }
}

fn inject_target_labels(samples: &mut [MetricSample], target: &MetricTarget) {
    for sample in samples {
        let mut labels = BTreeMap::new();
        labels.append(&mut sample.labels);
        labels.insert("component".to_string(), target.component.clone());
        let instance = if target.component == "master" {
            ACTIVE_MASTER_INSTANCE.to_string()
        } else {
            target.instance.clone()
        };
        labels.insert("instance".to_string(), instance);
        if let Some(worker_id) = target.worker_id {
            labels.insert("worker_id".to_string(), worker_id.to_string());
        }
        if let Some(worker_state) = &target.worker_state {
            labels.insert("worker_state".to_string(), worker_state.clone());
        }
        sample.labels = labels;
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_warning_does_not_populate_last_error_when_scrape_succeeds() {
        let last_error = String::new();
        let last_warning = "master info timed out".to_string();
        assert!(last_error.is_empty());
        assert_eq!(last_warning, "master info timed out");
    }

    #[test]
    fn scrape_error_and_discovery_warning_are_independent() {
        let scrape_error = "HTTP 500".to_string();
        let last_warning = "master info timed out".to_string();
        assert_eq!(scrape_error, "HTTP 500");
        assert_eq!(last_warning, "master info timed out");
    }

    #[test]
    fn injects_master_labels_without_dropping_raw_labels() {
        let mut labels = BTreeMap::new();
        labels.insert("tag".to_string(), "live".to_string());
        let mut samples = vec![MetricSample {
            name: "worker_num".to_string(),
            labels,
            value: 1.0,
        }];
        let target = MetricTarget::master(
            ACTIVE_MASTER_INSTANCE.to_string(),
            "http://m1:9003/metrics".to_string(),
            None,
        );
        inject_target_labels(&mut samples, &target);
        assert_eq!(samples[0].labels.get("component").unwrap(), "master");
        assert_eq!(
            samples[0].labels.get("instance").unwrap(),
            ACTIVE_MASTER_INSTANCE
        );
        assert_eq!(samples[0].labels.get("tag").unwrap(), "live");
    }

    #[test]
    fn injects_worker_labels_with_id_and_state() {
        let mut samples = vec![MetricSample {
            name: "read_bytes".to_string(),
            labels: BTreeMap::new(),
            value: 1.0,
        }];
        let target = MetricTarget::worker(
            "w1:9001".to_string(),
            "http://w1:9001/metrics".to_string(),
            12,
            "live",
        );
        inject_target_labels(&mut samples, &target);
        assert_eq!(samples[0].labels.get("component").unwrap(), "worker");
        assert_eq!(samples[0].labels.get("instance").unwrap(), "w1:9001");
        assert_eq!(samples[0].labels.get("worker_id").unwrap(), "12");
        assert_eq!(samples[0].labels.get("worker_state").unwrap(), "live");
    }

    #[test]
    fn master_and_worker_capacity_do_not_share_series_key_labels() {
        let mut master = BTreeMap::new();
        master.insert("tag".to_string(), "live".to_string());
        let mut master_samples = vec![MetricSample {
            name: "capacity".to_string(),
            labels: master,
            value: 1.0,
        }];
        inject_target_labels(
            &mut master_samples,
            &MetricTarget::master(
                ACTIVE_MASTER_INSTANCE.to_string(),
                "http://m1:9000/metrics".to_string(),
                None,
            ),
        );

        let mut worker_samples = vec![MetricSample {
            name: "capacity".to_string(),
            labels: BTreeMap::new(),
            value: 2.0,
        }];
        inject_target_labels(
            &mut worker_samples,
            &MetricTarget::worker(
                "w1:9001".to_string(),
                "http://w1:9001/metrics".to_string(),
                1,
                "live",
            ),
        );

        assert_ne!(master_samples[0].labels, worker_samples[0].labels);
        assert_eq!(master_samples[0].labels.get("component").unwrap(), "master");
        assert_eq!(worker_samples[0].labels.get("component").unwrap(), "worker");
    }

    fn resolve_last_success_ms(
        scrape_succeeded: bool,
        timestamp_ms: i64,
        previous: Option<i64>,
    ) -> Option<i64> {
        if scrape_succeeded {
            Some(timestamp_ms)
        } else {
            previous
        }
    }

    #[test]
    fn scrape_up_requires_valid_samples() {
        let has_error = false;
        let sample_count = 0;
        let scrape_succeeded = !has_error && sample_count > 0;
        assert!(!scrape_succeeded);
    }

    #[test]
    fn last_success_ms_updates_on_success_and_persists_on_failure() {
        let previous = Some(1_000);
        assert_eq!(resolve_last_success_ms(true, 2_000, previous), Some(2_000));
        assert_eq!(resolve_last_success_ms(false, 3_000, previous), Some(1_000));
        assert_eq!(resolve_last_success_ms(false, 3_000, None), None);
    }

    #[test]
    fn discovery_failure_merges_previous_worker_targets() {
        let mut scrape_targets = vec![MetricTarget::master(
            "m1:9000".to_string(),
            "http://m1:9000/metrics".to_string(),
            None,
        )];
        let previous = vec![TargetHealth {
            id: "worker:1".to_string(),
            component: "worker".to_string(),
            instance: "w1:9001".to_string(),
            url: "http://w1:9001/metrics".to_string(),
            worker_id: Some(1),
            worker_state: Some("live".to_string()),
            ..Default::default()
        }];
        merge_stale_worker_targets(&mut scrape_targets, &previous);
        assert_eq!(scrape_targets.len(), 2);
        assert_eq!(scrape_targets[1].id, "worker:1");
    }

    #[test]
    fn discovery_failure_marks_worker_targets_stale() {
        let targets = vec![MetricTarget::worker(
            "w1:9001".to_string(),
            "http://w1:9001/metrics".to_string(),
            1,
            "live",
        )];
        let mut warnings = std::collections::HashMap::new();
        apply_stale_worker_warnings(
            &mut warnings,
            &targets,
            &Some("master info timed out".to_string()),
        );
        assert_eq!(
            warnings.get("worker:1").map(String::as_str),
            Some("worker discovery stale: master info timed out")
        );
    }

    #[test]
    fn master_info_fallback_samples_include_capacity_and_worker_counts() {
        let info = FilesystemInfo {
            capacity: 1_000,
            available: 400,
            fs_used: 600,
            inode_dir_num: 12,
            inode_file_num: 34,
            block_num: 56,
            live_workers: vec![Default::default(), Default::default()],
            blacklist_workers: vec![Default::default()],
            ..Default::default()
        };
        let samples = master_info_fallback_samples(&info);
        assert!(samples
            .iter()
            .any(|s| s.name == "capacity" && s.value == 1_000.0));
        assert!(samples.iter().any(|s| {
            s.name == "worker_num"
                && s.labels.get("tag") == Some(&"live".to_string())
                && s.value == 2.0
        }));
    }

    #[tokio::test]
    async fn scrape_ingest_query_and_dashboard_pipeline() {
        use std::sync::Arc;

        use axum::body::{to_bytes, Body};
        use axum::http::{Request, StatusCode};
        use axum::Router;
        use curvine_config::ClusterConf;
        use curvine_runtime::runtime::Runtime;
        use curvine_unified_fs::UnifiedFileSystem;
        use tokio::io::AsyncWriteExt;
        use tower::ServiceExt;

        use crate::observability::api::router;
        use crate::observability::dashboard::build_dashboard;
        use crate::observability::ObservabilityService;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let body = "read_bytes 1000\n";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        let conf = ClusterConf::default();
        let unified_fs =
            UnifiedFileSystem::with_rt(conf.clone(), Arc::new(Runtime::single())).expect("ufs");
        let service = Arc::new(ObservabilityService::new(conf, unified_fs));
        let provider = service.builtin().clone();
        let scraper = provider.scraper().clone();
        let timestamp_ms = now_ms();
        let worker_target = MetricTarget::worker(
            "pipeline-worker:9001".to_string(),
            format!("http://{addr}/metrics"),
            7,
            "live",
        );
        scrape_one_target(
            &provider,
            &scraper,
            worker_target.clone(),
            timestamp_ms,
            None,
            std::collections::HashMap::new(),
        )
        .await;
        scrape_one_target(
            &provider,
            &scraper,
            worker_target,
            timestamp_ms + 15_000,
            None,
            std::collections::HashMap::new(),
        )
        .await;
        provider.set_last_collect_ms(timestamp_ms + 15_000);

        let series = provider.tsdb().range(
            Some("read_bytes"),
            Some("worker"),
            None,
            timestamp_ms - 60_000,
            timestamp_ms + 1_000,
            Some(15_000),
        );
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].metric, "read_bytes");
        assert_eq!(
            series[0].labels.get("worker_id").map(String::as_str),
            Some("7")
        );
        assert_eq!(series[0].points.last().map(|point| point.1), Some(1000.0));

        let app = Router::new().nest("/api/v1/observability", router(service.clone()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/observability/query?metric=read_bytes&component=worker&range=15m")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(json["success"], true);
        assert!(json["data"]["series"]
            .as_array()
            .is_some_and(|items| !items.is_empty()));

        let payload = build_dashboard(&service, "15m", "15s", timestamp_ms + 15_000, 900_000).await;
        let worker_group = payload
            .groups
            .iter()
            .find(|group| group.id == "workers")
            .expect("workers group");
        let read_panel = worker_group
            .panels
            .iter()
            .find(|panel| panel.id == "worker-read-throughput")
            .expect("worker read throughput");
        assert!(!read_panel.series.is_empty());
    }
}
