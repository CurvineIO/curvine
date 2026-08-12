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

use std::collections::{BTreeMap, HashSet};

use curvine_config::ClusterConf;

use crate::util::addr::{master_metrics_instance, master_metrics_scrape_url};

use super::model::{InstantMetric, RangeSeries, TargetHealth, ACTIVE_MASTER_INSTANCE};

/// Match keys for the currently active master metrics endpoint.
#[derive(Clone, Debug, Default)]
pub struct ActiveMasterMatchSet {
    instance_keys: HashSet<String>,
    host_keys: HashSet<String>,
}

impl ActiveMasterMatchSet {
    pub fn from_target(target: &TargetHealth, conf: &ClusterConf) -> Self {
        let mut instance_keys = HashSet::new();
        let mut host_keys = HashSet::new();

        push_instance_key(&mut instance_keys, ACTIVE_MASTER_INSTANCE);
        push_instance_key(&mut instance_keys, &target.instance);
        if let Some(authority) = url_authority(&target.url) {
            push_instance_key(&mut instance_keys, &authority);
            push_host_key(&mut host_keys, &authority);
        }

        if let Some(rpc) = target
            .active_master
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            push_instance_key(&mut instance_keys, rpc);
            push_instance_key(&mut instance_keys, &master_metrics_instance(conf, rpc));
            push_host_key(&mut host_keys, rpc);
            let scrape_url = master_metrics_scrape_url(conf, rpc);
            if let Some(authority) = url_authority(&scrape_url) {
                push_instance_key(&mut instance_keys, &authority);
                push_host_key(&mut host_keys, &authority);
            }
        }

        Self {
            instance_keys,
            host_keys,
        }
    }

    pub fn matches_instance(&self, instance: Option<&str>) -> bool {
        let Some(raw) = instance.map(str::trim).filter(|value| !value.is_empty()) else {
            return false;
        };
        let normalized = raw.to_lowercase();
        if self.instance_keys.contains(&normalized) {
            return true;
        }
        let host = normalized.split(':').next().unwrap_or("").trim();
        !host.is_empty() && self.host_keys.contains(host)
    }
}

/// Apply active-master filtering for open-ended master queries (dashboard default).
pub fn should_apply_active_master_filter(component: Option<&str>, instance: Option<&str>) -> bool {
    let Some(component) = component.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    if !component.eq_ignore_ascii_case("master") {
        return false;
    }
    match instance.map(str::trim).filter(|value| !value.is_empty()) {
        None => true,
        Some(value) => value.eq_ignore_ascii_case(ACTIVE_MASTER_INSTANCE),
    }
}

pub fn filter_range_series_to_active_master(
    series: Vec<RangeSeries>,
    match_set: Option<&ActiveMasterMatchSet>,
) -> Vec<RangeSeries> {
    let master_series: Vec<RangeSeries> = series
        .into_iter()
        .filter(|item| is_master_series(&item.labels))
        .collect();
    if master_series.is_empty() {
        return Vec::new();
    }

    if let Some(match_set) = match_set {
        let matched: Vec<RangeSeries> = master_series
            .iter()
            .filter(|item| {
                match_set.matches_instance(item.labels.get("instance").map(String::as_str))
            })
            .cloned()
            .map(normalize_master_series)
            .collect();
        if !matched.is_empty() {
            return matched;
        }
    }

    let canonical: Vec<RangeSeries> = master_series
        .into_iter()
        .filter(|item| {
            item.labels
                .get("instance")
                .map(|value| value.eq_ignore_ascii_case(ACTIVE_MASTER_INSTANCE))
                .unwrap_or(false)
        })
        .map(normalize_master_series)
        .collect();
    if !canonical.is_empty() {
        return canonical;
    }

    Vec::new()
}

pub fn filter_instant_metrics_to_active_master(
    metrics: Vec<InstantMetric>,
    match_set: Option<&ActiveMasterMatchSet>,
) -> Vec<InstantMetric> {
    let master_metrics: Vec<InstantMetric> = metrics
        .into_iter()
        .filter(|item| is_master_series(&item.labels))
        .collect();
    if master_metrics.is_empty() {
        return Vec::new();
    }

    if let Some(match_set) = match_set {
        let matched: Vec<InstantMetric> = master_metrics
            .iter()
            .filter(|item| {
                match_set.matches_instance(item.labels.get("instance").map(String::as_str))
            })
            .cloned()
            .map(normalize_instant_metric)
            .collect();
        if !matched.is_empty() {
            return matched;
        }
    }

    let canonical: Vec<InstantMetric> = master_metrics
        .into_iter()
        .filter(|item| {
            item.labels
                .get("instance")
                .map(|value| value.eq_ignore_ascii_case(ACTIVE_MASTER_INSTANCE))
                .unwrap_or(false)
        })
        .map(normalize_instant_metric)
        .collect();
    if !canonical.is_empty() {
        return canonical;
    }

    Vec::new()
}

fn is_master_series(labels: &BTreeMap<String, String>) -> bool {
    labels
        .get("component")
        .map(|value| value.eq_ignore_ascii_case("master"))
        .unwrap_or(false)
}

fn normalize_master_series(mut series: RangeSeries) -> RangeSeries {
    series
        .labels
        .insert("component".to_string(), "master".to_string());
    series
        .labels
        .insert("instance".to_string(), ACTIVE_MASTER_INSTANCE.to_string());
    series
}

fn normalize_instant_metric(mut metric: InstantMetric) -> InstantMetric {
    metric
        .labels
        .insert("component".to_string(), "master".to_string());
    metric
        .labels
        .insert("instance".to_string(), ACTIVE_MASTER_INSTANCE.to_string());
    metric
}

fn push_instance_key(keys: &mut HashSet<String>, raw: &str) {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return;
    }
    keys.insert(trimmed.to_lowercase());
}

fn push_host_key(keys: &mut HashSet<String>, raw: &str) {
    let Some(host) = host_from_endpoint(raw) else {
        return;
    };
    keys.insert(host);
}

fn host_from_endpoint(raw: &str) -> Option<String> {
    let authority = url_authority(raw)?;
    let host = authority
        .rsplit('@')
        .next()
        .unwrap_or(authority.as_str())
        .split(':')
        .next()?
        .trim();
    if host.is_empty() {
        None
    } else {
        Some(host.to_lowercase())
    }
}

fn url_authority(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let without_scheme = trimmed.split("://").nth(1).unwrap_or(trimmed);
    let authority = without_scheme.split('/').next()?.split('?').next()?.trim();
    if authority.is_empty() {
        None
    } else {
        Some(authority.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::model::RangePoint;

    fn sample_master_target() -> TargetHealth {
        TargetHealth {
            id: "master:active-master".to_string(),
            component: "master".to_string(),
            instance: ACTIVE_MASTER_INSTANCE.to_string(),
            url: "http://curvine-demo-master-2:29000/metrics".to_string(),
            worker_id: None,
            worker_state: None,
            up: true,
            last_scrape_ms: None,
            last_success_ms: None,
            last_scrape_duration_ms: None,
            last_error: String::new(),
            last_warning: String::new(),
            sample_count: 0,
            parse_error_count: 0,
            active_master: Some("cv-master-2:8995".to_string()),
        }
    }

    #[test]
    fn should_filter_open_master_queries_only() {
        assert!(should_apply_active_master_filter(Some("master"), None));
        assert!(should_apply_active_master_filter(
            Some("master"),
            Some("active-master")
        ));
        assert!(!should_apply_active_master_filter(
            Some("master"),
            Some("standby-master-1:29000")
        ));
        assert!(!should_apply_active_master_filter(Some("worker"), None));
    }

    #[test]
    fn match_set_accepts_scrape_instance_and_host() {
        let match_set =
            ActiveMasterMatchSet::from_target(&sample_master_target(), &ClusterConf::default());
        assert!(match_set.matches_instance(Some("active-master")));
        assert!(match_set.matches_instance(Some("curvine-demo-master-2:29000")));
        assert!(match_set.matches_instance(Some("CURVINE-DEMO-MASTER-2:29000")));
        assert!(!match_set.matches_instance(Some("curvine-demo-master-1:29000")));
    }

    #[test]
    fn filter_range_series_keeps_only_active_master() {
        let match_set =
            ActiveMasterMatchSet::from_target(&sample_master_target(), &ClusterConf::default());
        let mut active = BTreeMap::new();
        active.insert("component".to_string(), "master".to_string());
        active.insert(
            "instance".to_string(),
            "curvine-demo-master-2:29000".to_string(),
        );
        let mut standby = BTreeMap::new();
        standby.insert("component".to_string(), "master".to_string());
        standby.insert(
            "instance".to_string(),
            "curvine-demo-master-1:29000".to_string(),
        );
        let filtered = filter_range_series_to_active_master(
            vec![
                RangeSeries {
                    metric: "capacity".to_string(),
                    labels: active,
                    points: vec![RangePoint(1, 100.0)],
                },
                RangeSeries {
                    metric: "capacity".to_string(),
                    labels: standby,
                    points: vec![RangePoint(1, 50.0)],
                },
            ],
            Some(&match_set),
        );
        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered[0].labels.get("instance").map(String::as_str),
            Some(ACTIVE_MASTER_INSTANCE)
        );
    }

    #[test]
    fn filter_range_series_prefers_canonical_label_when_present() {
        let mut canonical = BTreeMap::new();
        canonical.insert("component".to_string(), "master".to_string());
        canonical.insert("instance".to_string(), ACTIVE_MASTER_INSTANCE.to_string());
        let mut standby = BTreeMap::new();
        standby.insert("component".to_string(), "master".to_string());
        standby.insert(
            "instance".to_string(),
            "curvine-demo-master-1:29000".to_string(),
        );
        let filtered = filter_range_series_to_active_master(
            vec![
                RangeSeries {
                    metric: "capacity".to_string(),
                    labels: canonical,
                    points: vec![RangePoint(1, 100.0)],
                },
                RangeSeries {
                    metric: "capacity".to_string(),
                    labels: standby,
                    points: vec![RangePoint(1, 50.0)],
                },
            ],
            None,
        );
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].points[0].1, 100.0);
    }
}
