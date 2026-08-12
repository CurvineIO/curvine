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

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::RwLock;

use super::model::{InstantMetric, MetricSample, RangePoint, RangeSeries, SeriesKey, SeriesPoint};

pub struct InMemoryTsdb {
    retention_ms: RwLock<i64>,
    max_samples_per_series: RwLock<usize>,
    max_total_samples: usize,
    max_series: usize,
    series: RwLock<HashMap<SeriesKey, VecDeque<SeriesPoint>>>,
}

impl InMemoryTsdb {
    pub fn new(
        retention_ms: i64,
        max_samples_per_series: usize,
        max_series: usize,
        max_total_samples: usize,
    ) -> Self {
        Self {
            retention_ms: RwLock::new(retention_ms),
            max_samples_per_series: RwLock::new(max_samples_per_series),
            max_total_samples: max_total_samples.max(1),
            max_series,
            series: RwLock::new(HashMap::new()),
        }
    }

    pub fn configure(&self, retention_ms: i64, max_samples_per_series: usize) {
        if let Ok(mut value) = self.retention_ms.write() {
            *value = retention_ms.max(1);
        }
        if let Ok(mut value) = self.max_samples_per_series.write() {
            *value = max_samples_per_series.max(1);
        }
        self.prune_expired(Self::now_ms());
    }

    fn enforce_total_sample_budget(
        series: &mut HashMap<SeriesKey, VecDeque<SeriesPoint>>,
        max_total: usize,
    ) {
        let total: usize = series.values().map(VecDeque::len).sum();
        if total <= max_total {
            return;
        }
        let mut overage = total - max_total;
        let mut heap: BinaryHeap<OldestFront> = series
            .iter()
            .filter_map(|(key, points)| {
                points.front().map(|point| OldestFront {
                    timestamp_ms: point.timestamp_ms,
                    key: key.clone(),
                })
            })
            .collect();
        while overage > 0 {
            let Some(front) = heap.pop() else {
                break;
            };
            let Some(points) = series.get_mut(&front.key) else {
                continue;
            };
            points.pop_front();
            overage -= 1;
            if points.is_empty() {
                series.remove(&front.key);
            } else if let Some(point) = points.front() {
                heap.push(OldestFront {
                    timestamp_ms: point.timestamp_ms,
                    key: front.key,
                });
            }
        }
    }

    fn retention_ms(&self) -> i64 {
        self.retention_ms
            .read()
            .map(|value| *value)
            .unwrap_or(3_600_000)
    }

    fn max_samples_per_series(&self) -> usize {
        self.max_samples_per_series
            .read()
            .map(|value| *value)
            .unwrap_or(720)
    }

    fn prune_expired(&self, timestamp_ms: i64) {
        let Ok(mut series) = self.series.write() else {
            return;
        };
        let min_timestamp = timestamp_ms.saturating_sub(self.retention_ms());
        let max_samples = self.max_samples_per_series();
        series.retain(|_, points| {
            while points
                .front()
                .map(|point| point.timestamp_ms < min_timestamp)
                .unwrap_or(false)
            {
                points.pop_front();
            }
            while points.len() > max_samples {
                points.pop_front();
            }
            !points.is_empty()
        });
        Self::enforce_total_sample_budget(&mut series, self.max_total_samples);
    }

    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or(0)
    }

    pub fn insert(&self, timestamp_ms: i64, samples: Vec<MetricSample>) {
        let Ok(mut series) = self.series.write() else {
            return;
        };
        let min_timestamp = timestamp_ms.saturating_sub(self.retention_ms());
        let max_samples = self.max_samples_per_series();
        // Reclaim inactive label sets before enforcing max_series. Without this
        // batch-wide GC, expired empty series permanently consume cardinality and
        // eventually cause every new worker/label set to be silently rejected.
        series.retain(|_, points| {
            while points
                .front()
                .map(|point| point.timestamp_ms < min_timestamp)
                .unwrap_or(false)
            {
                points.pop_front();
            }
            while points.len() > max_samples {
                points.pop_front();
            }
            !points.is_empty()
        });
        for sample in samples {
            let key = SeriesKey {
                metric: sample.name,
                labels: sample.labels,
            };
            if !series.contains_key(&key) && series.len() >= self.max_series {
                continue;
            }
            let points = series.entry(key).or_insert_with(VecDeque::new);
            points.push_back(SeriesPoint {
                timestamp_ms,
                value: sample.value,
            });
            while points
                .front()
                .map(|point| point.timestamp_ms < min_timestamp)
                .unwrap_or(false)
            {
                points.pop_front();
            }
            while points.len() > max_samples {
                points.pop_front();
            }
        }
        Self::enforce_total_sample_budget(&mut series, self.max_total_samples);
    }

    pub fn max_total_samples(&self) -> usize {
        self.max_total_samples
    }

    pub fn max_series(&self) -> usize {
        self.max_series
    }

    pub fn names(&self) -> Vec<String> {
        let Ok(series) = self.series.read() else {
            return Vec::new();
        };
        let mut names: Vec<_> = series.keys().map(|key| key.metric.clone()).collect();
        names.sort();
        names.dedup();
        names
    }

    pub fn instant(
        &self,
        metric: Option<&str>,
        component: Option<&str>,
        instance: Option<&str>,
    ) -> Vec<InstantMetric> {
        let Ok(series) = self.series.read() else {
            return Vec::new();
        };
        series
            .iter()
            .filter(|(key, _)| matches_selector(key, metric, component, instance))
            .filter_map(|(key, points)| {
                points.back().map(|point| InstantMetric {
                    metric: key.metric.clone(),
                    labels: key.labels.clone(),
                    timestamp_ms: point.timestamp_ms,
                    value: point.value,
                })
            })
            .collect()
    }

    /// Returns points in `[start_ms, end_ms]` for series matching the selectors.
    ///
    /// When `step_ms` is `None`, all raw stored points in the window are returned.
    /// When `step_ms` is `Some`, points are downsampled to one sample per step bucket
    /// using the last raw value in each bucket (Prometheus-style range alignment).
    pub fn range(
        &self,
        metric: Option<&str>,
        component: Option<&str>,
        instance: Option<&str>,
        start_ms: i64,
        end_ms: i64,
        step_ms: Option<i64>,
    ) -> Vec<RangeSeries> {
        let Ok(series) = self.series.read() else {
            return Vec::new();
        };
        series
            .iter()
            .filter(|(key, _)| matches_selector(key, metric, component, instance))
            .filter_map(|(key, points)| {
                let points: Vec<RangePoint> = points
                    .iter()
                    .filter(|point| point.timestamp_ms >= start_ms && point.timestamp_ms <= end_ms)
                    .map(RangePoint::from)
                    .collect();
                if points.is_empty() {
                    return None;
                }
                let points = match step_ms {
                    Some(step) if step > 0 => downsample_points(&points, start_ms, step),
                    _ => points,
                };
                if points.is_empty() {
                    return None;
                }
                Some(RangeSeries {
                    metric: key.metric.clone(),
                    labels: key.labels.clone(),
                    points,
                })
            })
            .collect()
    }

    pub fn series_count(&self) -> usize {
        self.series.read().map(|series| series.len()).unwrap_or(0)
    }

    pub fn sample_count(&self) -> usize {
        self.series
            .read()
            .map(|series| series.values().map(VecDeque::len).sum())
            .unwrap_or(0)
    }

    /// Drop master series that still use per-node instance labels from before HA
    /// stable-instance migration.
    pub fn purge_legacy_master_instances(&self, canonical_instance: &str) {
        let Ok(mut series) = self.series.write() else {
            return;
        };
        series.retain(|key, _| {
            if key.labels.get("component").map(String::as_str) != Some("master") {
                return true;
            }
            key.labels.get("instance").map(String::as_str) == Some(canonical_instance)
        });
    }
}

#[derive(Eq, PartialEq)]
struct OldestFront {
    timestamp_ms: i64,
    key: SeriesKey,
}

impl Ord for OldestFront {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .timestamp_ms
            .cmp(&self.timestamp_ms)
            .then_with(|| series_key_ord(&self.key, &other.key))
    }
}

impl PartialOrd for OldestFront {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn series_key_ord(left: &SeriesKey, right: &SeriesKey) -> Ordering {
    left.metric
        .cmp(&right.metric)
        .then_with(|| left.labels.cmp(&right.labels))
}

fn matches_selector(
    key: &SeriesKey,
    metric: Option<&str>,
    component: Option<&str>,
    instance: Option<&str>,
) -> bool {
    metric.map(|value| key.metric == value).unwrap_or(true)
        && component
            .map(|value| key.labels.get("component").map(String::as_str) == Some(value))
            .unwrap_or(true)
        && instance
            .map(|value| key.labels.get("instance").map(String::as_str) == Some(value))
            .unwrap_or(true)
}

/// Align points to step buckets and keep the last raw sample in each bucket.
pub fn downsample_points(points: &[RangePoint], start_ms: i64, step_ms: i64) -> Vec<RangePoint> {
    if step_ms <= 0 || points.is_empty() {
        return points.to_vec();
    }
    let mut buckets: std::collections::BTreeMap<i64, f64> = std::collections::BTreeMap::new();
    for point in points {
        let offset = point.0.saturating_sub(start_ms);
        let bucket = start_ms + (offset / step_ms) * step_ms;
        buckets.insert(bucket, point.1);
    }
    buckets
        .into_iter()
        .map(|(timestamp_ms, value)| RangePoint(timestamp_ms, value))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn sample(name: &str, component: &str, value: f64) -> MetricSample {
        let mut labels = BTreeMap::new();
        labels.insert("component".to_string(), component.to_string());
        MetricSample {
            name: name.to_string(),
            labels,
            value,
        }
    }

    #[test]
    fn keeps_series_separate_by_labels() {
        let tsdb = InMemoryTsdb::new(60_000, 10, 100, 1_000_000);
        tsdb.insert(
            1,
            vec![
                sample("capacity", "master", 1.0),
                sample("capacity", "worker", 2.0),
            ],
        );
        assert_eq!(tsdb.series_count(), 2);
        assert_eq!(
            tsdb.instant(Some("capacity"), Some("master"), None)[0].value,
            1.0
        );
    }

    #[test]
    fn prunes_old_points_and_max_samples() {
        let tsdb = InMemoryTsdb::new(10, 2, 100, 1_000_000);
        tsdb.insert(1, vec![sample("capacity", "master", 1.0)]);
        tsdb.insert(2, vec![sample("capacity", "master", 2.0)]);
        tsdb.insert(20, vec![sample("capacity", "master", 3.0)]);
        assert_eq!(tsdb.sample_count(), 1);
        assert_eq!(tsdb.instant(Some("capacity"), None, None)[0].value, 3.0);
    }

    #[test]
    fn expired_series_releases_cardinality_for_new_labels() {
        let tsdb = InMemoryTsdb::new(10, 10, 1, 1_000_000);
        tsdb.insert(1, vec![sample("old_metric", "master", 1.0)]);
        tsdb.insert(20, vec![sample("new_metric", "worker", 2.0)]);

        assert_eq!(tsdb.series_count(), 1);
        assert!(tsdb.instant(Some("old_metric"), None, None).is_empty());
        assert_eq!(tsdb.instant(Some("new_metric"), None, None)[0].value, 2.0);
    }

    #[test]
    fn appends_points_for_existing_series() {
        let tsdb = InMemoryTsdb::new(60_000, 10, 100, 1_000_000);
        tsdb.insert(1, vec![sample("capacity", "master", 1.0)]);
        tsdb.insert(2, vec![sample("capacity", "master", 2.0)]);
        assert_eq!(tsdb.series_count(), 1);
        assert_eq!(tsdb.sample_count(), 2);
        assert_eq!(
            tsdb.instant(Some("capacity"), Some("master"), None)[0].value,
            2.0
        );
    }

    #[test]
    fn instant_filters_by_instance() {
        let mut labels = BTreeMap::new();
        labels.insert("component".to_string(), "master".to_string());
        labels.insert("instance".to_string(), "m1:9000".to_string());
        let mut other = BTreeMap::new();
        other.insert("component".to_string(), "master".to_string());
        other.insert("instance".to_string(), "m2:9000".to_string());
        let tsdb = InMemoryTsdb::new(60_000, 10, 100, 1_000_000);
        tsdb.insert(
            1,
            vec![
                MetricSample {
                    name: "capacity".to_string(),
                    labels,
                    value: 1.0,
                },
                MetricSample {
                    name: "capacity".to_string(),
                    labels: other,
                    value: 2.0,
                },
            ],
        );
        let hits = tsdb.instant(Some("capacity"), Some("master"), Some("m1:9000"));
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].value, 1.0);
    }

    #[test]
    fn enforces_max_series_limit() {
        let tsdb = InMemoryTsdb::new(60_000, 10, 2, 1_000_000);
        tsdb.insert(1, vec![sample("a", "master", 1.0)]);
        tsdb.insert(1, vec![sample("b", "master", 2.0)]);
        tsdb.insert(1, vec![sample("c", "master", 3.0)]);
        assert_eq!(tsdb.series_count(), 2);
    }

    #[test]
    fn range_filters_by_metric_component_and_instance() {
        let tsdb = InMemoryTsdb::new(60_000, 10, 100, 1_000_000);
        let mut worker_labels = BTreeMap::new();
        worker_labels.insert("component".to_string(), "worker".to_string());
        worker_labels.insert("instance".to_string(), "w1:9001".to_string());
        worker_labels.insert("worker_id".to_string(), "1".to_string());
        worker_labels.insert("worker_state".to_string(), "live".to_string());
        tsdb.insert(
            1_000,
            vec![MetricSample {
                name: "read_bytes".to_string(),
                labels: worker_labels.clone(),
                value: 10.0,
            }],
        );
        tsdb.insert(
            2_000,
            vec![MetricSample {
                name: "read_bytes".to_string(),
                labels: worker_labels,
                value: 20.0,
            }],
        );
        tsdb.insert(1_500, vec![sample("capacity", "master", 100.0)]);

        let hits = tsdb.range(
            Some("read_bytes"),
            Some("worker"),
            Some("w1:9001"),
            500,
            2_500,
            None,
        );
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].points.len(), 2);
        assert_eq!(hits[0].points[0], RangePoint(1_000, 10.0));
        assert_eq!(hits[0].points[1], RangePoint(2_000, 20.0));
    }

    #[test]
    fn range_does_not_mix_series_with_different_labels() {
        let tsdb = InMemoryTsdb::new(60_000, 10, 100, 1_000_000);
        let mut worker_a = BTreeMap::new();
        worker_a.insert("component".to_string(), "worker".to_string());
        worker_a.insert("instance".to_string(), "w1:9001".to_string());
        worker_a.insert("worker_id".to_string(), "1".to_string());
        worker_a.insert("worker_state".to_string(), "live".to_string());
        let mut worker_b = worker_a.clone();
        worker_b.insert("instance".to_string(), "w2:9001".to_string());
        worker_b.insert("worker_id".to_string(), "2".to_string());
        tsdb.insert(
            1_000,
            vec![
                MetricSample {
                    name: "read_bytes".to_string(),
                    labels: worker_a,
                    value: 1.0,
                },
                MetricSample {
                    name: "read_bytes".to_string(),
                    labels: worker_b,
                    value: 2.0,
                },
            ],
        );
        let hits = tsdb.range(Some("read_bytes"), Some("worker"), None, 0, 5_000, None);
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn range_respects_retention_and_max_samples() {
        let tsdb = InMemoryTsdb::new(10, 2, 100, 1_000_000);
        tsdb.insert(1, vec![sample("capacity", "master", 1.0)]);
        tsdb.insert(2, vec![sample("capacity", "master", 2.0)]);
        tsdb.insert(20, vec![sample("capacity", "master", 3.0)]);
        let hits = tsdb.range(Some("capacity"), Some("master"), None, 0, 100, None);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].points.len(), 1);
        assert_eq!(hits[0].points[0], RangePoint(20, 3.0));
    }

    #[test]
    fn range_downsamples_by_step_bucket() {
        let tsdb = InMemoryTsdb::new(60_000, 100, 100, 1_000_000);
        for ts in (0..=30_000).step_by(1_000) {
            tsdb.insert(ts, vec![sample("capacity", "master", ts as f64)]);
        }
        let raw = tsdb.range(Some("capacity"), Some("master"), None, 0, 30_000, None);
        assert_eq!(raw[0].points.len(), 31);

        let downsampled = tsdb.range(
            Some("capacity"),
            Some("master"),
            None,
            0,
            30_000,
            Some(5_000),
        );
        assert_eq!(downsampled[0].points.len(), 7);
        assert_eq!(
            downsampled[0].points.last(),
            Some(&RangePoint(30_000, 30_000.0))
        );
    }

    #[test]
    fn purge_legacy_master_instances_keeps_canonical_label_only() {
        let mut legacy = BTreeMap::new();
        legacy.insert("component".to_string(), "master".to_string());
        legacy.insert("instance".to_string(), "m1:19000".to_string());
        let mut canonical = BTreeMap::new();
        canonical.insert("component".to_string(), "master".to_string());
        canonical.insert("instance".to_string(), "active-master".to_string());
        let mut worker = BTreeMap::new();
        worker.insert("component".to_string(), "worker".to_string());
        worker.insert("instance".to_string(), "w1:9001".to_string());

        let tsdb = InMemoryTsdb::new(60_000, 10, 100, 1_000_000);
        tsdb.insert(
            1,
            vec![
                MetricSample {
                    name: "capacity".to_string(),
                    labels: legacy,
                    value: 1.0,
                },
                MetricSample {
                    name: "capacity".to_string(),
                    labels: canonical.clone(),
                    value: 2.0,
                },
                MetricSample {
                    name: "read_bytes".to_string(),
                    labels: worker,
                    value: 3.0,
                },
            ],
        );
        assert_eq!(tsdb.series_count(), 3);
        tsdb.purge_legacy_master_instances("active-master");
        assert_eq!(tsdb.series_count(), 2);
        let hits = tsdb.instant(Some("capacity"), Some("master"), None);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].value, 2.0);
        assert_eq!(
            hits[0].labels.get("instance").map(String::as_str),
            Some("active-master")
        );
    }

    #[test]
    fn enforces_total_sample_budget() {
        let tsdb = InMemoryTsdb::new(60_000, 100, 100, 5);
        for ts in 0..10 {
            tsdb.insert(
                ts,
                vec![
                    sample("metric_a", "master", ts as f64),
                    sample("metric_b", "worker", ts as f64),
                ],
            );
        }
        assert_eq!(tsdb.sample_count(), 5);
    }

    #[test]
    fn total_sample_budget_drops_globally_oldest_points() {
        let tsdb = InMemoryTsdb::new(60_000, 100, 100, 3);
        tsdb.insert(1, vec![sample("metric_a", "master", 1.0)]);
        tsdb.insert(2, vec![sample("metric_a", "master", 2.0)]);
        tsdb.insert(20, vec![sample("metric_b", "worker", 20.0)]);
        tsdb.insert(30, vec![sample("metric_b", "worker", 30.0)]);
        assert_eq!(tsdb.sample_count(), 3);
        let a = tsdb.range(Some("metric_a"), None, None, 0, 100, None);
        let b = tsdb.range(Some("metric_b"), None, None, 0, 100, None);
        assert_eq!(a[0].points.len(), 1);
        assert_eq!(a[0].points[0], RangePoint(2, 2.0));
        assert_eq!(b[0].points.len(), 2);
        assert_eq!(b[0].points[0], RangePoint(20, 20.0));
    }

    #[test]
    fn total_sample_budget_handles_large_single_insert() {
        let tsdb = InMemoryTsdb::new(60_000, 10_000, 10_000, 100);
        let samples: Vec<_> = (0..500)
            .map(|index| sample("metric", &format!("inst-{index}"), index as f64))
            .collect();
        tsdb.insert(1_000, samples);
        assert_eq!(tsdb.sample_count(), 100);
    }
}
