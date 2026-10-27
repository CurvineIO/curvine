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

use std::hash::BuildHasherDefault;
use std::sync::Arc;
use std::time::Instant;

use curvine_core_error::CommonResult;
use dashmap::DashMap;
use fxhash::FxHasher;
use once_cell::sync::{Lazy, OnceCell};
use prometheus::core::{
    AtomicI64, Collector, GenericCounter, GenericCounterVec, GenericGauge, GenericGaugeVec,
};
use prometheus::proto::{LabelPair, MetricFamily};
use prometheus::{default_registry, Encoder, HistogramOpts, Opts, Registry, TextEncoder};

pub use prometheus::proto::MetricType as MetricFamilyType;

/// Label injected on every exported sample to identify the cluster a process
/// belongs to. Its value is the configured `cluster_id`, written by each
/// component at startup (see [`Metrics::set_cluster_id`]) and appended at
/// gather time, so existing metric definitions never have to declare it.
pub const CLUSTER_ID_LABEL: &str = "cluster_id";

/// Process wide cluster id exported as [`CLUSTER_ID_LABEL`]. Written once at
/// startup; later writes are ignored so that a process cannot end up with
/// mixed labels on one scrape.
static CLUSTER_ID: OnceCell<String> = OnceCell::new();

pub type Counter = GenericCounter<AtomicI64>;
pub type CounterVec = GenericCounterVec<AtomicI64>;
pub type Gauge = GenericGauge<AtomicI64>;
pub type GaugeVec = GenericGaugeVec<AtomicI64>;
pub type Histogram = prometheus::Histogram;
pub type HistogramVec = prometheus::HistogramVec;

type MetricsMap = DashMap<String, Metrics, BuildHasherDefault<FxHasher>>;

static METRICS_MAP: Lazy<MetricsMap> = Lazy::new(MetricsMap::default);

#[derive(Clone)]
pub enum Metrics {
    Counter(Counter),
    CounterVec(CounterVec),
    Gauge(Gauge),
    GaugeVec(GaugeVec),
    Histogram(Histogram),
    HistogramVec(HistogramVec),
}

impl Metrics {
    pub fn boxed(&self) -> Box<dyn Collector> {
        match self {
            Metrics::Counter(v) => Box::new(v.clone()),
            Metrics::CounterVec(v) => Box::new(v.clone()),
            Metrics::Gauge(v) => Box::new(v.clone()),
            Metrics::GaugeVec(v) => Box::new(v.clone()),
            Metrics::Histogram(v) => Box::new(v.clone()),
            Metrics::HistogramVec(v) => Box::new(v.clone()),
        }
    }

    pub fn get_name(&self) -> &str {
        match self {
            Metrics::Counter(v) => &v.desc()[0].fq_name,
            Metrics::CounterVec(v) => &v.desc()[0].fq_name,
            Metrics::Gauge(v) => &v.desc()[0].fq_name,
            Metrics::GaugeVec(v) => &v.desc()[0].fq_name,
            Metrics::Histogram(v) => &v.desc()[0].fq_name,
            Metrics::HistogramVec(v) => &v.desc()[0].fq_name,
        }
    }

    fn register(m: Metrics) -> CommonResult<()> {
        let name = m.get_name().to_string();

        if METRICS_MAP.contains_key(&name) {
            return Ok(());
        }

        let res = METRICS_MAP.entry(name.clone()).or_insert(m);

        // Ignore already registered errors as they can happen in concurrent test scenarios.
        match default_registry().register(res.boxed()) {
            Ok(_) => Ok(()),
            Err(e) => {
                let err_msg = format!("{}", e);
                if err_msg.contains("Duplicate") || err_msg.contains("duplicate") {
                    Ok(())
                } else {
                    Err(e.into())
                }
            }
        }
    }

    pub fn new_counter<T: Into<String>>(name: T, help: T) -> CommonResult<Counter> {
        let c = Counter::new(name, help)?;
        Self::register(Self::Counter(c.clone()))?;
        Ok(c)
    }

    pub fn new_counter_vec<T: Into<String>>(
        name: T,
        help: T,
        label_names: &[&str],
    ) -> CommonResult<CounterVec> {
        let c = CounterVec::new(Opts::new(name, help), label_names)?;
        Self::register(Self::CounterVec(c.clone()))?;
        Ok(c)
    }

    pub fn new_gauge<T: Into<String>>(name: T, help: T) -> CommonResult<Gauge> {
        let g = Gauge::new(name, help)?;
        Self::register(Self::Gauge(g.clone()))?;
        Ok(g)
    }

    pub fn new_gauge_vec<T: Into<String>>(
        name: T,
        help: T,
        label_names: &[&str],
    ) -> CommonResult<GaugeVec> {
        let g = GaugeVec::new(Opts::new(name, help), label_names)?;
        Self::register(Self::GaugeVec(g.clone()))?;
        Ok(g)
    }

    pub fn new_histogram<T: Into<String>>(name: T, help: T) -> CommonResult<Histogram> {
        let h = Histogram::with_opts(HistogramOpts::new(name, help))?;
        Self::register(Self::Histogram(h.clone()))?;
        Ok(h)
    }

    pub fn new_histogram_with_buckets<T: Into<String>>(
        name: T,
        help: T,
        buckets: &[f64],
    ) -> CommonResult<Histogram> {
        let mut opts = HistogramOpts::new(name, help);
        opts.buckets = buckets.to_vec();
        let h = Histogram::with_opts(opts)?;
        Self::register(Self::Histogram(h.clone()))?;
        Ok(h)
    }

    pub fn new_histogram_vec<T: Into<String>>(
        name: T,
        help: T,
        label_names: &[&str],
    ) -> CommonResult<HistogramVec> {
        let opts = HistogramOpts::new(name, help).buckets(vec![
            10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0,
        ]);
        let h = HistogramVec::new(opts, label_names)?;
        Self::register(Self::HistogramVec(h.clone()))?;
        Ok(h)
    }

    pub fn new_histogram_vec_with_buckets<T: Into<String>>(
        name: T,
        help: T,
        label_names: &[&str],
        buckets: &[f64],
    ) -> CommonResult<HistogramVec> {
        let mut opts = HistogramOpts::new(name, help);
        opts.buckets = buckets.to_vec();
        let h = HistogramVec::new(opts, label_names)?;
        Self::register(Self::HistogramVec(h.clone()))?;
        Ok(h)
    }

    pub fn text_output() -> CommonResult<String> {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let metric_families = Self::gather();
        encoder.encode(&metric_families, &mut buffer)?;

        let output = String::from_utf8(buffer.clone())?;
        Ok(output)
    }

    /// Gather every registered metric family and attach the process wide
    /// cluster label to each sample.
    ///
    /// Labels are added here - on the gathered snapshot - instead of on the
    /// collectors because metrics are registered as soon as they are created
    /// and a collector cannot be re-labelled afterwards. The operation is
    /// idempotent: a metric that already carries `cluster_id` keeps its own
    /// value.
    pub fn gather() -> Vec<MetricFamily> {
        let mut families = default_registry().gather();
        let Some(cluster_id) = Self::cluster_id() else {
            return families;
        };

        for family in families.iter_mut() {
            for metric in family.mut_metric().iter_mut() {
                if metric
                    .get_label()
                    .iter()
                    .any(|l| l.get_name() == CLUSTER_ID_LABEL)
                {
                    continue;
                }

                let mut labels = metric.take_label();
                let mut pair = LabelPair::default();
                pair.set_name(CLUSTER_ID_LABEL.to_string());
                pair.set_value(cluster_id.to_string());
                labels.push(pair);
                metric.set_label(labels);
            }
        }

        families
    }

    /// Set the cluster id exported as the `cluster_id` label value.
    ///
    /// Only the first call takes effect; the value is process wide because
    /// every metric in a process belongs to the same cluster. Subsequent calls
    /// (for example a second cluster conf loaded by a test) are ignored instead
    /// of panicking.
    pub fn set_cluster_id(cluster_id: impl Into<String>) {
        let cluster_id = cluster_id.into();
        let cluster_id = cluster_id.trim();
        if cluster_id.is_empty() {
            return;
        }
        let _ = CLUSTER_ID.set(cluster_id.to_string());
    }

    /// Cluster id currently exported as the `cluster_id` label value.
    pub fn cluster_id() -> Option<&'static str> {
        CLUSTER_ID.get().map(|s| s.as_str())
    }

    pub fn registry() -> &'static Registry {
        default_registry()
    }

    pub fn get(name: impl AsRef<str>) -> Option<Metrics> {
        METRICS_MAP.get(name.as_ref()).map(|x| x.clone())
    }

    pub fn try_into_counter_vec(self) -> CommonResult<CounterVec> {
        match self {
            Metrics::CounterVec(v) => Ok(v),
            _ => curvine_core_error::err_box!("Not CounterVec"),
        }
    }

    pub fn try_into_histogram_vec(self) -> CommonResult<HistogramVec> {
        match self {
            Metrics::HistogramVec(v) => Ok(v),
            _ => curvine_core_error::err_box!("Not HistogramVec"),
        }
    }
}

// RAII guard for Counter metrics.
pub struct MetricTimer {
    start: Instant,
    metric: Arc<Counter>,
}

impl MetricTimer {
    pub fn new(metric: Arc<Counter>) -> Self {
        Self {
            start: Instant::now(),
            metric,
        }
    }
}

impl Drop for MetricTimer {
    fn drop(&mut self) {
        self.metric.inc_by(self.start.elapsed().as_micros() as i64);
    }
}

pub struct MetricTimerVec {
    start: Instant,
    metric: Arc<HistogramVec>,
    label_values: Vec<String>,
}

impl MetricTimerVec {
    pub fn new(metric: Arc<HistogramVec>, label_values: Vec<String>) -> Self {
        Self {
            start: Instant::now(),
            metric,
            label_values,
        }
    }
}

impl Drop for MetricTimerVec {
    fn drop(&mut self) {
        let values: Vec<&str> = self.label_values.iter().map(|s| s.as_str()).collect();
        self.metric
            .with_label_values(&values)
            .observe(self.start.elapsed().as_micros() as f64);
    }
}

#[cfg(test)]
mod test {
    use crate::{Metrics, CLUSTER_ID_LABEL};

    #[test]
    fn sample() {
        let counter = Metrics::new_counter("m1", "m1").unwrap();
        let counter_vec = Metrics::new_counter_vec("m2", "m2", &["l1"]).unwrap();
        let gauge = Metrics::new_gauge("g1", "g1").unwrap();
        let gauge_vec = Metrics::new_gauge_vec("g2", "g2", &["l2"]).unwrap();
        let histogram = Metrics::new_histogram("h1", "h1").unwrap();
        let histogram_vec = Metrics::new_histogram_vec("h2", "h2", &["l3"]).unwrap();

        counter.inc_by(100);
        counter_vec.with_label_values(&["v1"]).inc();
        gauge.set(1000);
        gauge_vec.with_label_values(&["v2"]).set(2000);
        histogram.observe(1.5);
        histogram_vec.with_label_values(&["v3"]).observe(2.5);

        assert_eq!(counter.get(), 100);
        assert_eq!(gauge.get(), 1000);
        assert_eq!(histogram.get_sample_count(), 1);
        assert_eq!(histogram.get_sample_sum(), 1.5);

        let histogram_instance = histogram_vec.with_label_values(&["v3"]);
        assert_eq!(histogram_instance.get_sample_count(), 1);
        assert_eq!(histogram_instance.get_sample_sum(), 2.5);

        let output = Metrics::text_output().unwrap();
        println!("output = {}", output);

        #[cfg(target_os = "linux")]
        {
            assert!(output.contains("process_threads"));
            assert!(output.contains("process_cpu_seconds_total"));
            assert!(output.contains("process_open_fds"));
            assert!(output.contains("process_resident_memory_bytes"));
        }
    }

    #[test]
    fn cluster_id_label_is_appended_to_every_sample() {
        // The process wide value may already be set by another test in this
        // binary; either way it must never be empty and must appear on both
        // labelled and unlabelled metrics.
        Metrics::set_cluster_id("cluster-metrics-test");
        let cluster_id = Metrics::cluster_id().expect("cluster id must be set");

        Metrics::new_counter("cluster_label_plain", "cluster label test").unwrap();
        Metrics::new_counter_vec("cluster_label_vec", "cluster label test", &["result"])
            .unwrap()
            .with_label_values(&["timeout"])
            .inc();

        let output = Metrics::text_output().unwrap();
        assert!(
            output.contains(&format!(
                "cluster_label_plain{{{}=\"{}\"}}",
                CLUSTER_ID_LABEL, cluster_id
            )),
            "unlabelled metric misses {CLUSTER_ID_LABEL}: {output}"
        );
        assert!(
            output.contains(&format!(
                "cluster_label_vec{{result=\"timeout\",{}=\"{}\"}}",
                CLUSTER_ID_LABEL, cluster_id
            )),
            "labelled metric misses {CLUSTER_ID_LABEL}: {output}"
        );
    }
}
