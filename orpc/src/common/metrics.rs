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

use once_cell::sync::Lazy;
use prometheus::core::{
    AtomicI64, Collector, GenericCounter, GenericCounterVec, GenericGauge, GenericGaugeVec,
};
use prometheus::{default_registry, Encoder, Opts, Registry, TextEncoder};

use crate::sync::FastDashMap;
use crate::{err_box, CommonResult};

pub type Counter = GenericCounter<AtomicI64>;
pub type CounterVec = GenericCounterVec<AtomicI64>;
pub type Gauge = GenericGauge<AtomicI64>;
pub type GaugeVec = GenericGaugeVec<AtomicI64>;

static METRICS_MAP: Lazy<FastDashMap<String, Metrics>> = Lazy::new(FastDashMap::default);

#[derive(Clone)]
pub enum Metrics {
    Counter(Counter),
    CounterVec(CounterVec),
    Gauge(Gauge),
    GaugeVec(GaugeVec),
}

impl Metrics {
    pub fn boxed(&self) -> Box<dyn Collector> {
        match self {
            Metrics::Counter(v) => Box::new(v.clone()),
            Metrics::CounterVec(v) => Box::new(v.clone()),
            Metrics::Gauge(v) => Box::new(v.clone()),
            Metrics::GaugeVec(v) => Box::new(v.clone()),
        }
    }

    pub fn get_name(&self) -> &str {
        match self {
            Metrics::Counter(v) => &v.desc()[0].fq_name,
            Metrics::CounterVec(v) => &v.desc()[0].fq_name,
            Metrics::Gauge(v) => &v.desc()[0].fq_name,
            Metrics::GaugeVec(v) => &v.desc()[0].fq_name,
        }
    }

    fn register(m: Metrics) -> CommonResult<()> {
        if METRICS_MAP.contains_key(m.get_name()) {
            return Ok(());
        }

        let res = METRICS_MAP.entry(m.get_name().to_string()).or_insert(m);
        default_registry().register(res.boxed())?;
        Ok(())
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

    pub fn text_output() -> CommonResult<String> {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let metric_families = default_registry().gather();
        encoder.encode(&metric_families, &mut buffer)?;

        let output = String::from_utf8(buffer.clone())?;
        Ok(output)
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
            _ => err_box!("Not CounterVec"),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::common::Metrics;

    #[test]
    fn sample() {
        let counter = Metrics::new_counter("m1", "m1").unwrap();
        let counter_vec = Metrics::new_counter_vec("m2", "m2", &["l1"]).unwrap();
        let gauge = Metrics::new_gauge("g1", "g1").unwrap();
        let gauge_vec = Metrics::new_gauge_vec("g2", "g2", &["l2"]).unwrap();

        counter.inc_by(100);
        counter_vec.with_label_values(&["v1"]).inc();
        gauge.set(1000);
        gauge_vec.with_label_values(&["v2"]).set(2000);

        assert_eq!(counter.get(), 100);
        assert_eq!(gauge.get(), 1000);

        let output = Metrics::text_output().unwrap();
        println!("output = {}", output)
    }
}
