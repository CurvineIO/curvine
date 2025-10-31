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

use crate::file::FsContext;
use curvine_common::state::{MetricType, MetricValue};
use orpc::common::Metrics;
use orpc::common::{CounterVec, Metrics as m};
use orpc::sync::FastDashMap;
use orpc::CommonResult;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

pub struct ClientMetrics {
    pub mount_cache_hits: CounterVec,
    pub mount_cache_misses: CounterVec,
    pub last_value_map: FastDashMap<String, f64>,
}

impl ClientMetrics {
    pub const PREFIX: &'static str = "client";

    pub fn new() -> CommonResult<Self> {
        let cm = Self {
            mount_cache_hits: m::new_counter_vec(
                "client_mount_cache_hits",
                "mount cache miss count",
                &["id"],
            )?,
            mount_cache_misses: m::new_counter_vec(
                "client_mount_cache_misses",
                "mount cache miss count",
                &["id"],
            )?,

            last_value_map: FastDashMap::default(),
        };

        Ok(cm)
    }

    pub fn text_output(&self) -> CommonResult<String> {
        Metrics::text_output()
    }

    pub fn encode() -> CommonResult<Vec<MetricValue>> {
        let cm = FsContext::get_metrics();

        let mut metric_values = Vec::new();
        let metric_families = Metrics::registry().gather();
        for mf in metric_families {
            let name = mf.get_name().to_string();
            if !name.starts_with(Self::PREFIX) {
                break;
            }

            let metric_type = match mf.get_field_type() {
                prometheus::proto::MetricType::COUNTER => MetricType::Counter,
                prometheus::proto::MetricType::GAUGE => MetricType::Gauge,
                _ => MetricType::Gauge,
            };

            for metric in mf.get_metric() {
                let mut tags = HashMap::new();
                for label_pair in metric.get_label() {
                    tags.insert(
                        label_pair.get_name().to_string(),
                        label_pair.get_value().to_string(),
                    );
                }

                let value = match metric_type {
                    MetricType::Counter => {
                        if metric.has_counter() {
                            metric.get_counter().get_value()
                        } else {
                            0.0
                        }
                    }
                    MetricType::Gauge => {
                        if metric.has_gauge() {
                            metric.get_gauge().get_value()
                        } else {
                            0.0
                        }
                    }
                    MetricType::Histogram => {
                        if metric.has_histogram() {
                            metric.get_histogram().get_sample_count() as f64
                        } else {
                            0.0
                        }
                    }
                };

                let incr_value = {
                    let mut last_value = cm.last_value_map.entry(name.clone()).or_insert(0.0);
                    let incr_value = value - *last_value;
                    *last_value = value;
                    incr_value
                };

                if incr_value > 0f64 {
                    metric_values.push(MetricValue {
                        metric_type,
                        name: name.clone(),
                        value: incr_value,
                        tags,
                    });
                }
            }
        }

        Ok(metric_values)
    }
}

impl Debug for ClientMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientMetrics")
    }
}
