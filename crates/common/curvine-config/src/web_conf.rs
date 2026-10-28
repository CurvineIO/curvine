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

use crate::{ClusterConf, ObservabilityConf};
use serde::{Deserialize, Serialize};

/// Standalone curvine-web admin console configuration.
///
/// This is intentionally separate from `master.web_port`, which belongs to the
/// master process and exposes only Prometheus `/metrics`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WebConf {
    pub hostname: String,
    pub port: u16,
    pub observability: ObservabilityConf,
    /// Directory scanned for imported cluster configurations (`*.toml`).
    ///
    /// Empty means "derive it from the location of the config file" — see
    /// `curvine-web`'s `ClusterRegistry::default_dir`.
    pub clusters_dir: String,
    /// Upper bound on simultaneously registered clusters (imported + default).
    ///
    /// Each cluster holds its own client and in-memory observability TSDB, so
    /// this is a memory guard as much as a policy limit.
    pub max_clusters: usize,
    /// Human-readable name for the local (default) cluster shown in the
    /// cluster switcher. Empty means "use the cluster id".
    pub display_name: String,
}

impl Default for WebConf {
    fn default() -> Self {
        Self {
            hostname: ClusterConf::DEFAULT_HOSTNAME.to_string(),
            port: ClusterConf::DEFAULT_WEB_PORT,
            observability: ObservabilityConf::default(),
            clusters_dir: String::new(),
            max_clusters: ClusterConf::DEFAULT_MAX_CLUSTERS,
            display_name: String::new(),
        }
    }
}
