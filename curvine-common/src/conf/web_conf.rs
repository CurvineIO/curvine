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

use crate::conf::observability_conf::ObservabilityConf;
use crate::conf::ClusterConf;
use serde::{Deserialize, Serialize};

/// Standalone curvine-web admin console configuration.
///
/// This is intentionally separate from `master.web_port`, which belongs to the
/// master process and exposes Prometheus `/metrics` plus lightweight runtime
/// metadata.
///
/// Default port is 9000. New clusters should keep `web.port` at 9000 and
/// `master.web_port` at 9010. Legacy configs that still set `[master] web_port = 9000`
/// are auto-migrated in `ClusterConf::from`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WebConf {
    pub hostname: String,
    pub port: u16,
    pub observability: ObservabilityConf,
}

impl Default for WebConf {
    fn default() -> Self {
        Self {
            hostname: ClusterConf::DEFAULT_HOSTNAME.to_string(),
            port: ClusterConf::DEFAULT_WEB_PORT,
            observability: ObservabilityConf::default(),
        }
    }
}
