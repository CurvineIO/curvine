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

//! Compatibility policy configuration shared by master and worker.
//!
//! Defaults are intentionally lenient: `mode = "diagnose"` with no version
//! bounds and no blocked versions, so old components are never rejected
//! without explicit configuration.

use curvine_model::{CompatibilityMode, CompatibilityPolicy};
use curvine_sys::version::ReleaseVersion;
use serde::{Deserialize, Serialize};

/// Compatibility enforcement configuration.
///
/// Only `mode`, `min_*` bounds and `blocked_versions` are operator-tunable;
/// the protocol version range is a product contract carried by the code
/// constants (`PROTOCOL_VERSION` / `MIN_PROTOCOL_VERSION`), so a misconfigured
/// deployment cannot silently widen the wire protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CompatibilityConf {
    /// Enforcement mode: `"diagnose"` (default, lenient) or `"enforce"`.
    pub mode: String,
    /// Lowest worker release version accepted by the master. Empty = not
    /// enforced.
    pub min_worker_version: String,
    /// Lowest client release version accepted. Empty = not enforced.
    pub min_client_version: String,
    /// Release versions explicitly rejected regardless of mode (emergency
    /// backstop). Empty by default.
    pub blocked_versions: Vec<String>,
}

impl Default for CompatibilityConf {
    fn default() -> Self {
        Self {
            mode: "diagnose".to_string(),
            min_worker_version: String::new(),
            min_client_version: String::new(),
            blocked_versions: vec![],
        }
    }
}

impl CompatibilityConf {
    /// Build a [`CompatibilityPolicy`] from this config. Unknown mode strings
    /// and unparseable version bounds degrade to the lenient default (diagnose
    /// / not enforced) instead of failing closed.
    pub fn to_policy(&self) -> CompatibilityPolicy {
        CompatibilityPolicy {
            mode: CompatibilityMode::parse(&self.mode).unwrap_or_default(),
            protocol_version: curvine_sys::version::PROTOCOL_VERSION,
            min_protocol_version: curvine_sys::version::MIN_PROTOCOL_VERSION,
            min_worker_version: parse_version_opt(&self.min_worker_version),
            min_client_version: parse_version_opt(&self.min_client_version),
            blocked_versions: self
                .blocked_versions
                .iter()
                .filter_map(|v| ReleaseVersion::parse(v).ok())
                .collect(),
        }
    }
}

fn parse_version_opt(s: &str) -> Option<ReleaseVersion> {
    if s.trim().is_empty() {
        None
    } else {
        ReleaseVersion::parse(s.trim()).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_conf_is_lenient_diagnose_without_bounds() {
        let policy = CompatibilityConf::default().to_policy();
        assert_eq!(policy.mode, CompatibilityMode::Diagnose);
        assert!(policy.min_worker_version.is_none());
        assert!(policy.min_client_version.is_none());
        assert!(policy.blocked_versions.is_empty());
        assert_eq!(
            policy.protocol_version,
            curvine_sys::version::PROTOCOL_VERSION
        );
    }

    #[test]
    fn explicit_enforce_and_bounds_are_honored() {
        let conf = CompatibilityConf {
            mode: "enforce".to_string(),
            min_worker_version: "0.2.0".to_string(),
            min_client_version: "0.1.5".to_string(),
            blocked_versions: vec!["0.2.5".to_string()],
        };
        let policy = conf.to_policy();
        assert_eq!(policy.mode, CompatibilityMode::Enforce);
        assert_eq!(
            policy.min_worker_version,
            Some(ReleaseVersion::parse("0.2.0").unwrap())
        );
        assert_eq!(
            policy.min_client_version,
            Some(ReleaseVersion::parse("0.1.5").unwrap())
        );
        assert_eq!(
            policy.blocked_versions,
            vec![ReleaseVersion::parse("0.2.5").unwrap()]
        );
    }

    #[test]
    fn invalid_mode_and_bounds_degrade_to_lenient() {
        let conf = CompatibilityConf {
            mode: "enforc".to_string(),
            min_worker_version: "not-a-version".to_string(),
            min_client_version: String::new(),
            blocked_versions: vec!["junk".to_string()],
        };
        let policy = conf.to_policy();
        assert_eq!(policy.mode, CompatibilityMode::Diagnose);
        assert!(policy.min_worker_version.is_none());
        assert!(policy.blocked_versions.is_empty());
    }
}
