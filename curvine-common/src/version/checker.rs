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

use crate::version::{CompatibilityPolicy, CompatibilityResult, IncompatibilityReason, Version};

/// Version compatibility checker with range-based policy:
/// Compatible range: [min_version, master_version]
#[derive(Debug, Clone)]
pub struct VersionChecker {
    master_version: Version,
    policy: CompatibilityPolicy,
}

impl VersionChecker {
    pub fn new(master_version: Version, policy: CompatibilityPolicy) -> Self {
        Self {
            master_version,
            policy,
        }
    }

    /// Check compatibility between master and worker versions
    ///
    /// Rule: min_version <= worker_version <= master_version
    pub fn check_compatibility(&self, worker_version: &Version) -> CompatibilityResult {
        // Check if worker version is too old
        if worker_version < &self.policy.min_version {
            return CompatibilityResult::Incompatible(IncompatibilityReason::VersionTooOld {
                required: self.policy.min_version,
                actual: *worker_version,
            });
        }

        // Check if worker version is too new
        if worker_version > &self.master_version {
            return CompatibilityResult::Incompatible(IncompatibilityReason::VersionTooNew {
                max_supported: self.master_version,
                actual: *worker_version,
            });
        }

        CompatibilityResult::Compatible
    }

    pub fn master_version(&self) -> &Version {
        &self.master_version
    }

    pub fn policy(&self) -> &CompatibilityPolicy {
        &self.policy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_version_range_compatible() {
        let master_version = Version::from_str("2.5.0").unwrap();
        let policy = CompatibilityPolicy::new(Version::from_str("2.0.0").unwrap());
        let checker = VersionChecker::new(master_version, policy);

        // Within range - compatible
        let result = checker.check_compatibility(&Version::from_str("2.3.0").unwrap());
        assert!(result.is_compatible());

        // At min boundary - compatible
        let result = checker.check_compatibility(&Version::from_str("2.0.0").unwrap());
        assert!(result.is_compatible());

        // At max boundary - compatible
        let result = checker.check_compatibility(&Version::from_str("2.5.0").unwrap());
        assert!(result.is_compatible());
    }

    #[test]
    fn test_version_too_old() {
        let master_version = Version::from_str("2.5.0").unwrap();
        let policy = CompatibilityPolicy::new(Version::from_str("2.0.0").unwrap());
        let checker = VersionChecker::new(master_version, policy);

        // Below min - incompatible
        let result = checker.check_compatibility(&Version::from_str("1.9.0").unwrap());
        assert!(result.is_incompatible());

        if let CompatibilityResult::Incompatible(reason) = result {
            assert!(matches!(
                reason,
                IncompatibilityReason::VersionTooOld { .. }
            ));
        }
    }

    #[test]
    fn test_version_too_new() {
        let master_version = Version::from_str("2.5.0").unwrap();
        let policy = CompatibilityPolicy::new(Version::from_str("2.0.0").unwrap());
        let checker = VersionChecker::new(master_version, policy);

        // Above max - incompatible
        let result = checker.check_compatibility(&Version::from_str("2.6.0").unwrap());
        assert!(result.is_incompatible());

        if let CompatibilityResult::Incompatible(reason) = result {
            assert!(matches!(
                reason,
                IncompatibilityReason::VersionTooNew { .. }
            ));
        }
    }

    #[test]
    fn test_cross_major_versions() {
        let master_version = Version::from_str("2.5.0").unwrap();
        let policy = CompatibilityPolicy::new(Version::from_str("1.8.0").unwrap());
        let checker = VersionChecker::new(master_version, policy);

        // 1.9.0 is within range [1.8.0, 2.5.0] - compatible
        let result = checker.check_compatibility(&Version::from_str("1.9.0").unwrap());
        assert!(result.is_compatible());

        // 0.9.0 is below min 1.8.0 - incompatible
        let result = checker.check_compatibility(&Version::from_str("0.9.0").unwrap());
        assert!(result.is_incompatible());

        // 3.0.0 is above max 2.5.0 - incompatible
        let result = checker.check_compatibility(&Version::from_str("3.0.0").unwrap());
        assert!(result.is_incompatible());
    }
}
