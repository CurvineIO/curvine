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

use super::compatibility::{CompatibilityPolicy, CompatibilityResult, IncompatibilityReason};
use super::types::Version;

/// Version compatibility checker
///
/// This component checks if a worker/client version is compatible with the master version
/// based on the configured compatibility policy.
#[derive(Debug, Clone)]
pub struct VersionChecker {
    /// Master version (used as upper bound for workers)
    master_version: Version,
    /// Compatibility policy
    policy: CompatibilityPolicy,
}

impl VersionChecker {
    /// Create a new version checker
    pub fn new(master_version: Version, policy: CompatibilityPolicy) -> Self {
        Self {
            master_version,
            policy,
        }
    }

    /// Check if a worker/client version is compatible
    ///
    /// Compatibility rule: min_version <= worker_version <= master_version
    ///
    /// # Arguments
    ///
    /// * `version` - The version to check
    ///
    /// # Returns
    ///
    /// * `CompatibilityResult::Compatible` if the version is compatible
    /// * `CompatibilityResult::Incompatible(reason)` if the version is incompatible
    pub fn check_compatibility(&self, version: &Version) -> CompatibilityResult {
        // Check minimum version (lower bound)
        if version < &self.policy.min_version {
            return CompatibilityResult::Incompatible(IncompatibilityReason::VersionTooOld {
                required: self.policy.min_version.clone(),
                actual: version.clone(),
            });
        }

        // Check maximum version (upper bound)
        if version > &self.master_version {
            return CompatibilityResult::Incompatible(IncompatibilityReason::VersionTooNew {
                max_supported: self.master_version.clone(),
                actual: version.clone(),
            });
        }

        CompatibilityResult::Compatible
    }

    /// Get the master version
    pub fn master_version(&self) -> &Version {
        &self.master_version
    }

    /// Get the compatibility policy
    pub fn policy(&self) -> &CompatibilityPolicy {
        &self.policy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_version_checker_basic() {
        let master = Version::new(2, 5, 0);
        let policy = CompatibilityPolicy::new(Version::new(2, 0, 0));
        let checker = VersionChecker::new(master, policy);

        // Compatible versions
        assert!(checker.check_compatibility(&Version::new(2, 0, 0)).is_compatible());
        assert!(checker.check_compatibility(&Version::new(2, 3, 0)).is_compatible());
        assert!(checker.check_compatibility(&Version::new(2, 5, 0)).is_compatible());

        // Too old
        assert!(checker.check_compatibility(&Version::new(1, 9, 0)).is_incompatible());

        // Too new
        assert!(checker.check_compatibility(&Version::new(2, 6, 0)).is_incompatible());
        assert!(checker.check_compatibility(&Version::new(3, 0, 0)).is_incompatible());
    }

    #[test]
    fn test_version_checker_cross_major_versions() {
        let master = Version::new(2, 5, 0);
        let policy = CompatibilityPolicy::new(Version::new(1, 8, 0));
        let checker = VersionChecker::new(master, policy);

        // 1.x versions
        assert!(checker.check_compatibility(&Version::new(1, 8, 0)).is_compatible());
        assert!(checker.check_compatibility(&Version::new(1, 9, 0)).is_compatible());
        assert!(checker.check_compatibility(&Version::new(1, 7, 0)).is_incompatible());

        // 2.x versions
        assert!(checker.check_compatibility(&Version::new(2, 0, 0)).is_compatible());
        assert!(checker.check_compatibility(&Version::new(2, 5, 0)).is_compatible());
        assert!(checker.check_compatibility(&Version::new(2, 6, 0)).is_incompatible());
    }

    #[test]
    fn test_version_checker_boundary_values() {
        let master = Version::new(2, 5, 0);
        let policy = CompatibilityPolicy::new(Version::new(2, 0, 0));
        let checker = VersionChecker::new(master, policy);

        // Exact boundaries
        assert!(checker.check_compatibility(&Version::new(2, 0, 0)).is_compatible());
        assert!(checker.check_compatibility(&Version::new(2, 5, 0)).is_compatible());

        // One patch off boundaries
        assert!(checker.check_compatibility(&Version::new(1, 9, 999)).is_incompatible());
        assert!(checker.check_compatibility(&Version::new(2, 5, 1)).is_incompatible());
    }

    #[test]
    fn test_version_checker_from_str() {
        let master = Version::from_str("2.5.0").unwrap();
        let policy = CompatibilityPolicy::new(Version::from_str("2.0.0").unwrap());
        let checker = VersionChecker::new(master, policy);

        assert!(checker
            .check_compatibility(&Version::from_str("2.3.0").unwrap())
            .is_compatible());
        assert!(checker
            .check_compatibility(&Version::from_str("1.9.0").unwrap())
            .is_incompatible());
        assert!(checker
            .check_compatibility(&Version::from_str("2.6.0").unwrap())
            .is_incompatible());
    }

    #[test]
    fn test_version_checker_getters() {
        let master = Version::new(2, 5, 0);
        let policy = CompatibilityPolicy::new(Version::new(2, 0, 0));
        let checker = VersionChecker::new(master.clone(), policy.clone());

        assert_eq!(checker.master_version(), &master);
        assert_eq!(checker.policy().min_version, policy.min_version);
    }

    #[test]
    fn test_version_checker_client_looser_policy() {
        // Client uses looser policy with very large upper bound
        let policy = CompatibilityPolicy::new(Version::new(1, 0, 0));
        let checker = VersionChecker::new(Version::new(u32::MAX, u32::MAX, u32::MAX), policy);

        // Old clients
        assert!(checker.check_compatibility(&Version::new(1, 0, 0)).is_compatible());
        assert!(checker.check_compatibility(&Version::new(1, 5, 0)).is_compatible());

        // New clients
        assert!(checker.check_compatibility(&Version::new(2, 0, 0)).is_compatible());
        assert!(checker.check_compatibility(&Version::new(3, 0, 0)).is_compatible());
        assert!(checker.check_compatibility(&Version::new(100, 0, 0)).is_compatible());

        // Too old
        assert!(checker.check_compatibility(&Version::new(0, 9, 0)).is_incompatible());
    }

    #[test]
    fn test_version_checker_incompatibility_reasons() {
        let master = Version::new(2, 5, 0);
        let policy = CompatibilityPolicy::new(Version::new(2, 0, 0));
        let checker = VersionChecker::new(master, policy);

        // Test VersionTooOld reason
        match checker.check_compatibility(&Version::new(1, 9, 0)) {
            CompatibilityResult::Incompatible(reason) => match reason {
                IncompatibilityReason::VersionTooOld { required, actual } => {
                    assert_eq!(required, Version::new(2, 0, 0));
                    assert_eq!(actual, Version::new(1, 9, 0));
                }
                _ => panic!("Expected VersionTooOld"),
            },
            _ => panic!("Expected Incompatible"),
        }

        // Test VersionTooNew reason
        match checker.check_compatibility(&Version::new(2, 6, 0)) {
            CompatibilityResult::Incompatible(reason) => match reason {
                IncompatibilityReason::VersionTooNew {
                    max_supported,
                    actual,
                } => {
                    assert_eq!(max_supported, Version::new(2, 5, 0));
                    assert_eq!(actual, Version::new(2, 6, 0));
                }
                _ => panic!("Expected VersionTooNew"),
            },
            _ => panic!("Expected Incompatible"),
        }
    }

    #[test]
    fn test_rolling_upgrade_scenario() {
        // Simulate a rolling upgrade scenario

        // Phase 1: Initial state (Master 2.3.0, min 2.0.0)
        let mut master = Version::new(2, 3, 0);
        let min = Version::new(2, 0, 0);
        let mut checker = VersionChecker::new(master, CompatibilityPolicy::new(min));

        let worker_2_1 = Version::new(2, 1, 0);
        let worker_2_3 = Version::new(2, 3, 0);

        assert!(checker.check_compatibility(&worker_2_1).is_compatible());
        assert!(checker.check_compatibility(&worker_2_3).is_compatible());

        // Phase 2: Upgrade master to 2.5.0
        master = Version::new(2, 5, 0);
        checker = VersionChecker::new(master, CompatibilityPolicy::new(min));

        // Old workers still compatible
        assert!(checker.check_compatibility(&worker_2_1).is_compatible());
        assert!(checker.check_compatibility(&worker_2_3).is_compatible());

        // New worker also compatible
        let worker_2_5 = Version::new(2, 5, 0);
        assert!(checker.check_compatibility(&worker_2_5).is_compatible());

        // Phase 3: After all workers upgraded, tighten min_version
        let new_min = Version::new(2, 5, 0);
        checker = VersionChecker::new(master, CompatibilityPolicy::new(new_min));

        // Old workers now rejected
        assert!(checker.check_compatibility(&worker_2_1).is_incompatible());
        assert!(checker.check_compatibility(&worker_2_3).is_incompatible());

        // Only new workers accepted
        assert!(checker.check_compatibility(&worker_2_5).is_compatible());
    }
}
