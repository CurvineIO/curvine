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

use crate::version::Version;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Compatibility policy: support versions within a configurable range
/// Range: [min_version, master_version]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompatibilityPolicy {
    /// Minimum supported worker version
    pub min_version: Version,
}

impl CompatibilityPolicy {
    pub fn new(min_version: Version) -> Self {
        Self { min_version }
    }
}

impl Default for CompatibilityPolicy {
    fn default() -> Self {
        // Default: support workers from 0.1.0
        Self::new(Version::new(0, 1, 0))
    }
}

impl fmt::Display for CompatibilityPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Range(min: {})", self.min_version)
    }
}

/// Compatibility check result
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompatibilityResult {
    /// Worker is compatible
    Compatible,

    /// Worker is incompatible (with detailed reason)
    Incompatible(IncompatibilityReason),
}

impl CompatibilityResult {
    pub fn is_compatible(&self) -> bool {
        matches!(self, Self::Compatible)
    }

    pub fn is_incompatible(&self) -> bool {
        matches!(self, Self::Incompatible(_))
    }
}

/// Detailed reason for incompatibility
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IncompatibilityReason {
    /// Worker version is too old (below minimum supported version)
    VersionTooOld { required: Version, actual: Version },

    /// Worker version is too new (above master version)
    VersionTooNew {
        max_supported: Version,
        actual: Version,
    },
}

impl fmt::Display for IncompatibilityReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VersionTooOld { required, actual } => {
                write!(
                    f,
                    "Worker version too old: required >= {}, actual {}",
                    required, actual
                )
            }
            Self::VersionTooNew {
                max_supported,
                actual,
            } => {
                write!(
                    f,
                    "Worker version too new: max supported {}, actual {}",
                    max_supported, actual
                )
            }
        }
    }
}

impl IncompatibilityReason {
    /// Get a suggestion message for how to fix the incompatibility
    pub fn suggestion(&self) -> String {
        match self {
            Self::VersionTooOld { required, .. } => {
                format!("Please upgrade worker to version {} or later", required)
            }
            Self::VersionTooNew { max_supported, .. } => {
                format!(
                    "Please upgrade master to support newer workers, or downgrade worker to {}",
                    max_supported
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compatibility_result() {
        let compatible = CompatibilityResult::Compatible;
        assert!(compatible.is_compatible());
        assert!(!compatible.is_incompatible());

        let incompatible =
            CompatibilityResult::Incompatible(IncompatibilityReason::VersionTooOld {
                required: Version::new(2, 0, 0),
                actual: Version::new(1, 9, 0),
            });
        assert!(!incompatible.is_compatible());
        assert!(incompatible.is_incompatible());
    }

    #[test]
    fn test_incompatibility_reason_display() {
        let reason = IncompatibilityReason::VersionTooOld {
            required: Version::new(2, 0, 0),
            actual: Version::new(1, 9, 0),
        };
        assert_eq!(
            reason.to_string(),
            "Worker version too old: required >= 2.0.0, actual 1.9.0"
        );

        let suggestion = reason.suggestion();
        assert!(suggestion.contains("2.0.0"));
    }

    #[test]
    fn test_default_policy() {
        let policy = CompatibilityPolicy::default();
        assert_eq!(policy.min_version.major, 0);
        assert_eq!(policy.min_version.minor, 1);
        assert_eq!(policy.min_version.patch, 0);
    }
}
