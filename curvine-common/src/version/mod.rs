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

//! Version management module for curvine
//!
//! This module provides version compatibility checking between master and worker nodes.
//! Versions follow Semantic Versioning 2.0.
//!
//! # Version Format
//!
//! Versions are represented as: `major.minor.patch`
//!
//! Example: "1.2.3"
//!
//! # Compatibility Policy
//!
//! - **Range-based**: Worker version must be within [min_version, master_version]
//! - min_version is configurable
//! - master_version is the current master's version
//!
//! # Example
//!
//! ```rust
//! use curvine_common::version::{Version, VersionChecker, CompatibilityPolicy};
//! use std::str::FromStr;
//!
//! let master_version = Version::from_str("2.5.0").unwrap();
//! let policy = CompatibilityPolicy::new(Version::from_str("2.0.0").unwrap());
//! let checker = VersionChecker::new(master_version, policy);
//!
//! let worker_version = Version::from_str("2.3.0").unwrap();
//! let result = checker.check_compatibility(&worker_version);
//!
//! assert!(result.is_compatible());
//! ```

mod checker;
mod compatibility;
mod types;

pub use checker::VersionChecker;
pub use compatibility::{CompatibilityPolicy, CompatibilityResult, IncompatibilityReason};
pub use types::Version;

// Re-export for backward compatibility with the old version module
// This includes the auto-generated version info from build.rs
include!(concat!(env!("OUT_DIR"), "/version.rs"));
