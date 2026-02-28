// Copyright 2025 OPPO.

use curvine_common::conf::ClusterConf;
use curvine_common::version::{CompatibilityPolicy, Version, VersionChecker};
use std::str::FromStr;

#[test]
fn test_version_checker_worker_compatible() {
    let master_version = Version::from_str("2.5.0").unwrap();
    let min_version = Version::from_str("2.0.0").unwrap();
    let policy = CompatibilityPolicy::new(min_version);
    let checker = VersionChecker::new(master_version, policy);

    assert!(checker.check_compatibility(&Version::from_str("2.3.0").unwrap()).is_compatible());
    assert!(checker.check_compatibility(&Version::from_str("2.0.0").unwrap()).is_compatible());
    assert!(checker.check_compatibility(&Version::from_str("2.5.0").unwrap()).is_compatible());
}

#[test]
fn test_version_checker_worker_too_old() {
    let master_version = Version::from_str("2.5.0").unwrap();
    let min_version = Version::from_str("2.0.0").unwrap();
    let policy = CompatibilityPolicy::new(min_version);
    let checker = VersionChecker::new(master_version, policy);

    assert!(checker.check_compatibility(&Version::from_str("1.9.0").unwrap()).is_incompatible());
}

#[test]
fn test_version_checker_worker_too_new() {
    let master_version = Version::from_str("2.5.0").unwrap();
    let min_version = Version::from_str("2.0.0").unwrap();
    let policy = CompatibilityPolicy::new(min_version);
    let checker = VersionChecker::new(master_version, policy);

    assert!(checker.check_compatibility(&Version::from_str("2.6.0").unwrap()).is_incompatible());
    assert!(checker.check_compatibility(&Version::from_str("3.0.0").unwrap()).is_incompatible());
}

#[test]
fn test_client_looser_policy() {
    let min_version = Version::from_str("1.0.0").unwrap();
    let policy = CompatibilityPolicy::new(min_version);
    let checker = VersionChecker::new(Version::new(u32::MAX, u32::MAX, u32::MAX), policy);

    assert!(checker.check_compatibility(&Version::from_str("1.5.0").unwrap()).is_compatible());
    assert!(checker.check_compatibility(&Version::from_str("3.0.0").unwrap()).is_compatible());
    assert!(checker.check_compatibility(&Version::from_str("0.9.0").unwrap()).is_incompatible());
}

#[test]
fn test_default_configuration() {
    let conf = ClusterConf::default();
    assert_eq!(conf.master.min_worker_version, "0.1.0");
    assert_eq!(conf.master.min_client_version, "0.1.0");
}
