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

use curvine_core_error::{err_box, CommonResult};
use curvine_runtime::common::{ByteUnit, DurationUnit};
use serde::{Deserialize, Deserializer, Serialize};

/// Runtime key for the dir-space scan switch (`cv admin config` / dir-space ops).
pub const DIR_SPACE_ENABLED_RUNTIME_KEY: &str = "master.dir_space_enabled";
/// Runtime key for the explicit dir-space path list.
pub const DIR_SPACE_TRACKED_PATHS_RUNTIME_KEY: &str = "master.dir_space_tracked_paths";

/// Directory-usage snapshot settings. Nested under `[master.dir_space]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DirSpaceConf {
    pub enabled: bool,
    pub global_scan_interval: String,
    #[serde(skip)]
    pub global_scan_interval_unit: DurationUnit,
    pub global_scan_timeout: String,
    #[serde(skip)]
    pub global_scan_timeout_unit: DurationUnit,
    pub tracked_scan_interval: String,
    #[serde(skip)]
    pub tracked_scan_interval_unit: DurationUnit,
    pub tracked_scan_timeout: String,
    #[serde(skip)]
    pub tracked_scan_timeout_unit: DurationUnit,
    pub max_inodes: u64,
    pub max_edges: u64,
    pub max_dirs: u64,
    pub memory_budget: String,
    #[serde(skip)]
    pub memory_budget_unit: ByteUnit,
    pub max_depth: u32,
    pub track_max_inodes: u64,
    pub track_max_edges: u64,
    pub track_memory_budget: String,
    #[serde(skip)]
    pub track_memory_budget_unit: ByteUnit,
    pub track_max_depth: u32,
    pub max_tracked_points: u32,
    pub max_inode_record_bytes: String,
    #[serde(skip)]
    pub max_inode_record_bytes_unit: ByteUnit,
    /// Parsed for compatibility. Mounts are not added to the tracked-path list.
    pub auto_track_mounts: bool,
    pub tracked_paths: Vec<String>,
    /// How long a Raft snapshot restore waits for dir-space scan leases to drop.
    /// This does not bound the RocksDB copy or the in-memory tree rebuild.
    pub restore_wait_timeout: String,
    #[serde(skip)]
    pub restore_wait_timeout_unit: DurationUnit,
    /// Normalized absolute tracked paths. `/` selects the global scan.
    #[serde(skip)]
    pub tracked_paths_normalized: Vec<String>,
}

impl DirSpaceConf {
    pub fn init(&mut self) -> CommonResult<()> {
        self.global_scan_interval_unit = DurationUnit::from_str(&self.global_scan_interval)?;
        self.global_scan_timeout_unit = DurationUnit::from_str(&self.global_scan_timeout)?;
        self.tracked_scan_interval_unit = DurationUnit::from_str(&self.tracked_scan_interval)?;
        self.tracked_scan_timeout_unit = DurationUnit::from_str(&self.tracked_scan_timeout)?;
        self.restore_wait_timeout_unit = DurationUnit::from_str(&self.restore_wait_timeout)?;
        self.memory_budget_unit = ByteUnit::from_str(&self.memory_budget)?;
        self.track_memory_budget_unit = ByteUnit::from_str(&self.track_memory_budget)?;
        self.max_inode_record_bytes_unit = ByteUnit::from_str(&self.max_inode_record_bytes)?;
        self.validate()?;
        Ok(())
    }

    fn validate(&mut self) -> CommonResult<()> {
        if self.global_scan_interval_unit.as_millis() == 0 {
            return err_box!("dir_space.global_scan_interval must be greater than zero");
        }
        if self.global_scan_timeout_unit.as_millis() == 0 {
            return err_box!("dir_space.global_scan_timeout must be greater than zero");
        }
        if self.tracked_scan_interval_unit.as_millis() == 0 {
            return err_box!("dir_space.tracked_scan_interval must be greater than zero");
        }
        if self.tracked_scan_timeout_unit.as_millis() == 0 {
            return err_box!("dir_space.tracked_scan_timeout must be greater than zero");
        }
        if self.restore_wait_timeout_unit.as_millis() == 0 {
            return err_box!("dir_space.restore_wait_timeout must be greater than zero");
        }

        if self.global_scan_timeout_unit > self.global_scan_interval_unit {
            return err_box!(
                "dir_space.global_scan_timeout must not be greater than dir_space.global_scan_interval"
            );
        }
        if self.tracked_scan_timeout_unit > self.tracked_scan_interval_unit {
            return err_box!(
                "dir_space.tracked_scan_timeout must not be greater than dir_space.tracked_scan_interval"
            );
        }

        for field in [
            ("dir_space.max_inodes", self.max_inodes),
            ("dir_space.max_edges", self.max_edges),
            ("dir_space.max_dirs", self.max_dirs),
            ("dir_space.track_max_inodes", self.track_max_inodes),
            ("dir_space.track_max_edges", self.track_max_edges),
        ] {
            if field.1 == 0 {
                return err_box!("{} must be greater than zero", field.0);
            }
        }
        if self.max_depth == 0 {
            return err_box!("dir_space.max_depth must be greater than zero");
        }
        if self.track_max_depth == 0 {
            return err_box!("dir_space.track_max_depth must be greater than zero");
        }
        if self.max_tracked_points == 0 {
            return err_box!("dir_space.max_tracked_points must be greater than zero");
        }
        if self.memory_budget_unit.as_byte() == 0 {
            return err_box!("dir_space.memory_budget must be greater than zero");
        }
        if self.track_memory_budget_unit.as_byte() == 0 {
            return err_box!("dir_space.track_memory_budget must be greater than zero");
        }
        if self.max_inode_record_bytes_unit.as_byte() == 0 {
            return err_box!("dir_space.max_inode_record_bytes must be greater than zero");
        }

        self.tracked_paths_normalized =
            Self::normalize_tracked_path_list(&self.tracked_paths, self.max_tracked_points)?;
        Ok(())
    }

    /// Normalize an explicit dir-space path list.
    ///
    /// Empty entries are dropped. `/` is kept and selects the global scan.
    /// Other entries are absolute paths without a trailing slash. Duplicates
    /// collapse. More entries than `max_points` is an error.
    pub fn normalize_tracked_path_list(
        raw_paths: &[String],
        max_points: u32,
    ) -> CommonResult<Vec<String>> {
        let mut normalized = Vec::new();
        for raw in raw_paths {
            let path = raw.trim();
            if path.is_empty() {
                continue;
            }
            if !path.starts_with('/') {
                return err_box!("dir_space.tracked_paths entries must be absolute paths");
            }
            if path.contains('*') || path.contains('?') || path.contains('[') {
                return err_box!("dir_space.tracked_paths must not contain glob characters");
            }
            let mut norm = path.to_string();
            while norm.len() > 1 && norm.ends_with('/') {
                norm.pop();
            }
            if !normalized.iter().any(|existing| existing == &norm) {
                normalized.push(norm);
            }
        }
        if normalized.len() > max_points as usize {
            return err_box!("dir_space.tracked_paths exceeds dir_space.max_tracked_points");
        }
        Ok(normalized)
    }

    /// Add or drop one statistics path. Canonicalizes like the file config.
    /// Adding `/` is rejected. The master enforces `max_tracked_points`
    /// when the list is stored.
    pub fn with_tracked_path(
        paths: &[String],
        path: &str,
        tracked: bool,
    ) -> CommonResult<Vec<String>> {
        let path = path.trim();
        if path.is_empty() {
            return err_box!("dir-space path is required");
        }
        let canonical = Self::normalize_tracked_path_list(&[path.to_string()], u32::MAX)?;
        let Some(path) = canonical.into_iter().next() else {
            return err_box!("dir-space path is required");
        };
        if tracked && path == "/" {
            return err_box!("dir-space cannot track the root directory");
        }
        let mut next = paths.to_vec();
        if tracked {
            if !next.iter().any(|existing| existing == &path) {
                next.push(path);
            }
        } else {
            next.retain(|existing| existing != &path);
        }
        Self::normalize_tracked_path_list(&next, u32::MAX)
    }

    pub fn global_scan_interval_ms(&self) -> u64 {
        self.global_scan_interval_unit.as_millis()
    }

    pub fn global_scan_timeout_ms(&self) -> u64 {
        self.global_scan_timeout_unit.as_millis()
    }

    pub fn tracked_scan_interval_ms(&self) -> u64 {
        self.tracked_scan_interval_unit.as_millis()
    }

    pub fn tracked_scan_timeout_ms(&self) -> u64 {
        self.tracked_scan_timeout_unit.as_millis()
    }

    pub fn restore_wait_timeout_ms(&self) -> u64 {
        self.restore_wait_timeout_unit.as_millis()
    }

    pub fn memory_budget_bytes(&self) -> u64 {
        self.memory_budget_unit.as_byte()
    }

    pub fn track_memory_budget_bytes(&self) -> u64 {
        self.track_memory_budget_unit.as_byte()
    }

    pub fn max_inode_record_bytes(&self) -> u64 {
        self.max_inode_record_bytes_unit.as_byte()
    }
}

impl Default for DirSpaceConf {
    fn default() -> Self {
        let mut conf = Self {
            enabled: false,
            global_scan_interval: "10m".to_string(),
            global_scan_interval_unit: Default::default(),
            global_scan_timeout: "5m".to_string(),
            global_scan_timeout_unit: Default::default(),
            tracked_scan_interval: "2m".to_string(),
            tracked_scan_interval_unit: Default::default(),
            tracked_scan_timeout: "90s".to_string(),
            tracked_scan_timeout_unit: Default::default(),
            max_inodes: 5_000_000,
            max_edges: 10_000_000,
            max_dirs: 1_000_000,
            memory_budget: "768MB".to_string(),
            memory_budget_unit: Default::default(),
            max_depth: 1024,
            track_max_inodes: 1_000_000,
            track_max_edges: 2_000_000,
            track_memory_budget: "256MB".to_string(),
            track_memory_budget_unit: Default::default(),
            track_max_depth: 1024,
            max_tracked_points: 128,
            max_inode_record_bytes: "16MB".to_string(),
            max_inode_record_bytes_unit: Default::default(),
            auto_track_mounts: false,
            tracked_paths: Vec::new(),
            restore_wait_timeout: "30s".to_string(),
            restore_wait_timeout_unit: Default::default(),
            tracked_paths_normalized: Vec::new(),
        };
        conf.init().expect("dir-space defaults must be valid");
        conf
    }
}

impl DirSpaceConf {
    /// A partial `[master.dir_space]` table inherits these defaults.
    /// Explicit false, zero and list values in the table are kept.
    pub fn deserialize_with_defaults<'de, D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let overrides = toml::Table::deserialize(deserializer)?;
        let mut defaults =
            toml::Value::try_from(Self::default()).map_err(serde::de::Error::custom)?;
        defaults
            .as_table_mut()
            .expect("DirSpaceConf must serialize to a table")
            .extend(overrides);
        defaults.try_into().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_disabled_with_design_values() {
        let conf = DirSpaceConf::default();
        assert!(!conf.enabled);
        assert_eq!(conf.global_scan_interval, "10m");
        assert_eq!(conf.global_scan_timeout, "5m");
        assert_eq!(conf.tracked_scan_interval, "2m");
        assert_eq!(conf.tracked_scan_timeout, "90s");
        assert_eq!(conf.max_inodes, 5_000_000);
        assert_eq!(conf.max_edges, 10_000_000);
        assert_eq!(conf.max_dirs, 1_000_000);
        assert_eq!(conf.memory_budget, "768MB");
        assert_eq!(conf.max_depth, 1024);
        assert_eq!(conf.track_max_inodes, 1_000_000);
        assert_eq!(conf.track_max_edges, 2_000_000);
        assert_eq!(conf.track_memory_budget, "256MB");
        assert_eq!(conf.track_max_depth, 1024);
        assert_eq!(conf.max_tracked_points, 128);
        assert_eq!(conf.max_inode_record_bytes, "16MB");
        assert!(!conf.auto_track_mounts);
        assert_eq!(conf.restore_wait_timeout, "30s");
        assert_eq!(conf.global_scan_interval_ms(), 600_000);
        assert_eq!(conf.memory_budget_bytes(), ByteUnit::mb(768).as_byte());
    }

    #[test]
    fn zero_limits_rejected() {
        let mut conf = DirSpaceConf {
            max_inodes: 0,
            ..Default::default()
        };
        let err = conf.init().unwrap_err();
        assert!(err.to_string().contains("dir_space.max_inodes"));
    }

    #[test]
    fn invalid_duration_rejected() {
        let mut conf = DirSpaceConf {
            global_scan_interval: "not-a-duration".to_string(),
            ..Default::default()
        };
        assert!(conf.init().is_err());
    }

    #[test]
    fn global_timeout_must_not_exceed_interval() {
        let mut conf = DirSpaceConf {
            global_scan_interval: "1m".to_string(),
            global_scan_timeout: "2m".to_string(),
            ..Default::default()
        };
        let err = conf.init().unwrap_err();
        assert!(err.to_string().contains("dir_space.global_scan_timeout"));
    }

    #[test]
    fn relative_tracked_path_rejected() {
        let mut conf = DirSpaceConf {
            tracked_paths: vec!["relative/path".to_string()],
            ..Default::default()
        };
        let err = conf.init().unwrap_err();
        assert!(err
            .to_string()
            .contains("dir_space.tracked_paths entries must be absolute"));
    }

    #[test]
    fn tracked_paths_deduped_and_root_kept() {
        let mut conf = DirSpaceConf {
            tracked_paths: vec![
                "/".to_string(),
                "/data/".to_string(),
                "/data".to_string(),
                "/other".to_string(),
            ],
            ..Default::default()
        };
        conf.init().unwrap();
        assert_eq!(
            conf.tracked_paths_normalized,
            vec!["/".to_string(), "/data".to_string(), "/other".to_string()]
        );
    }

    #[test]
    fn with_tracked_path_adds_and_removes_canonical_paths() {
        let added =
            DirSpaceConf::with_tracked_path(&["/data".to_string()], "/track/", true).unwrap();
        assert_eq!(added, vec!["/data".to_string(), "/track".to_string()]);
        let again = DirSpaceConf::with_tracked_path(&added, "/track", true).unwrap();
        assert_eq!(again, added);
        let removed = DirSpaceConf::with_tracked_path(&added, "/track/", false).unwrap();
        assert_eq!(removed, vec!["/data".to_string()]);
        assert!(DirSpaceConf::with_tracked_path(&[], "relative", true).is_err());
        assert!(DirSpaceConf::with_tracked_path(&[], "  ", true).is_err());
        assert!(DirSpaceConf::with_tracked_path(&[], "/", true)
            .unwrap_err()
            .to_string()
            .contains("root"));
        assert!(DirSpaceConf::with_tracked_path(&[], "//", true).is_err());
        let without_root =
            DirSpaceConf::with_tracked_path(&["/".to_string(), "/data".to_string()], "/", false)
                .unwrap();
        assert_eq!(without_root, vec!["/data".to_string()]);
    }
}
