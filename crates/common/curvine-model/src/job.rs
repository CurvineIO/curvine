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

use crate::state::{MountInfo, StorageType, TtlAction, WorkerAddress};
use curvine_runtime::common::ByteUnit;
use num_enum::{FromPrimitive, IntoPrimitive};
use serde::{Deserialize, Serialize};

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    IntoPrimitive,
    FromPrimitive,
    Serialize,
    Deserialize,
    Default,
)]
#[repr(i8)]
pub enum JobTaskState {
    #[default]
    UNKNOWN = 0,
    Pending = 1,
    Loading = 2,
    Completed = 3,
    Failed = 4,
    Canceled = 5,
    /// Some sub-tasks succeeded and some failed (transfer load jobs).
    PartialSuccess = 6,
}

impl JobTaskState {
    pub fn is_finish(&self) -> bool {
        matches!(
            self,
            JobTaskState::Completed
                | JobTaskState::Failed
                | JobTaskState::Canceled
                | JobTaskState::PartialSuccess
        )
    }

    pub fn is_running(&self) -> bool {
        matches!(self, JobTaskState::Pending | JobTaskState::Loading)
    }
}

/// Outcome of a load submit or status query: `job_id` / `target_path` identify the
/// job; `state` is the current phase.
///
/// **Callers must not assume this struct always reflects the latest
/// [`LoadJobCommand`]:** concurrent submits for the same path are defined by the
/// server as **first submitter wins**; a later `Ok` may describe the in-flight job
/// only (paths and state), not the superseded request’s options.
#[derive(Debug, Clone)]
pub struct LoadJobResult {
    pub job_id: String,
    pub target_path: String,
    pub state: JobTaskState,
}

impl LoadJobResult {
    pub fn with_job(job: &LoadJobInfo) -> Self {
        Self {
            job_id: job.job_id.to_owned(),
            target_path: job.target_path.to_owned(),
            state: JobTaskState::Pending,
        }
    }

    pub fn with_state(job: &LoadJobInfo, state: JobTaskState) -> Self {
        Self {
            job_id: job.job_id.to_owned(),
            target_path: job.target_path.to_owned(),
            state,
        }
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    IntoPrimitive,
    FromPrimitive,
    Serialize,
    Deserialize,
)]
#[repr(i32)]
pub enum JobTaskType {
    #[num_enum(default)]
    Load = 1,
    Export = 2,
}

#[derive(Default)]
pub struct JobStatus {
    pub job_id: String,
    pub state: JobTaskState,
    pub source_path: String,
    pub target_path: String,
    pub progress: JobTaskProgress,
}

impl JobStatus {
    /// Returns a formatted progress string with percentage and byte counts
    pub fn progress_string(&self, show_bar: bool) -> String {
        self.progress.progress_string(show_bar)
    }
}

/// What a load does with files that are already in Curvine.
///
/// `Always` is the planning behaviour of the original load path: every planned
/// file is copied again. It also disables the whole-source fast path that used to
/// complete a re-run of one already-cached file immediately, so that shortcut now
/// only applies to the skipping modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadRefresh {
    /// Copy every file, whatever Curvine already holds.
    #[default]
    Always,
    /// Skip a file only when the cached copy has the same size and UFS mtime.
    IfNewer,
    /// Skip a file whenever a complete copy exists, without comparing it to UFS.
    SkipCached,
}

impl LoadRefresh {
    /// Parses the CLI / option spellings: exactly `always`, `if-newer` and
    /// `skip-cached`. There are no aliases -- the flag is new, so every
    /// accepted spelling is one a user could have read in the help text.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "always" => Some(Self::Always),
            "if-newer" => Some(Self::IfNewer),
            "skip-cached" => Some(Self::SkipCached),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::IfNewer => "if-newer",
            Self::SkipCached => "skip-cached",
        }
    }
}

impl std::fmt::Display for LoadRefresh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for LoadRefresh {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value).ok_or_else(|| {
            format!(
                "invalid refresh mode '{}': expected always, if-newer or skip-cached",
                value
            )
        })
    }
}

/// Whether an existing Curvine copy means `source` does not need loading.
///
/// The comparison is size plus the UFS mtime recorded on the cached copy, which
/// is what `cv_valid` checks; `SkipCached` deliberately does not look at either,
/// but it still requires Curvine *data*: a complete inode may be metadata only
/// (its data freed to UFS), and loading it is the whole point of running the job.
pub fn load_target_is_current(
    refresh: LoadRefresh,
    source: &crate::file_status::FileStatus,
    target: Option<&crate::file_status::FileStatus>,
) -> bool {
    let Some(target) = target else {
        return false;
    };
    if target.is_dir || !target.is_complete {
        return false;
    }

    match refresh {
        LoadRefresh::Always => false,
        LoadRefresh::SkipCached => target.cv_exists(),
        LoadRefresh::IfNewer => target.cv_valid(Some(source)),
    }
}

/// Whether a "not current" verdict rests on a source mtime the listing could not
/// tell: the target is a complete, valid copy of the same size, but its recorded
/// UFS mtime differs from the listing's. Object listings may omit the
/// last-modified time (OpenDAL then reports 0), in which case one fresh
/// `get_status` decides instead of re-copying an unchanged file.
pub fn needs_source_status_refresh(
    source: &crate::file_status::FileStatus,
    target: Option<&crate::file_status::FileStatus>,
) -> bool {
    let Some(target) = target else {
        return false;
    };
    !target.is_dir
        && target.is_complete
        && target.cv_valid(None)
        && target.len == source.len
        && target.storage_policy.ufs_mtime != source.mtime
}

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct LoadJobCommand {
    pub source_path: String,
    pub target_path: Option<String>,
    pub replicas: Option<i32>,
    pub block_size: Option<i64>,
    pub storage_type: Option<StorageType>,
    pub ttl_ms: Option<i64>,
    pub ttl_action: Option<TtlAction>,
    pub overwrite: Option<bool>,
    /// What to do with files that are already cached; `None` keeps `Always`.
    pub refresh: Option<LoadRefresh>,
}

impl LoadJobCommand {
    pub fn builder(source_path: impl Into<String>) -> LoadJobCommandBuilder {
        LoadJobCommandBuilder::new(source_path).overwrite(true)
    }
}

#[derive(Default)]
pub struct LoadJobCommandBuilder {
    source_path: String,
    target_path: Option<String>,
    replicas: Option<i32>,
    block_size: Option<i64>,
    storage_type: Option<StorageType>,
    ttl_ms: Option<i64>,
    ttl_action: Option<TtlAction>,
    overwrite: Option<bool>,
    refresh: Option<LoadRefresh>,
}

impl LoadJobCommandBuilder {
    pub fn new(source_path: impl Into<String>) -> Self {
        Self {
            source_path: source_path.into(),
            ..Default::default()
        }
    }

    pub fn target_path(mut self, target_path: impl Into<String>) -> Self {
        let _ = self.target_path.insert(target_path.into());
        self
    }

    pub fn replicas(mut self, replicas: i32) -> Self {
        let _ = self.replicas.insert(replicas);
        self
    }

    pub fn block_size(mut self, block_size: i64) -> Self {
        let _ = self.block_size.insert(block_size);
        self
    }

    pub fn storage_type(mut self, storage_type: StorageType) -> Self {
        let _ = self.storage_type.insert(storage_type);
        self
    }

    pub fn ttl_ms(mut self, ttl_ms: i64) -> Self {
        let _ = self.ttl_ms.insert(ttl_ms);
        self
    }

    pub fn ttl_action(mut self, ttl_action: TtlAction) -> Self {
        let _ = self.ttl_action.insert(ttl_action);
        self
    }

    pub fn overwrite(mut self, overwrite: bool) -> Self {
        let _ = self.overwrite.insert(overwrite);
        self
    }

    pub fn refresh(mut self, refresh: LoadRefresh) -> Self {
        let _ = self.refresh.insert(refresh);
        self
    }

    pub fn build(self) -> LoadJobCommand {
        LoadJobCommand {
            source_path: self.source_path,
            target_path: self.target_path,
            replicas: self.replicas,
            block_size: self.block_size,
            storage_type: self.storage_type,
            ttl_ms: self.ttl_ms,
            ttl_action: self.ttl_action,
            overwrite: self.overwrite,
            refresh: self.refresh,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadJobInfo {
    pub job_id: String,
    pub source_path: String,
    pub target_path: String,
    pub block_size: i64,
    pub replicas: i32,
    pub storage_type: StorageType,
    pub ttl_ms: i64,
    pub ttl_action: TtlAction,
    pub mount_info: MountInfo,
    pub create_time: i64,
    pub overwrite: Option<bool>,
    /// Resolved cache policy for this job.
    #[serde(default)]
    pub refresh: LoadRefresh,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadTaskInfo {
    pub job: LoadJobInfo,
    pub task_id: String,
    pub worker: WorkerAddress,
    pub source_path: String,
    pub target_path: String,
    pub create_time: i64,
    #[serde(default)]
    pub source_read_plan_json: String,
    #[serde(default)]
    pub transfer_report: Option<TransferTaskReportInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferTaskReportInfo {
    pub run_id: u64,
    pub attempt_id: u64,
    pub worker_id: u32,
    pub worker_session_id: String,
    #[serde(default)]
    pub report_target: String,
    #[serde(default)]
    pub report_endpoints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobTaskProgress {
    pub state: JobTaskState,
    pub loaded_size: i64,
    pub total_size: i64,
    /// Bytes of files a cache policy skipped because Curvine already had them.
    #[serde(default)]
    pub skipped_size: i64,
    /// Number of files the same policy skipped.
    #[serde(default)]
    pub skipped_files: i64,
    pub update_time: i64,
    pub message: String,
}

impl Default for JobTaskProgress {
    fn default() -> Self {
        Self {
            state: JobTaskState::Pending,
            total_size: 0,
            loaded_size: 0,
            skipped_size: 0,
            skipped_files: 0,
            update_time: 0,
            message: String::new(),
        }
    }
}

impl JobTaskProgress {
    /// Returns a formatted progress string with percentage and byte counts
    /// Format: "[████████░░░░░░░░░░] 45.2% (123.4 MB / 273.0 MB)"
    /// If show_bar is false, format: "45.2% (123.4 MB / 273.0 MB)"
    pub fn progress_string(&self, show_bar: bool) -> String {
        let loaded = self.loaded_size.max(0) as u64;
        let total = self.total_size.max(0) as u64;
        let display_loaded = if total == 0 {
            loaded
        } else {
            loaded.min(total)
        };

        let percentage = if total == 0 {
            0.0
        } else {
            display_loaded as f64 / total as f64 * 100.0
        };

        if show_bar {
            if total == 0 {
                return format!(
                    "[{}] 0.0% ({} / {})",
                    "░".repeat(20),
                    ByteUnit::byte_to_string(display_loaded),
                    ByteUnit::byte_to_string(total)
                );
            }

            let filled = (percentage / 100.0 * 20.0) as usize;
            let empty = 20 - filled.min(20);

            let progress_bar = format!("{}{}", "█".repeat(filled.min(20)), "░".repeat(empty));

            format!(
                "[{}] {:.1}% ({} / {})",
                progress_bar,
                percentage,
                ByteUnit::byte_to_string(display_loaded),
                ByteUnit::byte_to_string(total)
            )
        } else {
            format!(
                "{:.1}% ({} / {})",
                percentage,
                ByteUnit::byte_to_string(display_loaded),
                ByteUnit::byte_to_string(total)
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JobTaskProgress;
    use curvine_runtime::common::ByteUnit;

    #[test]
    fn progress_string_caps_loaded_bytes_at_total() {
        let progress = JobTaskProgress {
            loaded_size: 66,
            total_size: 33,
            ..Default::default()
        };
        let expected_counts = format!(
            "{} / {}",
            ByteUnit::byte_to_string(33),
            ByteUnit::byte_to_string(33)
        );

        assert!(progress.progress_string(false).contains(&expected_counts));
        assert!(progress.progress_string(true).contains(&expected_counts));
    }
}

#[cfg(test)]
mod refresh_tests {
    use super::{load_target_is_current, LoadRefresh};
    use crate::file_status::FileStatus;
    use crate::storage_policy::{StoragePolicy, StorageState};

    fn ufs(len: i64, mtime: i64) -> FileStatus {
        FileStatus {
            path: "s3://bucket/file".to_string(),
            name: "file".to_string(),
            len,
            mtime,
            is_complete: true,
            ..Default::default()
        }
    }

    fn cached(len: i64, ufs_mtime: i64) -> FileStatus {
        let mut status = ufs(len, ufs_mtime);
        status.storage_policy = StoragePolicy {
            ufs_mtime,
            state: StorageState::Both,
            ..Default::default()
        };
        status.is_complete = true;
        status
    }

    #[test]
    fn always_never_skips() {
        let source = ufs(10, 100);
        assert!(!load_target_is_current(
            LoadRefresh::Always,
            &source,
            Some(&cached(10, 100))
        ));
    }

    #[test]
    fn if_newer_requires_size_and_mtime_to_match() {
        let source = ufs(10, 100);
        assert!(load_target_is_current(
            LoadRefresh::IfNewer,
            &source,
            Some(&cached(10, 100))
        ));
        // same size, different mtime
        assert!(!load_target_is_current(
            LoadRefresh::IfNewer,
            &source,
            Some(&cached(10, 99))
        ));
        // different size, same mtime
        assert!(!load_target_is_current(
            LoadRefresh::IfNewer,
            &source,
            Some(&cached(11, 100))
        ));
    }

    #[test]
    fn skip_cached_ignores_freshness_but_needs_a_complete_copy() {
        let source = ufs(10, 100);
        assert!(load_target_is_current(
            LoadRefresh::SkipCached,
            &source,
            Some(&cached(10, 1))
        ));
        let mut partial = cached(10, 100);
        partial.is_complete = false;
        assert!(!load_target_is_current(
            LoadRefresh::SkipCached,
            &source,
            Some(&partial)
        ));
        // A complete inode whose data was freed back to UFS holds no Curvine
        // data, so "already cached" is false and the file must be loaded again.
        let mut ufs_only = cached(10, 100);
        ufs_only.storage_policy.state = StorageState::Ufs;
        assert!(!ufs_only.cv_exists(), "the fixture must not hold CV data");
        assert!(!load_target_is_current(
            LoadRefresh::SkipCached,
            &source,
            Some(&ufs_only)
        ));
    }

    #[test]
    fn a_missing_target_is_never_current() {
        let source = ufs(10, 100);
        for mode in [
            LoadRefresh::Always,
            LoadRefresh::IfNewer,
            LoadRefresh::SkipCached,
        ] {
            assert!(!load_target_is_current(mode, &source, None));
        }
    }

    #[test]
    fn directory_targets_are_not_current() {
        let source = ufs(10, 100);
        let mut dir = cached(10, 100);
        dir.is_dir = true;
        assert!(!load_target_is_current(
            LoadRefresh::SkipCached,
            &source,
            Some(&dir)
        ));
    }

    #[test]
    fn parses_exactly_the_three_documented_spellings() {
        assert_eq!(LoadRefresh::parse("always"), Some(LoadRefresh::Always));
        assert_eq!(LoadRefresh::parse("if-newer"), Some(LoadRefresh::IfNewer));
        assert_eq!(
            LoadRefresh::parse("skip-cached"),
            Some(LoadRefresh::SkipCached)
        );
        // Case/underscore folding is a convenience of the parser, but nothing
        // beyond the three names is an accepted mode.
        for rejected in [
            "newer",
            "if-newer-mode",
            "conditional",
            "if-modified",
            "mtime",
            "full",
            "overwrite",
            "skip",
            "cached",
            "skip_cached",
            "",
        ] {
            assert_eq!(
                LoadRefresh::parse(rejected),
                None,
                "{rejected} must not parse"
            );
        }
        assert_eq!(LoadRefresh::default(), LoadRefresh::Always);
        assert_eq!(LoadRefresh::IfNewer.to_string(), "if-newer");
    }
}
