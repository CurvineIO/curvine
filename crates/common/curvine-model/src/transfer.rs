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

use crate::file_gen::FileGen;
use curvine_error::{FsError, FsResult};
use curvine_runtime::common::Utils;
use num_enum::{FromPrimitive, IntoPrimitive};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const TRANSFER_TEMP_PATH_MARKER: &str = ".curvine-transfer-tmp-";

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    IntoPrimitive,
    FromPrimitive,
    Serialize,
    Deserialize,
)]
#[repr(i32)]
pub enum TransferKind {
    #[default]
    Load = 1,
    Export = 2,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    IntoPrimitive,
    FromPrimitive,
    Serialize,
    Deserialize,
)]
#[repr(i32)]
pub enum TransferState {
    #[default]
    Pending = 1,
    Planning = 2,
    Dispatching = 3,
    Running = 4,
    Canceling = 5,
    Completed = 6,
    Failed = 7,
    Canceled = 8,
    PartialSuccess = 9,
}

impl TransferState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TransferState::Completed
                | TransferState::Failed
                | TransferState::Canceled
                | TransferState::PartialSuccess
        )
    }

    pub fn is_runnable(self) -> bool {
        matches!(
            self,
            TransferState::Pending
                | TransferState::Planning
                | TransferState::Dispatching
                | TransferState::Running
                | TransferState::Canceling
        )
    }

    pub fn is_executing(self) -> bool {
        matches!(
            self,
            TransferState::Planning
                | TransferState::Dispatching
                | TransferState::Running
                | TransferState::Canceling
        )
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    IntoPrimitive,
    FromPrimitive,
    Serialize,
    Deserialize,
)]
#[repr(i32)]
pub enum TransferTaskState {
    #[default]
    Pending = 1,
    Running = 2,
    Completed = 3,
    Failed = 4,
    Canceled = 5,
    Stale = 6,
}

impl TransferTaskState {
    pub fn is_running(self) -> bool {
        matches!(
            self,
            TransferTaskState::Pending | TransferTaskState::Running
        )
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TransferTaskState::Completed | TransferTaskState::Failed | TransferTaskState::Canceled
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferCommand {
    pub kind: TransferKind,
    pub source_path: String,
    pub target_path: String,
    pub client_request_id: String,
    pub submitter: String,
    pub tenant: String,
    pub options: BTreeMap<String, String>,
    /// fs_mode export batch: one job carrying several independent single-file
    /// exports. Each entry is planned as its own task so a stale entry fails
    /// alone instead of taking the whole job down.
    ///
    /// `serde(default)` keeps older `command_json` payloads readable.
    #[serde(default)]
    pub fs_mode_files: Vec<FsModeFile>,
}

/// One file inside an fs_mode export batch.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsModeFile {
    /// Path the export reads from, in Curvine.
    pub source: String,
    /// Path the object is written to, in UFS.
    pub target: String,
    /// Version the submitter read when it enqueued the export. The planner and
    /// the worker both re-check it, so an object always holds the version the
    /// submitter claimed.
    pub gen: FileGen,
}

impl TransferCommand {
    pub const OVERWRITE_OPTION: &'static str = "overwrite";
    pub const REPLICAS_OPTION: &'static str = "replicas";
    /// Upper bound on files in one fs_mode export batch. Keeps `command_json`
    /// and the submit RPC bounded.
    pub const FS_MODE_BATCH_MAX_FILES: usize = 256;
    /// Submitter prefix reserved for fs_mode exports. Only commands carrying
    /// `fs_mode_files` may use it (enforced at submit).
    pub const FS_MODE_SUBMITTER_PREFIX: &'static str = "fs-mode:";

    /// Submitter used by a cluster's fs_mode exports.
    pub fn fs_mode_submitter(cluster_id: &str) -> String {
        format!("{}{}", Self::FS_MODE_SUBMITTER_PREFIX, cluster_id)
    }

    /// True when this command carries an fs_mode export batch.
    pub fn is_fs_mode_batch(&self) -> bool {
        !self.fs_mode_files.is_empty()
    }

    /// fs_mode jobs are exempt from target-overlap scheduling: every entry is an
    /// independent single-file copy guarded by its generation, so ordering does
    /// not depend on scheduler serialization.
    pub fn is_fs_mode_submitter(submitter: &str) -> bool {
        submitter.starts_with(Self::FS_MODE_SUBMITTER_PREFIX)
    }

    pub fn job_key(&self) -> String {
        format!("{:?}:{}:{}", self.kind, self.source_path, self.target_path)
    }

    pub fn default_client_request_id(
        kind: TransferKind,
        source_path: impl AsRef<str>,
        target_path: impl AsRef<str>,
    ) -> String {
        format!(
            "job_{}",
            Utils::md5(format!(
                "{:?}:{}:{}",
                kind,
                source_path.as_ref(),
                target_path.as_ref()
            ))
        )
    }

    pub fn default_client_request_id_with_overwrite(
        kind: TransferKind,
        source_path: impl AsRef<str>,
        target_path: impl AsRef<str>,
        overwrite: bool,
    ) -> String {
        format!(
            "job_{}",
            Utils::md5(format!(
                "{:?}:{}:{}:overwrite={}",
                kind,
                source_path.as_ref(),
                target_path.as_ref(),
                overwrite
            ))
        )
    }

    pub fn default_client_request_id_with_overwrite_and_replicas(
        kind: TransferKind,
        source_path: impl AsRef<str>,
        target_path: impl AsRef<str>,
        overwrite: bool,
        replicas: Option<i32>,
    ) -> String {
        if replicas.is_none() {
            return Self::default_client_request_id_with_overwrite(
                kind,
                source_path,
                target_path,
                overwrite,
            );
        }

        format!(
            "job_{}",
            Utils::md5(format!(
                "{:?}:{}:{}:overwrite={}:replicas={}",
                kind,
                source_path.as_ref(),
                target_path.as_ref(),
                overwrite,
                replicas.unwrap_or_default(),
            ))
        )
    }

    pub fn overwrite(&self) -> bool {
        self.options
            .get(Self::OVERWRITE_OPTION)
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(true)
    }

    pub fn set_overwrite(&mut self, overwrite: bool) {
        self.options
            .insert(Self::OVERWRITE_OPTION.to_string(), overwrite.to_string());
    }

    pub fn replicas(&self) -> FsResult<Option<i32>> {
        let Some(value) = self.options.get(Self::REPLICAS_OPTION) else {
            return Ok(None);
        };
        let replicas = value
            .parse::<i32>()
            .map_err(|_| FsError::common(format!("Invalid transfer replicas option: {}", value)))?;
        if replicas <= 0 {
            return Err(FsError::common("Transfer replicas option must be positive"));
        }
        Ok(Some(replicas))
    }

    pub fn set_replicas(&mut self, replicas: i32) {
        self.options
            .insert(Self::REPLICAS_OPTION.to_string(), replicas.to_string());
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferProgress {
    pub loaded_size: i64,
    pub total_size: i64,
    pub update_time: i64,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferJobRecord {
    pub job_key: String,
    pub job_id: String,
    pub run_id: u64,
    pub kind: TransferKind,
    pub source_path: String,
    pub target_path: String,
    pub command_json: String,
    pub mount_snapshot_json: String,
    pub secret_ref_json: String,
    pub cluster_snapshot_version: u64,
    #[serde(default)]
    pub mount_snapshot_attempts: u32,
    pub cv_metadata_epoch: Option<u64>,
    pub state: TransferState,
    pub owner: String,
    pub lease_epoch: u64,
    pub lease_expire_at: i64,
    pub cancel_requested: bool,
    pub summary: TransferProgress,
    pub client_request_id: String,
    pub submitter: String,
    pub tenant: String,
    pub created_at: i64,
    pub updated_at: i64,
    /// Consecutive retryable planning failures (UFS rate limiting, timeouts).
    /// JSON-only; 0 means none (legacy rows). Appended at the tail per the
    /// public-struct additive-evolution convention.
    #[serde(default)]
    pub planning_attempts: u32,
    /// Terminal task snapshot (progress + structured counts) persisted
    /// synchronously before the job lands terminal, so TTL task purge never
    /// renders detail as 0/empty. JSON-only additive; None means not snapshotted.
    #[serde(default)]
    pub terminal_summary: Option<TransferProgress>,
    #[serde(default)]
    pub terminal_counts: Option<TransferTaskCounts>,
}

/// Multi-file batch manifest carried in a batch task's `source_read_plan_json`
/// (design §7). The `{"batch": ...}` wrapper distinguishes it from a per-file
/// CV block-read plan (a bare JSON array) and from a file segment
/// (`{"segment": ...}`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchManifest {
    pub batch: Vec<BatchManifestEntry>,
}

/// A single-file segment plan carried in a segment task's `source_read_plan_json`
/// (design §14): one task copies one byte range of one file, aligned to the CV
/// target block size so each segment writes exactly one block on one worker.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SegmentPlan {
    /// Byte offset in the source file (aligned to CV block size).
    pub offset: i64,
    /// Bytes in this segment (<= one CV block).
    pub length: i64,
    /// Total file length; the first segment pre-allocates the whole target
    /// (resize) so concurrent segments find every block's layout ready.
    pub total_len: i64,
    /// Target block index this segment writes. Planner computes it from the
    /// job's configured block size (offset / block_size) so the runner does
    /// not re-derive it from the live layout (which may differ during rolling
    /// upgrades or when block_size != SEGMENT_BYTES).
    pub block_index: i64,
    /// Source mtime the planner observed with its HEAD fingerprint (0 when
    /// unavailable). Scheduler convergence stamps it as the target's UFS
    /// identity so the cached copy is readable (`cv_valid`) and skippable by
    /// the next incremental load. It must stay the *plan-time* value: reading
    /// the source again at finalize would claim a freshness the cached bytes
    /// may not have, and a mixed old/new copy would be served as fresh.
    #[serde(default)]
    pub source_mtime: i64,
}

/// Upper bound for `source_read_plan_json` / dispatch RPC plan payloads.
/// A range manifest is ~10^2 B; the bound rejects accidental `Vec<SegmentPlan>`
/// payloads (10^4 B級, §8 #7) without constraining legal plans.
pub const TRANSFER_PLAN_JSON_LIMIT_BYTES: usize = 8192;

/// Structured plan identity shared by Worker routing and scheduler finalize.
/// Parses the JSON object tag instead of matching string prefixes (§8 #23).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferPlanKind {
    Batch,
    Segment,
    SegmentGroup,
    Plain,
}

/// Why a fresh HEAD observation no longer identifies a group manifest's source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentGroupMismatch {
    /// The manifest carries neither an immutable version nor a stat pair.
    NoIdentity,
    /// The manifest pins a version but the backend reported none.
    VersionUnavailable,
    VersionChanged,
    LengthChanged,
    MtimeChanged,
}

/// Compact range manifest for a segment group (design §8裁决3).
/// A group binds consecutive whole segments; cut points stay block-aligned.
/// `loaded_size` in progress is cumulative bytes relative to `start_offset`.
///
/// Two identities can re-identify the source at execution time:
/// * `source_version`: an immutable backend version/etag (OpenDAL mounts).
/// * `source_len` + `source_mtime`: one HEAD observation (OSS-HDFS reports a
///   millisecond mtime identical to `listDir`, measured on production),
///   which is the same trust level `unchanged_load_target` already uses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentGroupPlan {
    pub start_offset: i64,
    pub end_offset: i64,
    pub total_len: i64,
    pub first_block_index: i64,
    pub segment_bytes: i64,
    #[serde(default)]
    pub source_len: i64,
    #[serde(default)]
    pub source_mtime: i64,
    #[serde(default)]
    pub source_version: String,
}

impl SegmentGroupPlan {
    pub fn group_len(&self) -> i64 {
        self.end_offset.saturating_sub(self.start_offset).max(0)
    }

    /// True when the manifest pins a HEAD-observed `(len, mtime)` pair.
    pub fn has_stat_identity(&self) -> bool {
        self.source_len > 0 && self.source_mtime > 0
    }

    /// True when the source can be re-identified after a retry or a worker
    /// change; a manifest without identity must not be dispatched.
    pub fn has_identity(&self) -> bool {
        !self.source_version.is_empty() || self.has_stat_identity()
    }

    /// Re-identify the source from one fresh HEAD observation. `len`/`mtime`
    /// must come from the same call the planner fingerprinted with, so an
    /// unchanged source compares equal; a version-carrying manifest never
    /// falls back to mtime alone when the backend stops reporting a version.
    pub fn verify_observation(
        &self,
        len: i64,
        mtime: i64,
        version: &str,
    ) -> Result<(), SegmentGroupMismatch> {
        if !self.has_identity() {
            return Err(SegmentGroupMismatch::NoIdentity);
        }
        if len != self.total_len || len != self.source_len {
            return Err(SegmentGroupMismatch::LengthChanged);
        }
        if !self.source_version.is_empty() {
            if version.is_empty() {
                return Err(SegmentGroupMismatch::VersionUnavailable);
            }
            if version != self.source_version {
                return Err(SegmentGroupMismatch::VersionChanged);
            }
        }
        if self.source_mtime > 0 && mtime != self.source_mtime {
            return Err(SegmentGroupMismatch::MtimeChanged);
        }
        Ok(())
    }

    pub fn validate(&self) -> FsResult<()> {
        let ok = self.total_len > 0
            && self.segment_bytes > 0
            && self.first_block_index >= 0
            && self.start_offset >= 0
            && self.end_offset > self.start_offset
            && self.end_offset <= self.total_len
            && self.source_len == self.total_len
            && self.start_offset % self.segment_bytes == 0
            && self.first_block_index == self.start_offset / self.segment_bytes
            && (self.end_offset == self.total_len || self.end_offset % self.segment_bytes == 0);
        match ok {
            true => Ok(()),
            false => Err(FsError::common(format!(
                "Invalid segment group plan start={} end={} total={} first_block={} seg={}",
                self.start_offset,
                self.end_offset,
                self.total_len,
                self.first_block_index,
                self.segment_bytes
            ))),
        }
    }

    pub fn resume_floor(&self, reported: i64) -> i64 {
        let len = self.group_len();
        if len <= 0 || reported <= 0 {
            return 0;
        }
        if reported >= len {
            return len;
        }
        let seg = self.segment_bytes.max(1);
        (reported / seg) * seg
    }

    pub fn iter_from(&self, resume: i64) -> Vec<(i64, i64, i64)> {
        let floor = self.resume_floor(resume).min(self.group_len()).max(0);
        let mut out = Vec::new();
        let mut offset = self.start_offset.saturating_add(floor);
        let mut index = self
            .first_block_index
            .saturating_add(floor / self.segment_bytes.max(1));
        while offset < self.end_offset {
            let length = self.segment_bytes.max(1).min(self.end_offset - offset);
            out.push((offset, length, index));
            offset += length;
            index += 1;
        }
        out
    }
}

pub fn classify_transfer_plan(plan_json: &str) -> TransferPlanKind {
    let value: serde_json::Value = match serde_json::from_str(plan_json) {
        Ok(value) => value,
        Err(_) => return TransferPlanKind::Plain,
    };
    let obj = match value.as_object() {
        Some(obj) => obj,
        None => return TransferPlanKind::Plain,
    };
    if obj.contains_key("segment_group") {
        TransferPlanKind::SegmentGroup
    } else if obj.contains_key("segment") {
        TransferPlanKind::Segment
    } else if obj.contains_key("batch") {
        TransferPlanKind::Batch
    } else {
        TransferPlanKind::Plain
    }
}

pub fn parse_segment_group(plan_json: &str) -> FsResult<SegmentGroupPlan> {
    let value: serde_json::Value = serde_json::from_str(plan_json)
        .map_err(|err| FsError::common(format!("Invalid segment group plan: {}", err)))?;
    let plan: SegmentGroupPlan = serde_json::from_value(
        value
            .get("segment_group")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    )
    .map_err(|err| FsError::common(format!("Invalid segment group plan: {}", err)))?;
    plan.validate()?;
    Ok(plan)
}

pub fn segment_group_json(plan: &SegmentGroupPlan) -> FsResult<String> {
    plan.validate()?;
    let json = serde_json::to_string(&serde_json::json!({ "segment_group": plan }))
        .map_err(|_| FsError::common("Unable to serialize segment group plan"))?;
    check_transfer_plan_size(&json)?;
    Ok(json)
}

/// Plan-time source mtime carried by a segment plan, `0` when the plan pins
/// none. Group manifests always carried it; single-segment manifests carry it
/// so the scheduler can stamp the target's UFS identity at convergence.
pub fn plan_source_mtime(plan_json: &str) -> i64 {
    match classify_transfer_plan(plan_json) {
        TransferPlanKind::SegmentGroup => parse_segment_group(plan_json)
            .map(|plan| plan.source_mtime)
            .unwrap_or(0),
        TransferPlanKind::Segment => serde_json::from_str::<serde_json::Value>(plan_json)
            .ok()
            .and_then(|value| value.get("segment").cloned())
            .and_then(|segment| serde_json::from_value::<SegmentPlan>(segment).ok())
            .map(|plan| plan.source_mtime)
            .unwrap_or(0),
        TransferPlanKind::Batch | TransferPlanKind::Plain => 0,
    }
}

/// Source mtime shared by every segment plan of one target. `None` when a
/// plan pins none, or when the plans disagree: the source moved while it was
/// being copied, so no single mtime describes the bytes now cached and the
/// target must stay cache-invalid rather than claim a freshness it lacks.
pub fn agreed_plan_source_mtime<'a>(plans: impl IntoIterator<Item = &'a str>) -> Option<i64> {
    let mut agreed: Option<i64> = None;
    for mtime in plans.into_iter().map(plan_source_mtime) {
        if mtime <= 0 {
            return None;
        }
        match agreed {
            Some(previous) if previous != mtime => return None,
            Some(_) => {}
            None => agreed = Some(mtime),
        }
    }
    agreed
}

pub fn check_transfer_plan_size(plan_json: &str) -> FsResult<()> {
    match plan_json.len() <= TRANSFER_PLAN_JSON_LIMIT_BYTES {
        true => Ok(()),
        false => Err(FsError::common(format!(
            "Transfer plan payload {} exceeds {} bytes",
            plan_json.len(),
            TRANSFER_PLAN_JSON_LIMIT_BYTES
        ))),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchManifestEntry {
    /// Source path relative to the job's source root (design §7.1): a batch
    /// may span subdirectories, so the runner resolves target as
    /// target_root + rel_path.
    pub rel_path: String,
    pub size: i64,
    /// A cached incremental-load target has the wrong replica count. The
    /// scheduler removes it only after the complete plan is durable.
    #[serde(default)]
    pub replace_target: bool,
}

/// Scheduler-side per-worker in-flight aggregation (state = Running only).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerInflight {
    pub worker_id: u32,
    pub task_count: u64,
    pub estimated_bytes: i64,
    /// How many of `task_count` have unknown (0) size; the scheduler adds the
    /// configured default estimate per such row (design §3.1).
    pub unknown_bytes_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferTaskRecord {
    pub job_id: String,
    pub run_id: u64,
    pub task_id: String,
    pub attempt_id: u64,
    pub source_path: String,
    pub target_path: String,
    pub worker_id: u32,
    pub worker_session_id: String,
    pub source_read_plan_json: String,
    pub report_target_json: String,
    pub state: TransferTaskState,
    pub progress: TransferProgress,
    /// Planner-estimated source bytes. JSON-only (record_json authoritative);
    /// 0 means unknown (legacy rows) — consumers substitute a default estimate.
    #[serde(default)]
    pub estimated_size: i64,
    pub retry_count: u32,
    pub attempt_started_at: i64,
    pub last_report_at: i64,
    pub stale_deadline_at: i64,
    pub updated_at: i64,
    /// Worker accept time (ms, 0 = unknown). JSON-only; terminal reports
    /// carry it for phase breakdown.
    #[serde(default)]
    pub accepted_at_ms: i64,
    /// First source byte time (ms, 0 = unknown).
    #[serde(default)]
    pub first_byte_at_ms: i64,
    /// Delay destructive replacement of an incremental-load target until all
    /// planned task rows have been persisted. Legacy records default false.
    #[serde(default)]
    pub replace_target: bool,
    /// Segment tasks: blocks written by the completing attempt, reported via
    /// the terminal progress and persisted here for finalize. Legacy rows
    /// default empty.
    #[serde(default)]
    pub block_locations: Vec<TaskBlockLocation>,
}

/// Status-query projection. Task planning data stays in `TransferTaskRecord`;
/// polling callers never need `source_read_plan_json` or report endpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferTaskStatus {
    pub task_id: String,
    pub attempt_id: u64,
    pub source_path: String,
    pub target_path: String,
    pub worker_id: u32,
    pub worker_session_id: String,
    pub state: TransferTaskState,
    pub progress: TransferProgress,
    pub retry_count: u32,
    pub updated_at: i64,
}

impl From<TransferTaskRecord> for TransferTaskStatus {
    fn from(task: TransferTaskRecord) -> Self {
        Self::from(&task)
    }
}

impl From<&TransferTaskRecord> for TransferTaskStatus {
    fn from(task: &TransferTaskRecord) -> Self {
        Self {
            task_id: task.task_id.clone(),
            attempt_id: task.attempt_id,
            source_path: task.source_path.clone(),
            target_path: task.target_path.clone(),
            worker_id: task.worker_id,
            worker_session_id: task.worker_session_id.clone(),
            state: task.state,
            progress: task.progress.clone(),
            retry_count: task.retry_count,
            updated_at: task.updated_at,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferTaskSummary {
    pub progress: TransferProgress,
    pub counts: TransferTaskCounts,
    pub has_task: bool,
    pub has_failed: bool,
    pub all_completed: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferTaskCounts {
    pub pending: u64,
    pub running: u64,
    pub completed: u64,
    pub failed: u64,
    pub canceled: u64,
    pub stale: u64,
    pub completed_size: i64,
}

pub fn summarize_transfer_tasks<'a>(
    tasks: impl IntoIterator<Item = &'a TransferTaskRecord>,
    update_time: i64,
) -> TransferTaskSummary {
    let mut summary = TransferTaskSummary {
        progress: TransferProgress {
            update_time,
            ..Default::default()
        },
        counts: TransferTaskCounts::default(),
        has_task: false,
        has_failed: false,
        all_completed: true,
    };
    let mut failed_count = 0;
    let mut first_error: Option<(i64, String, String)> = None;
    let mut last_message: Option<(i64, String, String)> = None;

    for task in tasks {
        summary.has_task = true;
        summary.has_failed |= task.state == TransferTaskState::Failed;
        summary.all_completed &= task.state == TransferTaskState::Completed;
        summary.progress.loaded_size += task.progress.loaded_size;
        summary.progress.total_size += task.progress.total_size;

        match task.state {
            TransferTaskState::Pending => summary.counts.pending += 1,
            TransferTaskState::Running => summary.counts.running += 1,
            TransferTaskState::Completed => {
                summary.counts.completed += 1;
                summary.counts.completed_size += task.progress.loaded_size.max(0);
            }
            TransferTaskState::Failed => summary.counts.failed += 1,
            TransferTaskState::Canceled => summary.counts.canceled += 1,
            TransferTaskState::Stale => summary.counts.stale += 1,
        }

        if !task.progress.message.is_empty()
            && match &last_message {
                None => true,
                Some((updated_at, task_id, _)) => {
                    task.updated_at > *updated_at
                        || (task.updated_at == *updated_at
                            && task.task_id.as_str() > task_id.as_str())
                }
            }
        {
            last_message = Some((
                task.updated_at,
                task.task_id.clone(),
                task.progress.message.clone(),
            ));
        }
        if task.state == TransferTaskState::Failed {
            failed_count += 1;
            if !task.progress.message.is_empty()
                && match &first_error {
                    None => true,
                    Some((updated_at, task_id, _)) => {
                        task.updated_at < *updated_at
                            || (task.updated_at == *updated_at
                                && task.task_id.as_str() < task_id.as_str())
                    }
                }
            {
                first_error = Some((
                    task.updated_at,
                    task.task_id.clone(),
                    task.progress.message.clone(),
                ));
            }
        }
    }

    if failed_count > 0 {
        let first_error = first_error
            .map(|(_, _, message)| message)
            .unwrap_or_else(|| "transfer task failed".to_string());
        summary.progress.message = format!(
            "{} transfer task(s) failed; first failure: {}",
            failed_count,
            normalize_summary_message(&first_error)
        );
    } else {
        summary.progress.message = last_message
            .map(|(_, _, message)| message)
            .unwrap_or_default();
    }
    summary
}

/// Builds a task summary from Store-side aggregates. SQL backends use this to
/// avoid loading immutable task definitions just to update progress counters.
pub fn transfer_task_summary_from_aggregate(
    counts: TransferTaskCounts,
    loaded_size: i64,
    total_size: i64,
    first_error: Option<&str>,
    last_message: Option<&str>,
    update_time: i64,
) -> TransferTaskSummary {
    let has_task = counts.pending
        + counts.running
        + counts.completed
        + counts.failed
        + counts.canceled
        + counts.stale
        > 0;
    let has_failed = counts.failed > 0;
    let message = if has_failed {
        let first_error = first_error
            .filter(|message| !message.is_empty())
            .unwrap_or("transfer task failed");
        format!(
            "{} transfer task(s) failed; first failure: {}",
            counts.failed,
            normalize_summary_message(first_error)
        )
    } else {
        last_message.unwrap_or_default().to_string()
    };
    TransferTaskSummary {
        progress: TransferProgress {
            loaded_size,
            total_size,
            update_time,
            message,
        },
        counts,
        has_task,
        has_failed,
        all_completed: counts.completed
            == counts.pending
                + counts.running
                + counts.completed
                + counts.failed
                + counts.canceled
                + counts.stale,
    }
}

fn normalize_summary_message(message: &str) -> String {
    message
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(512)
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferLease {
    pub job_id: String,
    pub run_id: u64,
    pub owner: String,
    pub lease_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferStateUpdate {
    pub job_id: String,
    pub run_id: u64,
    pub owner: String,
    pub lease_epoch: u64,
    pub from_states: Vec<TransferState>,
    pub to_state: TransferState,
    pub message: String,
    pub now_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskAttemptStart {
    pub job_id: String,
    pub run_id: u64,
    pub owner: String,
    pub lease_epoch: u64,
    pub task_id: String,
    pub attempt_id: u64,
    pub worker_id: u32,
    pub worker_session_id: String,
    pub report_target_json: String,
    pub now_ms: i64,
    pub stale_deadline_at: i64,
    pub resume_loaded_size: i64,
}

pub fn normalize_group_resume(plan_json: &str, reported: i64) -> i64 {
    let plan = match parse_segment_group(plan_json) {
        Ok(plan) => plan,
        Err(_) => return 0,
    };
    let clamped = reported.clamp(0, plan.group_len());
    plan.resume_floor(clamped)
}

pub fn group_resume_remaining(plan_json: &str, reported: i64) -> (i64, i64) {
    let plan = match parse_segment_group(plan_json) {
        Ok(plan) => plan,
        Err(_) => return (0, 0),
    };
    let resume = plan.resume_floor(reported.clamp(0, plan.group_len()));
    (resume, plan.group_len().saturating_sub(resume))
}

/// One block a segment task actually wrote, with the workers holding it.
/// The scheduler aggregates these from terminal reports to commit real block
/// locations when finalizing a segmented target file; without them the file
/// completes with empty locations and every read must fall back to UFS.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskBlockLocation {
    pub block_id: i64,
    pub worker_ids: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferTaskReport {
    pub job_id: String,
    pub run_id: u64,
    pub task_id: String,
    pub attempt_id: u64,
    pub worker_id: u32,
    pub worker_session_id: String,
    pub state: TransferTaskState,
    pub progress: TransferProgress,
    pub now_ms: i64,
    pub stale_deadline_at: i64,
    /// Worker accept time (ms, 0 = unknown, older workers omit it).
    pub accepted_at_ms: i64,
    /// First source byte time (ms, 0 = unknown).
    pub first_byte_at_ms: i64,
    /// Segment tasks: blocks written by this attempt (terminal reports only).
    pub block_locations: Vec<TaskBlockLocation>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransferListFilter {
    pub kind: Option<TransferKind>,
    /// State selection; empty means every state passes (a UI bucket expands to one
    /// or more of these before the store sees it).
    pub states: Vec<TransferState>,
    pub submitter: Option<String>,
    pub tenant: Option<String>,
    pub limit: usize,
    pub offset: usize,
}

impl TransferListFilter {
    pub fn matches_state(&self, state: TransferState) -> bool {
        self.states.is_empty() || self.states.contains(&state)
    }
}

/// Predicate for counting jobs. There is deliberately no state filter: the UI's
/// bucket tallies are global, so one grouped count serves all of them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransferCountFilter {
    pub kind: Option<TransferKind>,
    pub submitter: Option<String>,
    pub tenant: Option<String>,
}

impl TransferCountFilter {
    pub fn matches(&self, job: &TransferJobRecord) -> bool {
        self.kind.is_none_or(|kind| job.kind == kind)
            && self
                .submitter
                .as_ref()
                .is_none_or(|submitter| job.submitter == *submitter)
            && self
                .tenant
                .as_ref()
                .is_none_or(|tenant| job.tenant == *tenant)
    }
}

/// Per-state job counts over the whole store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransferStateCounts {
    pub pending: u64,
    pub planning: u64,
    pub dispatching: u64,
    pub running: u64,
    pub canceling: u64,
    pub completed: u64,
    pub failed: u64,
    pub canceled: u64,
    pub partial_success: u64,
}

/// The buckets the console groups states into, plus the grand total.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransferBucketCounts {
    pub all: u64,
    pub active: u64,
    pub completed: u64,
    pub failed: u64,
    pub canceled: u64,
}

/// The states that make up each UI bucket. Exposed so the web layer and the
/// service expand the same bucket names the same way.
pub const TRANSFER_BUCKET_ACTIVE: &[TransferState] = &[
    TransferState::Pending,
    TransferState::Planning,
    TransferState::Dispatching,
    TransferState::Running,
    TransferState::Canceling,
];
pub const TRANSFER_BUCKET_COMPLETED: &[TransferState] = &[TransferState::Completed];
pub const TRANSFER_BUCKET_FAILED: &[TransferState] =
    &[TransferState::Failed, TransferState::PartialSuccess];
pub const TRANSFER_BUCKET_CANCELED: &[TransferState] = &[TransferState::Canceled];

impl TransferStateCounts {
    pub fn add(&mut self, state: TransferState, count: u64) {
        match state {
            TransferState::Pending => self.pending += count,
            TransferState::Planning => self.planning += count,
            TransferState::Dispatching => self.dispatching += count,
            TransferState::Running => self.running += count,
            TransferState::Canceling => self.canceling += count,
            TransferState::Completed => self.completed += count,
            TransferState::Failed => self.failed += count,
            TransferState::Canceled => self.canceled += count,
            TransferState::PartialSuccess => self.partial_success += count,
        }
    }

    pub fn total(&self) -> u64 {
        self.pending
            + self.planning
            + self.dispatching
            + self.running
            + self.canceling
            + self.completed
            + self.failed
            + self.canceled
            + self.partial_success
    }

    /// Rows matching a (possibly empty) state selection, for the paged total.
    pub fn total_for(&self, states: &[TransferState]) -> u64 {
        if states.is_empty() {
            return self.total();
        }
        states.iter().map(|state| self.count_of(*state)).sum()
    }

    pub fn count_of(&self, state: TransferState) -> u64 {
        match state {
            TransferState::Pending => self.pending,
            TransferState::Planning => self.planning,
            TransferState::Dispatching => self.dispatching,
            TransferState::Running => self.running,
            TransferState::Canceling => self.canceling,
            TransferState::Completed => self.completed,
            TransferState::Failed => self.failed,
            TransferState::Canceled => self.canceled,
            TransferState::PartialSuccess => self.partial_success,
        }
    }

    pub fn bucket_counts(&self) -> TransferBucketCounts {
        TransferBucketCounts {
            all: self.total(),
            active: self.total_for(TRANSFER_BUCKET_ACTIVE),
            completed: self.total_for(TRANSFER_BUCKET_COMPLETED),
            failed: self.total_for(TRANSFER_BUCKET_FAILED),
            canceled: self.total_for(TRANSFER_BUCKET_CANCELED),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransferTenantSummary {
    pub tenant: String,
    pub pending: u64,
    pub executing: u64,
    pub completed: u64,
    pub failed: u64,
    pub canceled: u64,
    pub partial_success: u64,
    pub total: u64,
}

impl TransferTenantSummary {
    pub fn active(&self) -> u64 {
        self.pending.saturating_add(self.executing)
    }
}

#[cfg(test)]
mod tests {
    use super::{TransferCommand, TransferKind};

    #[test]
    fn overwrite_changes_default_client_request_id() {
        let overwrite = TransferCommand::default_client_request_id_with_overwrite(
            TransferKind::Export,
            "/source",
            "s3://bucket/target",
            true,
        );
        let no_overwrite = TransferCommand::default_client_request_id_with_overwrite(
            TransferKind::Export,
            "/source",
            "s3://bucket/target",
            false,
        );

        assert_ne!(overwrite, no_overwrite);
    }

    #[test]
    fn replicas_option_round_trips_and_rejects_invalid_values() {
        let mut command = TransferCommand::default();
        command.set_replicas(3);
        assert_eq!(command.replicas().unwrap(), Some(3));

        command.options.insert(
            TransferCommand::REPLICAS_OPTION.to_string(),
            "0".to_string(),
        );
        assert!(command.replicas().is_err());
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StaleTaskAttempt {
    pub task: TransferTaskRecord,
}

/// Formats a terminal transfer failure without guessing whether the underlying
/// error belongs to the source or target. `FsError` carries the concrete path;
/// preserving it avoids misreporting a missing target as a missing source.
pub fn transfer_failure_message(
    kind: TransferKind,
    phase: &str,
    source_path: &str,
    target_path: &str,
    err: &FsError,
) -> String {
    let direction = match kind {
        TransferKind::Load => "Load (UFS -> Curvine)",
        TransferKind::Export => "Export (Curvine -> UFS)",
    };
    let cause = err.to_string();
    let cause = cause.strip_suffix(": ").unwrap_or(&cause);
    format!(
        "{direction} failed during {phase}: source={source_path}; target={target_path}; cause={cause}"
    )
}
