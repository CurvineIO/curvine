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

use base64::{engine::general_purpose::STANDARD, Engine as _};
use curvine_error::{FsError, FsResult};
use curvine_model::{TransferTaskRecord, TransferTaskReport, TransferTaskState};
use serde::{Deserialize, Serialize};

use super::store::is_duplicate_terminal_report;

/// Compact, lossless representation in `transfer_tasks.result_json`; an empty
/// column means no result. References are opaque and never fetched here.
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum StoredTaskResult {
    Inline {
        base64: String,
    },
    Reference {
        reference: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        size: Option<u64>,
    },
}

impl StoredTaskResult {
    fn encode_report(report: &TransferTaskReport) -> FsResult<Option<String>> {
        super::service::validate_result_fields(
            report.result_inline.as_deref(),
            report.result_ref.as_deref(),
            report.result_size,
        )?;
        let result = match (
            &report.result_inline,
            &report.result_ref,
            report.result_size,
        ) {
            (None, None, None) => return Ok(None),
            (Some(bytes), None, size) if size.is_none_or(|size| size == bytes.len() as u64) => {
                Self::Inline {
                    base64: STANDARD.encode(bytes),
                }
            }
            (None, Some(reference), size) if !reference.trim().is_empty() => Self::Reference {
                reference: reference.clone(),
                size,
            },
            _ => return Err(FsError::common("Invalid transfer task result fields")),
        };
        serde_json::to_string(&result)
            .map(Some)
            .map_err(|err| FsError::common(format!("Unable to encode transfer task result: {err}")))
    }

    pub(super) fn populate_status(
        json: &str,
        status: &mut curvine_proto::TransferTaskStatusProto,
    ) -> FsResult<()> {
        if json.is_empty() {
            return Ok(());
        }
        match serde_json::from_str(json).map_err(|err| {
            FsError::common(format!(
                "Unable to decode stored transfer task result: {err}"
            ))
        })? {
            Self::Inline { base64 } => {
                let bytes = STANDARD
                    .decode(base64)
                    .map_err(|_| FsError::common("Invalid stored inline transfer task result"))?;
                status.result_size = Some(bytes.len() as u64);
                status.result_inline = Some(bytes);
            }
            Self::Reference { reference, size } => {
                status.result_ref = Some(reference);
                status.result_size = size;
            }
        }
        Ok(())
    }
}

pub(super) enum TaskReportWrite {
    Rejected,
    Duplicate,
    ResultOnly(String),
    Update(Option<String>),
}

/// Call under the same lock/transaction as the task progress update. A missing
/// result leaves an earlier result from the same attempt intact.
pub(super) fn task_report_write(
    task: &TransferTaskRecord,
    report: &TransferTaskReport,
) -> FsResult<TaskReportWrite> {
    if task.attempt_id != report.attempt_id
        || task.worker_id != report.worker_id
        || task.worker_session_id != report.worker_session_id
    {
        return Ok(TaskReportWrite::Rejected);
    }
    let has_result = report.result_inline.is_some()
        || report.result_ref.is_some()
        || report.result_size.is_some();
    let abandons_attempt = matches!(
        report.state,
        TransferTaskState::Pending | TransferTaskState::Stale
    );
    if task.state == TransferTaskState::Pending && task.attempt_id != 0 {
        // A requeued task still carries the abandoned attempt's identity.
        // Even a progress-only report must not reactivate it before dispatch.
        return Ok(TaskReportWrite::Rejected);
    }
    if has_result && (task.state == TransferTaskState::Pending || abandons_attempt) {
        return Ok(TaskReportWrite::Rejected);
    }
    // An exact replay of the committed terminal report is acknowledged even
    // without a new result, so a lost ACK does not downgrade the worker's
    // completed attempt.
    if !has_result && is_duplicate_terminal_report(task, report) {
        return Ok(TaskReportWrite::Duplicate);
    }
    if !task.state.is_running()
        && !(has_result && task.state.is_terminal() && task.state == report.state)
    {
        return Ok(TaskReportWrite::Rejected);
    }

    let result = StoredTaskResult::encode_report(report)?;
    if let Some(json) = &result {
        if task.state == report.state
            && task.progress == report.progress
            && &task.result_json == json
        {
            // A retry after commit must not reapply progress or change timestamps.
            return Ok(TaskReportWrite::Duplicate);
        }
    }
    if !task.state.is_running() {
        return Ok(match result {
            Some(json) if task.result_json.is_empty() => TaskReportWrite::ResultOnly(json),
            Some(json) if task.result_json == json => TaskReportWrite::Duplicate,
            _ => TaskReportWrite::Rejected,
        });
    }
    Ok(TaskReportWrite::Update(
        if abandons_attempt && !task.result_json.is_empty() {
            Some(String::new())
        } else {
            result
        },
    ))
}
