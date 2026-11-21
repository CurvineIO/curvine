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

use curvine_error::{FsError, FsResult};
use curvine_model::{JobTaskType, LoadTaskInfo};
use curvine_runtime::common::SerdeUtils;

/// Framework-owned identity and opaque payload for a submitted task.
#[derive(Debug, Clone)]
pub struct TaskEnvelope {
    pub task_type: JobTaskType,
    pub task_id: String,
    pub job_id: String,
    pub run_id: Option<u64>,
    pub attempt_id: Option<u64>,
    pub worker_session_id: Option<String>,
    pub payload: Vec<u8>,
}

impl TaskEnvelope {
    /// Adapt the existing LoadTaskInfo representation without changing its
    /// bincode payload. This adapter is temporary until task submission is
    /// routed through JobPlugin.
    pub fn from_load_task(task: &LoadTaskInfo, task_type: JobTaskType) -> FsResult<Self> {
        let report = task.transfer_report.as_ref();
        Ok(Self {
            task_type,
            task_id: task.task_id.clone(),
            job_id: task.job.job_id.clone(),
            run_id: report.map(|x| x.run_id),
            attempt_id: report.map(|x| x.attempt_id),
            worker_session_id: report.map(|x| x.worker_session_id.clone()),
            payload: SerdeUtils::serialize(task)?,
        })
    }

    pub fn validate(
        &self,
        current_worker_session_id: &str,
        has_report_endpoint: bool,
    ) -> FsResult<()> {
        if self.run_id.is_some() && !has_report_endpoint {
            return Err(FsError::common(format!(
                "Reject transfer task {} because no Transfer report endpoint is available",
                self.task_id
            )));
        }
        if let Some(session_id) = &self.worker_session_id {
            if session_id != current_worker_session_id {
                return Err(FsError::common(format!(
                    "Reject transfer task {} for stale worker session {}, current session {}",
                    self.task_id, session_id, current_worker_session_id
                )));
            }
        }
        Ok(())
    }

    pub fn has_same_attempt(&self, other: &Self) -> bool {
        self.run_id.is_some() && self.run_id == other.run_id && self.attempt_id == other.attempt_id
    }

    pub fn is_older_than(&self, other: &Self) -> bool {
        match (self.run_id, self.attempt_id, other.run_id, other.attempt_id) {
            (Some(run), Some(attempt), Some(other_run), Some(other_attempt)) => {
                run < other_run || (run == other_run && attempt < other_attempt)
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TaskEnvelope;
    use curvine_model::{
        JobTaskType, LoadJobInfo, LoadTaskInfo, MountInfo, StorageType, TransferTaskReportInfo,
        TtlAction, WorkerAddress,
    };

    fn task(run_id: Option<u64>, attempt_id: Option<u64>, session: Option<&str>) -> LoadTaskInfo {
        LoadTaskInfo {
            job: LoadJobInfo {
                job_id: "job".into(),
                source_path: "source".into(),
                target_path: "target".into(),
                block_size: 4096,
                replicas: 1,
                storage_type: StorageType::default(),
                ttl_ms: 0,
                ttl_action: TtlAction::default(),
                mount_info: MountInfo::default(),
                create_time: 0,
                overwrite: None,
            },
            task_id: "task".into(),
            worker: WorkerAddress::default(),
            source_path: "source".into(),
            target_path: "target".into(),
            create_time: 0,
            source_read_plan_json: String::new(),
            transfer_report: run_id.map(|run_id| TransferTaskReportInfo {
                run_id,
                attempt_id: attempt_id.unwrap_or_default(),
                worker_id: 1,
                worker_session_id: session.unwrap_or_default().into(),
                report_target: String::new(),
                report_endpoints: vec!["endpoint".into()],
            }),
        }
    }

    #[test]
    fn load_task_is_adapted_without_changing_task_identity() {
        let envelope = TaskEnvelope::from_load_task(
            &task(Some(3), Some(4), Some("session")),
            JobTaskType::Load,
        )
        .unwrap();
        assert_eq!(envelope.task_type, JobTaskType::Load);
        assert_eq!(envelope.task_id, "task");
        assert_eq!(envelope.job_id, "job");
        assert_eq!(envelope.run_id, Some(3));
        assert_eq!(envelope.attempt_id, Some(4));
        assert_eq!(envelope.worker_session_id.as_deref(), Some("session"));
        assert!(!envelope.payload.is_empty());
    }

    #[test]
    fn envelope_validates_session_and_report_endpoint() {
        let envelope = TaskEnvelope::from_load_task(
            &task(Some(3), Some(4), Some("session")),
            JobTaskType::Load,
        )
        .unwrap();
        assert!(envelope.validate("session", true).is_ok());
        assert!(envelope.validate("other", true).is_err());
        assert!(envelope.validate("session", false).is_err());
    }

    #[test]
    fn envelope_compares_attempts() {
        let first =
            TaskEnvelope::from_load_task(&task(Some(1), Some(2), Some("s")), JobTaskType::Load)
                .unwrap();
        let duplicate =
            TaskEnvelope::from_load_task(&task(Some(1), Some(2), Some("s")), JobTaskType::Load)
                .unwrap();
        let newer =
            TaskEnvelope::from_load_task(&task(Some(1), Some(3), Some("s")), JobTaskType::Load)
                .unwrap();
        let older =
            TaskEnvelope::from_load_task(&task(Some(1), Some(1), Some("s")), JobTaskType::Load)
                .unwrap();
        assert!(first.has_same_attempt(&duplicate));
        assert!(older.is_older_than(&newer));
        assert!(!newer.is_older_than(&older));
    }
}
