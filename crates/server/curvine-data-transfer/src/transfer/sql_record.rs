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
use curvine_model::{TransferJobRecord, TransferTaskRecord};
use serde::ser::{SerializeMap, Serializer};

// Project only the legacy metadata into record_json. Do not serialize fields
// backed by dedicated columns, even as an intermediate JSON value.
macro_rules! record_json {
    ($name:ident, $record:ty, $($field:ident),+ $(,)?) => {
        pub(super) fn $name(value: &$record) -> FsResult<String> {
            let mut bytes = Vec::new();
            let mut serializer = serde_json::Serializer::new(&mut bytes);
            let result = (|| -> serde_json::Result<()> {
                let mut map = serializer.serialize_map(None)?;
                $(map.serialize_entry(stringify!($field), &value.$field)?;)+
                map.end()
            })();
            result.map_err(|_| FsError::common("Unable to encode transfer record metadata"))?;
            String::from_utf8(bytes).map_err(|_| FsError::common("Transfer metadata is not UTF-8"))
        }
    };
}

record_json!(
    job_record_json,
    TransferJobRecord,
    job_key,
    job_id,
    run_id,
    kind,
    source_path,
    target_path,
    command_json,
    mount_snapshot_json,
    secret_ref_json,
    cluster_snapshot_version,
    cv_metadata_epoch,
    state,
    owner,
    lease_epoch,
    lease_expire_at,
    cancel_requested,
    summary,
    client_request_id,
    submitter,
    tenant,
    created_at,
    updated_at,
);
record_json!(
    task_record_json,
    TransferTaskRecord,
    job_id,
    run_id,
    task_id,
    attempt_id,
    source_path,
    target_path,
    worker_id,
    worker_session_id,
    source_read_plan_json,
    report_target_json,
    state,
    progress,
    retry_count,
    attempt_started_at,
    last_report_at,
    stale_deadline_at,
    updated_at,
);

pub(super) const SCHEMA_LOCK_WAIT_SECS: i32 = 30;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transfer::store_test::{job, task};
    use curvine_model::TransferKind;

    #[test]
    fn dedicated_columns_are_not_duplicated_in_record_json() {
        let job = job(TransferKind::Export);
        let task = task(&job);
        for (json, mut expected, fields) in [
            (
                job_record_json(&job).unwrap(),
                serde_json::to_value(&job).unwrap(),
                &["spec_version", "spec_blob"][..],
            ),
            (
                task_record_json(&task).unwrap(),
                serde_json::to_value(&task).unwrap(),
                &["spec_blob", "result_json"][..],
            ),
        ] {
            for field in fields {
                expected.as_object_mut().unwrap().remove(*field);
            }
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&json).unwrap(),
                expected
            );
        }
    }
}
