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

use crate::{FsError, FsResult};
use curvine_proto::TaskEnvelopeProto;
use prost::Message;

/// Encode a task envelope for a non-legacy task. Load/Export use `LoadTaskInfo` instead.
pub fn encode_task_envelope(envelope: &TaskEnvelopeProto) -> FsResult<Vec<u8>> {
    validate(envelope)?;
    Ok(envelope.encode_to_vec())
}

/// Decode a task envelope without interpreting the plugin's spec or choosing a plugin.
/// Unknown protobuf fields are ignored. The plugin must check the spec version it supports.
pub fn decode_task_envelope(bytes: &[u8]) -> FsResult<TaskEnvelopeProto> {
    let envelope = TaskEnvelopeProto::decode(bytes)
        .map_err(|_| FsError::invalid_argument("Invalid protobuf task envelope"))?;
    validate(&envelope)?;
    Ok(envelope)
}

fn validate(envelope: &TaskEnvelopeProto) -> FsResult<()> {
    // Prost can default absent proto2 required fields; reject those defaults for identity.
    if envelope.job_id.is_empty()
        || envelope.task_id.is_empty()
        || envelope.run_id == 0
        || envelope.attempt_id == 0
        || envelope.worker_id.is_none()
        || envelope.worker_session_id.is_empty()
    {
        return Err(FsError::invalid_argument(
            "Task envelope requires a job, task, run, attempt, worker, and worker session",
        ));
    }
    if envelope.spec_version == 0 {
        return Err(FsError::invalid_argument(
            "Task envelope spec_version must be nonzero",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope() -> TaskEnvelopeProto {
        TaskEnvelopeProto {
            job_id: "job-1".into(),
            task_id: "task-1".into(),
            run_id: 2,
            attempt_id: 3,
            worker_id: Some(0),
            worker_session_id: "worker-session-1".into(),
            spec_version: 17,
            spec: Some(vec![0, 0xff, 1]),
            report_endpoints: vec!["transfer-1:9010".into(), "transfer-2:9010".into()],
        }
    }

    #[test]
    fn envelope_roundtrip() {
        let envelope = envelope();
        let encoded = encode_task_envelope(&envelope).unwrap();
        assert_eq!(decode_task_envelope(&encoded).unwrap(), envelope);
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let envelope = envelope();
        let mut encoded = encode_task_envelope(&envelope).unwrap();
        // An unknown length-delimited field (tag 15, length 3).
        encoded.extend_from_slice(&[0x7a, 3, 1, 2, 3]);
        assert_eq!(decode_task_envelope(&encoded).unwrap(), envelope);
    }

    #[test]
    fn missing_optional_fields_use_defaults() {
        let envelope = TaskEnvelopeProto {
            spec: None,
            report_endpoints: Vec::new(),
            ..envelope()
        };
        let decoded = decode_task_envelope(&encode_task_envelope(&envelope).unwrap()).unwrap();
        assert_eq!(decoded.spec, None);
        assert!(decoded.spec().is_empty());
        assert!(decoded.report_endpoints.is_empty());
    }

    #[test]
    fn invalid_identity_or_spec_version_is_rejected() {
        let no_session = TaskEnvelopeProto {
            worker_session_id: String::new(),
            ..envelope()
        };
        let no_version = TaskEnvelopeProto {
            spec_version: 0,
            ..envelope()
        };
        let no_worker = TaskEnvelopeProto {
            worker_id: None,
            ..envelope()
        };
        for invalid in [no_session, no_version, no_worker] {
            assert!(encode_task_envelope(&invalid).is_err());
            assert!(decode_task_envelope(&invalid.encode_to_vec()).is_err());
        }
        assert!(decode_task_envelope(&[]).is_err());
    }

    #[test]
    fn truncated_envelope_is_rejected() {
        let mut encoded = encode_task_envelope(&envelope()).unwrap();
        encoded.pop();
        assert!(decode_task_envelope(&encoded).is_err());
    }
}
