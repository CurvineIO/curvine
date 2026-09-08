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

use curvine_model::{TransferCommand, TransferKind};

#[test]
fn replicas_change_only_explicit_load_request_ids() {
    let legacy = TransferCommand::default_client_request_id_with_overwrite(
        TransferKind::Load,
        "s3://bucket/source",
        "/target",
        true,
    );
    let unspecified = TransferCommand::default_client_request_id_with_overwrite_and_replicas(
        TransferKind::Load,
        "s3://bucket/source",
        "/target",
        true,
        None,
    );
    let replicas_one = TransferCommand::default_client_request_id_with_overwrite_and_replicas(
        TransferKind::Load,
        "s3://bucket/source",
        "/target",
        true,
        Some(1),
    );
    let replicas_three = TransferCommand::default_client_request_id_with_overwrite_and_replicas(
        TransferKind::Load,
        "s3://bucket/source",
        "/target",
        true,
        Some(3),
    );

    assert_eq!(unspecified, legacy);
    assert_ne!(replicas_one, replicas_three);
}
