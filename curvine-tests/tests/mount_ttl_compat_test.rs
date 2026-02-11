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

use curvine_common::proto::MountOptionsProto;
use curvine_common::state::{DEFAULT_MOUNT_TTL_ACTION, DEFAULT_MOUNT_TTL_MS};
use curvine_common::utils::ProtoUtils;
use std::collections::HashMap;

#[test]
fn test_mount_options_proto_without_ttl_uses_default_ttl() {
    let mut add_properties = HashMap::new();
    add_properties.insert(
        "s3.endpoint_url".to_string(),
        "http://localhost:9000".to_string(),
    );

    // Simulate an old client payload that does not carry ttl fields.
    let opts_pb = MountOptionsProto {
        update: false,
        add_properties,
        ttl_ms: None,
        ttl_action: None,
        consistency_strategy: None,
        storage_type: None,
        block_size: None,
        replicas: None,
        mount_type: 0,
        remove_properties: vec![],
        write_type: 2,
        provider: None,
    };

    let opts = ProtoUtils::mount_options_from_pb(opts_pb);
    assert_eq!(
        opts.ttl_ms,
        Some(DEFAULT_MOUNT_TTL_MS),
        "pb decode should backfill missing ttl_ms"
    );
    assert_eq!(
        opts.ttl_action,
        Some(DEFAULT_MOUNT_TTL_ACTION),
        "pb decode should backfill missing ttl_action"
    );

    let info = opts.to_info(1, "/mnt/s3", "s3://flink/user");

    assert_eq!(
        info.ttl_ms, DEFAULT_MOUNT_TTL_MS,
        "missing ttl_ms should fall back to mount default"
    );
    assert_eq!(
        info.ttl_action, DEFAULT_MOUNT_TTL_ACTION,
        "missing ttl_action should fall back to mount default"
    );
    assert!(
        info.auto_cache(),
        "missing ttl should not disable async cache load"
    );
}
