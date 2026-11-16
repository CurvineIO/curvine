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

//! Wire contract of the `ClientHeartbeat` RPC (#1877).
//!
//! The session lease is the first master RPC whose payload is consumed by every
//! client type (FUSE, SDK, CLI, libsdk), so the field numbers below are part of
//! an external contract: a renumbering would silently break clients that already
//! decode these tags. These tests pin the tags and the proto2 `optional`
//! behaviour of the diagnostic fields, which is what lets an older peer omit
//! them.

use curvine_proto::{ClientAddressProto, ClientHeartbeatRequest, ClientHeartbeatResponse};
use prost::Message;

fn sample_address() -> ClientAddressProto {
    ClientAddressProto {
        client_name: "client-uuid".to_string(),
        hostname: "client-host".to_string(),
        ip_addr: "127.0.0.1".to_string(),
        port: 0,
    }
}

fn full_request() -> ClientHeartbeatRequest {
    ClientHeartbeatRequest {
        client: sample_address(),
        heartbeat_interval_ms: 12_000,
        pid: Some(4242),
        mount_point: Some("/mnt/cv".to_string()),
        client_version: Some("0.5.0-alpha".to_string()),
    }
}

#[test]
fn request_round_trips_every_field() {
    let encoded = full_request().encode_to_vec();
    let decoded = ClientHeartbeatRequest::decode(encoded.as_slice()).unwrap();

    assert_eq!(decoded.client, sample_address());
    assert_eq!(decoded.heartbeat_interval_ms, 12_000);
    assert_eq!(decoded.pid, Some(4242));
    assert_eq!(decoded.mount_point.as_deref(), Some("/mnt/cv"));
    assert_eq!(decoded.client_version.as_deref(), Some("0.5.0-alpha"));
}

/// The diagnostic fields are optional: a peer that knows only the session
/// identity and its cadence must still produce a decodable heartbeat.
#[test]
fn request_without_diagnostic_fields_decodes_with_defaults() {
    let minimal = ClientHeartbeatRequest {
        client: sample_address(),
        heartbeat_interval_ms: 12_000,
        pid: None,
        mount_point: None,
        client_version: None,
    };

    let decoded = ClientHeartbeatRequest::decode(minimal.encode_to_vec().as_slice()).unwrap();

    assert_eq!(decoded.client.client_name, "client-uuid");
    assert_eq!(decoded.heartbeat_interval_ms, 12_000);
    assert_eq!(decoded.pid, None);
    assert_eq!(decoded.mount_point, None);
    assert_eq!(decoded.client_version, None);
}

#[test]
fn response_round_trips() {
    let response = ClientHeartbeatResponse {
        session_timeout_ms: 60_000,
        new_session: true,
    };

    let decoded = ClientHeartbeatResponse::decode(response.encode_to_vec().as_slice()).unwrap();

    assert_eq!(decoded.session_timeout_ms, 60_000);
    assert!(decoded.new_session);
}

/// Pinned decoders: the response carries the enforced lease window on tag 1 and
/// the registration flag on tag 2.
#[test]
fn response_field_numbers_are_stable() {
    #[derive(Clone, PartialEq, ::prost::Message)]
    struct PinnedClientHeartbeatResponse {
        #[prost(uint64, required, tag = "1")]
        session_timeout_ms: u64,
        #[prost(bool, required, tag = "2")]
        new_session: bool,
    }

    let encoded = ClientHeartbeatResponse {
        session_timeout_ms: 60_000,
        new_session: true,
    }
    .encode_to_vec();

    let pinned = PinnedClientHeartbeatResponse::decode(encoded.as_slice()).unwrap();
    assert_eq!(pinned.session_timeout_ms, 60_000);
    assert!(pinned.new_session);
}

/// Pinned decoders: the request carries the session identity on tag 1 and the
/// client-reported cadence on tag 2.
#[test]
fn request_field_numbers_are_stable() {
    #[derive(Clone, PartialEq, ::prost::Message)]
    struct PinnedClientHeartbeatRequest {
        #[prost(message, required, tag = "1")]
        client: ClientAddressProto,
        #[prost(uint64, required, tag = "2")]
        heartbeat_interval_ms: u64,
    }

    let encoded = full_request().encode_to_vec();
    let pinned = PinnedClientHeartbeatRequest::decode(encoded.as_slice()).unwrap();

    assert_eq!(pinned.client, sample_address());
    assert_eq!(pinned.heartbeat_interval_ms, 12_000);
}
