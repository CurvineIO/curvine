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

use curvine_proto::{GetFilesystemCapacityRequest, GetFilesystemCapacityResponse};
use prost::Message;

#[test]
fn test_get_filesystem_capacity_request_is_empty() {
    let request = GetFilesystemCapacityRequest {};
    let encoded = request.encode_to_vec();

    assert!(encoded.is_empty());
    assert_eq!(request.encoded_len(), 0);
    assert_eq!(
        GetFilesystemCapacityRequest::decode(encoded.as_slice()).unwrap(),
        request
    );
}

#[test]
fn test_get_filesystem_capacity_response_round_trip() {
    for (capacity, available) in [(0, 0), (4096, 2048), (i64::MAX, 0), (i64::MAX, i64::MAX)] {
        // Exhaustive construction also catches schema additions, even empty topology lists.
        let response = GetFilesystemCapacityResponse {
            capacity,
            available,
        };
        let encoded = response.encode_to_vec();
        let decoded = GetFilesystemCapacityResponse::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded, response);
    }
}

#[test]
fn test_get_filesystem_capacity_response_exact_two_field_encoding() {
    let response = GetFilesystemCapacityResponse {
        capacity: 150,
        available: 75,
    };

    // Exactly two int64 varints: capacity on tag 1, available on tag 2.
    assert_eq!(response.encode_to_vec(), [0x08, 0x96, 0x01, 0x10, 0x4b]);

    let zero = GetFilesystemCapacityResponse {
        capacity: 0,
        available: 0,
    };
    // Required fields remain on the wire even when both counters are zero.
    assert_eq!(zero.encode_to_vec(), [0x08, 0x00, 0x10, 0x00]);
}

#[test]
fn test_get_filesystem_capacity_response_payload_is_bounded() {
    // Negative values exercise the int64 wire bound, not valid capacity semantics.
    let values = [i64::MIN, -1, 0, 1, 127, 128, i64::MAX];
    for capacity in values {
        for available in values {
            let response = GetFilesystemCapacityResponse {
                capacity,
                available,
            };
            let encoded = response.encode_to_vec();

            assert_eq!(encoded.len(), response.encoded_len());
            assert!(
                encoded.len() <= 22,
                "capacity-only response exceeds two one-byte tags and two int64 varints: {} bytes",
                encoded.len()
            );
            assert_eq!(
                GetFilesystemCapacityResponse::decode(encoded.as_slice()).unwrap(),
                response
            );
        }
    }

    let worst_case = GetFilesystemCapacityResponse {
        capacity: i64::MIN,
        available: i64::MIN,
    };
    assert_eq!(worst_case.encoded_len(), 22);
}
