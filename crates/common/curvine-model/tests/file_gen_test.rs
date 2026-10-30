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

use curvine_model::{ExtendedBlock, FileBlocks, FileGen, FileStatus, LocatedBlock};

#[test]
fn round_trips_through_wire_form() {
    let gen = FileGen::new(9001, 2147483648, 1000, 77);
    assert_eq!(gen.to_string(), "9001:2147483648:1000:77");
    assert_eq!(gen.to_string().parse::<FileGen>().unwrap(), gen);
}

#[test]
fn rejects_wrong_field_count_and_non_numbers() {
    for bad in ["", "1:2:3", "1:2:3:4:5", "1:2:3:x", "a:2:3:4"] {
        assert!(bad.parse::<FileGen>().is_err(), "accepted {:?}", bad);
    }
}

#[test]
fn of_blocks_uses_status_and_last_block() {
    let status = FileStatus {
        id: 7,
        len: 10,
        mtime: 99,
        ..Default::default()
    };
    let located = |id: i64| LocatedBlock {
        block: ExtendedBlock {
            id,
            ..Default::default()
        },
        ..Default::default()
    };

    let blocks = FileBlocks::new(status.clone(), vec![located(11), located(12)]);
    assert_eq!(FileGen::of_blocks(&blocks), FileGen::new(7, 10, 99, 12));

    let empty = FileBlocks::new(status, vec![]);
    assert_eq!(FileGen::of_blocks(&empty), FileGen::new(7, 10, 99, 0));
}
