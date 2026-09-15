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

use super::{join_path, optional_batch_target_status};
use curvine_error::FsError;
use curvine_fs_api::Path;

#[test]
fn join_path_handles_trailing_separator_and_scheme_prefix() {
    let dir = Path::from_str("file:///data/source/").unwrap();
    let joined = join_path(&dir, "part-0001.parquet").unwrap();
    assert_eq!(joined.full_path(), "file:///data/source/part-0001.parquet");

    let dir = Path::from_str("file:///data/source").unwrap();
    let joined = join_path(&dir, "a.bin").unwrap();
    assert_eq!(joined.full_path(), "file:///data/source/a.bin");
}

#[test]
fn join_path_preserves_nested_relative_name() {
    let dir = Path::from_str("hdfs://ns/data").unwrap();
    let joined = join_path(&dir, "sub/part-0").unwrap();
    assert_eq!(joined.full_path(), "hdfs://ns/data/sub/part-0");
}

#[test]
fn missing_batch_target_is_not_an_error() {
    let target = optional_batch_target_status(Err(FsError::file_not_found("/target"))).unwrap();
    assert!(target.is_none());
}
