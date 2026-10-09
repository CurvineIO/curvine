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

use curvine_data_transfer::transfer::SqliteTransferStore;
use curvine_error::ErrorKind;
use std::fs;
use std::path::PathBuf;

fn unusable_store_path() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "curvine-transfer-store-error-{}",
        std::process::id()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn sqlite_store_unavailable_error_keeps_backend_detail() {
    // A directory cannot host the database file, so SQLite rejects the open and
    // the reported error must carry that cause.
    let path = unusable_store_path();
    let err = match SqliteTransferStore::open(&path) {
        Ok(_) => panic!("a directory must not be accepted as the sqlite store"),
        Err(err) => err,
    };
    let _ = fs::remove_dir_all(&path);

    assert!(matches!(err.kind(), ErrorKind::TransferStoreUnavailable));
    let message = err.to_string();
    assert!(message.contains("Transfer metadata store is unavailable"));
    assert!(
        message.contains("unable to open database file"),
        "the backend cause must stay in the message: {message}"
    );
}
