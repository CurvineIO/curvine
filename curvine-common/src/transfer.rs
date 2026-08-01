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

use crate::error::{ErrorKind, FsError};
use crate::state::TransferKind;

pub fn transfer_failure_message(
    kind: TransferKind,
    source_path: &str,
    target_path: &str,
    err: &FsError,
) -> String {
    let source_label = match kind {
        TransferKind::Load => "source object",
        TransferKind::Export => "source file",
    };
    match err.kind() {
        ErrorKind::FileNotFound | ErrorKind::Expired => {
            format!("{source_label} not found: {source_path}")
        }
        ErrorKind::FileAlreadyExists => format!("target already exists: {target_path}"),
        ErrorKind::ParentNotDir | ErrorKind::NotADirectory => {
            format!("target parent is not a directory: {target_path}")
        }
        ErrorKind::IsADirectory => format!("target is a directory: {target_path}"),
        ErrorKind::ReadOnly => format!("target is read-only: {target_path}"),
        ErrorKind::DiskOutOfSpace => {
            format!("Not enough Curvine worker disk space to transfer {source_path}; free space and retry")
        }
        ErrorKind::Timeout => format!(
            "Timed out while accessing {source_path}; verify storage connectivity and retry"
        ),
        ErrorKind::IO | ErrorKind::Ufs | ErrorKind::Pipeline => format!(
            "Cannot access transfer storage for {source_path}; verify the mount, credentials, and network connectivity"
        ),
        ErrorKind::TransferStoreUnavailable => {
            "Transfer metadata store is unavailable; retry after it recovers".to_string()
        }
        ErrorKind::TransferOverloaded => "Transfer service is busy; retry later".to_string(),
        _ => format!("Transfer from {source_path} to {target_path} failed: {err}"),
    }
}
