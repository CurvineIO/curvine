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

use crate::FileStatus;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrashEntry {
    pub path: String,
    pub origin: String,
    pub delete_ms: i64,
    pub status: FileStatus,
}

// Result of a restore. Both fields are absent on a retry-cache hit: the
// original attempt already moved the entry, so the master cannot resolve
// either value a second time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RestoreTrashResult {
    pub dst: Option<String>,
    pub status: Option<FileStatus>,
}
