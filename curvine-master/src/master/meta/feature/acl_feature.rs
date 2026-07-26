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

use serde::{Deserialize, Serialize};

// File and directory permission control.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclFeature {
    pub(crate) owner: String,
    pub(crate) group: String,
    pub(crate) mode: u32,
}

impl AclFeature {
    pub const DEFAULT_MODE: u32 = 0o777;

    pub fn with_mode(mode: u32) -> Self {
        Self {
            owner: "".to_string(),
            group: "".to_string(),
            mode,
        }
    }
}

impl Default for AclFeature {
    fn default() -> Self {
        Self::with_mode(Self::DEFAULT_MODE)
    }
}
