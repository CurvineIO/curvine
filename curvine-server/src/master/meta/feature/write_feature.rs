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

use orpc::common::Utils;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

// Tag file is being written
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteFeature {
    pub clients: HashSet<String>,
}

impl WriteFeature {
    pub fn new(client_name: String) -> Self {
        let mut clients = HashSet::new();
        clients.insert(client_name);
        Self { clients }
    }
}

impl Default for WriteFeature {
    fn default() -> Self {
        Self::new(Utils::rand_str(8))
    }
}
