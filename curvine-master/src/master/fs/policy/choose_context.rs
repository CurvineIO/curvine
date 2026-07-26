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

use crate::master::fs::context::ValidateAddBlock;
use curvine_common::state::StoragePolicy;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct ChooseContext {
    pub replicas: u16,
    pub block_size: i64,
    pub storage_policy: StoragePolicy,
    pub client_host: String,
    pub exclude_workers: HashSet<u32>,
}

impl ChooseContext {
    pub fn with_block(block: ValidateAddBlock, exclude_workers: Vec<u32>) -> Self {
        Self {
            replicas: block.replicas,
            block_size: block.block_size,
            storage_policy: block.storage_policy,
            client_host: block.client_host,
            exclude_workers: HashSet::from_iter(exclude_workers),
        }
    }

    pub fn with_num(num: u16, size: i64, exclude_workers: Vec<u32>) -> Self {
        Self {
            replicas: num,
            block_size: size,
            storage_policy: StoragePolicy::default(),
            client_host: "".to_string(),
            exclude_workers: HashSet::from_iter(exclude_workers),
        }
    }
}
