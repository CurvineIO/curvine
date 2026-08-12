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

use dashmap::DashMap;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;

// Thread-safe map encapsulation, achieving concurrent security based on atomic operations.
#[derive(Clone)]
pub struct SyncMap<K, V>(Arc<DashMap<K, V>>);

impl<K, V> SyncMap<K, V>
where
    K: Eq + Hash,
{
    pub fn new() -> Self {
        Self(Arc::new(DashMap::new()))
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(DashMap::with_capacity(capacity)))
    }
}

impl<K, V> Deref for SyncMap<K, V> {
    type Target = DashMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V> Default for SyncMap<K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}
