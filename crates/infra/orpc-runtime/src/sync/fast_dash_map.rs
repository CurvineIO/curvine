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
use fxhash::FxHasher;
use std::hash::{BuildHasherDefault, Hash};
use std::ops::Deref;

pub struct FastDashMap<K, V>(DashMap<K, V, BuildHasherDefault<FxHasher>>);

impl<K: Eq + Hash, V> FastDashMap<K, V> {
    pub fn new(capacity: usize, shard_amount: usize) -> Self {
        let inner = DashMap::with_capacity_and_hasher_and_shard_amount(
            capacity,
            Default::default(),
            shard_amount,
        );
        FastDashMap(inner)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let inner = DashMap::with_capacity_and_hasher(capacity, Default::default());
        FastDashMap(inner)
    }

    pub fn into_inner(self) -> DashMap<K, V, BuildHasherDefault<FxHasher>> {
        self.0
    }
}

impl<K: Eq + Hash, V> Deref for FastDashMap<K, V> {
    type Target = DashMap<K, V, BuildHasherDefault<FxHasher>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K: Eq + Hash, V> Default for FastDashMap<K, V> {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}
