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

use fxhash::FxHasher;
use moka::sync::{Cache, CacheBuilder};
use std::hash::{BuildHasherDefault, Hash};
use std::ops::Deref;
use std::time::Duration;

pub struct FastSyncCache<K, V>(Cache<K, V, BuildHasherDefault<FxHasher>>);

impl<K, V> FastSyncCache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(capacity: u64, ttl: Duration) -> Self {
        let inner = CacheBuilder::new(capacity)
            .time_to_live(ttl)
            .build_with_hasher(BuildHasherDefault::<FxHasher>::default());

        Self(inner)
    }

    pub fn with_ttl(ttl: Duration) -> Self {
        let inner = CacheBuilder::default()
            .time_to_live(ttl)
            .build_with_hasher(BuildHasherDefault::<FxHasher>::default());

        Self(inner)
    }

    pub fn into_inner(self) -> Cache<K, V, BuildHasherDefault<FxHasher>> {
        self.0
    }
}

impl<K, V> Deref for FastSyncCache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    type Target = Cache<K, V, BuildHasherDefault<FxHasher>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
