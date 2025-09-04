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
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::hash::{BuildHasherDefault, Hash};
use std::ops::{Deref, DerefMut};

pub struct FastHashMap<K, V>(HashMap<K, V, BuildHasherDefault<FxHasher>>);

impl<K: Eq + Hash, V> FastHashMap<K, V> {
    pub fn new() -> Self {
        let inner = HashMap::with_hasher(BuildHasherDefault::<FxHasher>::default());
        Self(inner)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let inner =
            HashMap::with_capacity_and_hasher(capacity, BuildHasherDefault::<FxHasher>::default());
        Self(inner)
    }
}

impl<K: Eq + Hash, V> Default for FastHashMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash, V> Deref for FastHashMap<K, V> {
    type Target = HashMap<K, V, BuildHasherDefault<FxHasher>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K: Eq + Hash, V> DerefMut for FastHashMap<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K, V> Debug for FastHashMap<K, V>
where
    K: Debug,
    V: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.0.iter()).finish()
    }
}
