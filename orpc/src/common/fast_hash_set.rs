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
use std::collections::HashSet;
use std::hash::{BuildHasherDefault, Hash};
use std::ops::{Deref, DerefMut};

pub struct FastHashSet<T>(HashSet<T, BuildHasherDefault<FxHasher>>);

impl<T: Eq + Hash> FastHashSet<T> {
    pub fn new() -> Self {
        let inner = HashSet::with_hasher(BuildHasherDefault::<FxHasher>::default());
        Self(inner)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let inner =
            HashSet::with_capacity_and_hasher(capacity, BuildHasherDefault::<FxHasher>::default());
        Self(inner)
    }

    pub fn with_vec(vec: Vec<T>) -> Self {
        let mut inner = Self::with_capacity(vec.len());
        for item in vec {
            inner.insert(item);
        }
        inner
    }
}

impl<T: Eq + Hash> Default for FastHashSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Eq + Hash> Deref for FastHashSet<T> {
    type Target = HashSet<T, BuildHasherDefault<FxHasher>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Eq + Hash> DerefMut for FastHashSet<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K> Clone for FastHashSet<K>
where
    K: Clone,
{
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
