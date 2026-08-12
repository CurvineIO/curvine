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

use crate::common::FastHashMap;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct RwLockHashMap<K, V>(pub RwLock<FastHashMap<K, V>>);

impl<K: Eq + Hash, V> RwLockHashMap<K, V> {
    pub fn new() -> Self {
        Self(RwLock::new(FastHashMap::new()))
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(RwLock::new(FastHashMap::with_capacity(capacity)))
    }

    pub fn read(&self) -> RwLockReadGuard<'_, FastHashMap<K, V>> {
        self.0.read().unwrap()
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, FastHashMap<K, V>> {
        self.0.write().unwrap()
    }
}

impl<K: Eq + Hash, V> Default for RwLockHashMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash, V> Deref for RwLockHashMap<K, V> {
    type Target = RwLock<FastHashMap<K, V>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
