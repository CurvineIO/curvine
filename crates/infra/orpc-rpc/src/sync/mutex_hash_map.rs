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
use std::sync::{Mutex, MutexGuard};

pub struct MutexHashMap<K, V>(pub Mutex<FastHashMap<K, V>>);

impl<K: Eq + Hash, V> MutexHashMap<K, V> {
    pub fn new() -> Self {
        Self(Mutex::new(FastHashMap::new()))
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(Mutex::new(FastHashMap::with_capacity(capacity)))
    }

    pub fn lock(&self) -> MutexGuard<'_, FastHashMap<K, V>> {
        self.0.lock().unwrap()
    }
}

impl<K: Eq + Hash, V> Default for MutexHashMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Deref for MutexHashMap<K, V> {
    type Target = Mutex<FastHashMap<K, V>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
