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

use std::ops::Deref;
use std::sync::atomic::{AtomicI32, AtomicI64 as StdAtomicI64, AtomicU64, AtomicUsize, Ordering};

pub struct AtomicLong(StdAtomicI64);

const ATOMIC_ORDERING: Ordering = Ordering::SeqCst;

impl AtomicLong {
    pub fn new(value: i64) -> Self {
        Self(StdAtomicI64::new(value))
    }

    pub fn get(&self) -> i64 {
        self.0.load(ATOMIC_ORDERING)
    }

    pub fn set(&self, value: i64) {
        self.0.store(value, ATOMIC_ORDERING)
    }

    pub fn get_and_add(&self, value: i64) -> i64 {
        self.0.fetch_add(value, ATOMIC_ORDERING)
    }

    pub fn add_and_get(&self, value: i64) -> i64 {
        self.0.fetch_add(value, ATOMIC_ORDERING) + value
    }

    pub fn incr(&self) {
        self.0.fetch_add(1, ATOMIC_ORDERING);
    }

    pub fn decr(&self) {
        self.0.fetch_sub(1, ATOMIC_ORDERING);
    }

    pub fn next(&self) -> i64 {
        self.0.fetch_add(1, ATOMIC_ORDERING) + 1
    }

    pub fn compare_and_set(&self, old: i64, new: i64) -> bool {
        let res = self
            .0
            .compare_exchange(old, new, ATOMIC_ORDERING, ATOMIC_ORDERING);
        res.is_ok()
    }
}

pub struct AtomicCounter(AtomicU64);

impl AtomicCounter {
    pub fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }

    pub fn get(&self) -> u64 {
        self.0.load(ATOMIC_ORDERING)
    }

    pub fn set(&self, value: u64) {
        self.0.store(value, ATOMIC_ORDERING)
    }

    pub fn get_and_add(&self, value: u64) -> u64 {
        self.0.fetch_add(value, ATOMIC_ORDERING)
    }

    pub fn add_and_get(&self, value: u64) -> u64 {
        self.0.fetch_add(value, ATOMIC_ORDERING) + value
    }

    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, ATOMIC_ORDERING) + 1
    }

    pub fn incr(&self) {
        self.0.fetch_add(1, ATOMIC_ORDERING);
    }

    pub fn decr(&self) {
        self.0.fetch_sub(1, ATOMIC_ORDERING);
    }

    pub fn compare_and_set(&self, old: u64, new: u64) -> bool {
        let res = self
            .0
            .compare_exchange(old, new, ATOMIC_ORDERING, ATOMIC_ORDERING);
        res.is_ok()
    }
}

pub struct AtomicLen(AtomicUsize);

impl AtomicLen {
    pub fn new(value: usize) -> Self {
        Self(AtomicUsize::new(value))
    }

    pub fn get(&self) -> usize {
        self.0.load(ATOMIC_ORDERING)
    }

    pub fn set(&self, value: usize) {
        self.0.store(value, ATOMIC_ORDERING)
    }

    pub fn get_and_add(&self, value: usize) -> usize {
        self.0.fetch_add(value, ATOMIC_ORDERING)
    }

    pub fn add_and_get(&self, value: usize) -> usize {
        self.0.fetch_add(value, ATOMIC_ORDERING) + value
    }

    pub fn next(&self) -> usize {
        self.0.fetch_add(1, ATOMIC_ORDERING) + 1
    }

    pub fn incr(&self) {
        self.0.fetch_add(1, ATOMIC_ORDERING);
    }

    pub fn decr(&self) {
        self.0.fetch_sub(1, ATOMIC_ORDERING);
    }

    pub fn compare_and_set(&self, old: usize, new: usize) -> bool {
        let res = self
            .0
            .compare_exchange(old, new, ATOMIC_ORDERING, ATOMIC_ORDERING);
        res.is_ok()
    }
}

pub struct AtomicHandle(AtomicI32);

impl AtomicHandle {
    pub fn new(value: i32) -> Self {
        Self(AtomicI32::new(value))
    }

    pub fn get(&self) -> i32 {
        self.0.load(ATOMIC_ORDERING)
    }

    pub fn set(&self, value: i32) {
        self.0.store(value, ATOMIC_ORDERING)
    }

    pub fn get_and_add(&self, value: i32) -> i32 {
        self.0.fetch_add(value, ATOMIC_ORDERING)
    }

    pub fn add_and_get(&self, value: i32) -> i32 {
        self.0.fetch_add(value, ATOMIC_ORDERING) + value
    }

    pub fn next(&self) -> i32 {
        self.0.fetch_add(1, ATOMIC_ORDERING) + 1
    }

    pub fn incr(&self) {
        self.0.fetch_add(1, ATOMIC_ORDERING);
    }

    pub fn compare_and_set(&self, old: i32, new: i32) -> bool {
        let res = self
            .0
            .compare_exchange(old, new, ATOMIC_ORDERING, ATOMIC_ORDERING);
        res.is_ok()
    }
}

pub struct AtomicBool(std::sync::atomic::AtomicBool);

impl AtomicBool {
    pub fn new(v: bool) -> Self {
        Self(std::sync::atomic::AtomicBool::new(v))
    }

    pub fn get(&self) -> bool {
        self.0.load(ATOMIC_ORDERING)
    }

    pub fn set(&self, v: bool) {
        self.0.store(v, ATOMIC_ORDERING)
    }

    pub fn compare_and_set(&self, old: bool, new: bool) -> bool {
        let res = self
            .0
            .compare_exchange(old, new, ATOMIC_ORDERING, ATOMIC_ORDERING);
        res.is_ok()
    }
}

impl Deref for AtomicBool {
    type Target = std::sync::atomic::AtomicBool;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
