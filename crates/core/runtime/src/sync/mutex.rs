//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use parking_lot::{Mutex, RwLock};
use std::ops::Deref;
use std::sync::{Mutex as StdSyncMutex, RwLock as StdSyncRwLock};

pub struct FastMutex<T>(Mutex<T>);

impl<T> FastMutex<T> {
    pub fn new(t: T) -> Self {
        FastMutex(Mutex::new(t))
    }
}

impl<T> Deref for FastMutex<T> {
    type Target = Mutex<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct FastRwLock<T>(RwLock<T>);

impl<T> FastRwLock<T> {
    pub fn new(t: T) -> Self {
        FastRwLock(RwLock::new(t))
    }
}

impl<T> Deref for FastRwLock<T> {
    type Target = RwLock<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct StdMutex<T>(StdSyncMutex<T>);

impl<T> StdMutex<T> {
    pub fn new(t: T) -> Self {
        StdMutex(StdSyncMutex::new(t))
    }
}

impl<T> Deref for StdMutex<T> {
    type Target = StdSyncMutex<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct StdRwLock<T>(StdSyncRwLock<T>);

impl<T> StdRwLock<T> {
    pub fn new(t: T) -> Self {
        StdRwLock(StdSyncRwLock::new(t))
    }

    pub fn write(&self) -> std::sync::RwLockWriteGuard<'_, T> {
        self.0.write().unwrap()
    }

    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, T> {
        self.0.read().unwrap()
    }
}

impl<T> Deref for StdRwLock<T> {
    type Target = StdSyncRwLock<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct AsyncMutex<T>(tokio::sync::Mutex<T>);

impl<T> AsyncMutex<T> {
    pub fn new(t: T) -> Self {
        AsyncMutex(tokio::sync::Mutex::new(t))
    }
}

impl<T> Deref for AsyncMutex<T> {
    type Target = tokio::sync::Mutex<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct AsyncRwLock<T>(tokio::sync::RwLock<T>);

impl<T> AsyncRwLock<T> {
    pub fn new(t: T) -> Self {
        AsyncRwLock(tokio::sync::RwLock::new(t))
    }
}

impl<T> Deref for AsyncRwLock<T> {
    type Target = tokio::sync::RwLock<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
