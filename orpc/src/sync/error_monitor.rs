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

use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

// Used to set errors in asynchronous environments.
pub struct ErrorMonitor<E: Error> {
    has_error: AtomicBool,
    error: Mutex<Option<E>>,
}

impl<E: Error> ErrorMonitor<E> {
    pub fn new() -> Self {
        Self {
            has_error: AtomicBool::new(false),
            error: Mutex::new(None),
        }
    }

    pub fn has_error(&self) -> bool {
        self.has_error.load(Ordering::SeqCst)
    }

    // If this error has been set, it will be returned directly.
    pub fn set_error(&self, error: E) {
        if self.has_error() {
            return;
        }
        let mut e = self.error.lock().unwrap();
        self.has_error.store(true, Ordering::SeqCst);
        let _ = (*e).replace(error);
    }

    pub fn take_error(&self) -> Option<E> {
        if !self.has_error() {
            return None;
        }
        let mut e = self.error.lock().unwrap();
        e.take()
    }
}

impl<E: Error> Default for ErrorMonitor<E> {
    fn default() -> Self {
        Self::new()
    }
}
