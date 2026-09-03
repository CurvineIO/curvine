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

use crate::sync::AtomicBool;
use std::error::Error;
use std::sync::Mutex;

// Used to set errors in asynchronous environments.
pub struct ErrorMonitor<E: Error> {
    pub has_error: AtomicBool,
    pub error: Mutex<Option<E>>,
}

impl<E: Error> ErrorMonitor<E> {
    pub fn new() -> Self {
        Self {
            has_error: AtomicBool::new(false),
            error: Mutex::new(None),
        }
    }

    pub fn has_error(&self) -> bool {
        self.has_error.get()
    }

    pub fn error(&self) -> &Mutex<Option<E>> {
        &self.error
    }

    // Keep the first error; later callers return without overwriting.
    pub fn set_error(&self, error: E) {
        if self.has_error() {
            return;
        }
        let mut e = self.error.lock().unwrap();
        if e.is_some() {
            return;
        }
        self.has_error.set(true);
        *e = Some(error);
    }

    pub fn take_error(&self) -> Option<E> {
        if !self.has_error() {
            return None;
        }
        let mut e = self.error.lock().unwrap();
        e.take()
    }

    pub fn check_error(&self) -> Result<(), E> {
        match self.take_error() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl<E: Error> Default for ErrorMonitor<E> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::thread::Barrier;

    use crate::sync::ErrorMonitor;

    /// A later set_error must not overwrite the first stored error.
    #[test]
    fn first_error_wins() {
        let monitor = ErrorMonitor::<String>::new();
        monitor.set_error("first".to_string());
        monitor.set_error("second".to_string());

        assert!(monitor.has_error());
        assert_eq!(monitor.take_error().as_deref(), Some("first"));
        assert_eq!(monitor.take_error(), None);
    }

    /// Concurrent set_error callers race, but exactly one error is stored
    /// and it is one of the attempted values (first-wins invariant).
    #[test]
    fn concurrent_set_error_stores_single_error() {
        let monitor = Arc::new(ErrorMonitor::<String>::new());
        let barrier = Arc::new(Barrier::new(2));

        let m1 = monitor.clone();
        let b1 = barrier.clone();
        let t1 = thread::spawn(move || {
            b1.wait();
            m1.set_error("first".to_string());
        });

        let b2 = barrier.clone();
        let t2 = thread::spawn(move || {
            b2.wait();
            monitor.set_error("second".to_string());
        });

        t1.join().unwrap();
        t2.join().unwrap();

        assert!(monitor.has_error());
        let e = monitor.take_error();
        assert!(e.as_deref() == Some("first") || e.as_deref() == Some("second"));
    }
}
