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
    // Description of the first error, captured when it was set. `take_error`
    // hands the typed error to a single caller, so later callers would
    // otherwise fall back to a bare channel symptom ("the sender dropped")
    // with no usable cause. The text keeps the original message verbatim so
    // message-based classification still sees the transport cause.
    message: Mutex<Option<String>>,
}

impl<E: Error> ErrorMonitor<E> {
    pub fn new() -> Self {
        Self {
            has_error: AtomicBool::new(false),
            error: Mutex::new(None),
            message: Mutex::new(None),
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
        *self.message.lock().unwrap() = Some(error.to_string());
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

    /// Description of the recorded error, available after `take_error` consumed
    /// the typed value.
    pub fn error_message(&self) -> Option<String> {
        self.message.lock().unwrap().clone()
    }

    /// Resolve a caller-visible error without ever degrading to a bare channel
    /// symptom: the first caller receives the recorded error unchanged, later
    /// callers receive `describe` applied to the recorded message.
    ///
    /// Returns `None` when no error was ever recorded, so the caller keeps its
    /// own error (typically the real cause of the channel closing).
    pub fn resolve_error<F>(&self, describe: F) -> Option<E>
    where
        F: FnOnce(&str) -> E,
    {
        if let Some(error) = self.take_error() {
            return Some(error);
        }
        self.error_message().map(|message| describe(&message))
    }
}

impl<E: Error> Default for ErrorMonitor<E> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use crate::sync::ErrorMonitor;

    #[derive(Debug, PartialEq)]
    struct TestError(&'static str);

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl std::error::Error for TestError {}

    /// A later set_error must not overwrite the first stored error.
    #[test]
    fn first_error_wins() {
        let monitor = ErrorMonitor::<TestError>::new();
        monitor.set_error(TestError("first"));
        monitor.set_error(TestError("second"));

        assert!(monitor.has_error());
        assert_eq!(monitor.take_error().as_ref().map(|e| e.0), Some("first"));
        assert_eq!(monitor.take_error(), None);
    }

    /// The typed error goes to one caller, but the recorded description must
    /// outlive it so later callers do not degrade to a bare channel symptom.
    #[test]
    fn error_message_survives_take_error() {
        let monitor = ErrorMonitor::<TestError>::new();
        monitor.set_error(TestError("connection refused"));

        assert_eq!(
            monitor.take_error().as_ref().map(|e| e.0),
            Some("connection refused")
        );
        assert_eq!(
            monitor.error_message().as_deref(),
            Some("connection refused")
        );
    }

    /// resolve_error serves the typed error first, then falls back to the
    /// recorded message so every caller observes the original cause.
    #[test]
    fn resolve_error_repeats_the_cause_for_every_caller() {
        let monitor = ErrorMonitor::<TestError>::new();
        monitor.set_error(TestError("connection refused"));

        assert_eq!(
            monitor
                .resolve_error(|m| TestError(Box::leak(m.to_string().into_boxed_str())))
                .as_ref()
                .map(|e| e.0),
            Some("connection refused")
        );
        assert_eq!(
            monitor
                .resolve_error(|m| TestError(Box::leak(m.to_string().into_boxed_str())))
                .as_ref()
                .map(|e| e.0),
            Some("connection refused")
        );
    }

    /// With no recorded error the caller keeps its own error instead of
    /// inheriting a stale cause.
    #[test]
    fn resolve_error_keeps_caller_error_when_nothing_recorded() {
        let monitor = ErrorMonitor::<TestError>::new();
        assert!(monitor.resolve_error(|_| TestError("derived")).is_none());
    }

    /// Concurrent set_error callers race, but exactly one error is stored
    /// and it is one of the attempted values (first-wins invariant).
    #[test]
    fn concurrent_set_error_stores_single_error() {
        let monitor = Arc::new(ErrorMonitor::<TestError>::new());
        let barrier = Arc::new(Barrier::new(2));

        let m1 = monitor.clone();
        let b1 = barrier.clone();
        let t1 = thread::spawn(move || {
            b1.wait();
            m1.set_error(TestError("first"));
        });

        let b2 = barrier.clone();
        let m2 = monitor.clone();
        let t2 = thread::spawn(move || {
            b2.wait();
            m2.set_error(TestError("second"));
        });

        t1.join().unwrap();
        t2.join().unwrap();

        assert!(monitor.has_error());
        let e = monitor.take_error();
        let msg = e.as_ref().map(|e| e.0);
        assert!(msg == Some("first") || msg == Some("second"));
    }
}
