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

//! A failing JVM builder must surface as an error, not a panic: release builds use
//! `panic = "abort"`, so a panic would kill the host process (e.g. a Spark executor).
//! Runs in its own test binary so no JVM exists in the process.

use curvine_hdfs_jni::jni::JVM;
use std::sync::atomic::{AtomicU32, Ordering};

static CALLS: AtomicU32 = AtomicU32::new(0);

#[test]
fn builder_error_is_returned_and_retried() {
    JVM.register_jvm(Box::new(|| {
        CALLS.fetch_add(1, Ordering::SeqCst);
        Err(anyhow::anyhow!("no libjvm here"))
    }));

    for attempt in 1..=2 {
        let err = JVM.get_or_init().expect_err("builder fails");
        let msg = format!("{err:#}");
        assert!(msg.contains("no libjvm here"), "{msg}");
        assert!(!msg.contains("panicked"), "{msg}");
        assert_eq!(CALLS.load(Ordering::SeqCst), attempt);
    }
    assert!(JVM.get().is_none());
}
