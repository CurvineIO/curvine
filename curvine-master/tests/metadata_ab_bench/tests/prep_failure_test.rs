//! Fixture preparation must fail **in bounded time with a recorded error**, never
//! wedge the measurement.
//!
//! `args` runs on every worker thread *before* the start barrier. A panic there
//! would kill that worker while the others (and the main thread) block forever in
//! `barrier.wait()`. The by-id close therefore carries its preparation failure in
//! `Arg` and reports it from the body, which happens after the barrier and is
//! counted like any other error.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::fixtures::close_id_arg;
use crate::harness::{run, Env, Layout};

const THREADS: usize = 2;
const OPS: u64 = 3;

#[test]
fn missing_fixture_is_reported_without_hanging() {
    let budget = Duration::from_secs(20);
    let started = Instant::now();

    // No `make_files`: every `file_status` lookup fails, so `close_id_arg`
    // yields `id = None` and the body must surface a counted error.
    let report = crate::isolated(|fs| {
        let env = Arc::new(Env::new("prep_failure_probe", Layout::Same, THREADS, OPS));
        run(fs, &env, close_id_arg, crate::ops::complete_file_by_id_body, None)
    });

    let elapsed = started.elapsed();
    assert!(
        elapsed < budget,
        "preparation failure must be bounded, took {elapsed:?}"
    );
    // Every call failed, and none of them silently succeeded.
    assert_eq!(
        report.ok, 0,
        "no call may succeed when the fixture is missing"
    );
    assert_eq!(
        report.err,
        (THREADS as u64) * OPS,
        "every measured call must be counted as an error"
    );
    let first = report
        .first_err
        .as_deref()
        .expect("the failure reason must be recorded");
    assert!(
        first.contains("fixture inode id was not resolved"),
        "unexpected error text: {first}"
    );
}
