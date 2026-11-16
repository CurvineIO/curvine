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
        run(
            fs,
            &env,
            close_id_arg,
            crate::ops::complete_file_by_id_body,
            None,
        )
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

#[test]
fn successful_recycle_keeps_successful_samples() {
    let report = crate::isolated(|fs| {
        let env = Arc::new(Env::new(
            "recycle_success_probe",
            Layout::Same,
            THREADS,
            OPS,
        ));
        run(
            fs,
            &env,
            crate::fixtures::path_arg,
            |_, _| Ok(()),
            Some(|_, _, _, _| Ok(())),
        )
    });

    assert_eq!(report.ok, THREADS as u64 * OPS);
    assert_eq!(report.err, 0);
    assert_eq!(report.lat.len(), THREADS * OPS as usize);
}

#[test]
fn recycle_failure_is_reported_and_stops_the_worker() {
    let report = crate::isolated(|fs| {
        let env = Arc::new(Env::new(
            "recycle_failure_probe",
            Layout::Same,
            THREADS,
            OPS,
        ));
        run(
            fs,
            &env,
            crate::fixtures::path_arg,
            |_, _| Ok(()),
            Some(|fs, _, _, _| fs.file_status("/missing-recycle-fixture").map(|_| ())),
        )
    });

    assert_eq!(
        report.ok, 0,
        "a failed recycle must invalidate its iteration"
    );
    assert_eq!(
        report.err, THREADS as u64,
        "each worker must stop at its first recycle failure"
    );
    let first = report
        .first_err
        .as_deref()
        .expect("recycle failure must be recorded");
    assert!(
        first.contains("fixture recycle failed"),
        "unexpected error text: {first}"
    );
}

#[test]
fn operation_and_recycle_failures_are_both_recorded() {
    let report = crate::isolated(|fs| {
        let env = Arc::new(Env::new("both_failures_probe", Layout::Same, THREADS, OPS));
        run(
            fs,
            &env,
            crate::fixtures::path_arg,
            |fs, _| fs.file_status("/missing-operation-fixture").map(|_| ()),
            Some(|fs, _, _, _| fs.file_status("/missing-recycle-fixture").map(|_| ())),
        )
    });

    assert_eq!(report.ok, 0);
    assert_eq!(report.err, THREADS as u64);
    let first = report
        .first_err
        .as_deref()
        .expect("both failures must be recorded");
    assert!(
        first.contains("operation failed"),
        "missing operation error: {first}"
    );
    assert!(
        first.contains("fixture recycle failed"),
        "missing recycle error: {first}"
    );
}
