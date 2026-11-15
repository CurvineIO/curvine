//! Deterministic checks for the background rate computation.
//!
//! Lives in the bench's own `tests/` subdirectory (registered by `main.rs`) so
//! the harness source itself stays free of inline unit tests.

use crate::background::rate_ratio;

#[cfg(test)]
#[test]
fn total_qps_is_split_across_threads() {
    // 1 thread: total == per-thread.
    assert!((rate_ratio(20_000.0, 1, 20_000) - 1.0).abs() < 1e-9);
    // 8 threads: a correct run reaches 8x the per-thread rate.
    assert!((rate_ratio(160_000.0, 8, 20_000) - 1.0).abs() < 1e-9);
    // 32 threads likewise; the old denominator would report 32.0 here.
    assert!((rate_ratio(640_000.0, 32, 20_000) - 1.0).abs() < 1e-9);
    // Half the target.
    assert!((rate_ratio(320_000.0, 32, 20_000) - 0.5).abs() < 1e-9);
}

#[test]
fn degenerate_inputs_do_not_panic() {
    assert_eq!(rate_ratio(1_000.0, 0, 20_000), 0.0);
    assert_eq!(rate_ratio(1_000.0, 8, 0), 0.0);
    assert_eq!(rate_ratio(f64::NAN, 8, 20_000), 0.0);
    assert_eq!(rate_ratio(f64::INFINITY, 8, 20_000), 0.0);
    // Absurd inputs must saturate rather than wrap: the denominator becomes
    // u64::MAX, so the result is a tiny positive number inside [0, 1] — the
    // point is "no panic, no overflow", not an exact value.
    let extreme = rate_ratio(1.0, usize::MAX, u64::MAX);
    assert!((0.0..=1.0).contains(&extreme), "got {extreme}");
}
