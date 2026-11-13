//! Minimal regressions for terminal failure-source attribution: a real
//! fatal entry point (commit/publication/restore) records its source, an
//! ordinary UFS note cannot overwrite it, and a holder terminal with no
//! recorded fatal source stays neutral.
use super::RecoveryGate;

#[test]
fn fatal_source_survives_ordinary_notes() {
    let recovery = RecoveryGate::new();
    recovery.record_fatal_source("metadata journal commit failed after mutation");
    // An ordinary UFS diagnostic lands in `last_error`, never in the fatal
    // source.
    recovery.note_error("No workers available");
    assert_eq!(
        recovery.fatal_source().as_deref(),
        Some("metadata journal commit failed after mutation"),
        "an ordinary UFS note must not overwrite the recorded fatal source"
    );
}

#[test]
fn ordinary_note_alone_is_not_a_fatal_source() {
    let recovery = RecoveryGate::new();
    recovery.note_error("No workers available");
    assert_eq!(
        recovery.fatal_source(),
        None,
        "an ordinary UFS note must not be misattributed as a namespace fatal cause"
    );
}

#[test]
fn first_fatal_source_wins() {
    let recovery = RecoveryGate::new();
    recovery.record_fatal_source("metadata publication failed after durable commit");
    recovery.record_fatal_source("metadata restore failed after store replacement began");
    assert_eq!(
        recovery.fatal_source().as_deref(),
        Some("metadata publication failed after durable commit"),
        "the first recorded fatal source is kept (FAILED is one-way)"
    );
}
