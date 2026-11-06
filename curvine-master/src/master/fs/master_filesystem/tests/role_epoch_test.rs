use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[test]
fn metadata_write_rejects_epoch_from_before_role_fence() {
    let fs = test_fs("metadata-write-role-epoch");
    let old_epoch = fs.journal_writer.metadata_epoch().unwrap();
    fs.journal_writer.fence_metadata();
    fs.journal_writer.unfence_metadata();
    let changed = AtomicBool::new(false);
    let result = fs.run_metadata_write_epoch(old_epoch, || {
        changed.store(true, Ordering::Relaxed);
        Ok(())
    });
    assert!(result.is_err());
    assert!(!changed.load(Ordering::Relaxed));
    fs.run_metadata_write(|| {
        changed.store(true, Ordering::Relaxed);
        Ok(())
    })
    .unwrap();
    assert!(changed.load(Ordering::Relaxed));
}
