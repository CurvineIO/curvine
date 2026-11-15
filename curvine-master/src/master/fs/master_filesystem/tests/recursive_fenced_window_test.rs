// Owned by deputy-2 / restored from C2-FU-01 & C2-FU-05 (G3 admission-window
// matrix on a real fixture). One shared driver, three real public recursive
// RPCs, no per-op copies.
//
// Registration anchor: master_filesystem.rs cfg(test) declaration block,
// `#[path = "tests/recursive_fenced_window_test.rs"]`.

use super::recursive_confirmation_support::{
    assert_common_invariants, await_worker, block_signature, completed_file_fixture,
    remove_case_dirs, shutdown_case, WORKER_TIMEOUT,
};
use crate::master::fs::{DeleteMode, MasterFilesystem};
use curvine_error::FsResult;
use curvine_model::SetAttrOpts;

#[test]
fn fenced_window_rejects_real_recursive_rpcs_by_original_epoch() -> FsResult<()> {
    let (conf, journal, fs, inode_id) = completed_file_fixture("recursive-fenced-window-matrix");
    let writer = fs.journal_writer.clone();
    let blocks_before = block_signature(&fs);

    // (name, real public op mapped to (), post-check) - three rows, one
    // shared driver; no per-op copies, no generic runner framework.
    type Op = Box<dyn FnOnce(&MasterFilesystem) -> FsResult<()> + Send>;
    let ops: Vec<(&'static str, Op)> = vec![
        (
            "recursive delete",
            Box::new(|fs: &MasterFilesystem| {
                fs.delete_with_mode("/dir", true, DeleteMode::SkipTrash)
                    .map(|_| ())
            }),
        ),
        (
            "recursive free",
            Box::new(|fs: &MasterFilesystem| fs.free_or_defer("/dir", true).map(|_| ())),
        ),
        (
            "recursive set_attr",
            Box::new(|fs: &MasterFilesystem| {
                fs.set_attr(
                    "/dir",
                    SetAttrOpts {
                        recursive: true,
                        mode: Some(0o600),
                        ..Default::default()
                    },
                )
                .map(|_| ())
            }),
        ),
    ];

    for (case, op) in ops {
        let seq_before = writer.failure_seq();
        let (pre_hot_tx, pre_hot_rx) = std::sync::mpsc::channel::<()>();
        let (pre_hot_release_tx, pre_hot_release_rx) = std::sync::mpsc::channel::<()>();
        writer.arm_pre_hot_hook(pre_hot_tx, pre_hot_release_rx);

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker_fs = fs.clone();
        let worker = std::thread::spawn(move || {
            let _ = done_tx.send(op(&worker_fs));
        });

        // At this point the outer epoch is captured and ensured; the
        // commit slot is held; hot has not run.
        pre_hot_rx
            .recv_timeout(WORKER_TIMEOUT)
            .expect("the real public admission must reach the pre-hot checkpoint");
        writer.fence_metadata();
        writer.unfence_metadata();
        pre_hot_release_tx
            .send(())
            .expect("the driver must release the pre-hot pause");

        let result = await_worker(&done_rx, case)
            .expect("the fenced-window admission must report its result");
        worker.join().expect("the worker must not panic");
        // The single accepted error: the original-epoch NotLeader from
        // the locked re-verify of the caller's own epoch.
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("role changed"),
            "{case}: the rejection must be the original-epoch error, got {err:?}"
        );
        assert_common_invariants(&fs, inode_id, &blocks_before);
        assert!(
            !fs.fs_dir.is_failed(),
            "{case}: the fence must not terminalize the holder"
        );
        assert_eq!(
            writer.failure_seq(),
            seq_before,
            "{case}: a role fence must not move the failure seq"
        );
        assert!(
            journal.master_monitor().recovery().fatal_source().is_none(),
            "{case}: a role fence must not be recorded as a fatal source"
        );
        // The take-once hook must not fire again for the next op.
        assert!(writer.consume_pre_hot_hook().is_none());
    }

    drop(writer);
    drop(fs);
    shutdown_case(journal);
    remove_case_dirs(&conf);
    Ok(())
}
