// Copyright 2026 OPPO
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

//! Deterministic interleavings for the `file_status` directory-version probe.
//!
//! Each case parks a reader at a `cfg(test)` gate, optionally mutates the
//! namespace on this thread, then releases the reader. Arrival is proved by a
//! channel and the gate is always released; no sleep is used as arrival proof.
//!
//! The probe reuses the first plan's identity only. A version change must fall
//! back or retry, and the returned status must still come from the live rows
//! read inside the locked window.

use super::*;
use crate::master::fs::master_filesystem::memory_read_gates;
use std::sync::mpsc;
use std::sync::Mutex;
use std::time::Duration;

/// Gate reached after the first plan releases its view and before the inode
/// locks are taken.
const GATE_PLAN: &str = "file-status-vp-plan-";
/// Gate only the reuse branch reaches.
const GATE_HIT: &str = "file-status-vp-hit-";
/// Bounded wait for the gate handshake; a stuck case fails, never hangs.
const WAIT: Duration = Duration::from_secs(30);
/// The mutation must finish far inside the hold the gate applies, so a parked
/// reader cannot have timed out and closed the window unnoticed. If the
/// production hold is reduced below this budget, reduce it here too.
const MUTATION_BUDGET: Duration = Duration::from_secs(5);

/// Branch counters are keyed by path but still shared in-process, so the cases
/// that read them run one at a time.
static COUNTERS: Mutex<()> = Mutex::new(());

fn counter_guard() -> std::sync::MutexGuard<'static, ()> {
    COUNTERS.lock().unwrap_or_else(|error| error.into_inner())
}

fn branches(path: &str) -> (u64, u64) {
    (
        memory_read_gates::vp_hits_for(path),
        memory_read_gates::vp_fallbacks_for(path),
    )
}

/// Runs `file_status(path)` on a worker, waits until it parks at `gate`, applies
/// `mutate` on this thread, releases the reader and returns its result.
fn status_parked<F>(
    fs: &MasterFilesystem,
    path: &str,
    gate: String,
    mutate: F,
) -> FsResult<FileStatus>
where
    F: FnOnce(&MasterFilesystem),
{
    let (release, entered) = memory_read_gates::install_with_entered(&gate);
    let timeouts_before = memory_read_gates::vp_gate_timeouts();
    let reader_fs = fs.clone();
    let reader_path = path.to_string();
    let reader = std::thread::spawn(move || reader_fs.file_status(&reader_path));

    entered
        .recv_timeout(WAIT)
        .expect("the reader never reached the file_status version gate");

    let started = std::time::Instant::now();
    mutate(fs);
    let mutated_within_budget = started.elapsed() < MUTATION_BUDGET;

    release.send(()).ok();
    let result = reader.join().expect("the reader panicked");

    assert_eq!(
        memory_read_gates::vp_gate_timeouts(),
        timeouts_before,
        "the gate timed out instead of being released, so the window was not controlled"
    );
    assert!(
        mutated_within_budget,
        "the mutation overran the gate budget, so the parked window cannot be proved"
    );
    result
}

#[test]
fn stable_file_target_reuses_the_first_plan() {
    let _guard = counter_guard();
    let fs = test_fs("fsvp-stable-file");
    fs.mkdir("/fsvp", true).unwrap();
    let file_id = fs.create("/fsvp/file", true).unwrap().id;

    let (hits, fallbacks) = branches("/fsvp/file");
    let status = status_parked(&fs, "/fsvp/file", format!("{GATE_HIT}/fsvp/file"), |_| {}).unwrap();

    assert_eq!(status.id, file_id);
    assert!(!status.is_dir);
    let (hits_after, fallbacks_after) = branches("/fsvp/file");
    assert_eq!(
        hits_after - hits,
        1,
        "a stable file read must reuse the plan"
    );
    assert_eq!(
        fallbacks_after, fallbacks,
        "a stable file read must not fall back"
    );
}

#[test]
fn stable_directory_target_reuses_the_first_plan() {
    let _guard = counter_guard();
    let fs = test_fs("fsvp-stable-dir");
    fs.mkdir("/fsvp", true).unwrap();
    let dir_id = fs.mkdir("/fsvp/dir", true).unwrap().id;

    let (hits, fallbacks) = branches("/fsvp/dir");
    let status = status_parked(&fs, "/fsvp/dir", format!("{GATE_HIT}/fsvp/dir"), |_| {}).unwrap();

    assert_eq!(status.id, dir_id);
    assert!(status.is_dir);
    let (hits_after, fallbacks_after) = branches("/fsvp/dir");
    assert_eq!(
        hits_after - hits,
        1,
        "a stable directory read must reuse the plan"
    );
    assert_eq!(
        fallbacks_after, fallbacks,
        "a stable directory read must not fall back"
    );
}

#[test]
fn rename_away_between_plan_and_check_does_not_reuse_the_plan() {
    let _guard = counter_guard();
    let fs = test_fs("fsvp-rename");
    fs.mkdir("/fsvp", true).unwrap();
    fs.create("/fsvp/moved", true).unwrap();

    let (_hits, fallbacks) = branches("/fsvp/moved");
    let result = status_parked(
        &fs,
        "/fsvp/moved",
        format!("{GATE_PLAN}/fsvp/moved"),
        |writer| {
            writer
                .mkdir("/fsvp/other", true)
                .expect("the fixture must create the destination parent");
            writer
                .rename("/fsvp/moved", "/fsvp/other/moved", RenameFlags::empty())
                .expect("the fixture rename must succeed");
        },
    );

    match result {
        Ok(status) => panic!("a renamed-away path must not report {status:?}"),
        Err(error) => assert!(
            error.to_string().to_lowercase().contains("not found"),
            "the documented file-not-found contract must survive the probe, got {error}"
        ),
    }
    let (_hits_after, fallbacks_after) = branches("/fsvp/moved");
    assert!(
        fallbacks_after > fallbacks,
        "a changed parent must take the full second walk"
    );
    // A retry after the mismatch may legitimately reuse a stable plan, so the
    // hit counter is not asserted flat here.
    let moved = fs
        .file_status("/fsvp/other/moved")
        .expect("the destination stays readable after the interleaving");
    assert!(!moved.is_dir);
}

#[test]
fn whole_tree_restore_in_the_window_retries_instead_of_reusing() {
    let _guard = counter_guard();
    let fs = test_fs("fsvp-restore");
    fs.mkdir("/fsvp", true).unwrap();
    fs.create("/fsvp/restored", true).unwrap();

    // The checkpoint keeps the file while the live tree drops it, so a plan
    // captured before the restore must not answer for the restored tree.
    let checkpoint = fs.fs_dir.view().unwrap().create_checkpoint(1).unwrap();
    fs.delete("/fsvp/restored", false).unwrap();
    assert!(fs.file_status("/fsvp/restored").is_err());

    let (hits, fallbacks) = branches("/fsvp/restored");
    let status = status_parked(
        &fs,
        "/fsvp/restored",
        format!("{GATE_PLAN}/fsvp/restored"),
        |writer| {
            // A whole-tree restore: RocksDB and the in-memory namespace are
            // replaced from the checkpoint. Reusing the pre-restore plan would
            // answer absent for a file the restored tree holds again.
            writer
                .fs_dir
                .begin_cold(DEFAULT_COLD_TIMEOUT)
                .unwrap()
                .restore(&checkpoint, 0)
                .expect("the fixture restore must succeed");
        },
    )
    .unwrap();

    assert_eq!(
        status.id,
        fs.file_status("/fsvp/restored").unwrap().id,
        "the restored tree decides the answer"
    );
    let (hits_after, fallbacks_after) = branches("/fsvp/restored");
    assert_eq!(
        fallbacks_after, fallbacks,
        "a restore moves the epoch, so the retry runs before the version check"
    );
    assert!(
        hits_after > hits,
        "the retry reads the restored plan and reuses it"
    );
}

#[test]
fn failed_holder_in_the_window_keeps_the_terminal_contract() {
    let _guard = counter_guard();
    let fs = test_fs("fsvp-failed");
    fs.mkdir("/fsvp", true).unwrap();
    fs.create("/fsvp/file", true).unwrap();

    let path = "/fsvp/file";
    let (release, entered) = memory_read_gates::install_with_entered(&format!("{GATE_PLAN}{path}"));
    let timeouts_before = memory_read_gates::vp_gate_timeouts();
    let reader_fs = fs.clone();
    let reader = std::thread::spawn(move || reader_fs.file_status(path));
    entered
        .recv_timeout(WAIT)
        .expect("the reader never reached the file_status version gate");

    let cold = fs.fs_dir.begin_cold(DEFAULT_COLD_TIMEOUT).unwrap();
    cold.fail_closed();
    release.send(()).ok();
    let result = reader.join().expect("the reader panicked");
    assert_eq!(
        memory_read_gates::vp_gate_timeouts(),
        timeouts_before,
        "the gate timed out instead of being released, so the window was not controlled"
    );

    assert!(
        result.is_err(),
        "a terminal store must never answer a stable reuse: {result:?}"
    );
}

#[test]
fn missing_inode_row_behind_a_live_edge_keeps_the_file_not_found_contract() {
    let _guard = counter_guard();
    let fs = test_fs("fsvp-dangling");
    fs.mkdir("/fsvp", true).unwrap();
    let id = fs.create("/fsvp/file", true).unwrap().id;

    // Drop only the inode row; the directory edge stays live. The probe must
    // keep file_status's own not-found contract instead of inheriting the
    // exists path's store-load wording.
    {
        let view = fs.fs_dir.view().unwrap();
        let mut batch = view.store.new_batch();
        batch.delete_inode(id).unwrap();
        batch.commit().unwrap();
    }

    match fs.file_status("/fsvp/file") {
        Ok(status) => panic!("a dangling edge must not report {status:?}"),
        Err(error) => {
            let message = error.to_string();
            assert!(
                message.to_lowercase().contains("not found"),
                "the file-not-found contract must survive the probe, got {message}"
            );
            assert!(
                !message.contains("Failed to load inode"),
                "file_status must not inherit the exists store-load wording, got {message}"
            );
        }
    }
}

/// A directory's attributes are not part of its child map, so changing only the
/// mtime keeps every captured directory version current and the fast path is
/// taken. The status must still show the value read inside the locked window,
/// not the one sampled when the first plan was built. Without this case the
/// "identity is reused, attributes are read live" split is untested.
#[test]
fn directory_attributes_changed_in_the_window_are_read_live() {
    let _guard = counter_guard();
    let fs = test_fs("fsvp-dir-live-attrs");
    fs.mkdir("/fsvp", true).unwrap();
    let dir_id = fs.mkdir("/fsvp/dir", true).unwrap().id;

    let before = fs.file_status("/fsvp/dir").unwrap().mtime;
    let new_mtime = before + 4242;
    let (hits, fallbacks) = branches("/fsvp/dir");

    let status = status_parked(&fs, "/fsvp/dir", format!("{GATE_PLAN}/fsvp/dir"), |fs| {
        fs.set_attr(
            "/fsvp/dir",
            SetAttrOptsBuilder::new().mtime(new_mtime).build(),
        )
        .unwrap();
    })
    .unwrap();

    assert_eq!(status.id, dir_id);
    assert_eq!(
        status.mtime, new_mtime,
        "the status must be built from live attributes inside the locked window, \
         not from the attribute value sampled by the first plan"
    );

    let (hits_after, fallbacks_after) = branches("/fsvp/dir");
    assert_eq!(
        hits_after - hits,
        1,
        "an attribute change leaves the child map untouched, so the plan must be reused"
    );
    assert_eq!(
        fallbacks_after, fallbacks,
        "an attribute change must not force the second walk"
    );
}

/// The instance-bound watch fires after the reader took its inode locks and
/// before the second view, so it proves "this FS instance's reader is inside
/// the locked window". Unlike the process-global gates it cannot be consumed by
/// another filesystem instance, and its timeout is the test's own failure.
/// The post-merge `locked_build` point is inside the inode-lock window on both
/// the reuse and the fallback branch. The instance watch proves that THIS
/// filesystem's reader reached it, so another instance can neither consume the
/// signal nor time it out silently.
/// The post-merge `locked_build` point is inside the inode-lock window. The
/// instance watch proves THIS filesystem's reader reached it, so no other
/// instance can consume the signal or time it out silently.
///
/// The reader reports through its own channel: a normal release must let it
/// return well before the watch's own 30s bound, so a lost release cannot look
/// like success.
#[test]
fn locked_build_window_blocks_a_rename_on_the_reuse_branch() {
    let _guard = counter_guard();
    let fs = test_fs("fsvp-locked-build");
    let tag = Utils::rand_str(8);
    let src_dir = format!("/u{tag}");
    let dst_dir = format!("/v{tag}");
    fs.mkdir(&src_dir, true).unwrap();
    fs.mkdir(&dst_dir, true).unwrap();
    let src = format!("{src_dir}/a");
    let dst = format!("{dst_dir}/a");
    fs.create(&src, true).unwrap();

    let (reached_tx, reached_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    *fs.fs_status_locked_build_watch.lock().unwrap() = Some((reached_tx, release_rx));

    let (reader_done_tx, reader_done_rx) = mpsc::channel::<()>();
    let reader_fs = fs.clone();
    let reader_path = src.clone();
    let reader = std::thread::spawn(move || {
        let status = reader_fs.file_status(&reader_path);
        reader_done_tx.send(()).ok();
        status
    });

    reached_rx
        .recv_timeout(WAIT)
        .expect("the reader never reached this instance's locked-build window");

    let writer_fs = fs.clone();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let writer_src = src.clone();
    let writer = std::thread::spawn(move || {
        writer_fs
            .rename(&writer_src, &dst, RenameFlags::empty())
            .expect("the rename must succeed once the read window closes");
        done_tx.send(()).ok();
    });

    let blocked = done_rx.recv_timeout(Duration::from_millis(700));
    release_tx.send(()).ok();
    // A normal release resumes the reader far below the watch's 30s bound; if
    // the release were lost, this would fail instead of passing by timeout.
    let released = reader_done_rx.recv_timeout(Duration::from_secs(5));
    let status = reader.join().expect("the reader panicked");
    let finished = done_rx.recv_timeout(WAIT);
    writer.join().expect("the writer panicked");

    assert!(
        blocked.is_err(),
        "the rename completed while this instance held its inode locks"
    );
    assert!(
        released.is_ok(),
        "the parked reader did not resume on the explicit release"
    );
    status.expect("the parked reader must still report the source");
    assert!(
        finished.is_ok(),
        "the rename must complete after the read window closes"
    );
}

/// The same window must hold when the first plan goes stale and the reader
/// takes the full second walk: the fallback branch also passes the post-merge
/// `locked_build` point, so a writer must stay blocked there too.
#[test]
fn locked_build_window_blocks_a_rename_on_the_fallback_branch() {
    let _guard = counter_guard();
    let fs = test_fs("fsvp-locked-build-fallback");
    let tag = Utils::rand_str(8);
    let src_dir = format!("/u{tag}");
    let dst_dir = format!("/v{tag}");
    fs.mkdir(&src_dir, true).unwrap();
    fs.mkdir(&dst_dir, true).unwrap();
    let src = format!("{src_dir}/a");
    let dst = format!("{dst_dir}/a");
    fs.create(&src, true).unwrap();

    let (reached_tx, reached_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    *fs.fs_status_locked_build_watch.lock().unwrap() = Some((reached_tx, release_rx));
    let (plan_release, plan_entered) =
        memory_read_gates::install_with_entered(&format!("{GATE_PLAN}{src}"));

    let (reader_done_tx, reader_done_rx) = mpsc::channel::<()>();
    let reader_fs = fs.clone();
    let reader_path = src.clone();
    let reader = std::thread::spawn(move || {
        let status = reader_fs.file_status(&reader_path);
        reader_done_tx.send(()).ok();
        status
    });

    plan_entered
        .recv_timeout(WAIT)
        .expect("the reader never reached the plan gate");
    // Bump the directory version so the captured tokens go stale and the reader
    // takes the full second walk instead of reusing the first plan.
    let (hits, fallbacks) = branches(&src);
    fs.create(&format!("{src_dir}/b"), true).unwrap();
    plan_release.send(()).ok();

    reached_rx
        .recv_timeout(WAIT)
        .expect("the fallback reader never reached the locked-build window");

    let writer_fs = fs.clone();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let writer_src = src.clone();
    let writer = std::thread::spawn(move || {
        writer_fs
            .rename(&writer_src, &dst, RenameFlags::empty())
            .expect("the rename must succeed once the read window closes");
        done_tx.send(()).ok();
    });

    let blocked = done_rx.recv_timeout(Duration::from_millis(700));
    release_tx.send(()).ok();
    let released = reader_done_rx.recv_timeout(Duration::from_secs(5));
    let status = reader.join().expect("the reader panicked");
    let finished = done_rx.recv_timeout(WAIT);
    writer.join().expect("the writer panicked");

    let (_hits_after, fallbacks_after) = branches(&src);
    assert!(
        fallbacks_after > fallbacks,
        "the case must actually take the fallback branch"
    );
    assert!(
        blocked.is_err(),
        "the rename completed while the fallback reader held its inode locks"
    );
    assert!(
        released.is_ok(),
        "the parked reader did not resume on the explicit release"
    );
    status.expect("the parked reader must still report the source");
    assert!(
        finished.is_ok(),
        "the rename must complete after the read window closes"
    );
}

/// Four conditions together, none of them sufficient alone:
/// 1. this instance's reader reached the post-merge `locked_build` point;
/// 2. this instance's writer reached the rename lock entry;
/// 3. the inode's lock stripe is not writable while the reader is parked;
/// 4. after the explicit release the reader finishes and the same stripe is
///    writable again.
///
/// The stripe probe only reports "this stripe is not writable right now": the
/// pool is striped, so it cannot say which holder owns it.
#[test]
fn locked_build_holds_the_inode_while_a_writer_waits_at_the_lock_entry() {
    let _guard = counter_guard();
    let fs = test_fs("fsvp-stripe");
    let tag = Utils::rand_str(8);
    let src_dir = format!("/u{tag}");
    let dst_dir = format!("/v{tag}");
    fs.mkdir(&src_dir, true).unwrap();
    fs.mkdir(&dst_dir, true).unwrap();
    let src = format!("{src_dir}/a");
    let dst = format!("{dst_dir}/a");
    let inode = fs.create(&src, true).unwrap().id;

    let (reached_tx, reached_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    *fs.fs_status_locked_build_watch.lock().unwrap() = Some((reached_tx, release_rx));
    let (writer_tx, writer_rx) = mpsc::channel::<()>();
    let (writer_release_tx, writer_release_rx) = mpsc::channel::<()>();
    *fs.rename_planned_watch.lock().unwrap() = Some((writer_tx, writer_release_rx));

    let writer_fs = fs.clone();
    let writer_src = src.clone();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let writer = std::thread::spawn(move || {
        writer_fs
            .rename(&writer_src, &dst, RenameFlags::empty())
            .expect("the rename must succeed once the read window closes");
        done_tx.send(()).ok();
    });

    writer_rx
        .recv_timeout(WAIT)
        .expect("the writer never reached the rename lock entry");
    let (reader_done_tx, reader_done_rx) = mpsc::channel::<()>();
    let reader_fs = fs.clone();
    let reader_path = src.clone();
    let reader = std::thread::spawn(move || {
        let status = reader_fs.file_status(&reader_path);
        reader_done_tx.send(()).ok();
        status
    });
    reached_rx
        .recv_timeout(WAIT)
        .expect("the reader never reached this instance's locked-build window");

    // Condition 3: the stripe is not writable while the reader is parked.
    let blocked = fs.inode_locks.try_write_inode(inode).is_none();
    // Release the writer's entry gate first, then the reader's window.
    writer_release_tx.send(()).ok();
    let blocked_after_writer_gate = done_rx.recv_timeout(Duration::from_millis(500)).is_err();
    release_tx.send(()).ok();
    let released = reader_done_rx.recv_timeout(Duration::from_secs(5));
    let status = reader.join().expect("the reader panicked");
    let finished = done_rx.recv_timeout(WAIT);
    writer.join().expect("the writer panicked");

    assert!(
        blocked,
        "the inode stripe must not be writable while the reader holds it"
    );
    assert!(
        blocked_after_writer_gate,
        "the rename completed while the reader still held the inode"
    );
    assert!(
        released.is_ok(),
        "the parked reader did not resume on the explicit release"
    );
    status.expect("the parked reader must still report the source");
    assert!(
        finished.is_ok(),
        "the rename must complete after the read window closes"
    );
    // Condition 4: the same stripe is writable again once the window closed.
    assert!(
        fs.inode_locks.try_write_inode(inode).is_some(),
        "the inode stripe must be writable again after the window closes"
    );
}
