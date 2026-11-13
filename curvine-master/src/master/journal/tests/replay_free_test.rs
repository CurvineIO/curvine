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

//! G1: replay of a NON-recursive Free. Split into its own file because the
//! non-recursive branch is the one the de-dup guard used to skip entirely;
//! reuses the parent fixture through `use super::*`.

use super::*;
use curvine_model::{
    BlockLocation, ClientAddress, CreateFileOptsBuilder, OpenFlags, TtlAction, WorkerAddress,
    WorkerInfo, WorkerStatus,
};

/// G1: a NON-recursive Free that really completes.
///
/// The recursive case above only asserts that metadata survives after replay -
/// which is exactly what a replay that silently skipped every free would also
/// satisfy. The de-dup guard used to read "no dedup set allocated" as "already
/// seen" and skip every non-recursive file, so this case compares the target's
/// real block set against the source's and pins the 4096 x 2 = 8192
/// physical-byte oracle.
#[test]
fn non_recursive_free_replay_clears_the_same_blocks_as_the_source() -> FsResult<()> {
    fn install_workers(fs: &MasterFilesystem) {
        for (worker_id, session) in [(1u32, "session-1"), (2u32, "session-2")] {
            let mut worker = WorkerInfo::new(
                WorkerAddress {
                    worker_id,
                    ..Default::default()
                },
                worker_id,
            );
            worker.status = WorkerStatus::Live;
            worker.capacity = i64::MAX / 4;
            worker.available = i64::MAX / 4;
            worker.worker_session_id = session.to_string();
            fs.add_test_worker(worker);
        }
    }

    /// One file, one completed 4096 block, two worker locations (8192 bytes).
    fn freeable_file(fs: &MasterFilesystem, path: &str) -> FsResult<i64> {
        let opts = CreateFileOptsBuilder::new()
            .create_parent(true)
            .ttl_action(TtlAction::Delete)
            .replicas(2)
            .build();
        fs.create_with_opts(path, opts, OpenFlags::new_create().set_overwrite(true))?;
        let block = fs.add_block(
            path,
            None,
            ClientAddress::default(),
            vec![],
            vec![],
            0,
            None,
        )?;
        {
            let handle = fs.fs_dir.clone();
            let view = handle.view()?;
            for worker in 1..=2 {
                view.store
                    .add_block_location(block.id, BlockLocation::with_id(worker))?;
            }
        }
        let commit = curvine_model::CommitBlock {
            block_id: block.id,
            block_len: 4096,
            locations: vec![BlockLocation::with_id(1), BlockLocation::with_id(2)],
        };
        fs.complete_file(
            path.to_string(),
            None,
            4096,
            vec![commit],
            "free-replay-fixture".to_string(),
            false,
            None,
        )?;
        Ok(block.id)
    }

    fn blocks_of(fs: &MasterFilesystem, path: &str) -> FsResult<Vec<i64>> {
        let id = fs.file_status(path)?.id;
        let handle = fs.fs_dir.clone();
        let view = handle.view()?;
        let inode = view.store.get_inode(id, None)?.expect("inode row");
        Ok(inode.as_file_ref()?.block_ids())
    }

    let source = JournalSystem::from_conf(&test_conf("free1-source"))?;
    let target = JournalSystem::from_conf(&test_conf("free1-target"))?;
    let source_fs = source.fs();
    let target_fs = target.fs();
    install_workers(&source_fs);
    install_workers(&target_fs);

    // Source performs the real non-recursive free; its end state is the oracle.
    let source_block = freeable_file(&source_fs, "/single")?;
    let source_bytes = {
        let handle = source_fs.fs_dir.clone();
        let view = handle.view()?;
        view.get_block_locations(source_block)?.len() * 4096
    };
    assert_eq!(
        source_bytes, 8192,
        "fixture oracle: one 4096 block with two replicas must charge 8192 physical bytes"
    );
    let entry = capture_one(&source_fs, || {
        source_fs.free("/single", false).expect("the real free");
    });
    assert!(
        matches!(&entry, JournalEntry::Free(free) if !free.recursive),
        "the captured entry must be the typed NON-recursive free, got {entry:?}"
    );
    let source_blocks_after = blocks_of(&source_fs, "/single")?;
    assert!(
        source_blocks_after.is_empty(),
        "fixture oracle: the source free must really clear the block, otherwise \
         this case cannot tell a working replay from a skipped one"
    );

    freeable_file(&target_fs, "/single")?;
    assert_eq!(
        blocks_of(&target_fs, "/single")?.len(),
        1,
        "fixture: the target starts with the block in place"
    );
    echo_entries(&target_fs);
    let loader = target.journal_loader();
    loader.apply_entry(entry)?;

    assert_eq!(
        blocks_of(&target_fs, "/single")?,
        source_blocks_after,
        "the replayed non-recursive free must clear exactly the blocks the \\
         source cleared; a skipped free leaves them behind"
    );
    assert!(
        target_fs.file_status("/single").is_ok(),
        "free keeps the inode metadata"
    );
    assert!(
        echo_entries(&target_fs).is_empty(),
        "replay must not echo journal entries"
    );
    Ok(())
}
