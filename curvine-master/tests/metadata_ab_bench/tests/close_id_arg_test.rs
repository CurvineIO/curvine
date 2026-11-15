//! The by-id close case must resolve its inode id **outside** the timed region.
//!
//! These checks pin the two properties the A/B relies on: `close_id_arg`
//! returns `Some(id)` for an existing fixture file, and that id is stable and
//! unique per (thread, seq) for every layout/thread combination the matrix
//! runs — so the measured body is one `complete_file(Some(id), ..)` call and
//! never a hidden `file_status` lookup.

use crate::fixtures::{arg, arg2, close_id_arg, make_files};
use crate::harness::{Env, Layout};

const LAYOUTS: [Layout; 2] = Layout::ALL;
const THREADS: [usize; 3] = [1, 8, 32];

#[test]
fn path_addressed_args_carry_no_id() {
    // Path-addressed cases must not accidentally carry an id: the whole point
    // of the separate `complete_file_by_id` case is that it does.
    assert!(arg("/bench/x".to_string()).id.is_none());
    assert!(arg2("/bench/x".to_string(), "client".to_string())
        .id
        .is_none());
}

#[test]
fn close_id_arg_ids_are_unique_and_stable_per_cell() {
    for layout in LAYOUTS {
        for threads in THREADS {
            // `isolated` builds a throwaway MasterFilesystem and removes its
            // directories afterwards, so every matrix cell is independent.
            crate::isolated(|fs| {
                let op = "close_id_probe";
                let env = Env::new(op, layout, threads, 4);
                make_files(fs, &env).expect("fixture files");

                let mut seen = std::collections::BTreeSet::new();
                for thread in 0..threads {
                    for seq in 0..4u64 {
                        let arg = close_id_arg(fs, &env, thread, seq);
                        let id = arg.id.expect("by-id args must carry the id");
                        assert!(id > 0, "inode ids are positive: {id}");
                        assert!(
                            seen.insert(id),
                            "duplicate id {id} for {}/{threads}T ({thread},{seq})",
                            layout.tag()
                        );
                        // Re-resolving the same path must yield the same id: a
                        // changed id after creation would make the case measure a
                        // different inode than the fixture set up.
                        let again = close_id_arg(fs, &env, thread, seq);
                        assert_eq!(again.id, Some(id), "id must be stable for a path");
                    }
                }
            });
        }
    }
}
