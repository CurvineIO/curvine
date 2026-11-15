//! One case per metadata operation.
//!
//! Each case has three parts, and only the third runs inside the timed region:
//!
//! 1. `prepare` builds the objects the timed phase consumes.
//! 2. `args` builds every per-call argument, including path strings. Path
//!    formatting is a heap allocation, and leaving it inside the timed region
//!    would add allocator noise to every sample and compress the difference
//!    between the two builds.
//! 3. `body` calls exactly one `MasterFilesystem` method.
//!
//! `CreateFileOpts::with_create` and `SetAttrOptsBuilder::new` are built inside
//! `body` on purpose: both use zero-capacity strings, `HashMap::new` and
//! `vec![]`, so they allocate nothing. Only path strings and `ClientAddress`
//! (four non-empty strings) need pre-building.
//!
//! Only `MasterFilesystem` public API present in both `bff4e7dc5` and the
//! candidate branch is used, so this file compiles unchanged on both.

use crate::fixtures::*;
use crate::harness::Env;
use curvine_error::FsResult;
use curvine_master::master::fs::MasterFilesystem;
use curvine_model::{
    ClientAddress, CreateFileOpts, FileAllocOpts, ListOptions, OpenFlags, RenameFlags,
    SetAttrOptsBuilder, WorkerAddress, WorkerInfo, WorkerStatus,
};

/// Children per listed directory. Fixed so both layouts list the same shape.
pub const LIST_SIZE: usize = 256;
/// Page size for `list_options`.
const LIST_PAGE: usize = 64;
/// Attribute written by the `set_attr` case; re-applying it is idempotent.
const SET_ATTR_MODE: u32 = 0o640;
/// Fixed mtime for the attribute-carrying cases: a constant keeps the measured
/// work identical across runs and machines.
const SET_ATTR_MTIME: i64 = 1_700_000_000_000;

/// Everything one measured call needs, built before the timed region.
pub struct Arg {
    /// Primary path: source, target or directory.
    pub a: String,
    /// Secondary path (rename/link destination), or the close client name.
    pub b: String,
    /// Owned address for `add_block`, which takes it by value.
    pub client: ClientAddress,
}

pub struct Case {
    pub name: &'static str,
    /// Builds the objects the timed phase consumes. Runs outside the timed
    /// region and fails loudly: a broken fixture must not pass silently.
    pub prepare: fn(&MasterFilesystem, &Env) -> FsResult<()>,
    /// Builds this call's arguments outside the timed region.
    pub args: fn(&Env, usize, u64) -> Arg,
    /// The single measured call.
    pub body: fn(&MasterFilesystem, &mut Arg) -> FsResult<()>,
}

/// Registers one live worker with ample capacity. Needed by cases that
/// allocate blocks (`add_block`, `get_block_locations`).
pub fn register_worker(fs: &MasterFilesystem) {
    let mut worker = WorkerInfo::new(
        WorkerAddress {
            worker_id: 1,
            ..Default::default()
        },
        1,
    );
    worker.status = WorkerStatus::Live;
    worker.capacity = i64::MAX / 4;
    worker.available = i64::MAX / 4;
    fs.add_test_worker(worker);
}

// ---------------------------------------------------------------------------
// Cases.
// ---------------------------------------------------------------------------

pub const CASES: &[Case] = &[
    Case {
        name: "create",
        prepare: make_dirs,
        args: path_arg,
        body: |fs, arg| {
            fs.create(&arg.a, false)?;
            Ok(())
        },
    },
    Case {
        name: "mkdir",
        prepare: make_dirs,
        args: mkdir_arg,
        body: |fs, arg| {
            fs.mkdir(&arg.a, false)?;
            Ok(())
        },
    },
    Case {
        name: "delete",
        prepare: make_files,
        args: path_arg,
        body: |fs, arg| {
            fs.delete(&arg.a, false)?;
            Ok(())
        },
    },
    Case {
        name: "rename_same",
        prepare: make_files,
        args: rename_same_arg,
        body: |fs, arg| {
            fs.rename(&arg.a, &arg.b, RenameFlags::empty())?;
            Ok(())
        },
    },
    Case {
        name: "rename_cross",
        prepare: make_rename_cross_dirs,
        args: rename_cross_arg,
        body: |fs, arg| {
            fs.rename(&arg.a, &arg.b, RenameFlags::empty())?;
            Ok(())
        },
    },
    Case {
        name: "set_attr",
        prepare: make_files,
        args: path_arg,
        body: |fs, arg| {
            // Builder allocates nothing: all fields are `None`, plus an empty
            // HashMap and an empty Vec.
            let opts = SetAttrOptsBuilder::new().mode(SET_ATTR_MODE).build();
            fs.set_attr(&arg.a, opts)?;
            Ok(())
        },
    },
    Case {
        // Same call path as `set_attr`, but carrying the mtime field. It stays
        // a separate case from the mode-only one because the write path may
        // treat the two payloads differently (`SetAttrOpts` is built field by
        // field, so a mode-only and an mtime-only option are distinct inputs).
        name: "set_attr_mtime",
        prepare: make_files,
        args: path_arg,
        body: |fs, arg| {
            let opts = SetAttrOptsBuilder::new().mtime(SET_ATTR_MTIME).build();
            fs.set_attr(&arg.a, opts)?;
            Ok(())
        },
    },
    Case {
        name: "link",
        prepare: make_files,
        args: link_arg,
        body: |fs, arg| {
            fs.link(&arg.a, &arg.b)?;
            Ok(())
        },
    },
    Case {
        name: "symlink",
        prepare: make_dirs,
        args: symlink_arg,
        body: |fs, arg| {
            fs.symlink(arg.a.as_str(), arg.b.as_str(), false, 0o777)?;
            Ok(())
        },
    },
    Case {
        name: "overwrite",
        prepare: make_files,
        args: path_arg,
        body: |fs, arg| {
            // `create` sets O_TRUNC, so an existing path takes the overwrite
            // branch (`FsDir::overwrite_file`) rather than the create branch.
            fs.create(&arg.a, false)?;
            Ok(())
        },
    },
    Case {
        name: "open",
        prepare: make_files,
        args: path_arg,
        body: |fs, arg| {
            // Write-only without O_TRUNC/O_CREAT takes `FsDir::reopen_file`,
            // the real client open-for-write path. Read-only would just
            // delegate to `get_block_locations` and measure nothing new.
            // `with_create` uses zero-capacity strings, so it allocates nothing.
            fs.open_file(
                &arg.a,
                CreateFileOpts::with_create(false),
                OpenFlags::new_write_only(),
            )?;
            Ok(())
        },
    },
    Case {
        name: "add_block",
        prepare: make_files,
        args: path_arg,
        body: |fs, arg| {
            // `add_block` takes the address by value; it was built untimed.
            fs.add_block(
                &arg.a,
                None,
                std::mem::take(&mut arg.client),
                vec![],
                vec![],
                0,
                None,
            )?;
            Ok(())
        },
    },
    Case {
        name: "complete_file",
        prepare: make_files,
        args: close_arg,
        body: |fs, arg| {
            // Closing a freshly created empty file. Empty commit_blocks and
            // len 0 are consistent (complete_len == 0 == len), so this is the
            // real close path, not a retry shortcut.
            fs.complete_file(arg.a.as_str(), None, 0, vec![], arg.b.as_str(), false, None)?;
            Ok(())
        },
    },
    Case {
        // The `complete_file` case above passes `None`; this one carries a
        // `SetAttrOpts` (mtime). Both shapes are measured because `complete_file`
        // takes the options through the sharded commit path regardless.
        name: "complete_file_mtime",
        prepare: make_files,
        args: close_arg,
        body: |fs, arg| {
            let opts = SetAttrOptsBuilder::new().mtime(SET_ATTR_MTIME).build();
            fs.complete_file(
                arg.a.as_str(),
                None,
                0,
                vec![],
                arg.b.as_str(),
                false,
                Some(opts),
            )?;
            Ok(())
        },
    },
    Case {
        name: "free",
        prepare: make_files,
        args: path_arg,
        body: |fs, arg| {
            fs.free(&arg.a, false)?;
            Ok(())
        },
    },
    Case {
        name: "resize",
        prepare: make_files,
        args: path_arg,
        body: |fs, arg| {
            // `with_truncate` builds a plain struct, no allocation.
            fs.resize(&arg.a, FileAllocOpts::with_truncate(0))?;
            Ok(())
        },
    },
    Case {
        name: "file_status",
        prepare: make_read_targets,
        args: read_arg,
        body: |fs, arg| {
            fs.file_status(&arg.a)?;
            Ok(())
        },
    },
    Case {
        name: "exists",
        prepare: make_read_targets,
        args: read_arg,
        body: |fs, arg| {
            fs.exists(&arg.a)?;
            Ok(())
        },
    },
    Case {
        name: "list_status",
        prepare: make_list_dirs,
        args: list_arg,
        body: |fs, arg| {
            let entries = fs.list_status(&arg.a)?;
            assert_eq!(entries.len(), LIST_SIZE, "list_status returned wrong count");
            Ok(())
        },
    },
    Case {
        name: "list_page",
        prepare: make_list_dirs,
        args: list_arg,
        body: |fs, arg| {
            let entries = fs.list_options(
                &arg.a,
                ListOptions {
                    limit: Some(LIST_PAGE),
                    start_after: None,
                },
            )?;
            assert_eq!(entries.len(), LIST_PAGE, "list_page returned wrong count");
            Ok(())
        },
    },
    Case {
        name: "get_block_locations",
        prepare: make_block_targets,
        args: read_arg,
        body: |fs, arg| {
            fs.get_block_locations(&arg.a)?;
            Ok(())
        },
    },
];
