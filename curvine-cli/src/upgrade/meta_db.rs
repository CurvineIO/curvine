// Copyright 2025 OPPO.
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

//! Master metadata store conversion.
//!
//! Value layouts written by the pre-codec build:
//!
//! | CF      | key                   | value                                   |
//! |---------|-----------------------|-----------------------------------------|
//! | inodes  | BE i64                | `InodeView` (bincode)                   |
//! | edges   | BE i64 + name         | raw BE i64 (never encoded)              |
//! | block   | BE i64 + BE u32       | `BlockLocation` (bincode)               |
//! | location| BE u32 + BE i64       | `i64` block id (bincode)                |
//! | common  | `0x01` + BE u32       | `MountInfo` (bincode)                   |
//! | common  | `0x02` + BE i64       | `Vec<FileLock>` (bincode)               |
//!
//! The `common` family also holds client-owner markers (`0x03` + client id) and
//! other unprefixed keys; they carry no payload, so they are copied verbatim.
//!
//! The database is opened here through `DBEngine` rather than
//! `RocksInodeStore`: only the column family names are shared with the runtime
//! store, and going through `DBEngine` keeps this tool from needing any
//! `pub(crate)` access in `curvine-master`.

use super::{
    bincode_read, convert_i64, convert_value_struct, dir_conf, encode_meta, is_db_dir,
    reject_symlink, sub_dirs, Action, CfReport, DbReport, Progress, UpgradeOptions, BATCH_BYTES,
    BATCH_SIZE,
};
use curvine_core_error::{err_box, CommonResult};
use curvine_master::master::meta::inode::InodeView;
use curvine_master::master::meta::store::RocksInodeStore;
use curvine_model::{
    AccessMode, BlockLocation, FileLock, LockFlags, LockType, MountInfo, Provider, StorageType,
    TtlAction, WriteType,
};
use curvine_rocksdb::{DBConf, DBEngine, WriteBatchWithTransaction};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

/// Every column family the master store opens. `default` is created implicitly
/// by `DBConf` and is unused, but it must be carried over so the converted
/// database has the same shape.
const META_CFS: &[&str] = &[
    RocksInodeStore::CF_INODES,
    RocksInodeStore::CF_EDGES,
    RocksInodeStore::CF_BLOCK,
    RocksInodeStore::CF_LOCATION,
    RocksInodeStore::CF_COMMON,
    DBConf::DEFAULT_FAMILY,
];

/// Configuration for a directory that is itself the metadata database, matching
/// what `RocksInodeStore::new` adds on top of `<master.meta_dir>/data`.
pub(crate) fn db_conf(dir: &str) -> DBConf {
    let mut conf = dir_conf(dir);
    for cf in META_CFS {
        conf = conf.add_cf(cf);
    }
    conf
}

/// Same, opening exactly the families found in the source.
///
/// RocksDB refuses to open a database whose column families are not all listed,
/// so a family this build does not know about (written by a newer binary) has to
/// be carried over rather than dropped.
fn db_conf_with(dir: &str, cfs: &[String]) -> DBConf {
    let mut conf = dir_conf(dir);
    for cf in cfs {
        conf = conf.add_cf(cf);
    }
    conf
}

/// Column families of `dir`, the known ones first so the report order is stable.
fn source_cfs(dir: &str) -> CommonResult<Vec<String>> {
    let mut cfs = DBEngine::list_cf(dir)?;
    cfs.sort_by_key(|name| {
        (
            META_CFS
                .iter()
                .position(|known| known == name)
                .unwrap_or(META_CFS.len()),
            name.clone(),
        )
    });
    Ok(cfs)
}

/// Convert one database directory into a fresh one.
///
/// `src` is only ever read; `dst` is created from scratch, replacing anything
/// already there.
pub(crate) fn convert_db_dir(src: &str, dst: &str) -> CommonResult<Vec<CfReport>> {
    // Format mode deletes what is already at the destination; a link there
    // would redirect that delete outside the output tree.
    reject_symlink(Path::new(dst))?;

    // A checkpoint's parent directory (`<out>/meta/checkpoint`) may not exist yet.
    if let Some(parent) = Path::new(dst).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let cfs = source_cfs(src)?;
    eprintln!("converting {} -> {}", src, dst);
    // Read-only: a read-write open could replay a WAL or compact, which would
    // modify the copy the operator keeps for rollback.
    let src_db = DBEngine::open_read_only(db_conf_with(src, &cfs))?;
    // The WAL buys nothing for a database that is written once and closed; the
    // explicit flush below is what makes the output self-contained.
    let dst_db = DBEngine::new(db_conf_with(dst, &cfs).set_disable_wal(true), true)?;

    let mut reports = Vec::with_capacity(cfs.len());
    for cf in &cfs {
        reports.push(convert_cf(&src_db, &dst_db, cf)?);
    }

    // An explicit flush is what makes the converted directory self-contained
    // once the process exits, independent of the WAL setting it was opened with.
    dst_db.flush(true)?;
    drop(dst_db);
    drop(src_db);

    Ok(reports)
}

/// Walk a database directory without decoding anything, writing nothing.
///
/// `--dry-run` only reports the volume the real run would move, so the same
/// dispatch decides what counts as converted; the payload work is not done.
fn scan_db_dir(dir: &str) -> CommonResult<Vec<CfReport>> {
    let cfs = source_cfs(dir)?;
    eprintln!("scanning {}", dir);
    let db = DBEngine::open_read_only(db_conf_with(dir, &cfs))?;
    let mut reports = Vec::with_capacity(cfs.len());
    for cf in &cfs {
        let mut report = CfReport::new(cf);
        let mut progress = Progress::new(&db, cf);
        for item in db.bulk_scan(cf)? {
            let (key, _value) = item?;
            report.total += 1;
            progress.tick();
            if is_converted(cf, &key) {
                report.converted += 1;
            } else {
                report.copied += 1;
            }
        }
        progress.finish();
        reports.push(report);
    }
    Ok(reports)
}

/// Whether the real run would re-encode this record, decided by the same key
/// dispatch `convert_record` uses.
fn is_converted(cf: &str, key: &[u8]) -> bool {
    match cf {
        RocksInodeStore::CF_INODES | RocksInodeStore::CF_BLOCK | RocksInodeStore::CF_LOCATION => {
            true
        }
        RocksInodeStore::CF_COMMON => matches!(
            key.first(),
            Some(&RocksInodeStore::PREFIX_MOUNT) | Some(&RocksInodeStore::PREFIX_LOCK)
        ),
        _ => false,
    }
}

fn convert_cf(src: &DBEngine, dst: &DBEngine, cf: &str) -> CommonResult<CfReport> {
    let mut report = CfReport::new(cf);
    let mut progress = Progress::new(src, cf);
    let handle = dst.cf(cf)?;
    let mut batch = WriteBatchWithTransaction::<false>::default();
    let mut pending = 0usize;
    let mut pending_bytes = 0usize;

    for item in src.bulk_scan(cf)? {
        let (key, value) = item?;
        report.total += 1;
        progress.tick();

        let converted = match convert_record(cf, &key, &value) {
            Ok(Action::Converted(bytes)) => {
                report.converted += 1;
                Some(bytes)
            }
            Ok(Action::Copy) => {
                report.copied += 1;
                None
            }
            Err(e) => {
                report.failed += 1;
                if report.first_error.is_none() {
                    report.first_error = Some(format!("key {:?}: {}", hex(&key), e));
                }
                // Persist what we can read; a partial database is still useful
                // for inspection, and the run reports itself as failed.
                continue;
            }
        };

        // A record carried over untouched is written from the scanned slice, so
        // it does not cost a second copy of every edge and location record.
        let payload: &[u8] = converted.as_deref().unwrap_or(value.as_ref());
        pending_bytes += key.len() + payload.len();
        batch.put_cf(handle, &key, payload);
        pending += 1;
        if pending >= BATCH_SIZE || pending_bytes >= BATCH_BYTES {
            dst.write_batch(batch)?;
            batch = WriteBatchWithTransaction::<false>::default();
            pending = 0;
            pending_bytes = 0;
        }
    }

    if pending > 0 {
        dst.write_batch(batch)?;
    }
    progress.finish();
    Ok(report)
}

fn convert_record(cf: &str, key: &[u8], value: &[u8]) -> CommonResult<Action> {
    match cf {
        // Child edges hold a raw big-endian i64 child id.
        RocksInodeStore::CF_EDGES => Ok(Action::Copy),

        RocksInodeStore::CF_INODES => {
            convert_value_struct::<InodeView>(value).map(Action::Converted)
        }
        RocksInodeStore::CF_BLOCK => {
            convert_value_struct::<BlockLocation>(value).map(Action::Converted)
        }
        RocksInodeStore::CF_LOCATION => convert_i64(value).map(Action::Converted),

        RocksInodeStore::CF_COMMON => match key.first() {
            Some(&RocksInodeStore::PREFIX_MOUNT) => {
                convert_mount_info(value).map(Action::Converted)
            }
            Some(&RocksInodeStore::PREFIX_LOCK) => convert_file_locks(value).map(Action::Converted),
            // Unknown prefix entries are carried over untouched so a newer
            // writer's records are not silently dropped.
            _ => Ok(Action::Copy),
        },

        _ => Ok(Action::Copy),
    }
}

// ---------------------------------------------------------------------------
// Mount records
// ---------------------------------------------------------------------------

/// Convert one mount record, whichever layout the writing build used.
///
/// `bincode` is positional, so a build that appended a field wrote payloads the
/// current `MountInfo` reads past the end of. `MountInfo` gained
/// `auto_cache`/`access_mode`, then `write_cache`, and the live read path used
/// to walk those layouts in `MountInfo::decode_persisted`; the codec switch
/// removed it, and this tool is the only reader of the legacy records left. The
/// defaults are the ones that function filled in.
fn convert_mount_info(value: &[u8]) -> CommonResult<Vec<u8>> {
    match bincode_read::<MountInfo>(value) {
        Ok(info) => encode_meta(&info),
        Err(e) => match mount_info_from_legacy(value) {
            Some(info) => encode_meta(&info),
            None => err_box!("mount record matches no persisted MountInfo layout: {}", e),
        },
    }
}

/// The older layouts, newest first: each one ends at an earlier field, so at
/// most one of them can match a payload exactly.
fn mount_info_from_legacy(value: &[u8]) -> Option<MountInfo> {
    if let Ok(old) = bincode_read::<MountInfoBeforeWriteCache>(value) {
        return Some(old.into_current());
    }
    bincode_read::<MountInfoBeforeAutoCache>(value)
        .ok()
        .map(MountInfoBeforeAutoCache::into_current)
}

/// `MountInfo` as persisted before `write_cache` was appended.
#[derive(Deserialize)]
struct MountInfoBeforeWriteCache {
    cv_path: String,
    ufs_path: String,
    mount_id: u32,
    properties: HashMap<String, String>,
    ttl_ms: i64,
    ttl_action: TtlAction,
    read_verify_ufs: bool,
    storage_type: Option<StorageType>,
    block_size: Option<i64>,
    replicas: Option<i32>,
    write_type: WriteType,
    provider: Option<Provider>,
    auto_cache: bool,
    access_mode: AccessMode,
}

impl MountInfoBeforeWriteCache {
    fn into_current(self) -> MountInfo {
        MountInfo {
            cv_path: self.cv_path,
            ufs_path: self.ufs_path,
            mount_id: self.mount_id,
            properties: self.properties,
            ttl_ms: self.ttl_ms,
            ttl_action: self.ttl_action,
            read_verify_ufs: self.read_verify_ufs,
            storage_type: self.storage_type,
            block_size: self.block_size,
            replicas: self.replicas,
            write_type: self.write_type,
            provider: self.provider,
            auto_cache: self.auto_cache,
            access_mode: self.access_mode,
            write_cache: false,
        }
    }
}

/// The oldest layout: it ends at `provider`, before caching was configurable.
#[derive(Deserialize)]
struct MountInfoBeforeAutoCache {
    cv_path: String,
    ufs_path: String,
    mount_id: u32,
    properties: HashMap<String, String>,
    ttl_ms: i64,
    ttl_action: TtlAction,
    read_verify_ufs: bool,
    storage_type: Option<StorageType>,
    block_size: Option<i64>,
    replicas: Option<i32>,
    write_type: WriteType,
    provider: Option<Provider>,
}

impl MountInfoBeforeAutoCache {
    fn into_current(self) -> MountInfo {
        MountInfo {
            cv_path: self.cv_path,
            ufs_path: self.ufs_path,
            mount_id: self.mount_id,
            properties: self.properties,
            ttl_ms: self.ttl_ms,
            ttl_action: self.ttl_action,
            read_verify_ufs: self.read_verify_ufs,
            storage_type: self.storage_type,
            block_size: self.block_size,
            replicas: self.replicas,
            write_type: self.write_type,
            provider: self.provider,
            auto_cache: true,
            access_mode: AccessMode::ReadOnly,
            write_cache: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Lock records
// ---------------------------------------------------------------------------

/// Convert one lock-set record, whichever layout the writing build used.
///
/// `FileLock` gained a trailing `session_owned` flag (#1878). Lock sets written
/// before that have no such byte and `bincode` reads past their end, which is
/// why the master keeps a `LegacyFileLock` mirror for its own reader; this tool
/// is the other reader of the old records. A lock acquired before #1878 was
/// taken by a client that never sent a heartbeat, so it belongs to the legacy
/// TTL path: `session_owned = false`.
fn convert_file_locks(value: &[u8]) -> CommonResult<Vec<u8>> {
    match bincode_read::<Vec<FileLock>>(value) {
        Ok(locks) => encode_meta(&locks),
        Err(e) => match bincode_read::<Vec<FileLockBeforeSessionOwned>>(value) {
            Ok(old) => {
                let locks: Vec<FileLock> = old
                    .into_iter()
                    .map(FileLockBeforeSessionOwned::into_current)
                    .collect();
                encode_meta(&locks)
            }
            Err(_) => err_box!("lock record matches no persisted FileLock layout: {}", e),
        },
    }
}

/// `FileLock` as persisted before `session_owned` was appended (#1878).
#[derive(Deserialize)]
struct FileLockBeforeSessionOwned {
    client_id: String,
    owner_id: u64,
    pid: u32,
    acquire_time: u64,
    lock_type: LockType,
    lock_flags: LockFlags,
    start: u64,
    end: u64,
}

impl FileLockBeforeSessionOwned {
    fn into_current(self) -> FileLock {
        FileLock {
            client_id: self.client_id,
            owner_id: self.owner_id,
            pid: self.pid,
            acquire_time: self.acquire_time,
            lock_type: self.lock_type,
            lock_flags: self.lock_flags,
            start: self.start,
            end: self.end,
            session_owned: false,
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes.iter().take(16) {
        out.push_str(&format!("{:02x}", byte));
    }
    if bytes.len() > 16 {
        out.push_str("..");
    }
    out
}

// ---------------------------------------------------------------------------
// Base-directory driver
// ---------------------------------------------------------------------------

/// Convert the checkpoint the journal snapshot restores from and, optionally,
/// `<meta_dir>/data` as well, into `<out_dir>/meta`.
///
/// Only one checkpoint is converted. A meta directory can hold several, and on
/// startup the master replaces `meta/data` with the one its journal names, so
/// the others are stale copies that would only cost time and disk. Without a
/// journal snapshot the newest directory is the only available clue.
///
/// `data/` is a discardable working copy: on startup `install_snapshot` replaces
/// it with the checkpoint the journal names, so leaving it alone (the default)
/// saves one full copy and keeps the converted checkpoint as the only
/// authority. An empty store is written in its place either way, because a
/// master started with `format_master = false` rejects a meta directory that has
/// a `checkpoint/` child and no RocksDB store in `data/`.
pub(crate) fn upgrade_meta_dir(
    opts: &UpgradeOptions,
    snapshot_checkpoint: Option<&str>,
) -> CommonResult<DbReport> {
    let out_root = format!("{}/meta", opts.out_dir);
    let mut report = DbReport::new("meta", &opts.meta_dir);
    report.out = out_root.clone();
    report.dry_run = opts.dry_run;
    let checkpoint_root = format!("{}/checkpoint", opts.meta_dir);
    let data_dir = format!("{}/data", opts.meta_dir);
    let checkpoints = sub_dirs(&checkpoint_root, "ck-")?;

    if !is_db_dir(&data_dir) && checkpoints.is_empty() {
        report.error = Some(format!(
            "no metadata store found under {} (expected data/ or checkpoint/ck-*)",
            opts.meta_dir
        ));
        return Ok(report);
    }

    let chosen = match pick_checkpoint(&checkpoints, snapshot_checkpoint) {
        Ok(chosen) => chosen,
        Err(e) => {
            report.error = Some(e.to_string());
            return Ok(report);
        }
    };
    if let Some(ck) = &chosen {
        if !is_db_dir(ck) {
            report.error = Some(format!(
                "{} is not a rocksdb directory; the source looks like a partial copy",
                ck
            ));
            return Ok(report);
        }
        if checkpoints.len() > 1 {
            eprintln!(
                "note: {} checkpoints present, converting only {}",
                checkpoints.len(),
                ck
            );
        }
    }

    let mut converted = 0usize;
    for ck in chosen.iter() {
        let name = base_name(ck);
        report.checkpoints.push(name.clone());

        if opts.dry_run {
            match scan_db_dir(ck) {
                Ok(cfs) => cfs.into_iter().for_each(|cf| report.merge_cf(cf)),
                Err(e) => {
                    report.error = Some(format!("{}: {}", ck, e));
                    return Ok(report);
                }
            }
            converted += 1;
            continue;
        }

        let dst = format!("{}/checkpoint/{}", out_root, name);
        match convert_db_dir(ck, &dst) {
            Ok(cfs) => {
                cfs.into_iter().for_each(|cf| report.merge_cf(cf));
                if !report.ok() {
                    report.error =
                        Some(format!("{}: conversion failed, output kept at {}", ck, dst));
                    return Ok(report);
                }
                converted += 1;
            }
            Err(e) => {
                report.error = Some(format!("{}: {}", ck, e));
                return Ok(report);
            }
        }
    }

    let mut data_written = false;
    if opts.with_meta_data && is_db_dir(&data_dir) {
        if opts.dry_run {
            match scan_db_dir(&data_dir) {
                Ok(cfs) => {
                    cfs.into_iter().for_each(|cf| report.merge_cf(cf));
                    data_written = true;
                }
                Err(e) => report.error = Some(format!("{}: {}", data_dir, e)),
            }
        } else {
            let dst = format!("{}/data", out_root);
            match convert_db_dir(&data_dir, &dst) {
                Ok(cfs) => {
                    cfs.into_iter().for_each(|cf| report.merge_cf(cf));
                    if !report.ok() {
                        report.error = Some(format!(
                            "{}: conversion failed, output kept at {}",
                            data_dir, dst
                        ));
                    }
                    data_written = true;
                }
                Err(e) => report.error = Some(format!("{}: {}", data_dir, e)),
            }
        }
    }

    if opts.dry_run || !report.ok() {
        return Ok(report);
    }

    if !data_written {
        // With no checkpoint there is nothing the master could rebuild data/
        // from, so this is the one combination that has no valid output.
        if converted == 0 {
            report.error = Some(format!(
                "{} holds only data/ and --with-meta-data was not given; pass it to convert data/",
                opts.meta_dir
            ));
            return Ok(report);
        }

        if let Err(e) = write_empty_data_dir(&out_root) {
            report.error = Some(format!("{}: {}", out_root, e));
        }
    }

    Ok(report)
}

/// The single checkpoint to convert: the one the journal snapshot names.
///
/// That is the checkpoint the master installs `meta/data` from on startup. The
/// newest directory is not a safe substitute: the leader creates a checkpoint
/// before proposing the snapshot entry that registers it, so a crash leaves a
/// newer unregistered directory behind, and copying a deployment can change
/// every directory time. Only a member with no journal snapshot falls back to
/// directory time.
fn pick_checkpoint(
    checkpoints: &[String],
    snapshot_checkpoint: Option<&str>,
) -> CommonResult<Option<String>> {
    let Some(name) = snapshot_checkpoint else {
        return Ok(checkpoints
            .iter()
            .max_by(|a, b| modified(a).cmp(&modified(b)).then_with(|| a.cmp(b)))
            .cloned());
    };

    match checkpoints.iter().find(|dir| base_name(dir) == name) {
        Some(dir) => Ok(Some(dir.clone())),
        None => err_box!(
            "the journal snapshot restores from checkpoint {}, which is missing under the source \
             checkpoint directory; the journal and meta directories do not belong together",
            name
        ),
    }
}

fn base_name(dir: &str) -> String {
    Path::new(dir)
        .file_name()
        .map(|x| x.to_string_lossy().to_string())
        .unwrap_or_else(|| dir.to_string())
}

/// Directory modification time; the epoch when it cannot be read, so an
/// unreadable entry loses to any readable one.
fn modified(dir: &str) -> std::time::SystemTime {
    std::fs::metadata(dir)
        .and_then(|meta| meta.modified())
        .unwrap_or(std::time::UNIX_EPOCH)
}

/// Write an empty metadata store at `<out>/meta/data`.
///
/// It is the stand-in the master replaces with the checkpoint the journal names
/// on its next start; without it the meta directory is rejected outright.
fn write_empty_data_dir(out_root: &str) -> CommonResult<()> {
    let conf = db_conf(&format!("{}/data", out_root)).set_disable_wal(true);
    let db = DBEngine::new(conf, true)?;
    db.flush(true)?;
    Ok(())
}
