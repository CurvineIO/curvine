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

//! Raft log store conversion.
//!
//! The log entry envelope has always been a protobuf `Entry`; only its `data`
//! payload changed, from a `bincode` `JournalBatch` to a protobuf
//! `JournalBatchProto`. `index`, `term`, `entry_type` and most `meta` column
//! family records (HardState, ConfState, index range) are protobuf and are
//! copied verbatim.
//!
//! The snapshot record is the exception: `SnapshotData.files_data` lists the
//! checkpoint files the master links `meta/data` from on startup. Both the file
//! list and the directory are rewritten: the converted files carry other names
//! and lengths, and the recorded directory names the old deployment. It is
//! rebuilt under the directory the converted tree is installed at, so the master
//! recognises the checkpoint as its own instead of downloading it again. The
//! recorded member id is rewritten the same way, because a snapshot names the
//! member that created it.
//!
//! Rewriting `data` re-encodes the envelope, so this tool must be built from the
//! same source revision as the binaries that will read the result: protobuf
//! drops fields an older schema does not know about.

use super::{
    convert_batch, is_db_dir, Action, CfReport, DbReport, Progress, UpgradeOptions, BATCH_BYTES,
    BATCH_SIZE,
};
use curvine_core_error::{err_box, try_err, CommonResult};
use curvine_raft::proto::raft::{SnapshotData, SnapshotFileInfo, SnapshotFileList};
use curvine_rocksdb::{DBConf, DBEngine, WriteBatchWithTransaction};
use curvine_runtime::common::FileUtils;
use prost::Message;
use raft::eraftpb::{Entry, EntryType, Snapshot};
use std::path::{Path, PathBuf};

/// `RocksStorageCore::CF_ENTRIES`.
const CF_ENTRIES: &str = "default";
/// `RocksStorageCore::CF_META`.
const CF_META: &str = "meta";

/// `RocksStorageCore::SNAP_KEY`: single-byte marker holding the latest snapshot.
const SNAP_KEY: &[u8] = &[0x01u8];

/// `RocksStorageCore::RECOVERY_KEY`: raft id whose member recovery is unfinished.
const RECOVERY_KEY: &[u8] = &[0x05u8];

/// Durable marker of a member that has not rejoined the group yet
/// (`JournalConf::recovery_marker`). Its log is deliberately incomplete.
const RECOVERY_MARKER: &str = "member-recovery-in-progress";

/// Configuration the journal store opens with. `base_dir` holds `data/`.
///
/// Only the tests need this: the converter reads the family list off the disk.
#[cfg(test)]
pub(crate) fn journal_conf(base_dir: &str) -> DBConf {
    conf_with(base_dir, [CF_ENTRIES, CF_META], false)
}

fn conf_with<I, S>(base_dir: &str, cfs: I, disable_wal: bool) -> DBConf
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut conf = DBConf::new(base_dir);
    for cf in cfs {
        conf = conf.add_cf(cf);
    }
    conf.set_disable_wal(disable_wal)
}

/// Column families of a database, the known ones first for a stable report.
///
/// The list is read from the source rather than assumed: RocksDB refuses to
/// open a database whose families are not all listed, so a family a newer build
/// added has to be carried over rather than dropped.
fn cfs_of(data_dir: &str) -> CommonResult<Vec<String>> {
    let mut cfs = DBEngine::list_cf(data_dir)?;
    cfs.sort_by_key(|name| {
        let rank = match name.as_str() {
            CF_ENTRIES => 0,
            CF_META => 1,
            _ => 2,
        };
        (rank, name.clone())
    });
    Ok(cfs)
}

/// Convert `<journal_dir>/data` into a fresh database under `<out_dir>/journal`.
pub(crate) fn upgrade_journal_dir(base_dir: &str, opts: &UpgradeOptions) -> CommonResult<DbReport> {
    let out_base = format!("{}/journal", opts.out_dir);
    let mut report = DbReport::new("journal", base_dir);
    report.out = format!("{}/data", out_base);
    report.dry_run = opts.dry_run;

    // A member that is mid-recovery has a deliberately truncated log; converting
    // it would freeze a partial replica into the upgraded cluster.
    let marker = Path::new(base_dir).join(RECOVERY_MARKER);
    if marker.exists() {
        report.error = Some(format!(
            "{} exists: this member has not rejoined the group yet, finish or reset it first",
            marker.display()
        ));
        return Ok(report);
    }

    let data_dir = format!("{}/data", base_dir);
    if !Path::new(&data_dir).join("CURRENT").exists() {
        report.error = Some(format!("no journal store found at {}", data_dir));
        return Ok(report);
    }

    let cfs = cfs_of(&data_dir)?;
    // Read-only: the log store is the rollback copy and must not be touched.
    let src = DBEngine::open_read_only(conf_with(base_dir, &cfs, false))?;

    // The storage also records the recovery indicator, and clearing it happens
    // after the marker file is unlinked, so a crash in between leaves the
    // indicator without a marker. Startup rejects that state, and copying it
    // would make the upgraded member refuse to start.
    if let Some(value) = src.get_cf(CF_META, RECOVERY_KEY)? {
        report.error = Some(format!(
            "{} records an unfinished member recovery ({}), but {} is missing; finish or reset the member first",
            data_dir,
            recovery_target(&value),
            marker.display()
        ));
        return Ok(report);
    }

    let snapshot = read_snapshot(&src)?;
    report.snapshot_index = snapshot.as_ref().map(|x| x.get_metadata().index);
    let remapped = match &snapshot {
        Some(snapshot) => remap_snapshot(snapshot, opts, !opts.dry_run)?,
        None => None,
    };
    let snapshot_bytes = remapped.as_ref().map(|(bytes, _)| bytes.clone());
    report.snapshot_dir = remapped.map(|(_, dir)| dir);

    if opts.dry_run {
        eprintln!("scanning {}", data_dir);
        for cf in &cfs {
            report.merge_cf(scan_cf(&src, cf)?);
        }
        return Ok(report);
    }

    // The output lives in its own tree so the source stays read-only.
    std::fs::create_dir_all(&out_base)?;
    eprintln!("converting {} -> {}", data_dir, report.out);
    let dst = DBEngine::new(conf_with(&out_base, &cfs, true), true)?;

    for cf in &cfs {
        report.merge_cf(convert_cf(&src, &dst, cf, snapshot_bytes.as_deref())?);
    }

    // Journal writes are WAL-backed at runtime, but this database is written
    // once; the flush is what makes the output self-contained.
    dst.flush(true)?;
    drop(dst);
    drop(src);

    if !report.ok() {
        report.error = Some(format!("conversion failed, output kept at {}", out_base));
    }

    Ok(report)
}

fn convert_cf(
    src: &DBEngine,
    dst: &DBEngine,
    cf: &str,
    snapshot: Option<&[u8]>,
) -> CommonResult<CfReport> {
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

        let converted = if cf == CF_ENTRIES {
            match convert_entry(&value) {
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
                    continue;
                }
            }
        } else if cf == CF_META && key.as_ref() == SNAP_KEY {
            // Snapshot record, rebuilt against the converted checkpoint.
            match snapshot {
                Some(bytes) => {
                    report.converted += 1;
                    Some(bytes.to_vec())
                }
                None => {
                    report.copied += 1;
                    None
                }
            }
        } else {
            // Raft metadata (HardState, ConfState, index range) is protobuf and
            // unaffected by the payload change.
            report.copied += 1;
            None
        };

        // Records carried over untouched are written from the scanned slice.
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

/// Count what the real run would move, without decoding anything.
///
/// `--dry-run` reports volume only: every log entry is counted as converted
/// because that is what the run does with the column family, and the other
/// families are copied as they are.
fn scan_cf(db: &DBEngine, cf: &str) -> CommonResult<CfReport> {
    let mut report = CfReport::new(cf);
    let mut progress = Progress::new(db, cf);
    for item in db.bulk_scan(cf)? {
        let _ = item?;
        report.total += 1;
        progress.tick();
        if cf == CF_ENTRIES {
            report.converted += 1;
        } else {
            report.copied += 1;
        }
    }
    progress.finish();
    Ok(report)
}

/// Re-encode one raft log entry. Only the `data` payload changes.
fn convert_entry(bytes: &[u8]) -> CommonResult<Action> {
    let entry = try_err!(Entry::decode(bytes));

    // Only normal entries carry a journal batch. A membership change encodes a
    // `ConfChange` in the same field and a no-op carries nothing; the master's
    // replay path skips both, so they are copied verbatim.
    if entry.get_entry_type() != EntryType::EntryNormal || entry.data.is_empty() {
        return Ok(Action::Copy);
    }

    let data = convert_batch(&entry.data)?;
    let mut out = entry;
    out.data = data;
    Ok(Action::Converted(out.encode_to_vec()))
}

/// Basename of the checkpoint the persisted snapshot restores from, if this
/// member has one.
///
/// That snapshot is what makes the master install `meta/data` from a local
/// checkpoint, so the meta conversion has to convert exactly that directory:
/// newer directories on disk can be unregistered leftovers of a crash.
pub(crate) fn snapshot_checkpoint_name(base_dir: &str) -> CommonResult<Option<String>> {
    let data_dir = format!("{}/data", base_dir);
    if !Path::new(&data_dir).join("CURRENT").exists() {
        return Ok(None);
    }

    let db = DBEngine::open_read_only(conf_with(base_dir, cfs_of(&data_dir)?, false))?;
    let snapshot = read_snapshot(&db)?;
    drop(db);

    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    if snapshot.data.is_empty() {
        return Ok(None);
    }

    let data = try_err!(SnapshotData::decode(snapshot.data.as_slice()));
    Ok(data
        .files_data
        .as_ref()
        .and_then(|files| Path::new(&files.dir).file_name())
        .map(|name| name.to_string_lossy().to_string()))
}

/// What a stored recovery indicator points at, for the refusal message.
fn recovery_target(value: &[u8]) -> String {
    match <[u8; 8]>::try_from(value) {
        Ok(bytes) => format!("raft_id {}", u64::from_be_bytes(bytes)),
        Err(_) => format!("{} bytes", value.len()),
    }
}

/// Decode the snapshot record, if the store has one.
fn read_snapshot(db: &DBEngine) -> CommonResult<Option<Snapshot>> {
    let Some(bytes) = db.get_cf(CF_META, SNAP_KEY)? else {
        return Ok(None);
    };
    Ok(Some(try_err!(Snapshot::decode(bytes.as_slice()))))
}

/// Rebuild the snapshot manifest against the converted checkpoint, under the
/// directory it is installed at.
///
/// `dir` is the only persisted path the startup logic uses to decide whether a
/// checkpoint is already on this host: `SnapshotFetcher` reuses a local snapshot
/// only when every manifest file resolves under it with the recorded length, and
/// otherwise copies or downloads the whole checkpoint. Keeping the path the old
/// build recorded therefore breaks as soon as the data moves to another host or
/// another working directory, so the converted record names the installed path
/// from [`UpgradeOptions::installed_meta_dir`].
///
/// `SnapshotData.node_id` names the member that created the snapshot and is
/// rewritten to the local one for the same reason: a member that does not
/// recognise itself does not reuse the files in place.
///
/// Returns the encoded `Snapshot` to store plus the directory it now names. In a
/// real run that directory must be present in the output tree: a journal naming
/// a checkpoint that is not there cannot be restored.
fn remap_snapshot(
    snapshot: &Snapshot,
    opts: &UpgradeOptions,
    require_output: bool,
) -> CommonResult<Option<(Vec<u8>, String)>> {
    if snapshot.data.is_empty() {
        return Ok(None);
    }

    let mut data = try_err!(SnapshotData::decode(snapshot.data.as_slice()));
    let Some(files) = data.files_data.clone() else {
        // In-memory snapshot (`bytes_data`): no directory and no file list.
        return Ok(None);
    };

    let Some(name) = Path::new(&files.dir)
        .file_name()
        .map(|x| x.to_string_lossy().to_string())
    else {
        return err_box!("snapshot manifest has no directory: {:?}", files.dir);
    };

    // The converted copy of that checkpoint, under the output tree.
    let converted = Path::new(&opts.out_dir)
        .join("meta")
        .join("checkpoint")
        .join(&name);
    let converted = converted.to_string_lossy().to_string();

    // The same checkpoint once the tree is installed, which is what the master
    // resolves at startup.
    let installed = PathBuf::from(opts.installed_meta_dir())
        .join("checkpoint")
        .join(&name)
        .to_string_lossy()
        .to_string();

    let list = if is_db_dir(&converted) {
        snapshot_file_list(&converted, &installed)?
    } else if require_output {
        return err_box!(
            "the journal snapshot restores from {}, but the converted checkpoint {} is missing; \
             convert the meta directory with the same --out-dir first",
            files.dir,
            converted
        );
    } else {
        // Dry run: the checkpoint has not been written yet, so the recorded
        // file list stays; only the directory it will name is rebuilt.
        SnapshotFileList {
            dir: installed.clone(),
            files: files.files.clone(),
        }
    };

    data.files_data = Some(list);
    if let Some(node_id) = opts.node_id {
        data.node_id = node_id;
    }

    let mut out = snapshot.clone();
    out.set_data(data.encode_to_vec());
    Ok(Some((out.encode_to_vec(), installed)))
}

/// Manifest of one checkpoint directory, the way `RaftUtils::create_file_snapshot`
/// builds it for a locally created snapshot, recorded against `recorded` rather
/// than the directory that was read.
fn snapshot_file_list(dir: &str, recorded: &str) -> CommonResult<SnapshotFileList> {
    let mut files = Vec::new();
    for path in FileUtils::list_files(dir, false)? {
        let meta = FileUtils::metadata(PathBuf::from(dir).join(&path))?;
        files.push(SnapshotFileInfo {
            path,
            mtime: FileUtils::mtime(&meta).unwrap_or(0),
            ctime: FileUtils::ctime(&meta).unwrap_or(0),
            len: meta.len(),
        });
    }
    Ok(SnapshotFileList {
        dir: recorded.to_string(),
        files,
    })
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
