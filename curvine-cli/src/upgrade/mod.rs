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

//! Offline conversion of master metadata from `bincode` records to
//! `curvine-codec` records, exposed as `curvine-cli meta-upgrade`.
//!
//! This is a one-shot maintenance operation performed with the whole cluster
//! stopped. It is a temporary tool, so it is compiled into the CLI
//! unconditionally instead of behind a feature.
//!
//! Source directories are opened read-only and never modified. Every converted
//! database is built from scratch under `--out-dir`, mirroring the source
//! layout, so the source keeps working with the pre-upgrade binary and is itself
//! the rollback copy:
//!
//! ```text
//! <out-dir>/meta/checkpoint/<ck-name>/   converted checkpoint
//! <out-dir>/meta/data/                   converted meta working copy, or an
//!                                        empty stand-in when it is not converted
//! <out-dir>/journal/data/                converted raft log store
//! ```
//!
//! `meta/data` is always written, because a master started with
//! `format_master = false` refuses a meta directory that has a `checkpoint/`
//! child but no RocksDB store in `data/`. When `--with-meta-data` is not given
//! the directory only holds an empty store; the master replaces it with the
//! checkpoint recorded in the journal on startup.
//!
//! A failed conversion leaves the source untouched and only costs a retry.
//!
//! The output is meant to be installed by rename: back up the live `meta` and
//! `journal` directories, then move the converted ones into their place. The
//! journal snapshot is rewritten to restore from the checkpoint where the
//! converted tree will actually live (see [`UpgradeOptions::install_meta_dir`]),
//! so it is found on the installed path instead of being reported as a foreign
//! snapshot and downloaded again.

mod journal_db;
mod meta_db;

#[cfg(test)]
mod tests;

use curvine_codec::{Codec, Encode};
use curvine_core_error::{err_box, try_err, CommonResult};
use curvine_master::master::journal::JournalBatch;
use curvine_rocksdb::{DBConf, DBEngine};
use serde::de::DeserializeOwned;
use std::io::IsTerminal;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant};

pub(crate) const BATCH_SIZE: usize = 4096;

/// Byte ceiling for one write batch, in addition to `BATCH_SIZE`. Records are
/// arbitrarily large (an inode carries its xattrs), so a batch bounded by count
/// alone can pin hundreds of megabytes before it reaches RocksDB.
pub(crate) const BATCH_BYTES: usize = 8 * 1024 * 1024;

/// Suffix of directories left behind by the earlier in-place version of this
/// tool. They are never sources, or a rerun would rewrite its own output.
const TOOL_DIR_SUFFIXES: &[&str] = &[".new", ".legacy"];

#[derive(Debug, Clone)]
pub struct UpgradeOptions {
    /// `<master.meta_dir>`: contains `data/` and `checkpoint/`.
    pub meta_dir: String,
    /// `<journal.journal_dir>`: contains `data/` and `checkpoint/`.
    pub journal_dir: Option<String>,
    /// Root of the converted output. Must not overlap the sources; the sources
    /// are never written to.
    pub out_dir: String,
    /// Directory the converted tree is installed at, which is where the master
    /// reads the checkpoint after the upgrade. The journal snapshot names its
    /// checkpoint under this directory instead of the path the old build
    /// recorded, so a tree installed elsewhere (another host, another working
    /// directory) still reuses its local checkpoint. `None` means [`Self::meta_dir`],
    /// which is where a rename install puts it.
    pub install_meta_dir: Option<String>,
    /// Directory the converted journal store is installed at. Raft reads the
    /// log location from the config at startup, so a source override still has
    /// to end up at the configured path. `None` means [`Self::journal_dir`].
    pub install_journal_dir: Option<String>,
    /// Raft node id of the master that will run on the converted data. A
    /// snapshot records the member that created it, and a member that does not
    /// recognise itself copies or downloads the files again; naming the local
    /// member keeps the checkpoint reusable in place. `None` keeps it as is.
    pub node_id: Option<u64>,
    /// Also convert `meta/data`. It is a discardable working copy: on startup
    /// `install_snapshot` deletes it and hard-links the checkpoint directory in
    /// its place, so leaving it alone (the default) saves one full copy and
    /// keeps the checkpoint as the only authority. Without it an empty store is
    /// still written, because the master refuses a meta directory that has a
    /// `checkpoint/` child but no `data/` store.
    pub with_meta_data: bool,
    /// Report only; never writes anything.
    pub dry_run: bool,
}

impl UpgradeOptions {
    pub fn new(meta_dir: impl Into<String>, out_dir: impl Into<String>) -> Self {
        Self {
            meta_dir: meta_dir.into(),
            journal_dir: None,
            out_dir: out_dir.into(),
            install_meta_dir: None,
            install_journal_dir: None,
            node_id: None,
            with_meta_data: false,
            dry_run: false,
        }
    }

    pub fn with_journal_dir(mut self, dir: impl Into<String>) -> Self {
        self.journal_dir = Some(dir.into());
        self
    }

    pub fn with_install_meta_dir(mut self, dir: impl Into<String>) -> Self {
        self.install_meta_dir = Some(dir.into());
        self
    }

    pub fn with_install_journal_dir(mut self, dir: impl Into<String>) -> Self {
        self.install_journal_dir = Some(dir.into());
        self
    }

    pub fn with_node_id(mut self, node_id: u64) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// Meta directory the converted tree is installed at.
    pub fn installed_meta_dir(&self) -> &str {
        self.install_meta_dir.as_deref().unwrap_or(&self.meta_dir)
    }

    /// Journal directory the converted store is installed at, if a journal is
    /// being converted.
    pub fn installed_journal_dir(&self) -> Option<&str> {
        let source = self.journal_dir.as_deref()?;
        Some(self.install_journal_dir.as_deref().unwrap_or(source))
    }

    pub fn with_meta_data(mut self, enable: bool) -> Self {
        self.with_meta_data = enable;
        self
    }

    pub fn dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }
}

#[derive(Debug, Default, Clone)]
pub struct CfReport {
    pub cf: String,
    /// Records seen in the source column family.
    pub total: u64,
    /// Records rewritten with the new codec.
    pub converted: u64,
    /// Records copied verbatim (raw or non-record payloads).
    pub copied: u64,
    pub failed: u64,
    /// First failure detail, kept for the report.
    pub first_error: Option<String>,
}

impl CfReport {
    pub fn new(cf: &str) -> Self {
        Self {
            cf: cf.to_string(),
            ..Default::default()
        }
    }

    pub fn ok(&self) -> bool {
        self.failed == 0
    }

    fn merge(&mut self, other: CfReport) {
        self.total += other.total;
        self.converted += other.converted;
        self.copied += other.copied;
        self.failed += other.failed;
        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct DbReport {
    /// Source directory, as read from the configuration.
    pub dir: String,
    /// Directory the converted database is written to.
    pub out: String,
    /// `meta` (master metadata) or `journal` (raft log store).
    pub kind: String,
    /// Verification only; nothing was written.
    pub dry_run: bool,
    pub cfs: Vec<CfReport>,
    /// Checkpoint directory names handled under a meta base dir.
    pub checkpoints: Vec<String>,
    /// Snapshot index recorded in the journal `meta` CF, when present.
    pub snapshot_index: Option<u64>,
    /// Checkpoint directory the converted snapshot restores from, when present.
    /// It is the path the install produces, not the one the old build recorded:
    /// the master restores `meta/data` from it on startup, and a path that does
    /// not resolve locally makes it treat its own checkpoint as foreign.
    pub snapshot_dir: Option<String>,
    pub error: Option<String>,
}

impl DbReport {
    pub fn new(kind: &str, dir: impl Into<String>) -> Self {
        Self {
            dir: dir.into(),
            kind: kind.to_string(),
            ..Default::default()
        }
    }

    pub fn ok(&self) -> bool {
        self.error.is_none() && self.cfs.iter().all(|x| x.ok())
    }

    pub fn merge_cf(&mut self, cf: CfReport) {
        match self.cfs.iter_mut().find(|x| x.cf == cf.cf) {
            Some(existing) => existing.merge(cf),
            None => self.cfs.push(cf),
        }
    }

    pub fn print(&self) {
        let target = if self.dry_run {
            " (dry run)".to_string()
        } else {
            format!(" -> {}", self.out)
        };
        println!("[{}] {}{}", self.kind, self.dir, target);

        for cf in &self.cfs {
            let converted = if self.dry_run {
                "to_convert"
            } else {
                "converted"
            };
            println!(
                "    {:<10} total={:<10} {}={:<10} copied={:<8} failed={}",
                cf.cf, cf.total, converted, cf.converted, cf.copied, cf.failed
            );
            if let Some(err) = &cf.first_error {
                println!("        first error: {}", err);
            }
        }

        for ck in &self.checkpoints {
            println!("    checkpoint {}", ck);
        }

        if let Some(index) = self.snapshot_index {
            println!("    snapshot index: {}", index);
        }

        if let Some(dir) = &self.snapshot_dir {
            println!("    snapshot checkpoint: {}", dir);
        }

        if let Some(err) = &self.error {
            println!("    FAILED: {}", err);
        }
    }
}

#[derive(Debug, Default)]
pub struct UpgradeReport {
    pub meta: Option<DbReport>,
    pub journal: Option<DbReport>,
}

impl UpgradeReport {
    pub fn ok(&self) -> bool {
        self.meta.as_ref().map(|x| x.ok()).unwrap_or(true)
            && self.journal.as_ref().map(|x| x.ok()).unwrap_or(true)
    }

    pub fn print(&self) {
        if let Some(meta) = &self.meta {
            meta.print();
        }
        if let Some(journal) = &self.journal {
            journal.print();
        }
        println!(
            "result: {}",
            if self.ok() {
                "OK, sources untouched"
            } else {
                "FAILED, sources untouched"
            }
        );
    }
}

/// Run the upgrade described by `opts`.
///
/// Every database is converted independently; a failure stops the run so the
/// operator can inspect the output before retrying.
pub fn run(opts: &UpgradeOptions) -> CommonResult<UpgradeReport> {
    check_out_dir(opts)?;

    // The journal snapshot names the checkpoint the master restores `meta/data`
    // from, so it has to be known before a checkpoint is picked: the newest
    // directory on disk can be one a crash left behind without registering it.
    let snapshot_checkpoint = match &opts.journal_dir {
        Some(dir) => journal_db::snapshot_checkpoint_name(dir)?,
        None => None,
    };

    let mut report = UpgradeReport::default();

    let meta = meta_db::upgrade_meta_dir(opts, snapshot_checkpoint.as_deref())?;
    let failed = !meta.ok();
    report.meta = Some(meta);
    if failed {
        return Ok(report);
    }

    if let Some(dir) = &opts.journal_dir {
        let journal = journal_db::upgrade_journal_dir(dir, opts)?;
        let failed = !journal.ok();
        report.journal = Some(journal);
        if failed {
            return Ok(report);
        }
    }

    Ok(report)
}

/// Refuse an output directory that overlaps a source, in either direction. A
/// source nested inside the output would otherwise be overwritten while it is
/// still being read.
///
/// Both sides are normalised first: a literal comparison of `./out` against
/// `out`, or of `a/../b` against `b`, would miss the overlap.
fn check_out_dir(opts: &UpgradeOptions) -> CommonResult<()> {
    let out = normalize(Path::new(&opts.out_dir));
    let sources = std::iter::once(&opts.meta_dir).chain(opts.journal_dir.iter());

    for src in sources {
        let src = normalize(Path::new(src));
        if out.starts_with(&src) || src.starts_with(&out) {
            return err_box!(
                "output directory {} must not overlap source {}",
                opts.out_dir,
                src.display()
            );
        }
    }

    // Every destination is written with rocksdb's format mode, which deletes
    // what already sits there, and `DBEngine` follows symlinks on the way. A
    // link planted inside the output tree would therefore reach a source and
    // destroy the rollback copy.
    let out = Path::new(&opts.out_dir);
    let mut dests = vec![
        out.to_path_buf(),
        out.join("meta"),
        out.join("meta").join("checkpoint"),
        out.join("meta").join("data"),
    ];
    if opts.journal_dir.is_some() {
        dests.push(out.join("journal"));
        dests.push(out.join("journal").join("data"));
    }
    for dest in dests {
        reject_symlink(&dest)?;
    }

    Ok(())
}

/// Error when `path` exists as a symlink.
///
/// The converted tree is rebuilt in place, so every path the tool writes must
/// be a real directory: resolving a link could redirect the rebuild into a
/// directory the tool promised to leave alone.
pub(crate) fn reject_symlink(path: &Path) -> CommonResult<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => err_box!(
            "{} is a symlink; the converted tree is rebuilt in place, so every path under --out-dir must be a real directory",
            path.display()
        ),
        _ => Ok(()),
    }
}

/// Absolute, symlink-resolved form of a path that may not exist yet.
///
/// The deepest existing ancestor is canonicalised (which also fixes the case of
/// an existing path on Windows) and the missing tail is appended after a lexical
/// `..` fold.
fn normalize(path: &Path) -> PathBuf {
    if let Ok(canonical) = path.canonicalize() {
        return canonical;
    }

    let mut tail: Vec<PathBuf> = Vec::new();
    let mut cursor = path;

    loop {
        let Some(name) = cursor.file_name() else {
            // A trailing `.` or `..`: fold the whole path instead of guessing
            // which component belongs to the tail.
            return lexical_fold(path);
        };
        tail.push(PathBuf::from(name));

        let Some(parent) = cursor.parent() else {
            break;
        };
        cursor = parent;

        if cursor.exists() {
            break;
        }
    }

    let mut out = cursor
        .canonicalize()
        .unwrap_or_else(|_| cursor.to_path_buf());
    for name in tail.iter().rev() {
        out.push(name);
    }
    lexical_fold(&out)
}

/// Drop `.` components and resolve `..` without touching the filesystem.
fn lexical_fold(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    out.push(Component::ParentDir.as_os_str());
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Per-column-family progress on stderr. On a terminal each family owns a
/// single line that is rewritten in place as it advances, then left behind as
/// its final summary; a redirected stderr keeps periodic whole lines so a log
/// stays readable. The final report stays on stdout.
pub(crate) struct Progress {
    cf: String,
    /// `rocksdb.estimate-num-keys` of the source family: approximate, but free
    /// to read and good enough for a percentage. `None` when unreadable.
    estimate: Option<u64>,
    done: u64,
    started: Instant,
    last_print: Instant,
    tty: bool,
    /// Length of the line currently on screen, so a shorter rewrite can pad
    /// over the tail of a longer one.
    last_len: usize,
}

impl Progress {
    pub(crate) fn new(db: &DBEngine, cf: &str) -> Self {
        let estimate = db
            .cf(cf)
            .ok()
            .and_then(|handle| {
                db.get_db()
                    .property_int_value_cf(handle, "rocksdb.estimate-num-keys")
                    .ok()
                    .flatten()
            })
            .filter(|n| *n > 0);
        let now = Instant::now();
        Self {
            cf: cf.to_string(),
            estimate,
            done: 0,
            started: now,
            last_print: now,
            tty: std::io::stderr().is_terminal(),
            last_len: 0,
        }
    }

    pub(crate) fn tick(&mut self) {
        self.done += 1;
        // A terminal redraws one line, so refresh often; a log appends whole
        // lines, so keep them sparse.
        let interval = if self.tty {
            Duration::from_millis(200)
        } else {
            Duration::from_secs(2)
        };
        if self.last_print.elapsed() >= interval {
            self.render(false);
            self.last_print = Instant::now();
        }
    }

    /// Leave the family's final line. On a terminal every family gets one; in
    /// a log only a family that ran long enough is worth a line.
    pub(crate) fn finish(&mut self) {
        if self.done == 0 {
            return;
        }
        if self.tty {
            self.render(true);
        } else if self.started.elapsed() >= Duration::from_secs(2) {
            eprintln!("{}", self.line());
        }
    }

    fn render(&mut self, finalize: bool) {
        let line = self.line();
        if self.tty {
            if finalize {
                eprint!("\r{}\n", line);
            } else {
                let pad = self.last_len.saturating_sub(line.len());
                eprint!("\r{}{}", line, " ".repeat(pad));
            }
            self.last_len = line.len();
        } else if !finalize {
            eprintln!("{}", line);
        }
    }

    fn line(&self) -> String {
        let elapsed = self.started.elapsed().as_secs_f64();
        let rate = self.done as f64 / elapsed.max(f64::EPSILON);
        match self.estimate {
            Some(total) => {
                let percent = (self.done * 100 / total).min(100);
                format!(
                    "  {}: {}/~{} ({}%), {:.0} rec/s",
                    self.cf, self.done, total, percent, rate
                )
            }
            None => format!("  {}: {} records, {:.0} rec/s", self.cf, self.done, rate),
        }
    }
}

/// What to write for one source record.
pub(crate) enum Action {
    /// Legacy payload re-encoded with the new codec.
    Converted(Vec<u8>),
    /// Not a codec-managed payload (raw or opaque); copy verbatim.
    Copy,
}

/// Database configuration for a directory that is itself a RocksDB database,
/// such as a checkpoint. `DBConf::new` would otherwise point `data_dir` at a
/// `data/` child that checkpoints do not have.
pub(crate) fn dir_conf(dir: &str) -> DBConf {
    let mut conf = DBConf::new(dir);
    conf.data_dir = dir.to_string();
    conf
}

/// Encode one metadata record the way the master does.
///
/// The master wraps this in `MetadataCodec`, which is private to that crate: it
/// is only `Codec::encode` stamped with the current metadata schema version. The
/// tool reproduces it from `curvine-codec`, which is why it must be built from
/// the same revision as the binaries that will read the result.
pub(crate) fn encode_meta<T: Encode + ?Sized>(value: &T) -> CommonResult<Vec<u8>> {
    Ok(Codec::encode(curvine_codec::CURRENT_VERSION, value)?)
}

/// Decode one legacy record.
///
/// The options are the ones `bincode::serialize` writes with (fixed-width
/// integers), except that trailing bytes are rejected. A record in the new
/// layout carries a one-byte header, so it leaves a byte over and fails here
/// instead of being re-encoded as if it were a legacy value.
fn bincode_read<T: DeserializeOwned>(bytes: &[u8]) -> CommonResult<T> {
    use bincode::Options;

    let options = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes();
    let value: T = try_err!(options.deserialize(bytes));
    Ok(value)
}

/// Convert one structured record.
///
/// The source tree is always the legacy one, so a record is decoded with
/// `bincode` unconditionally. Detecting the format per record cannot be done
/// safely: a legacy payload can start with the codec magic by accident (a
/// `bincode` length of 129 starts with `0x81`), so sniffing would silently copy
/// an unconverted record through and the master would fail on it later.
///
/// The bytes are deliberately not rebuilt and compared: `InodeView` embeds a
/// `HashMap` (`x_attr`) and a `HashSet` (`clients`), which re-encode in iteration
/// order, so `encode(decode(bytes)) != bytes` for any record holding two or more
/// entries. Nothing is compared byte for byte for that reason.
pub(crate) fn convert_value_struct<T>(bytes: &[u8]) -> CommonResult<Vec<u8>>
where
    T: DeserializeOwned + Encode,
{
    let value: T = bincode_read(bytes)?;
    encode_meta(&value)
}

/// Convert a fixed-width scalar record.
///
/// A scalar payload is a plain `bincode` integer: eight bytes, no header.
pub(crate) fn convert_i64(bytes: &[u8]) -> CommonResult<Vec<u8>> {
    if bytes.len() != 8 {
        return err_box!("unexpected i64 record length {}", bytes.len());
    }

    let value: i64 = bincode_read(bytes)?;
    encode_meta(&value)
}

/// Convert one journal batch payload from `bincode` to the protobuf envelope
/// the new build writes. Used for raft log entries.
///
/// The batch is decoded through [`JournalBatch::decode_legacy_bincode`], which
/// walks the payload layouts older builds wrote: a journal directory can hold
/// batches from several of them, and only the matching one is readable.
pub(crate) fn convert_batch(bytes: &[u8]) -> CommonResult<Vec<u8>> {
    let batch = JournalBatch::decode_legacy_bincode(bytes)?;
    batch.encode()
}

/// `true` for `<db>.new` / `<db>.legacy` directories left behind by the earlier
/// in-place version of this tool.
pub(crate) fn is_tool_dir(name: &str) -> bool {
    TOOL_DIR_SUFFIXES
        .iter()
        .any(|suffix| name.ends_with(suffix))
}

pub(crate) fn sub_dirs(dir: &str, prefix: &str) -> CommonResult<Vec<String>> {
    let mut out = Vec::new();
    if !Path::new(dir).exists() {
        return Ok(out);
    }

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path: PathBuf = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if is_tool_dir(&name) {
            continue;
        }
        if name.starts_with(prefix) {
            out.push(path.to_string_lossy().to_string());
        }
    }
    out.sort();
    Ok(out)
}

/// A directory that is itself a RocksDB database (checkpoint directories), as
/// opposed to a base dir holding `data/` and `checkpoint/`.
pub(crate) fn is_db_dir(dir: &str) -> bool {
    Path::new(dir).join("CURRENT").exists()
}
