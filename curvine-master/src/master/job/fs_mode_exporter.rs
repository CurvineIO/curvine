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

//! Hands fs_mode exports to the Transfer service (transfer mode).
//!
//! Every submit waits only for Transfer's enqueue ACK, never for the copy.
//! Files of one mount are merged into one job per call; each entry is still an
//! independent single-file copy guarded by its generation. A submit that fails
//! leaves the files `Cv`; Free reconciliation re-exports them, so nothing here
//! needs to be durable or retried across calls.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use curvine_config::ClusterConf;
use curvine_error::FsResult;
use curvine_fs_api::Path;
use curvine_job_client::TransferClient;
use curvine_metrics::{Counter, Metrics as m};
use curvine_model::{FileGen, FsModeFile, TransferCommand, TransferKind};
use curvine_runtime::common::{LocalTime, Utils};
use curvine_runtime::runtime::Runtime;
use log::warn;
use once_cell::sync::Lazy;

/// Total time one batch may spend in submit, retries included. Short on purpose:
/// the replayer awaits it, and a lost batch is recovered by Free reconciliation.
const SUBMIT_TIMEOUT: Duration = Duration::from_millis(500);
const SUBMIT_RETRY_DELAY: Duration = Duration::from_millis(50);
/// After this many consecutive failed batches, submits are skipped for
/// `BREAKER_COOLDOWN_MS`, so an unavailable Transfer service costs one timeout
/// per cooldown instead of one per journal entry.
const BREAKER_FAILURES: u32 = 5;
const BREAKER_COOLDOWN_MS: i64 = 10_000;

struct ExportMetrics {
    submitted: Counter,
    failed: Counter,
    skipped: Counter,
}

static EXPORT_METRICS: Lazy<ExportMetrics> = Lazy::new(|| ExportMetrics {
    submitted: m::new_counter(
        "fs_mode_export_submitted_files_total",
        "fs_mode files accepted by the Transfer service",
    )
    .expect("register fs_mode export submitted metric"),
    failed: m::new_counter(
        "fs_mode_export_failed_files_total",
        "fs_mode files whose export submit failed",
    )
    .expect("register fs_mode export failed metric"),
    skipped: m::new_counter(
        "fs_mode_export_skipped_files_total",
        "fs_mode files not submitted because the submit breaker was open",
    )
    .expect("register fs_mode export skipped metric"),
});

/// One file to export: Curvine source, UFS target and the version to copy.
#[derive(Debug, Clone)]
pub struct FsModeExport {
    pub mount_id: u32,
    pub source: Path,
    pub target: Path,
    pub gen: FileGen,
}

pub struct FsModeExporter {
    client: TransferClient,
    cluster_id: String,
    submitter: String,
    /// Distinguishes request ids of this process from any earlier one, so a
    /// batch sequence restarting at zero never collides with an old job.
    nonce: String,
    seq: AtomicU64,
    failures: AtomicU32,
    open_until_ms: AtomicI64,
}

impl FsModeExporter {
    pub fn new(conf: &ClusterConf, rt: Arc<Runtime>) -> FsResult<Self> {
        Ok(Self {
            client: TransferClient::with_rt(conf, rt)?,
            cluster_id: conf.cluster_id.clone(),
            submitter: TransferCommand::fs_mode_submitter(&conf.cluster_id),
            nonce: Utils::uuid(),
            seq: AtomicU64::new(0),
            failures: AtomicU32::new(0),
            open_until_ms: AtomicI64::new(0),
        })
    }

    /// Submit `files`, one job per mount and per `FS_MODE_BATCH_MAX_FILES`.
    /// Returns how many files Transfer accepted; failures are logged and counted.
    pub async fn submit(&self, files: Vec<FsModeExport>) -> usize {
        let mut by_mount: BTreeMap<u32, Vec<FsModeExport>> = BTreeMap::new();
        for file in files {
            by_mount.entry(file.mount_id).or_default().push(file);
        }

        let mut accepted = 0;
        for files in by_mount.into_values() {
            for chunk in files.chunks(TransferCommand::FS_MODE_BATCH_MAX_FILES) {
                accepted += self.submit_batch(chunk).await;
            }
        }
        accepted
    }

    async fn submit_batch(&self, files: &[FsModeExport]) -> usize {
        if LocalTime::mills() as i64 <= self.open_until_ms.load(Ordering::Acquire) {
            EXPORT_METRICS.skipped.inc_by(files.len() as i64);
            return 0;
        }

        // Built once: a retry must resend the identical command under the same
        // request id, otherwise Transfer reports a conflict instead of a dedup.
        let command = self.batch_command(files);
        let deadline = tokio::time::Instant::now() + SUBMIT_TIMEOUT;
        let result = loop {
            let attempt = tokio::time::timeout_at(deadline, self.client.submit(command.clone()));
            match attempt.await {
                Ok(Ok(_)) => break Ok(()),
                Ok(Err(err)) if tokio::time::Instant::now() + SUBMIT_RETRY_DELAY < deadline => {
                    warn!(
                        "fs_mode export {} submit failed, retrying: {}",
                        command.client_request_id, err
                    );
                    tokio::time::sleep(SUBMIT_RETRY_DELAY).await;
                }
                Ok(Err(err)) => break Err(err.to_string()),
                Err(_) => break Err(format!("timed out after {:?}", SUBMIT_TIMEOUT)),
            }
        };

        match result {
            Ok(()) => {
                self.failures.store(0, Ordering::Release);
                EXPORT_METRICS.submitted.inc_by(files.len() as i64);
                files.len()
            }
            Err(err) => {
                EXPORT_METRICS.failed.inc_by(files.len() as i64);
                warn!(
                    "fs_mode export {} for {} file(s) not submitted, Free reconciliation will retry: {}",
                    command.client_request_id,
                    files.len(),
                    err
                );
                if self.failures.fetch_add(1, Ordering::AcqRel) + 1 >= BREAKER_FAILURES {
                    self.failures.store(0, Ordering::Release);
                    self.open_until_ms.store(
                        LocalTime::mills() as i64 + BREAKER_COOLDOWN_MS,
                        Ordering::Release,
                    );
                }
                0
            }
        }
    }

    fn batch_command(&self, files: &[FsModeExport]) -> TransferCommand {
        let seq = self.seq.fetch_add(1, Ordering::AcqRel);
        let client_request_id = format!(
            "fsm:{}:{}:{}:b{}",
            self.cluster_id.len(),
            self.cluster_id,
            self.nonce,
            seq
        );
        let source_path = common_ancestor(files.iter().map(|f| f.source.full_path()));
        let target_path = common_ancestor(files.iter().map(|f| f.target.full_path()));

        let mut command = TransferCommand {
            kind: TransferKind::Export,
            source_path,
            target_path,
            client_request_id,
            submitter: self.submitter.clone(),
            tenant: String::new(),
            options: Default::default(),
            fs_mode_files: files
                .iter()
                .map(|f| FsModeFile {
                    source: f.source.clone_uri(),
                    target: f.target.clone_uri(),
                    gen: f.gen,
                })
                .collect(),
        };
        command.set_overwrite(true);
        command
    }
}

/// Deepest common ancestor of `paths`, compared by `/`-separated components.
/// A single path is its own ancestor, so a one-file job keeps the file's own
/// source and target like a plain export.
pub fn common_ancestor<'a>(mut paths: impl Iterator<Item = &'a str>) -> String {
    let Some(first) = paths.next() else {
        return "/".to_string();
    };
    let mut common: Vec<&str> = first.split('/').collect();
    for path in paths {
        let len = common
            .iter()
            .zip(path.split('/'))
            .take_while(|(a, b)| **a == *b)
            .count();
        common.truncate(len);
    }
    let joined = common.join("/");
    if joined.is_empty() {
        "/".to_string()
    } else {
        joined
    }
}
