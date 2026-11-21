// Copyright 2025 The Curvine Authors
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

use std::fs::{self, DirBuilder, File};
use std::io;
use std::os::unix::fs::{DirBuilderExt, FileExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub(crate) const SPOOL_BUSY: &str = "SlowDown: signed upload buffer budget exhausted";
const SPOOL_ERROR: &str = "InternalError: failed to buffer signed aws-chunked record";

fn spool_error(err: impl std::fmt::Display) -> &'static str {
    tracing::warn!("Failed to buffer signed aws-chunked record: {err}");
    SPOOL_ERROR
}

#[derive(Debug)]
pub(crate) struct SignedSpool {
    dir: PathBuf,
    budget: u64,
    reserved: AtomicU64,
}

impl SignedSpool {
    pub(crate) fn new(dir: impl Into<PathBuf>, budget: u64) -> Arc<Self> {
        Arc::new(Self {
            dir: dir.into(),
            budget,
            reserved: AtomicU64::new(0),
        })
    }

    fn reserve(self: &Arc<Self>, bytes: u64) -> Result<Reservation, &'static str> {
        self.reserved
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes)
                    .filter(|&total| total <= self.budget)
            })
            .map_err(|_| SPOOL_BUSY)?;
        Ok(Reservation {
            spool: Arc::clone(self),
            bytes,
        })
    }

    pub(crate) async fn open(self: &Arc<Self>, bytes: u64) -> Result<SpoolFile, &'static str> {
        self.open_with(bytes, |path| fs::remove_file(path)).await
    }

    async fn open_with(
        self: &Arc<Self>,
        bytes: u64,
        unlink: impl FnOnce(&Path) -> io::Result<()> + Send + 'static,
    ) -> Result<SpoolFile, &'static str> {
        let reservation = self.reserve(bytes)?;
        tokio::task::spawn_blocking(move || -> io::Result<SpoolFile> {
            let file = open_unlinked(&reservation.spool.dir, unlink)?;
            Ok(SpoolFile(Arc::new(OpenSpool {
                file,
                _reservation: reservation,
            })))
        })
        .await
        .map_err(spool_error)?
        .map_err(spool_error)
    }
}

fn open_unlinked(dir: &Path, unlink: impl FnOnce(&Path) -> io::Result<()>) -> io::Result<File> {
    DirBuilder::new().recursive(true).mode(0o700).create(dir)?;
    let meta = fs::symlink_metadata(dir)?;
    if !meta.is_dir()
        || meta.permissions().mode() & 0o077 != 0
        || meta.uid() != nix::unistd::geteuid().as_raw()
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "signed spool directory must be private and owned by the gateway user",
        ));
    }
    let mut file = tempfile::NamedTempFile::new_in(dir)?;
    // Do not use tempfile::tempfile(): its Unix fallback ignores unlink errors.
    // On error the NamedTempFile will retry best-effort cleanup, but no request
    // bytes can have been written to the file.
    unlink(file.path())?;
    file.disable_cleanup(true);
    Ok(file.into_file())
}

struct Reservation {
    spool: Arc<SignedSpool>,
    bytes: u64,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.spool.reserved.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

struct OpenSpool {
    // Fields drop in order: close the actual file before returning its budget.
    file: File,
    _reservation: Reservation,
}

pub(crate) struct SpoolFile(Arc<OpenSpool>);

impl SpoolFile {
    async fn run<T: Send + 'static>(
        &self,
        io: impl FnOnce(&File) -> io::Result<T> + Send + 'static,
    ) -> Result<T, &'static str> {
        // A discarded JoinHandle does not stop an in-flight blocking syscall.
        // The operation holds the same file and reservation until it finishes.
        let spool = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || io(&spool.file))
            .await
            .map_err(spool_error)?
            .map_err(spool_error)
    }

    pub(crate) async fn write(&self, offset: u64, data: Vec<u8>) -> Result<(), &'static str> {
        self.run(move |file| file.write_all_at(&data, offset)).await
    }

    pub(crate) async fn read(&self, offset: u64, n: usize) -> Result<Vec<u8>, &'static str> {
        self.run(move |file| {
            let mut data = vec![0; n];
            file.read_exact_at(&mut data, offset)?;
            Ok(data)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unlink_failure_never_returns_a_writable_file_or_leaks_budget() {
        let dir = tempfile::tempdir().unwrap();
        let spool = SignedSpool::new(dir.path().join("spool"), 32);
        let result = spool
            .open_with(32, |path| {
                assert_eq!(fs::metadata(path).unwrap().len(), 0);
                Err(io::Error::from(io::ErrorKind::PermissionDenied))
            })
            .await;
        assert_eq!(result.err(), Some(SPOOL_ERROR));
        assert!(spool.dir.read_dir().unwrap().next().is_none());
        assert!(spool.open(32).await.is_ok());
    }

    #[tokio::test]
    async fn concurrent_requests_cannot_overbook_the_budget() {
        let dir = tempfile::tempdir().unwrap();
        let spool = SignedSpool::new(dir.path().join("spool"), 64);
        let mut requests = tokio::task::JoinSet::new();
        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        for _ in 0..8 {
            let spool = spool.clone();
            let barrier = barrier.clone();
            requests.spawn(async move {
                barrier.wait().await;
                spool.open(32).await
            });
        }
        let mut files = Vec::new();
        let mut denied = 0;
        while let Some(result) = requests.join_next().await {
            match result.unwrap() {
                Ok(file) => files.push(file),
                Err(SPOOL_BUSY) => denied += 1,
                Err(err) => panic!("unexpected spool error: {err}"),
            }
        }
        assert_eq!((files.len(), denied), (2, 6));
        assert!(spool.dir.read_dir().unwrap().next().is_none());
        drop(files);
        assert!(spool.open(64).await.is_ok());
    }

    #[tokio::test]
    async fn cancelled_blocking_io_keeps_budget_until_the_file_closes() {
        let dir = tempfile::tempdir().unwrap();
        let spool = SignedSpool::new(dir.path().join("spool"), 32);
        let file = spool.open(32).await.unwrap();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (resume, paused) = std::sync::mpsc::channel();
        let io = tokio::spawn(async move {
            file.run(move |f| {
                started.send(()).unwrap();
                paused.recv().unwrap();
                f.write_all_at(b"data", 0)
            })
            .await
        });
        ready.await.unwrap();
        io.abort();
        assert!(io.await.unwrap_err().is_cancelled());
        let denied = spool.open(1).await;
        resume.send(()).unwrap();
        assert_eq!(denied.err(), Some(SPOOL_BUSY));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while spool.reserved.load(Ordering::Relaxed) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(spool.open(32).await.is_ok());
        assert!(spool.dir.read_dir().unwrap().next().is_none());
    }
}
