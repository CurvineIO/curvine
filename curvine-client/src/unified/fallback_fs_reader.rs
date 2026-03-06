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

use crate::file::FsReader;
use crate::unified::{UfsFileSystem, UfsReader};
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path, Reader};
use curvine_common::state::{FileBlocks, FileStatus};
use curvine_common::FsResult;
use log::warn;
use orpc::err_box;
use orpc::sys::DataSlice;

/// A wrapper around `FsReader` that transparently falls back to UFS when Curvine
/// becomes unavailable (master down at open time handled externally, worker down
/// during read handled here).
///
/// Design:
/// - `cv_reader` is always present and maintains all internal state (pos, chunk, etc.).
/// - `ufs_reader` is created lazily on the first worker failure; once set, all
///   subsequent reads go through UFS (no retry back to Curvine).
/// - Before falling back, `validate_ufs_consistency` is called to ensure the UFS
///   data matches the snapshot recorded in Curvine metadata (`ufs_mtime`, `len`).
pub struct FallbackFsReader {
    /// Wraps the Curvine reader. Always present as the metadata authority (status, len).
    cv_reader: FsReader,
    /// Created lazily on first worker error. None = reading from Curvine.
    /// Uses UfsReader (not UnifiedReader) to avoid a recursive type/async-fn cycle:
    /// UnifiedReader::Fallback(FallbackFsReader) -> UfsReader (no Fallback variant).
    ufs_reader: Option<UfsReader>,
    ufs_path: Path,
    ufs_fs: UfsFileSystem,
}

impl FallbackFsReader {
    pub fn new(cv_reader: FsReader, ufs_path: Path, ufs_fs: UfsFileSystem) -> Self {
        Self {
            cv_reader,
            ufs_reader: None,
            ufs_path,
            ufs_fs,
        }
    }

    /// Expose Curvine block metadata for callers that need cache hints.
    pub fn file_blocks(&self) -> &FileBlocks {
        self.cv_reader.file_blocks()
    }

    /// Validates that the UFS data matches what Curvine recorded when it last flushed.
    /// Returns an error if UFS data cannot be trusted (modified externally or never flushed).
    async fn validate_ufs_consistency(&self) -> FsResult<()> {
        let cv_status = self.cv_reader.status();
        let cv_ufs_mtime = cv_status.storage_policy.ufs_mtime;
        let cv_len = cv_status.len;
        let ufs_status = self.ufs_fs.get_status(&self.ufs_path).await?;

        if let Err(e) =
            check_ufs_consistency(cv_ufs_mtime, cv_len, ufs_status.mtime, ufs_status.len)
        {
            return err_box!("UFS data inconsistent for {}: {}", self.ufs_path, e);
        }

        Ok(())
    }
}

/// Returns true for errors caused by a worker being unavailable (IO, pipeline, timeout).
/// These trigger a UFS fallback. Other error kinds (e.g. NotLeaderMaster) are not
/// worker errors — the RPC layer handles master failover via retry.
pub(crate) fn is_worker_err(e: &FsError) -> bool {
    if matches!(
        e,
        FsError::IO(_) | FsError::Pipeline(_) | FsError::Timeout(_)
    ) {
        return true;
    }

    // In some read paths, connection-level worker failures are wrapped as Common.
    // Treat well-known transport failure signatures as worker failures too.
    if matches!(e, FsError::Common(_)) {
        let msg = e.to_string().to_lowercase();
        return msg.contains("connection refused")
            || msg.contains("connection reset")
            || msg.contains("early eof")
            || msg.contains("broken pipe")
            || msg.contains("sender dropped");
    }

    false
}

/// Shared consistency check logic used by runtime path and tests.
pub(crate) fn check_ufs_consistency(
    cv_ufs_mtime: i64,
    cv_len: i64,
    ufs_mtime: i64,
    ufs_len: i64,
) -> FsResult<()> {
    if cv_ufs_mtime == 0 {
        return err_box!("data has not been flushed to UFS yet (ufs_mtime=0)");
    }
    if cv_ufs_mtime != ufs_mtime || cv_len != ufs_len {
        return err_box!(
            "UFS data inconsistent: cv_ufs_mtime={}, ufs_mtime={}, cv_len={}, ufs_len={}",
            cv_ufs_mtime,
            ufs_mtime,
            cv_len,
            ufs_len
        );
    }
    Ok(())
}

impl Reader for FallbackFsReader {
    // Always from cv_reader — Curvine is the metadata authority in FsMode.
    fn status(&self) -> &FileStatus {
        self.cv_reader.status()
    }

    fn path(&self) -> &Path {
        self.cv_reader.path()
    }

    fn len(&self) -> i64 {
        self.cv_reader.len()
    }

    // pos/chunk/chunk_size delegate to whichever reader is currently active.
    fn pos(&self) -> i64 {
        match &self.ufs_reader {
            Some(u) => u.pos(),
            None => self.cv_reader.pos(),
        }
    }

    fn pos_mut(&mut self) -> &mut i64 {
        match &mut self.ufs_reader {
            Some(u) => u.pos_mut(),
            None => self.cv_reader.pos_mut(),
        }
    }

    fn chunk_mut(&mut self) -> &mut DataSlice {
        match &mut self.ufs_reader {
            Some(u) => u.chunk_mut(),
            None => self.cv_reader.chunk_mut(),
        }
    }

    fn chunk_size(&self) -> usize {
        match &self.ufs_reader {
            Some(u) => u.chunk_size(),
            None => self.cv_reader.chunk_size(),
        }
    }

    async fn read_chunk0(&mut self) -> FsResult<DataSlice> {
        // Already fallen back to UFS — read directly, no retry to Curvine.
        if let Some(ufs) = &mut self.ufs_reader {
            return ufs.read_chunk0().await;
        }

        // Try reading from Curvine.
        match self.cv_reader.read_chunk0().await {
            Ok(data) => Ok(data),

            Err(e) if is_worker_err(&e) => {
                let pos = self.cv_reader.pos();
                warn!(
                    "Curvine worker error at pos {} for {}, validating UFS consistency before fallback: {}",
                    pos,
                    self.cv_reader.path(),
                    e
                );

                // Refuse to fall back if UFS data cannot be trusted.
                self.validate_ufs_consistency().await?;

                let mut ufs: UfsReader = self.ufs_fs.open_ufs(&self.ufs_path).await?;
                ufs.seek(pos).await?;
                let data = ufs.read_chunk0().await?;
                self.ufs_reader = Some(ufs);
                Ok(data)
            }

            Err(e) => Err(e),
        }
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        match &mut self.ufs_reader {
            Some(ufs) => ufs.seek(pos).await,
            None => self.cv_reader.seek(pos).await,
        }
    }

    async fn complete(&mut self) -> FsResult<()> {
        let fallback_used = self.ufs_reader.is_some();
        if let Some(mut ufs) = self.ufs_reader.take() {
            // Best-effort cleanup; the worker may already be down.
            let _ = ufs.complete().await;
        }
        // Always release cv_reader connections (block handles, cached readers).
        match self.cv_reader.complete().await {
            Ok(()) => Ok(()),
            Err(e) if fallback_used => {
                warn!(
                    "Ignore cv_reader complete error after UFS fallback for {}: {}",
                    self.cv_reader.path(),
                    e
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::error::FsError;
    use orpc::error::ErrorImpl;
    use std::io;
    use std::time::Duration;

    // --- TC-1 to TC-5: is_worker_err classification ---

    #[test]
    fn test_is_worker_err_io() {
        let e = FsError::IO(ErrorImpl::with_source(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "connection reset",
        )));
        assert!(is_worker_err(&e));
    }

    #[test]
    fn test_is_worker_err_pipeline() {
        // Uses the public constructor to avoid touching ErrorImpl internals.
        let e = FsError::pipeline_error(42, "worker unreachable");
        assert!(is_worker_err(&e));
    }

    #[tokio::test]
    async fn test_is_worker_err_timeout() {
        // Elapsed can only be created via an actual tokio timeout.
        let elapsed = tokio::time::timeout(Duration::from_nanos(1), std::future::pending::<()>())
            .await
            .unwrap_err();
        let e = FsError::Timeout(ErrorImpl::with_source(elapsed));
        assert!(is_worker_err(&e));
    }

    #[test]
    fn test_is_worker_err_not_leader_master() {
        // Master failover is handled by RPC retry, not UFS fallback.
        let e = FsError::not_leader("not leader");
        assert!(!is_worker_err(&e));
    }

    #[test]
    fn test_is_worker_err_file_not_found() {
        let e = FsError::file_not_found("/test/path");
        assert!(!is_worker_err(&e));
    }

    #[test]
    fn test_is_worker_err_common_connection_refused() {
        let e = FsError::common("Connection refused (os error 111)");
        assert!(is_worker_err(&e));
    }

    // --- TC-6 to TC-9: check_ufs_consistency pure function ---

    #[test]
    fn test_ufs_consistency_mtime_zero() {
        // Data has never been flushed to UFS — cannot fall back.
        let result = check_ufs_consistency(0, 100, 0, 100);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("ufs_mtime=0"));
    }

    #[test]
    fn test_ufs_consistency_mtime_mismatch() {
        // UFS was modified externally after Curvine last flushed.
        let result = check_ufs_consistency(1000, 100, 2000, 100);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("cv_ufs_mtime=1000"));
        assert!(msg.contains("ufs_mtime=2000"));
    }

    #[test]
    fn test_ufs_consistency_len_mismatch() {
        // mtime matches but file length differs — still inconsistent.
        let result = check_ufs_consistency(1000, 100, 1000, 150);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("cv_len=100"));
        assert!(msg.contains("ufs_len=150"));
    }

    #[test]
    fn test_ufs_consistency_all_match() {
        // Both mtime and len match — safe to fall back.
        let result = check_ufs_consistency(1000, 100, 1000, 100);
        assert!(result.is_ok());
    }
}
