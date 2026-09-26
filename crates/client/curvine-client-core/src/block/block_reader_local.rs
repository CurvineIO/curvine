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

use crate::block::{BlockClient, BlockReaderRemote};
use crate::file::FsContext;
use bytes::BytesMut;
use curvine_core_error::err_box;
use curvine_error::FsError;
use curvine_error::FsResult;
use curvine_io::IOResult;
use curvine_io::LocalFile;
use curvine_io::{AlignedBuf, CacheManager, DataSlice, ReadAheadTask, DIRECT_IO_ALIGN};
use curvine_model::{ExtendedBlock, WorkerAddress};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_sys::RawPtr;
use log::{debug, warn};
use std::sync::Arc;

/// Layout of one direct (O_DIRECT) read: the [`DIRECT_IO_ALIGN`]-aligned
/// region `[region_start, region_start + region_len)` covering the requested
/// `[off, off + len)` window, plus `delta`, the offset of the window inside
/// that region.
///
/// The region arithmetic mirrors `LocalFile::pread_aligned` exactly so the
/// `AlignedBuf` allocated from `region_len` always satisfies its buffer-size
/// check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DirectReadPlan {
    off: i64,
    len: usize,
    region_start: i64,
    region_len: usize,
    delta: usize,
}

impl DirectReadPlan {
    fn new(off: i64, len: usize) -> FsResult<Self> {
        const ALIGN: i64 = DIRECT_IO_ALIGN as i64;

        if off < 0 {
            return err_box!("direct read: negative offset {}", off);
        }
        if len == 0 {
            return err_box!("direct read: empty read at offset {}", off);
        }
        let Ok(len_i64) = i64::try_from(len) else {
            return err_box!("direct read: len {} exceeds i64 range", len);
        };
        let Some(end) = off
            .checked_add(len_i64)
            .and_then(|e| e.checked_add(ALIGN - 1))
        else {
            return err_box!("direct read: off {} + len {} overflows i64", off, len);
        };

        let region_start = off & !(ALIGN - 1);
        let region_end = end & !(ALIGN - 1);
        Ok(Self {
            off,
            len,
            region_start,
            region_len: (region_end - region_start) as usize,
            delta: (off - region_start) as usize,
        })
    }

    /// Bounds of the requested window inside the aligned buffer, given the
    /// RAW byte count returned by `LocalFile::pread_aligned` (relative to
    /// `region_start`). Errors when the raw count does not fully cover
    /// `[off, off + len)`: exposing bytes past `raw` would leak unfilled
    /// memory (fill-before-consume), so the caller degrades to a buffered
    /// retry which reproduces the buffered path's short-read behavior
    /// exactly (`read_exact` fails at EOF).
    fn crop_bounds(&self, raw: usize) -> FsResult<(usize, usize)> {
        if raw.saturating_sub(self.delta) < self.len {
            return err_box!(
                "direct read: short read, off {} len {} raw {}",
                self.off,
                self.len,
                raw
            );
        }
        Ok((self.delta, self.delta + self.len))
    }
}

pub struct BlockReaderLocal {
    rt: Arc<Runtime>,
    client: BlockClient,
    os_cache: CacheManager,
    last_task: Option<ReadAheadTask>,
    block: ExtendedBlock,
    file: RawPtr<LocalFile>,
    worker_address: WorkerAddress,
    len: i64,
    req_id: i64,
    seq_id: i32,
    chunk: BytesMut,
    chunk_size: usize,
    // Whether this reader reads through O_DIRECT. Set at open when
    // `client.enable_direct_read` is on and the platform/filesystem supports
    // it; cleared permanently on the first direct-read failure.
    direct: bool,
}

pub(crate) enum LocalReaderOpen {
    Local(BlockReaderLocal),
    Remote(BlockReaderRemote),
}

enum AdoptedRead<C, L, R> {
    Local { client: C, file: L },
    Remote(R),
}

trait ReadSessionCleanup {
    async fn finish_read(
        &mut self,
        block: &ExtendedBlock,
        req_id: i64,
        seq_id: i32,
    ) -> FsResult<()>;

    fn prevent_pool_reuse(&mut self);
}

impl ReadSessionCleanup for BlockClient {
    async fn finish_read(
        &mut self,
        block: &ExtendedBlock,
        req_id: i64,
        seq_id: i32,
    ) -> FsResult<()> {
        self.read_commit(block, req_id, seq_id).await
    }

    fn prevent_pool_reuse(&mut self) {
        self.clear_pool();
    }
}

async fn adopt_opened_session<C, L, R, E>(
    mut client: C,
    path: Option<String>,
    block: &ExtendedBlock,
    req_id: i64,
    seq_id: i32,
    open_local: impl FnOnce(&str) -> Result<L, E>,
    open_remote: impl FnOnce(C) -> R,
) -> Result<AdoptedRead<C, L, R>, E>
where
    C: ReadSessionCleanup,
{
    let Some(path) = path else {
        return Ok(AdoptedRead::Remote(open_remote(client)));
    };

    match open_local(&path) {
        Ok(file) => Ok(AdoptedRead::Local { client, file }),
        Err(e) => {
            // Release the Worker read context promptly, but never allow a failed
            // local open to return an uncertain session to the connection pool.
            let _ = client.finish_read(block, req_id, seq_id + 1).await;
            client.prevent_pool_reuse();
            Err(e)
        }
    }
}

impl BlockReaderLocal {
    pub(crate) async fn open(
        fs_context: Arc<FsContext>,
        block: ExtendedBlock,
        addr: WorkerAddress,
        off: i64,
        len: i64,
    ) -> FsResult<LocalReaderOpen> {
        let req_id = Utils::req_id();
        let seq_id = 0;

        let chunk_size = fs_context.read_chunk_size();
        let direct = fs_context.conf.client.enable_direct_read;
        let client = fs_context.acquire_read(&addr).await?;
        let read_context = client
            .open_block(
                &fs_context.conf.client,
                &block,
                off,
                len,
                req_id,
                seq_id,
                true,
            )
            .await?;

        let opened = adopt_opened_session(
            client,
            read_context.path,
            &block,
            req_id,
            seq_id,
            |path| Self::open_local(path, off, direct),
            |client| {
                BlockReaderRemote::from_opened(
                    client,
                    block.clone(),
                    addr.clone(),
                    off,
                    len,
                    req_id,
                    seq_id,
                )
            },
        )
        .await?;

        let (client, (file, direct)) = match opened {
            AdoptedRead::Local { client, file } => (client, file),
            // The worker can reject short-circuit mode when it must synthesize a
            // sparse logical tail. Reuse the already-open remote read session.
            AdoptedRead::Remote(reader) => return Ok(LocalReaderOpen::Remote(reader)),
        };

        let reader = Self {
            rt: fs_context.clone_runtime(),
            client,
            os_cache: fs_context.clone_os_cache(),
            last_task: None,
            block,
            file: RawPtr::from_owned(file),
            worker_address: addr.clone(),
            len,
            req_id,
            seq_id,
            chunk: BytesMut::with_capacity(chunk_size),
            chunk_size,
            direct,
        };

        Ok(LocalReaderOpen::Local(reader))
    }

    /// Open the local block file, preferring an O_DIRECT handle when `direct`
    /// is set. `Ok(None)` from [`LocalFile::with_read_direct`] means the
    /// platform or filesystem cannot do direct I/O; an open error is logged
    /// and retried buffered — either way the buffered open decides the
    /// outcome, so a direct-incapable path never fails the read.
    fn open_local(path: &str, off: i64, direct: bool) -> IOResult<(LocalFile, bool)> {
        if direct {
            match LocalFile::with_read_direct(path, off as u64) {
                Ok(Some(file)) => return Ok((file, true)),
                Ok(None) => debug!("O_DIRECT unavailable for {}, using buffered read", path),
                Err(e) => warn!(
                    "O_DIRECT open failed for {}, retrying with buffered read: {}",
                    path, e
                ),
            }
        }
        LocalFile::with_read(path, off as u64).map(|file| (file, false))
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    pub fn pos(&self) -> i64 {
        self.file.pos()
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn remaining(&self) -> i64 {
        self.len - self.file.pos()
    }

    pub fn seek(&mut self, pos: i64) -> FsResult<i64> {
        Ok(self.file.as_mut().seek(pos)?)
    }

    // Length of the next chunk read: `min(chunk_size, remaining)`.
    fn read_len(&self) -> FsResult<usize> {
        let read_size = self.chunk_size.min(self.remaining() as usize);
        if read_size == 0 {
            return err_box!("No readable data");
        }
        Ok(read_size)
    }

    fn get_chunk(&mut self) -> FsResult<BytesMut> {
        let read_size = self.read_len()?;

        self.chunk.reserve(read_size);
        unsafe {
            self.chunk.set_len(read_size);
        }
        Ok(self.chunk.split())
    }

    pub async fn read(&mut self) -> FsResult<DataSlice> {
        if !self.direct {
            return self.read_buffered().await;
        }

        // Plan before any I/O: a planning error (e.g. nothing left to read)
        // is pre-flight and must not trigger the fallback.
        let plan = DirectReadPlan::new(self.pos(), self.read_len()?)?;
        match self.read_direct(plan).await {
            Ok(slice) => Ok(slice),
            Err(e) => {
                // A direct-mode failure must never surface: re-open buffered
                // at the failed request's offset and retry once; the buffered
                // path then decides success or the real error (e.g. short
                // read at EOF) with unchanged semantics.
                self.degrade_to_buffered(plan.off, &e)?;
                self.read_buffered().await
            }
        }
    }

    pub fn blocking_read(&mut self) -> FsResult<DataSlice> {
        if !self.direct {
            return self.blocking_read_buffered();
        }

        let plan = DirectReadPlan::new(self.pos(), self.read_len()?)?;
        match Self::direct_read_window(self.file.as_mut(), &plan) {
            Ok(slice) => Ok(slice),
            Err(e) => {
                self.degrade_to_buffered(plan.off, &e)?;
                self.blocking_read_buffered()
            }
        }
    }

    async fn read_buffered(&mut self) -> FsResult<DataSlice> {
        let mut chunk = self.get_chunk()?;
        let file = self.file.clone();

        // Perform read-out.
        self.last_task = file
            .as_mut()
            .read_ahead(&self.os_cache, self.last_task.take());

        let chunk = self
            .rt
            .spawn_blocking(move || {
                file.as_mut().read_all(&mut chunk)?;
                Ok::<BytesMut, FsError>(chunk)
            })
            .await??;
        Ok(DataSlice::buffer(chunk))
    }

    fn blocking_read_buffered(&mut self) -> FsResult<DataSlice> {
        let mut chunk = self.get_chunk()?;
        self.last_task = self
            .file
            .as_mut()
            .read_ahead(&self.os_cache, self.last_task.take());
        self.file.as_mut().read_all(&mut chunk)?;
        Ok(DataSlice::buffer(chunk))
    }

    // Direct-mode read of one planned window. `os_cache.read_ahead` is
    // skipped: fadvise is meaningless for O_DIRECT.
    async fn read_direct(&self, plan: DirectReadPlan) -> FsResult<DataSlice> {
        let file = self.file.clone();
        self.rt
            .spawn_blocking(move || Self::direct_read_window(file.as_mut(), &plan))
            .await?
    }

    // Aligned pread of the planned region, cropped to the requested window
    // and returned zero-copy. Free-standing (no `self`) so the slicing
    // contract is unit-testable.
    fn direct_read_window(file: &mut LocalFile, plan: &DirectReadPlan) -> FsResult<DataSlice> {
        let mut buf = AlignedBuf::alloc(plan.region_len);
        let raw = file.pread_aligned(&mut buf, plan.off, plan.len)?;
        let (start, end) = plan.crop_bounds(raw)?;
        Ok(DataSlice::bytes(buf.into_bytes().slice(start..end)))
    }

    // Permanently degrade this reader to buffered mode after a direct-mode
    // failure: warn once (the flag flip makes it once per reader) and re-open
    // the block file without O_DIRECT at the failed request's offset, so the
    // caller can retry the same request. The re-open error, if any, is the
    // only real error this path surfaces.
    fn degrade_to_buffered(&mut self, off: i64, cause: &FsError) -> FsResult<()> {
        let path = self.file.path().to_owned();
        warn!(
            "direct read of block {} failed ({}), falling back to buffered read of {} at {}",
            self.block.id, cause, path, off
        );
        Self::reopen_buffered(&mut self.file, &path, off)?;
        self.direct = false;
        Ok(())
    }

    // Replace `slot` with a buffered handle re-opened at `off`. On failure
    // the old handle is left in place. Free-standing for unit tests.
    fn reopen_buffered(slot: &mut RawPtr<LocalFile>, path: &str, off: i64) -> FsResult<()> {
        let file = LocalFile::with_read(path, off as u64)?;
        *slot = RawPtr::from_owned(file);
        Ok(())
    }

    // Reading is completed and the server needs to be notified.
    pub async fn complete(&mut self) -> FsResult<()> {
        let next_seq_id = self.next_seq_id();
        self.client
            .read_commit(&self.block, self.req_id, next_seq_id)
            .await?;
        Ok(())
    }

    pub fn block_id(&self) -> i64 {
        self.block.id
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }
}

#[cfg(test)]
mod tests {
    use super::{adopt_opened_session, AdoptedRead, ReadSessionCleanup};
    use curvine_error::{FsError, FsResult};
    use curvine_model::{ExtendedBlock, FileType, StorageType};

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[derive(Debug)]
    struct FakeClient {
        id: u64,
        finish_called: Arc<AtomicBool>,
        prevent_pool_reuse_called: Arc<AtomicBool>,
        fail_finish: bool,
    }

    impl FakeClient {
        fn new(id: u64) -> Self {
            Self {
                id,
                finish_called: Arc::new(AtomicBool::new(false)),
                prevent_pool_reuse_called: Arc::new(AtomicBool::new(false)),
                fail_finish: false,
            }
        }
    }

    impl ReadSessionCleanup for FakeClient {
        async fn finish_read(
            &mut self,
            _block: &ExtendedBlock,
            _req_id: i64,
            _seq_id: i32,
        ) -> FsResult<()> {
            self.finish_called.store(true, Ordering::SeqCst);
            if self.fail_finish {
                Err(FsError::common("injected read_commit failure"))
            } else {
                Ok(())
            }
        }

        fn prevent_pool_reuse(&mut self) {
            self.prevent_pool_reuse_called.store(true, Ordering::SeqCst);
        }
    }

    fn test_block() -> ExtendedBlock {
        ExtendedBlock::new(1, 4096, StorageType::Disk, FileType::File)
    }

    #[tokio::test]
    async fn missing_short_circuit_path_adopts_the_opened_session() {
        let opened = adopt_opened_session(
            FakeClient::new(42),
            None,
            &test_block(),
            100,
            0,
            |_| -> Result<(), &'static str> { panic!("local open must not be attempted") },
            |client| client,
        )
        .await
        .unwrap();

        match opened {
            AdoptedRead::Remote(client) => {
                assert_eq!(client.id, 42);
                assert!(!client.finish_called.load(Ordering::SeqCst));
                assert!(!client.prevent_pool_reuse_called.load(Ordering::SeqCst));
            }
            AdoptedRead::Local { .. } => panic!("missing path must adopt remote mode"),
        }
    }

    #[tokio::test]
    async fn local_open_failure_finishes_session_and_prevents_pool_reuse() {
        let mut client = FakeClient::new(42);
        client.fail_finish = true;
        let finish_called = client.finish_called.clone();
        let prevent_pool_reuse_called = client.prevent_pool_reuse_called.clone();
        let result = adopt_opened_session(
            client,
            Some("/missing/block".to_string()),
            &test_block(),
            100,
            0,
            |_| -> Result<(), &'static str> { Err("injected local open failure") },
            |client| client,
        )
        .await;

        assert!(matches!(result, Err("injected local open failure")));
        assert!(finish_called.load(Ordering::SeqCst));
        assert!(prevent_pool_reuse_called.load(Ordering::SeqCst));
    }

    // ---- direct-read (O_DIRECT short-circuit) tests ----

    use super::{BlockReaderLocal, DirectReadPlan};
    use curvine_io::{DataSlice, LocalFile, DIRECT_IO_ALIGN};
    use curvine_sys::RawPtr;
    use std::fs::{remove_file, File};
    use std::io::Write;

    fn test_path() -> String {
        curvine_runtime::common::Utils::test_file()
    }

    /// Deterministic byte pattern: `pattern[i] = i % 251`.
    fn write_pattern_file(path: &str, len: usize) -> Vec<u8> {
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let mut file = File::create(path).unwrap();
        file.write_all(&data).unwrap();
        file.sync_all().unwrap();
        data
    }

    #[test]
    fn direct_read_plan_aligned_window() {
        let plan = DirectReadPlan::new(0, 4096).unwrap();
        assert_eq!(plan.region_start, 0);
        assert_eq!(plan.region_len, 4096);
        assert_eq!(plan.delta, 0);

        // Multi-page aligned window: no padding waste.
        let plan = DirectReadPlan::new(3 * 4096, 8 * 4096).unwrap();
        assert_eq!(plan.region_start, 3 * 4096);
        assert_eq!(plan.region_len, 8 * 4096);
        assert_eq!(plan.delta, 0);
    }

    #[test]
    fn direct_read_plan_unaligned_window_expands_to_covering_region() {
        // Unaligned start, unaligned end.
        let plan = DirectReadPlan::new(100, 5000).unwrap();
        assert_eq!(plan.region_start, 0);
        assert_eq!(plan.region_len, 2 * DIRECT_IO_ALIGN);
        assert_eq!(plan.delta, 100);

        // Window crossing a single page boundary.
        let plan = DirectReadPlan::new(4095, 2).unwrap();
        assert_eq!(plan.region_start, 0);
        assert_eq!(plan.region_len, 2 * DIRECT_IO_ALIGN);
        assert_eq!(plan.delta, 4095);

        // End exactly on a page boundary: no trailing padding page.
        let plan = DirectReadPlan::new(100, 4096 - 100).unwrap();
        assert_eq!(plan.region_start, 0);
        assert_eq!(plan.region_len, DIRECT_IO_ALIGN);
        assert_eq!(plan.delta, 100);
    }

    #[test]
    fn direct_read_plan_rejects_invalid_windows() {
        assert!(DirectReadPlan::new(-1, 4096).is_err());
        assert!(DirectReadPlan::new(0, 0).is_err());
        assert!(DirectReadPlan::new(i64::MAX - 10, 4096).is_err());
        assert!(DirectReadPlan::new(0, usize::MAX).is_err());
    }

    #[test]
    fn direct_read_plan_crop_bounds_trims_raw_count_to_window() {
        let plan = DirectReadPlan::new(100, 5000).unwrap();

        // Full region filled: expose exactly the requested window.
        assert_eq!(plan.crop_bounds(2 * DIRECT_IO_ALIGN).unwrap(), (100, 5100));

        // EOF exactly at the requested end: the raw count covers the window.
        assert_eq!(plan.crop_bounds(5100).unwrap(), (100, 5100));

        // EOF mid-window (raw short of delta + len): never expose unfilled
        // bytes.
        assert!(plan.crop_bounds(5099).is_err());

        // EOF before the window even starts (raw < delta): no underflow.
        assert!(plan.crop_bounds(50).is_err());
    }

    #[test]
    fn open_local_with_direct_disabled_opens_buffered() {
        let path = test_path();
        write_pattern_file(&path, 8192);

        let (mut file, direct) = BlockReaderLocal::open_local(&path, 0, false).unwrap();
        assert!(!direct);
        let mut buf = vec![0u8; 16];
        file.read_all(&mut buf).unwrap();
        let expected: Vec<u8> = (0..16).map(|i| (i % 251) as u8).collect();
        assert_eq!(buf, expected);

        remove_file(&path).unwrap();
    }

    #[test]
    fn open_local_with_direct_enabled_never_fails_the_read() {
        let path = test_path();
        let data = write_pattern_file(&path, 3 * DIRECT_IO_ALIGN);

        // On Linux this opens O_DIRECT; elsewhere with_read_direct reports
        // unsupported and open_local silently falls back to buffered.
        let (mut file, direct) = BlockReaderLocal::open_local(&path, 0, true).unwrap();
        #[cfg(not(target_os = "linux"))]
        assert!(!direct, "non-Linux must fall back to buffered");
        #[cfg(target_os = "linux")]
        let _ = direct; // filesystem-dependent: either mode must read correctly

        // Whichever mode was chosen, an aligned window read is byte-correct.
        let plan = DirectReadPlan::new(DIRECT_IO_ALIGN as i64, DIRECT_IO_ALIGN).unwrap();
        let slice = BlockReaderLocal::direct_read_window(&mut file, &plan).unwrap();
        assert_eq!(
            slice.as_slice(),
            &data[DIRECT_IO_ALIGN..2 * DIRECT_IO_ALIGN]
        );

        remove_file(&path).unwrap();
    }

    #[test]
    fn open_local_propagates_real_open_errors_in_both_modes() {
        for direct in [false, true] {
            let res = BlockReaderLocal::open_local("/definitely/missing/dir/block", 0, direct);
            assert!(res.is_err(), "missing file must error (direct={direct})");
        }
    }

    #[test]
    fn direct_read_window_matches_buffered_read_across_chunks() {
        let path = test_path();
        // Unaligned total length exercises the tail short region.
        let file_len = 2 * DIRECT_IO_ALIGN + 700;
        let data = write_pattern_file(&path, file_len);

        // Buffered reference: sequential read_all from an unaligned offset.
        let start = 100i64;
        let chunk_size = DIRECT_IO_ALIGN; // small chunks force multiple windows
        let end = data.len() as i64;
        let mut reference = Vec::new();
        {
            let mut file = LocalFile::with_read(&path, start as u64).unwrap();
            let mut buf = vec![0u8; chunk_size];
            while file.pos() < end {
                let n = chunk_size.min((end - file.pos()) as usize);
                file.read_all(&mut buf[..n]).unwrap();
                reference.extend_from_slice(&buf[..n]);
            }
        }

        // Direct windows over the same range (on non-Linux the fd is a plain
        // buffered one; the slicing contract under test is unchanged).
        let mut file = LocalFile::with_read(&path, start as u64).unwrap();
        let mut actual = Vec::new();
        let mut first = true;
        while file.pos() < end {
            let len = chunk_size.min((end - file.pos()) as usize);
            let plan = DirectReadPlan::new(file.pos(), len).unwrap();
            let slice = BlockReaderLocal::direct_read_window(&mut file, &plan).unwrap();
            if first {
                // The direct path returns the zero-copy Bytes variant.
                assert!(matches!(slice, DataSlice::Bytes(_)));
                first = false;
            }
            assert_eq!(slice.len(), len);
            actual.extend_from_slice(slice.as_slice());
        }

        assert_eq!(actual, reference);
        assert_eq!(actual, &data[start as usize..]);

        remove_file(&path).unwrap();
    }

    #[test]
    fn direct_read_short_at_eof_degrades_to_buffered_semantics() {
        let path = test_path();
        // The file holds 6000 bytes; the read window [4096, 8192) overruns EOF.
        let data = write_pattern_file(&path, 6000);

        let mut file = LocalFile::with_read(&path, 0).unwrap();
        let plan = DirectReadPlan::new(4096, 4096).unwrap();
        // The raw aligned read comes up short (1904 bytes), so the window is
        // not fully covered: error instead of exposing unfilled bytes.
        assert!(BlockReaderLocal::direct_read_window(&mut file, &plan).is_err());

        // The reader's remedy: re-open buffered at the failed request's
        // offset and retry, which reproduces buffered behavior exactly.
        let mut slot = RawPtr::from_owned(file);
        BlockReaderLocal::reopen_buffered(&mut slot, &path, plan.off).unwrap();
        assert_eq!(slot.pos(), 4096);

        // A full-length retry fails exactly like a pure buffered reader
        // (read_exact hits EOF): no fabricated bytes, no fake success.
        let mut buf = vec![0u8; 4096];
        assert!(slot.as_mut().read_all(&mut buf).is_err());
        let mut buffered = LocalFile::with_read(&path, 4096).unwrap();
        assert!(buffered.read_all(&mut vec![0u8; 4096]).is_err());

        // The real tail remains readable after a fresh degrade.
        let mut slot = RawPtr::from_owned(LocalFile::with_read(&path, 0).unwrap());
        BlockReaderLocal::reopen_buffered(&mut slot, &path, plan.off).unwrap();
        let tail = data.len() - 4096;
        let mut buf = vec![0u8; tail];
        slot.as_mut().read_all(&mut buf).unwrap();
        assert_eq!(&buf[..], &data[4096..]);

        remove_file(&path).unwrap();
    }

    #[test]
    fn reopen_buffered_reopens_at_the_requested_offset() {
        let path = test_path();
        let data = write_pattern_file(&path, 2 * DIRECT_IO_ALIGN);

        let mut slot = RawPtr::from_owned(LocalFile::with_read(&path, 0).unwrap());
        let mut scratch = vec![0u8; 512];
        slot.as_mut().read_all(&mut scratch).unwrap();

        BlockReaderLocal::reopen_buffered(&mut slot, &path, 1234).unwrap();
        assert_eq!(slot.pos(), 1234);
        let mut buf = vec![0u8; 1000];
        slot.as_mut().read_all(&mut buf).unwrap();
        assert_eq!(&buf[..], &data[1234..2234]);

        // A failed re-open propagates and leaves the old handle in place.
        assert!(
            BlockReaderLocal::reopen_buffered(&mut slot, "/definitely/missing/dir/block", 0)
                .is_err()
        );
        assert_eq!(slot.pos(), 2234);
        let mut buf = vec![0u8; 8];
        slot.as_mut().read_all(&mut buf).unwrap();
        assert_eq!(&buf[..], &data[2234..2242]);

        remove_file(&path).unwrap();
    }
}
