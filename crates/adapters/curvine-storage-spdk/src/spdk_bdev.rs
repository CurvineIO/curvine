//! SPDK block device I/O — implements BlockIO for NVMe-oF/RDMA.
//! Mirrors LocalFile API but uses SPDK bdev layer instead of kernel I/O.
//!
//! # Alignment: all I/O aligned to block size.
//! # Threading: I/O forwarded to dedicated poller thread via channel.
//!
//! # TODOs
//! - Reliability: NVMe retry (exp backoff via SpdkConf::io_retry_count). Retriable: SCT=0x03,0x00 SC=0x02/0x0A.
//! - Memory: DMA buffer size configurable via SpdkConf::dma_buf_size.
//! - Perf: zero-copy read for large sequential reads.

#![cfg(feature = "spdk")]

use crate::spdk_env::RetiredDmaRegistry;
use bytes::BytesMut;
use curvine_core_error::err_box;
use curvine_io::BlockIO;
use curvine_io::DataSlice;
use curvine_io::IOResult;
use log::{debug, error, warn};
use nix::sys::eventfd::EventFd;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
// ---------------------------------------------------------------------------
// SPDK I/O channel — bridge between Tokio and SPDK reactor
// ---------------------------------------------------------------------------

/// SPDK I/O channel — wraps qpair and poller sender.
pub struct SpdkIoChannel {
    pub qpair: *mut crate::spdk_ffi::spdk_nvme_qpair,
    pub poller_tx: crossbeam::channel::Sender<crate::spdk_poller::IoRequest>,
    /// Eventfd for waking poller on new I/O
    pub eventfd: std::sync::Arc<nix::sys::eventfd::EventFd>,
    /// Flag: poller is idle and blocked on eventfd; only write eventfd when true
    pub poller_is_sleeping: std::sync::Arc<AtomicBool>,
    /// Flag: the poller detected this qpair died — future I/Os fail immediately.
    pub qpair_dead: std::sync::Arc<AtomicBool>,
}
unsafe impl Send for SpdkIoChannel {}
unsafe impl Sync for SpdkIoChannel {}

fn submit_flush_request(
    name: &str,
    ns: *mut crate::spdk_ffi::spdk_nvme_ns,
    qpair: *mut crate::spdk_ffi::spdk_nvme_qpair,
    poller_tx: &crossbeam::channel::Sender<crate::spdk_poller::IoRequest>,
    eventfd: &EventFd,
    poller_is_sleeping: &AtomicBool,
    inflight: &Arc<AtomicUsize>,
    completion: Arc<crate::spdk_poller::IoCompletion>,
) -> IOResult<()> {
    use crate::spdk_poller::{IoOp, IoRequest};

    inflight.fetch_add(1, Ordering::Release);
    let req = IoRequest {
        op: IoOp::Flush { ns, qpair },
        completion,
        bdev_inflight: inflight.clone(),
    };
    if poller_tx.send(req).is_err() {
        inflight.fetch_sub(1, Ordering::Release);
        return err_box!("SPDK poller thread is gone");
    }
    if poller_is_sleeping.load(Ordering::SeqCst) {
        if let Err(e) = eventfd.write(1) {
            warn!(
                "SpdkBdev '{}': failed to wake poller (eventfd write): {}. \
                 I/O may be delayed until timeout.",
                name, e
            );
        }
    }
    Ok(())
}

/// DMA buffer (hugepage-backed).
pub struct DmaBuf {
    ptr: *mut std::ffi::c_void,
    capacity: usize,
    block_size: u32,
}

// SAFETY: exclusively owned, not aliased
unsafe impl Send for DmaBuf {}
unsafe impl Sync for DmaBuf {}

impl DmaBuf {
    /// Allocate DMA buffer (aligned to block_size, hugepage-backed).
    pub fn alloc(size: usize, block_size: u32) -> IOResult<Self> {
        if block_size == 0 || !block_size.is_power_of_two() {
            return err_box!("block_size must be a power of two, got {}", block_size);
        }

        // Round up to block alignment
        let aligned_size = Self::align_up(size, block_size as usize);
        let ptr = unsafe {
            crate::spdk_ffi::curvine_spdk_dma_malloc(aligned_size as u64, block_size as u64)
        };
        if ptr.is_null() {
            return err_box!(
                "curvine_spdk_dma_malloc failed: size={}, align={}",
                aligned_size,
                block_size
            );
        }
        Ok(Self {
            ptr,
            capacity: aligned_size,
            block_size,
        })
    }

    #[cfg(test)]
    pub(crate) fn null_for_test(capacity: usize, block_size: u32) -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            capacity,
            block_size,
        }
    }

    /// Round n up to nearest multiple of align.
    #[inline]
    pub fn align_up(n: usize, align: usize) -> usize {
        (n + align - 1) & !(align - 1)
    }

    /// Raw pointer for FFI.
    #[inline]
    pub fn as_ptr(&self) -> *mut std::ffi::c_void {
        self.ptr
    }

    /// Usable slice.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.capacity) }
    }

    /// Usable mutable slice.
    #[inline]
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut u8, self.capacity) }
    }

    /// Allocated capacity (aligned to block_size).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Block size.
    #[inline]
    pub fn block_size(&self) -> u32 {
        self.block_size
    }
}

impl Drop for DmaBuf {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { crate::spdk_ffi::curvine_spdk_dma_free(self.ptr) }
        }
    }
}

/// Allocate a DMA buffer, reclaiming already-retired (safe-to-free) DMA
/// buffers before the attempt, and once more on failure before retrying.
fn alloc_dma_buf_with_reclaim<F>(
    env: &crate::spdk_env::SpdkEnv,
    buf_size: usize,
    block_size: u32,
    alloc: F,
) -> IOResult<DmaBuf>
where
    F: Fn(usize, u32) -> IOResult<DmaBuf>,
{
    env.reclaim_retired_dma();
    match alloc(buf_size, block_size) {
        Ok(buf) => Ok(buf),
        Err(_) => {
            env.reclaim_retired_dma();
            alloc(buf_size, block_size)
        }
    }
}

/// Opened SPDK block device. Analogous to LocalFile.
/// Tracks position and size; I/O via SpdkIoChannel.
pub struct SpdkBdev {
    /// Bdev name (e.g., "Nvme0n1" or "NvmeOF0n1").
    name: String,

    /// Total device size in bytes.
    size: i64,
    /// Block size.
    block_size: u32,
    /// Current cursor position.
    pos: i64,
    /// Whether opened for writing.
    writable: bool,
    /// Read buffer (avoids repeated allocation). Only `None` after
    /// `defer_dma_buffers()` moves ownership during drop.
    read_buf: Option<DmaBuf>,
    /// Write buffer (avoids repeated allocation). Only `None` after
    /// `defer_dma_buffers()` moves ownership during drop.
    write_buf: Option<DmaBuf>,
    /// Raw namespace pointer.
    ns: *mut crate::spdk_ffi::spdk_nvme_ns,
    /// I/O channel for this bdev.
    io_channel: SpdkIoChannel,
    /// Raw controller pointer (for qpair pool on drop).
    ctrlr: *mut crate::spdk_ffi::spdk_nvme_ctrlr,
    /// I/O timeout in milliseconds. 0 = no timeout.
    io_timeout_ms: u64,
    inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    retired_dma: Arc<RetiredDmaRegistry>,
    /// Set on write/flush `ETIMEDOUT` (unconfirmed completion); sticky like `qpair_dead`.
    uncertain_writes: AtomicBool,
    /// Block identity for terminal-event reporting; 0 means "unknown/not set"
    /// (e.g. read-only handles), in which case no event is ever emitted.
    block_id: i64,
    /// Block's full physical extent, independent of this handle's offset/len.
    block_bdev_offset: i64,
    block_extent_size: i64,
    /// Sender for late-arriving terminal completions; cloned from the global
    /// `SpdkEnv` at open time so timeout-watcher threads don't need a lookup.
    terminal_tx: crossbeam::channel::Sender<crate::spdk_env::SpdkCommandTerminalEvent>,
}

// SAFETY: owns qpair and DMA buffers exclusively
unsafe impl Send for SpdkBdev {}
unsafe impl Sync for SpdkBdev {}
impl SpdkBdev {
    // mirror LocalFile::with_read/with_write

    pub fn open_read(name: &str, offset: u64, max_len: i64) -> IOResult<Self> {
        Self::open(name, offset as i64, false, max_len)
    }

    pub fn open_write(name: &str, offset: i64, max_len: i64) -> IOResult<Self> {
        Self::open(name, offset, true, max_len)
    }

    fn open(name: &str, offset: i64, writable: bool, max_len: i64) -> IOResult<Self> {
        // Look up bdev metadata from SpdkEnv
        {
            use crate::spdk_env::SpdkEnv;
            use crate::spdk_ffi;
            let env = SpdkEnv::global_or_err()?;
            // Register handle so shutdown knows we're alive
            env.acquire_handle()?;
            let info = match env.get_bdev(name) {
                Some(v) => v,
                None => {
                    env.release_handle();
                    return err_box!("bdev '{}' not found in SpdkEnv", name);
                }
            };

            let bdev_size = info.size_bytes as i64;
            let block_size = info.block_size;

            // Cap size to caller's allocated range
            let size = if max_len > 0 {
                let end = offset.saturating_add(max_len);
                end.min(bdev_size)
            } else {
                bdev_size
            };
            if offset < 0 || offset > size {
                env.release_handle();
                return err_box!(
                    "offset {} out of range for bdev '{}' (size={})",
                    offset,
                    name,
                    size
                );
            }

            // DMA buffers (1MB each) reused across I/O — larger I/Os chunked
            let buf_size = DmaBuf::align_up(1024 * 1024, block_size as usize);
            let read_buf =
                match alloc_dma_buf_with_reclaim(env, buf_size, block_size, DmaBuf::alloc) {
                    Ok(buf) => buf,
                    Err(e) => {
                        env.release_handle();
                        return Err(e);
                    }
                };
            let write_buf =
                match alloc_dma_buf_with_reclaim(env, buf_size, block_size, DmaBuf::alloc) {
                    Ok(buf) => buf,
                    Err(e) => {
                        env.release_handle();
                        return Err(e);
                    }
                };

            // Recover raw SPDK pointers from BdevInfo
            let ctrlr = info.ctrlr as *mut spdk_ffi::spdk_nvme_ctrlr;
            let ns = info.ns as *mut spdk_ffi::spdk_nvme_ns;
            // Acquire a qpair from the pool (reuses a cached one or allocates new).
            let qpair = match env.acquire_qpair(ctrlr) {
                Ok(qp) => qp,
                Err(e) => {
                    env.release_handle();
                    return Err(e.into());
                }
            };
            let poller_tx = env.poller_sender(); // Get sender from SpdkEnv
            let eventfd = env.poller_eventfd(); // Get eventfd for wake signaling
            let poller_is_sleeping = env.poller_is_sleeping(); // Skip eventfd if active
            let retired_dma = env.retired_dma_registry();
            let qpair_dead = std::sync::Arc::new(AtomicBool::new(false));
            let io_channel = SpdkIoChannel {
                qpair,
                poller_tx,
                eventfd,
                poller_is_sleeping,
                qpair_dead: qpair_dead.clone(),
            };
            let io_timeout_ms = info.io_timeout_ms;
            Ok(Self {
                name: name.to_string(),
                size,
                block_size,
                pos: offset,
                writable,
                io_channel,
                read_buf: Some(read_buf),
                write_buf: Some(write_buf),
                ns,
                ctrlr,
                io_timeout_ms,
                inflight: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                retired_dma,
                uncertain_writes: AtomicBool::new(false),
                block_id: 0,
                block_bdev_offset: 0,
                block_extent_size: 0,
                terminal_tx: env.terminal_event_sender(),
            })
        }
    }

    /// Associate this handle with the block it belongs to, so a late
    /// terminal completion (after a write/flush timeout) can be reported
    /// with enough identity to match a `QuarantineReleaseProof`. Only
    /// writers created for a quarantine-eligible block need to call this;
    /// handles without identity (`block_id == 0`) never emit terminal events.
    pub fn set_block_identity(&mut self, block_id: i64, bdev_offset: i64, extent_size: i64) {
        self.block_id = block_id;
        self.block_bdev_offset = bdev_offset;
        self.block_extent_size = extent_size;
    }

    /// Spawn a thread that blocks on the completion until it is truly
    /// resolved (by the real SPDK callback or a later force-complete), then
    /// reports a `SpdkCommandTerminalEvent`. Only called after a write/flush
    /// `ETIMEDOUT`, and only if this handle has known block identity.
    ///
    /// This thread can in principle live forever if SPDK never produces a
    /// terminal completion for the qpair (e.g. hung controller with no
    /// reset) — see the design doc's documented limitation. It is not
    /// cancelled early because there is no safe way to abandon an in-flight
    /// DMA buffer without a real terminal signal.
    fn spawn_terminal_watcher(&self, completion: Arc<crate::spdk_poller::IoCompletion>) {
        if self.block_id == 0 {
            return;
        }
        let block_id = self.block_id;
        let bdev_offset = self.block_bdev_offset;
        let size = self.block_extent_size;
        let tx = self.terminal_tx.clone();
        std::thread::spawn(move || {
            let status = completion.wait(0);
            let _ = tx.send(crate::spdk_env::SpdkCommandTerminalEvent {
                block_id,
                bdev_offset,
                size,
                success: status == 0,
            });
        });
    }

    /// Device block size in bytes.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Whether this handle is opened for writing.
    pub fn is_writable(&self) -> bool {
        self.writable
    }

    /// Device name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Align offset down to block boundary.
    #[inline]
    fn align_down(&self, n: i64) -> i64 {
        let bs = self.block_size as i64;
        n & !(bs - 1)
    }

    /// Decode NVMe error from C callback.
    fn nvme_error_str(rc: i32) -> String {
        if rc >= 0 {
            return "success".to_string();
        }
        let code = (-rc) as u32;
        let sct = code / 256;
        let sc = code % 256;
        let sct_name = match sct {
            0 => "Generic",
            1 => "CmdSpecific",
            2 => "Media",
            3 => "Path",
            7 => "Vendor",
            _ => "Unknown",
        };
        format!(
            "NVMe error: SCT=0x{:02x}({}), SC=0x{:02x}",
            sct, sct_name, sc
        )
    }

    /// Check that the current position + len doesn't exceed device size.
    fn check_bounds(&self, len: i64) -> IOResult<()> {
        if self.pos + len > self.size {
            return err_box!(
                "I/O range [{}, {}) exceeds bdev '{}' size {}",
                self.pos,
                self.pos + len,
                self.name,
                self.size
            );
        }
        Ok(())
    }

    /// Submit read to SPDK, wait for completion. Uses DMA buffer (large reads chunked).
    fn spdk_read(&mut self, offset: i64, len: usize) -> IOResult<BytesMut> {
        if self.io_channel.qpair_dead.load(Ordering::Acquire) {
            return err_box!("SPDK qpair is dead, device unreachable");
        }

        let read_buf = self
            .read_buf
            .as_mut()
            .expect("SPDK read buffer missing before drop");
        let buf_cap = read_buf.capacity();
        let dma_buf = read_buf.as_ptr();

        let bs = self.block_size as usize;
        let mut result = BytesMut::with_capacity(len);
        let mut remaining = len;
        let mut cur_off = offset;
        while remaining > 0 {
            // Align down, compute head skip
            let aligned_off = self.align_down(cur_off);
            let head_skip = (cur_off - aligned_off) as usize;
            // Usable payload = buf_cap - head_skip
            debug_assert!(buf_cap > head_skip, "DMA buffer too small");
            let chunk_data = remaining.min(buf_cap - head_skip);
            let aligned_len = DmaBuf::align_up(chunk_data + head_skip, bs);

            // Submit NVMe read
            use crate::spdk_poller::{IoCompletion, IoOp, IoRequest};
            let completion = IoCompletion::new();
            self.inflight
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            let req = IoRequest {
                op: IoOp::Read {
                    ns: self.ns,
                    qpair: self.io_channel.qpair,
                    buf: dma_buf,
                    offset: aligned_off as u64,
                    num_bytes: aligned_len as u64,
                },
                completion: completion.clone(),
                bdev_inflight: self.inflight.clone(),
            };
            if self.io_channel.poller_tx.send(req).is_err() {
                self.inflight
                    .fetch_sub(1, std::sync::atomic::Ordering::Release);
                return err_box!("SPDK poller thread is gone");
            }
            if self.io_channel.poller_is_sleeping.load(Ordering::SeqCst) {
                if let Err(e) = self.io_channel.eventfd.write(1) {
                    warn!(
                        "SpdkBdev '{}': failed to wake poller (eventfd write): {}. \
                         I/O may be delayed until timeout.",
                        self.name, e
                    );
                }
            }
            let rc = completion.wait(self.io_timeout_ms * 1000);
            if rc != 0 {
                if rc == -libc::EIO || rc == -libc::ETIMEDOUT {
                    self.io_channel.qpair_dead.store(true, Ordering::Release);
                }
                return err_box!(
                    "NVMe read failed on '{}' at offset={}: {}",
                    self.name,
                    aligned_off,
                    Self::nvme_error_str(rc)
                );
            }
            // Copy to heap (DMA buffer reused)
            let slice = unsafe {
                std::slice::from_raw_parts((dma_buf as *const u8).add(head_skip), chunk_data)
            };
            result.extend_from_slice(slice);
            cur_off += chunk_data as i64;
            remaining -= chunk_data;
        }
        Ok(result)
    }

    /// Submit write to SPDK, wait for completion. Uses DMA buffer (large writes chunked).
    fn spdk_write(&mut self, offset: i64, data: &[u8]) -> IOResult<()> {
        if self.io_channel.qpair_dead.load(Ordering::Acquire) {
            return err_box!("SPDK qpair is dead, device unreachable");
        }

        use crate::spdk_poller::{IoCompletion, IoOp, IoRequest};

        if !self.writable {
            return err_box!("bdev '{}' not opened for writing", self.name);
        }

        let len = data.len();
        if len == 0 {
            return Ok(());
        }
        let write_buf = self
            .write_buf
            .as_mut()
            .expect("SPDK write buffer missing before drop");
        let buf_cap = write_buf.capacity();
        let dma_buf = write_buf.as_ptr();

        let bs = self.block_size as usize;
        let mut written = 0usize;
        let mut cur_off = offset;
        while written < len {
            // Align current offset down and compute head skip for this chunk
            let aligned_off = self.align_down(cur_off);
            let head_skip = (cur_off - aligned_off) as usize;
            // How much user data fits in this iteration?
            let chunk_data = (len - written).min(buf_cap - head_skip);
            let aligned_len = DmaBuf::align_up(chunk_data + head_skip, bs);
            // Read-modify-write if the chunk doesn't cover full aligned blocks
            if head_skip > 0 || (chunk_data + head_skip) < aligned_len {
                let completion = IoCompletion::new();
                self.inflight
                    .fetch_add(1, std::sync::atomic::Ordering::Release);
                let req = IoRequest {
                    op: IoOp::Read {
                        ns: self.ns,
                        qpair: self.io_channel.qpair,
                        buf: dma_buf,
                        offset: aligned_off as u64,
                        num_bytes: aligned_len as u64,
                    },
                    completion: completion.clone(),
                    bdev_inflight: self.inflight.clone(),
                };
                if self.io_channel.poller_tx.send(req).is_err() {
                    self.inflight
                        .fetch_sub(1, std::sync::atomic::Ordering::Release);
                    return err_box!("SPDK poller thread is gone");
                }
                if self.io_channel.poller_is_sleeping.load(Ordering::SeqCst) {
                    if let Err(e) = self.io_channel.eventfd.write(1) {
                        warn!(
                            "SpdkBdev '{}': failed to wake poller (eventfd write): {}. \
                             I/O may be delayed until timeout.",
                            self.name, e
                        );
                    }
                }
                let rc = completion.wait(self.io_timeout_ms * 1000);

                if rc != 0 {
                    if rc == -libc::EIO || rc == -libc::ETIMEDOUT {
                        self.io_channel.qpair_dead.store(true, Ordering::Release);
                    }
                    return err_box!(
                        "Read-modify-write: read failed on '{}' at offset={}: {}",
                        self.name,
                        aligned_off,
                        Self::nvme_error_str(rc)
                    );
                }
            }
            // Copy caller's data into the DMA buffer at the correct offset
            unsafe {
                std::ptr::copy_nonoverlapping(
                    data[written..].as_ptr(),
                    (dma_buf as *mut u8).add(head_skip),
                    chunk_data,
                );
            }
            // Submit synchronous NVMe write
            let completion = IoCompletion::new();
            self.inflight
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            let req = IoRequest {
                op: IoOp::Write {
                    ns: self.ns,
                    qpair: self.io_channel.qpair,
                    buf: dma_buf,
                    offset: aligned_off as u64,
                    num_bytes: aligned_len as u64,
                },
                completion: completion.clone(),
                bdev_inflight: self.inflight.clone(),
            };
            if self.io_channel.poller_tx.send(req).is_err() {
                self.inflight
                    .fetch_sub(1, std::sync::atomic::Ordering::Release);
                return err_box!("SPDK poller thread is gone");
            }
            if self.io_channel.poller_is_sleeping.load(Ordering::SeqCst) {
                if let Err(e) = self.io_channel.eventfd.write(1) {
                    warn!(
                        "SpdkBdev '{}': failed to wake poller (eventfd write): {}. \
                         I/O may be delayed until timeout.",
                        self.name, e
                    );
                }
            }
            let rc = completion.wait(self.io_timeout_ms * 1000);
            if rc != 0 {
                if rc == -libc::EIO || rc == -libc::ETIMEDOUT {
                    self.io_channel.qpair_dead.store(true, Ordering::Release);
                }
                if rc == -libc::ETIMEDOUT {
                    // Write may still complete later; not a confirmed failure.
                    self.uncertain_writes.store(true, Ordering::Release);
                    self.spawn_terminal_watcher(completion.clone());
                }
                return err_box!(
                    "NVMe write failed on '{}' at offset={}, len={}: {}",
                    self.name,
                    aligned_off,
                    aligned_len,
                    Self::nvme_error_str(rc)
                );
            }
            cur_off += chunk_data as i64;
            written += chunk_data;
        }

        Ok(())
    }

    fn spdk_flush(&self) -> IOResult<()> {
        if self.io_channel.qpair_dead.load(Ordering::Acquire) {
            return err_box!("SPDK qpair is dead, device unreachable");
        }

        use crate::spdk_poller::IoCompletion;
        let completion = IoCompletion::new();
        submit_flush_request(
            &self.name,
            self.ns,
            self.io_channel.qpair,
            &self.io_channel.poller_tx,
            &self.io_channel.eventfd,
            &self.io_channel.poller_is_sleeping,
            &self.inflight,
            completion.clone(),
        )?;
        let rc = completion.wait(self.io_timeout_ms * 1000);
        if rc != 0 {
            if rc == -libc::EIO || rc == -libc::ETIMEDOUT {
                self.io_channel.qpair_dead.store(true, Ordering::Release);
            }
            if rc == -libc::ETIMEDOUT {
                // Flush may still complete later; not a confirmed failure.
                self.uncertain_writes.store(true, Ordering::Release);
                self.spawn_terminal_watcher(completion.clone());
            }
            return err_box!(
                "NVMe flush failed on '{}': {}",
                self.name,
                Self::nvme_error_str(rc)
            );
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// BlockIO implementation — the core trait
// ---------------------------------------------------------------------------

impl BlockIO for SpdkBdev {
    /// Read up to len bytes from current position. Returns DataSlice::Buffer.
    fn read_region(&mut self, _enable_send_file: bool, len: i32) -> IOResult<DataSlice> {
        let chunk = (len as i64).min(self.size - self.pos);
        if chunk <= 0 {
            return err_box!(
                "offset exceeds bdev length, length={}, offset={}",
                self.size,
                self.pos
            );
        }

        let buf = self.spdk_read(self.pos, chunk as usize)?;
        self.pos += chunk;
        Ok(DataSlice::Buffer(buf))
    }

    /// Write DataSlice at current position. Handles all except IOSlice (fd not for SPDK).
    fn write_region(&mut self, region: &DataSlice) -> IOResult<()> {
        let data: &[u8] = match region {
            DataSlice::Empty => return Ok(()),

            DataSlice::Buffer(bytes) => bytes,

            DataSlice::IOSlice(_) => {
                return err_box!(
                    "DataSlice::IOSlice not supported for SPDK bdev '{}'.
                     IOSlice wraps a kernel fd which cannot be used with userspace SPDK.",
                    self.name
                );
            }

            DataSlice::MemSlice(bytes) => bytes.as_slice(),

            DataSlice::Bytes(bytes) => bytes,
        };

        if data.is_empty() {
            return Ok(());
        }

        self.check_bounds(data.len() as i64)?;
        self.spdk_write(self.pos, data)?;
        self.pos += data.len() as i64;
        Ok(())
    }

    /// Write all bytes from buf at current position.
    fn write_all(&mut self, buf: &[u8]) -> IOResult<()> {
        if buf.is_empty() {
            return Ok(());
        }

        self.check_bounds(buf.len() as i64)?;
        self.spdk_write(self.pos, buf)?;
        self.pos += buf.len() as i64;
        Ok(())
    }

    /// Read exactly buf.len() bytes into buf.
    fn read_all(&mut self, buf: &mut [u8]) -> IOResult<()> {
        if buf.is_empty() {
            return Ok(());
        }

        self.check_bounds(buf.len() as i64)?;
        let data = self.spdk_read(self.pos, buf.len())?;
        buf.copy_from_slice(&data);
        self.pos += buf.len() as i64;
        Ok(())
    }

    /// Flush writes to NVMe device.
    fn flush(&mut self) -> IOResult<()> {
        self.spdk_flush()
    }

    fn has_uncertain_writes(&self) -> bool {
        self.uncertain_writes.load(Ordering::Acquire)
    }

    /// Seek to absolute offset.
    fn seek(&mut self, pos: i64) -> IOResult<i64> {
        if pos == self.pos {
            return Ok(pos);
        }

        if pos < 0 || pos > self.size {
            return err_box!(
                "seek position {} out of range for bdev '{}' (size={})",
                pos,
                self.name,
                self.size
            );
        }

        self.pos = pos;
        Ok(self.pos)
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn len(&self) -> i64 {
        self.size
    }

    fn path(&self) -> &str {
        &self.name
    }

    /// Resize not supported — NVMe namespaces have fixed capacity.
    fn resize(&mut self, _truncate: bool, _off: i64, _len: i64, _mode: i32) -> IOResult<()> {
        err_box!(
            "resize not supported for SPDK bdev '{}'.
             NVMe namespaces have fixed capacity ({})",
            self.name,
            self.size
        )
    }
}

impl Display for SpdkBdev {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SpdkBdev({}, size={}, block_size={}, pos={}, writable={})",
            self.name, self.size, self.block_size, self.pos, self.writable
        )
    }
}

/// Outcome of waiting for in-flight I/O to drain during `SpdkBdev::drop()`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DrainOutcome {
    /// `inflight` reached 0 before the deadline.
    Completed,
    /// Deadline reached with `inflight` still non-zero; caller must defer the
    /// DMA buffers (`defer_dma_buffers`) instead of dropping them, since a
    /// late completion may still write into them.
    TimedOut,
}

/// Wait for `inflight` to reach 0, retrying deferred qpair cleanup on every
/// iteration so a transient free failure isn't stuck until an unrelated retry.
/// Poll interval backs off (doubling, capped) to avoid hammering on a stuck free.
pub(crate) fn wait_for_inflight_drain(
    inflight: &std::sync::atomic::AtomicUsize,
    env: Option<&crate::spdk_env::SpdkEnv>,
    deadline: std::time::Instant,
    name: &str,
    initial_poll_interval: std::time::Duration,
    max_poll_interval: std::time::Duration,
) -> DrainOutcome {
    let mut logged = false;
    let mut poll_interval = initial_poll_interval;
    loop {
        let count = inflight.load(std::sync::atomic::Ordering::Acquire);
        if count == 0 {
            return DrainOutcome::Completed;
        }
        if std::time::Instant::now() >= deadline {
            error!(
                "SpdkBdev '{}': {} in-flight I/O(s) still pending after deadline. \
                 Deferring DMA buffers until in-flight I/O drains.",
                name, count
            );
            return DrainOutcome::TimedOut;
        }
        if !logged {
            warn!(
                "SpdkBdev '{}': waiting for {} in-flight I/O(s) to complete before drop",
                name, count
            );
            logged = true;
        }
        if let Some(env) = env {
            env.resolve_deferred_qpairs();
        }
        std::thread::sleep(poll_interval);
        poll_interval = next_poll_interval(poll_interval, max_poll_interval);
    }
}

/// Doubles `current`, capped at `max`. Pulled out of the drain loop so the
/// backoff schedule itself is directly unit-testable.
fn next_poll_interval(
    current: std::time::Duration,
    max: std::time::Duration,
) -> std::time::Duration {
    current.saturating_mul(2).min(max)
}

impl Drop for SpdkBdev {
    fn drop(&mut self) {
        // Retire the qpair before waiting on `inflight` below. Retirement
        // reclaims stale callback contexts, which is what releases DMA refs
        // after force-completion; if this ran after the wait loop, the loop
        // could never observe inflight == 0 for a force-completed qpair.
        let env = crate::spdk_env::SpdkEnv::global_including_shutdown();
        if let Some(env) = env {
            env.retire_qpair(self.ctrlr, self.io_channel.qpair);
        } else {
            // Unreachable: every live SpdkBdev is constructed via open(), which requires
            // SpdkEnv::global_or_err() to succeed first, and SPDK_ENV is never cleared.
            // Without an env there is no safe force-complete/stale-reclaim path, so leak
            // the qpair instead of freeing it while SPDK may still reference it.
            debug_assert!(
                false,
                "SpdkBdev dropped with no global SpdkEnv; invariant violated"
            );
            error!(
                "SpdkBdev '{}': no global SpdkEnv during drop; leaking qpair {:p} \
                 because there is no safe teardown path without an env",
                self.name, self.io_channel.qpair
            );
        }

        // Wait for in-flight I/O.
        let max_wait = if self.io_timeout_ms > 0 {
            std::time::Duration::from_millis(self.io_timeout_ms * 2)
        } else {
            std::time::Duration::from_secs(60)
        };
        let deadline = std::time::Instant::now() + max_wait;
        let outcome = wait_for_inflight_drain(
            &self.inflight,
            env,
            deadline,
            &self.name,
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(100),
        );
        self.handle_drain_outcome(outcome);

        if let Some(env) = env {
            env.release_handle();
        }

        debug!(
            "SpdkBdev '{}' closed (qpair retired, DMA buffers released or leaked)",
            self.name
        );
    }
}

impl SpdkBdev {
    /// Apply the result of waiting for in-flight I/O to drain during drop:
    /// on timeout, defer DMA buffers into the retired registry instead of
    /// dropping them, since a late completion may still write into them.
    fn handle_drain_outcome(&mut self, outcome: DrainOutcome) {
        if outcome == DrainOutcome::TimedOut {
            self.defer_dma_buffers();
        }
    }

    fn defer_dma_buffers(&mut self) {
        self.retired_dma.defer(
            self.name.clone(),
            self.read_buf.take(),
            self.write_buf.take(),
            self.inflight.clone(),
        );
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::spdk_env::RetiredDmaRegistry;
    use crate::spdk_env::SpdkEnv;
    use crate::spdk_poller::IoCompletion;
    use curvine_io::SpdkConf;
    use nix::sys::eventfd::EfdFlags;
    use std::mem::ManuallyDrop;

    fn ensure_spdk_init() {
        if SpdkEnv::global().is_some() {
            return;
        }
        let mut conf = SpdkConf::default();
        conf.enabled = true;
        conf.app_name = "curvine-test".to_string();
        let hugepage_mb = std::env::var("SPDK_HUGEPAGE_MB").unwrap_or_else(|_| "64".to_string());
        conf.hugepage_str = format!("{}MB", hugepage_mb);
        conf.reactor_mask = "0x1".to_string();
        let traddr = std::env::var("SPDK_TARGET_ADDR").unwrap_or_else(|_| "127.0.0.1".into());
        let trsvcid = std::env::var("SPDK_TARGET_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4420);
        let subnqn = std::env::var("SPDK_TARGET_NQN")
            .unwrap_or_else(|_| "nqn.2024-01.io.curvine:test".into());
        let trtype = std::env::var("SPDK_TRANSPORT_TYPE").unwrap_or_else(|_| "tcp".into());
        conf.iova_mode = std::env::var("SPDK_IOVA_MODE").unwrap_or_else(|_| "va".to_string());
        conf.targets = vec![curvine_io::NvmeTarget {
            traddr,
            trsvcid,
            subnqn,
            trtype,
            adrfam: "ipv4".to_string(),
            ..Default::default()
        }];
        SpdkEnv::init_global(conf).expect("SPDK init for tests");
    }

    #[test]
    fn next_poll_interval_doubles_then_caps() {
        let max = std::time::Duration::from_millis(100);
        let mut interval = std::time::Duration::from_millis(1);
        let expected_ms = [2, 4, 8, 16, 32, 64, 100, 100, 100];
        for expected in expected_ms {
            interval = next_poll_interval(interval, max);
            assert_eq!(interval, std::time::Duration::from_millis(expected));
        }
    }

    #[test]
    fn flush_send_failure_decrements_inflight() {
        let (tx, rx) = crossbeam::channel::unbounded();
        drop(rx);

        let inflight = Arc::new(AtomicUsize::new(0));
        let poller_is_sleeping = AtomicBool::new(false);
        let eventfd = EventFd::from_value_and_flags(0, EfdFlags::EFD_NONBLOCK).unwrap();

        let err = submit_flush_request(
            "test-bdev",
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &tx,
            &eventfd,
            &poller_is_sleeping,
            &inflight,
            IoCompletion::new(),
        )
        .expect_err("flush enqueue should fail when poller receiver is disconnected");

        assert!(
            err.to_string().contains("SPDK poller thread is gone"),
            "unexpected error: {}",
            err
        );
        assert_eq!(inflight.load(Ordering::Acquire), 0);
    }

    /// Builds a fake `SpdkBdev` (no real FFI pointers) for testing the DMA
    /// buffer drain/defer wiring in isolation.
    fn fake_bdev_for_drain_test(
        inflight: Arc<AtomicUsize>,
        retired_dma: Arc<RetiredDmaRegistry>,
    ) -> ManuallyDrop<SpdkBdev> {
        let (tx, _rx) = crossbeam::channel::unbounded();
        let eventfd = Arc::new(EventFd::from_value_and_flags(0, EfdFlags::EFD_NONBLOCK).unwrap());
        ManuallyDrop::new(SpdkBdev {
            name: "test-bdev".to_string(),
            size: 4096,
            block_size: 4096,
            pos: 0,
            writable: true,
            read_buf: Some(DmaBuf::null_for_test(4096, 4096)),
            write_buf: Some(DmaBuf::null_for_test(4096, 4096)),
            ns: std::ptr::null_mut(),
            io_channel: SpdkIoChannel {
                qpair: std::ptr::null_mut(),
                poller_tx: tx,
                eventfd,
                poller_is_sleeping: Arc::new(AtomicBool::new(false)),
                qpair_dead: Arc::new(AtomicBool::new(false)),
            },
            ctrlr: std::ptr::null_mut(),
            io_timeout_ms: 1,
            inflight,
            retired_dma,
            uncertain_writes: AtomicBool::new(false),
            block_id: 0,
            block_bdev_offset: 0,
            block_extent_size: 0,
            terminal_tx: crossbeam::channel::unbounded().0,
        })
    }

    #[test]
    fn defer_dma_buffers_moves_bdev_buffers_to_retired_registry() {
        let retired_dma = RetiredDmaRegistry::new();
        let inflight = Arc::new(AtomicUsize::new(1));
        let mut bdev = fake_bdev_for_drain_test(inflight.clone(), retired_dma.clone());

        bdev.defer_dma_buffers();

        assert_eq!(retired_dma.count(), 1);
        assert!(bdev.read_buf.is_none());
        assert!(bdev.write_buf.is_none());
        assert_eq!(retired_dma.reclaim(), 0);
        inflight.store(0, Ordering::Release);
        assert_eq!(retired_dma.reclaim(), 1);
        assert_eq!(retired_dma.count(), 0);
    }

    /// Guards `Drop::drop()`'s wiring: calls `handle_drain_outcome` directly, so
    /// this fails if TimedOut ever stops deferring the DMA buffers.
    #[test]
    fn handle_drain_outcome_defers_on_timeout() {
        let retired_dma = RetiredDmaRegistry::new();
        let inflight = Arc::new(AtomicUsize::new(1));
        let mut bdev = fake_bdev_for_drain_test(inflight.clone(), retired_dma.clone());

        bdev.handle_drain_outcome(DrainOutcome::TimedOut);

        assert_eq!(retired_dma.count(), 1);
        assert!(bdev.read_buf.is_none());
        assert!(bdev.write_buf.is_none());
        assert_eq!(retired_dma.reclaim(), 0);
        inflight.store(0, Ordering::Release);
        assert_eq!(retired_dma.reclaim(), 1);
        assert_eq!(retired_dma.count(), 0);
    }

    /// Complementary guard: on `Completed`, buffers must NOT be deferred —
    /// catches a regression that defers unconditionally regardless of outcome.
    #[test]
    fn handle_drain_outcome_keeps_buffers_on_completed() {
        let retired_dma = RetiredDmaRegistry::new();
        let inflight = Arc::new(AtomicUsize::new(0));
        let mut bdev = fake_bdev_for_drain_test(inflight, retired_dma.clone());

        bdev.handle_drain_outcome(DrainOutcome::Completed);

        assert_eq!(retired_dma.count(), 0);
        assert!(bdev.read_buf.is_some());
        assert!(bdev.write_buf.is_some());
    }

    /// A reclaimable retired entry must be released before
    /// the alloc attempt (proactive reclaim), and a failed attempt must
    /// reclaim once more and retry before giving up.
    #[test]
    fn alloc_dma_buf_with_reclaim_releases_reclaimable_entry_before_retry() {
        ensure_spdk_init();
        let env = SpdkEnv::new(SpdkConf::default()).unwrap();

        // Seed one reclaimable retired entry (inflight already 0).
        let stale_read = DmaBuf::alloc(512, 512).unwrap();
        let stale_write = DmaBuf::alloc(512, 512).unwrap();
        env.retired_dma_registry().defer(
            "stale-bdev".to_string(),
            Some(stale_read),
            Some(stale_write),
            Arc::new(AtomicUsize::new(0)),
        );
        assert_eq!(env.retired_dma_registry().count(), 1);

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_hook = calls.clone();
        let registry = env.retired_dma_registry();
        let result = alloc_dma_buf_with_reclaim(&env, 512, 512, move |size, block_size| {
            if calls_for_hook.fetch_add(1, Ordering::AcqRel) == 0 {
                // Proactive reclaim must have already run before this first attempt.
                assert_eq!(
                    registry.count(),
                    0,
                    "reclaimable entry must be released first"
                );
                err_box!("simulated alloc failure")
            } else {
                DmaBuf::alloc(size, block_size)
            }
        });

        assert!(result.is_ok());
        assert_eq!(
            calls.load(Ordering::Acquire),
            2,
            "expected one retry after reclaim"
        );
        assert_eq!(env.retired_dma_registry().count(), 0);
    }

    /// Proves `open()` routes DMA allocs through `alloc_dma_buf_with_reclaim`.
    #[test]
    fn open_reclaims_retired_dma_before_allocating_buffers() {
        ensure_spdk_init();
        let env = SpdkEnv::global().expect("SPDK env must be initialized");
        let bdev_name = env
            .bdev_names()
            .first()
            .cloned()
            .expect("at least one bdev must be discovered");

        let calls_before = env.dma_reclaim_call_count();

        let bdev = SpdkBdev::open_read(&bdev_name, 0, 0).expect("open should succeed");

        // Expect 4: 2 from alloc_dma_buf_with_reclaim() + 2 from acquire_qpair().
        assert_eq!(
            env.dma_reclaim_call_count() - calls_before,
            4,
            "open() must call reclaim_retired_dma via alloc_dma_buf_with_reclaim \
             for both DMA buffers, not just via acquire_qpair()"
        );

        drop(bdev);
    }

    #[test]
    fn dma_buf_alloc_aligned() {
        ensure_spdk_init();
        let buf = DmaBuf::alloc(1000, 512).unwrap();
        // 1000 rounded up to 512 alignment = 1024
        assert_eq!(buf.capacity(), 1024);
        assert_eq!(buf.block_size(), 512);
    }
    #[test]
    fn dma_buf_as_slice_roundtrip() {
        ensure_spdk_init();
        let mut buf = DmaBuf::alloc(4096, 4096).unwrap();
        assert_eq!(buf.capacity(), 4096);
        // Write a pattern and read it back
        buf.as_slice_mut()[0] = 0xAB;
        buf.as_slice_mut()[4095] = 0xCD;
        assert_eq!(buf.as_slice()[0], 0xAB);
        assert_eq!(buf.as_slice()[4095], 0xCD);
    }
    #[test]
    fn dma_buf_fixed_capacity() {
        ensure_spdk_init();
        let buf = DmaBuf::alloc(4096, 512).unwrap();
        // Buffer is fixed-size — capacity and pointer never change
        assert_eq!(buf.capacity(), 4096);
        assert!(!buf.as_ptr().is_null());
    }
    #[test]
    fn dma_buf_invalid_block_size() {
        // block_size = 0
        assert!(DmaBuf::alloc(1024, 0).is_err());
        // block_size not power of two
        assert!(DmaBuf::alloc(1024, 3).is_err());
    }
    #[test]
    fn dma_buf_drop_does_not_leak() {
        ensure_spdk_init();
        let buf = DmaBuf::alloc(1024 * 1024, 4096).unwrap();
        drop(buf);
    }

    /// Completion always times out (channel never drained). Caller MUST
    /// `mem::forget` it -- real `Drop` would free the dummy qpair via FFI.
    /// Leaks 2 DmaBufs (harmless: one-shot, OS reclaims on process exit).
    fn test_bdev_with_fake_channel(
        writable: bool,
    ) -> (
        SpdkBdev,
        crossbeam::channel::Receiver<crate::spdk_poller::IoRequest>,
    ) {
        ensure_spdk_init();
        let block_size = 512u32;
        let buf_size = block_size as usize; // one block is enough for every I/O these tests issue
        let read_buf = DmaBuf::alloc(buf_size, block_size).unwrap();
        let write_buf = DmaBuf::alloc(buf_size, block_size).unwrap();
        let (tx, rx) = crossbeam::channel::unbounded();
        let bdev = SpdkBdev {
            name: "test-dead-qpair-bdev".to_string(),
            size: 16 * 1024 * 1024,
            block_size,
            pos: 0,
            writable,
            read_buf: Some(read_buf),
            write_buf: Some(write_buf),
            ns: std::ptr::null_mut(),
            io_channel: SpdkIoChannel {
                qpair: 0x1234usize as *mut crate::spdk_ffi::spdk_nvme_qpair,
                poller_tx: tx,
                eventfd: std::sync::Arc::new(
                    EventFd::from_value_and_flags(0, EfdFlags::EFD_NONBLOCK).unwrap(),
                ),
                poller_is_sleeping: std::sync::Arc::new(AtomicBool::new(false)),
                qpair_dead: std::sync::Arc::new(AtomicBool::new(false)),
            },
            ctrlr: 0x5678usize as *mut crate::spdk_ffi::spdk_nvme_ctrlr,
            io_timeout_ms: 1,
            inflight: Arc::new(AtomicUsize::new(0)),
            retired_dma: RetiredDmaRegistry::new(),
            uncertain_writes: AtomicBool::new(false),
            block_id: 0,
            block_bdev_offset: 0,
            block_extent_size: 0,
            terminal_tx: crossbeam::channel::unbounded().0,
        };
        (bdev, rx)
    }

    #[test]
    fn spdk_read_timeout_marks_qpair_dead_and_blocks_further_submits() {
        let (mut bdev, rx) = test_bdev_with_fake_channel(true);

        let err = bdev.spdk_read(0, 512).expect_err("read should time out");
        assert!(err.to_string().contains("NVMe read failed"), "{}", err);
        assert!(bdev.io_channel.qpair_dead.load(Ordering::Acquire));
        assert!(
            rx.try_recv().is_ok(),
            "the timed-out read should have submitted once"
        );
        assert!(rx.try_recv().is_err(), "only one submit expected");

        // Every subsequent op must short-circuit on the dead flag, with no
        // further submit to the channel.
        let err = bdev
            .spdk_read(0, 512)
            .expect_err("read should short-circuit");
        assert!(err.to_string().contains("qpair is dead"), "{}", err);
        assert!(rx.try_recv().is_err(), "dead read must not submit");

        let err = bdev
            .spdk_write(0, &[0u8; 512])
            .expect_err("write should short-circuit");
        assert!(err.to_string().contains("qpair is dead"), "{}", err);
        assert!(rx.try_recv().is_err(), "dead write must not submit");

        let err = bdev.spdk_flush().expect_err("flush should short-circuit");
        assert!(err.to_string().contains("qpair is dead"), "{}", err);
        assert!(rx.try_recv().is_err(), "dead flush must not submit");

        // Read timeout must not mark writes as uncertain.
        assert!(!bdev.has_uncertain_writes());

        std::mem::forget(bdev);
    }

    #[test]
    fn spdk_write_aligned_timeout_marks_qpair_dead() {
        let (mut bdev, rx) = test_bdev_with_fake_channel(true);

        // Block-aligned offset/len: no read-modify-write sub-request.
        let err = bdev
            .spdk_write(0, &[0u8; 512])
            .expect_err("write should time out");
        assert!(err.to_string().contains("NVMe write failed"), "{}", err);
        assert!(bdev.io_channel.qpair_dead.load(Ordering::Acquire));
        assert!(bdev.has_uncertain_writes(), "timed-out write is uncertain");
        assert!(
            rx.try_recv().is_ok(),
            "the timed-out write should have submitted once"
        );
        assert!(rx.try_recv().is_err(), "only one submit expected, no RMW");

        std::mem::forget(bdev);
    }

    #[test]
    fn spdk_write_rmw_timeout_marks_qpair_dead() {
        let (mut bdev, rx) = test_bdev_with_fake_channel(true);

        // Unaligned offset forces the read-modify-write branch; its own read
        // times out before the write is ever submitted.
        let err = bdev
            .spdk_write(1, &[0u8; 8])
            .expect_err("RMW read should time out");
        assert!(err.to_string().contains("Read-modify-write"), "{}", err);
        assert!(bdev.io_channel.qpair_dead.load(Ordering::Acquire));
        // RMW pre-read timeout means the write was never submitted.
        assert!(!bdev.has_uncertain_writes());
        assert!(
            rx.try_recv().is_ok(),
            "the RMW read should have submitted once"
        );
        assert!(
            rx.try_recv().is_err(),
            "write must never be submitted once the RMW read fails"
        );

        std::mem::forget(bdev);
    }

    #[test]
    fn spdk_flush_timeout_marks_qpair_dead() {
        let (bdev, rx) = test_bdev_with_fake_channel(true);

        let err = bdev.spdk_flush().expect_err("flush should time out");
        assert!(err.to_string().contains("NVMe flush failed"), "{}", err);
        assert!(bdev.io_channel.qpair_dead.load(Ordering::Acquire));
        assert!(bdev.has_uncertain_writes(), "timed-out flush is uncertain");
        assert!(
            rx.try_recv().is_ok(),
            "the timed-out flush should have submitted once"
        );
        assert!(rx.try_recv().is_err(), "only one submit expected");

        std::mem::forget(bdev);
    }

    /// Control: confirmed EIO (not a timeout) must not mark uncertain.
    #[test]
    fn spdk_write_eio_does_not_mark_uncertain() {
        let (mut bdev, rx) = test_bdev_with_fake_channel(true);

        let responder = std::thread::spawn(move || {
            let req = rx.recv().expect("write should submit exactly once");
            assert!(req.completion.complete(-libc::EIO));
            rx
        });

        let err = bdev
            .spdk_write(0, &[0u8; 512])
            .expect_err("write should fail with a confirmed EIO");
        assert!(err.to_string().contains("NVMe write failed"), "{}", err);
        assert!(bdev.io_channel.qpair_dead.load(Ordering::Acquire));
        assert!(
            !bdev.has_uncertain_writes(),
            "confirmed EIO is not uncertain"
        );

        let rx = responder.join().expect("responder thread must not panic");
        assert!(rx.try_recv().is_err(), "only one submit expected");

        std::mem::forget(bdev);
    }

    /// A timed-out write with known block identity spawns a watcher thread
    /// that reports a `SpdkCommandTerminalEvent` once the real completion
    /// (simulated here via `IoCompletion::complete`) finally arrives.
    #[test]
    fn spdk_write_timeout_terminal_watcher_reports_late_completion() {
        let (mut bdev, rx) = test_bdev_with_fake_channel(true);
        bdev.set_block_identity(42, 1000, 4096);
        let (term_tx, term_rx) = crossbeam::channel::unbounded();
        bdev.terminal_tx = term_tx;

        let err = bdev
            .spdk_write(0, &[0u8; 512])
            .expect_err("write should time out");
        assert!(err.to_string().contains("NVMe write failed"), "{}", err);

        // Simulate the real SPDK callback finally firing, after the waiter
        // already gave up with ETIMEDOUT.
        let req = rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("write should have submitted exactly once");
        assert!(req.completion.complete(0), "first completion call must win");

        let event = term_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("terminal event must be reported once the real completion arrives");
        assert_eq!(event.block_id, 42);
        assert_eq!(event.bdev_offset, 1000);
        assert_eq!(event.size, 4096);
        assert!(event.success);

        std::mem::forget(bdev);
    }

    /// A handle with no block identity (block_id == 0, e.g. a reader) must
    /// never spawn a watcher or emit a terminal event.
    #[test]
    fn spdk_write_timeout_without_identity_emits_no_terminal_event() {
        let (mut bdev, rx) = test_bdev_with_fake_channel(true);
        let (term_tx, term_rx) = crossbeam::channel::unbounded();
        bdev.terminal_tx = term_tx;

        let err = bdev
            .spdk_write(0, &[0u8; 512])
            .expect_err("write should time out");
        assert!(err.to_string().contains("NVMe write failed"), "{}", err);

        let req = rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("write should have submitted exactly once");
        assert!(req.completion.complete(0));

        assert!(
            term_rx
                .recv_timeout(std::time::Duration::from_millis(200))
                .is_err(),
            "no identity set must mean no terminal event"
        );

        std::mem::forget(bdev);
    }
}
