#![cfg(feature = "spdk")]

//! SPDK I/O poller thread - handles NVMe submit/poll on dedicated thread.
use crate::io::spdk_ffi;
/// Qpairs not thread-safe: submit + poll must on same thread.
/// Single poller to demonstrate the correctness work.
/// Uses eventfd for instant wake on new I/O submission.
/// TODO: shard to multiple pollers (one per controller).
///
/// ## Disconnect Detection
/// Detected via periodic keep-alive poll every 1s while idle (~1s latency).
/// TODO: SPDK fabric eventfd for immediate detection.
use log::{error, info, warn};
use nix::sys::eventfd::{EfdFlags, EventFd};
use std::collections::HashMap;
use std::ffi::c_void;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const EVENTSZ: usize = std::mem::size_of::<u64>();
/// I/O operation submitted to the poller thread.
pub enum IoOp {
    Read {
        ns: *mut spdk_ffi::spdk_nvme_ns,
        qpair: *mut spdk_ffi::spdk_nvme_qpair,
        buf: *mut c_void,
        offset: u64,
        num_bytes: u64,
    },
    Write {
        ns: *mut spdk_ffi::spdk_nvme_ns,
        qpair: *mut spdk_ffi::spdk_nvme_qpair,
        buf: *mut c_void,
        offset: u64,
        num_bytes: u64,
    },
    Flush {
        ns: *mut spdk_ffi::spdk_nvme_ns,
        qpair: *mut spdk_ffi::spdk_nvme_qpair,
    },
    /// Unregister a qpair from the poller before it is freed.
    UnregisterQpair {
        qpair: *mut spdk_ffi::spdk_nvme_qpair,
        ack: mpsc::Sender<()>,
    },
}

// SAFETY: exclusive ownership - blocks until completion.
unsafe impl Send for IoOp {}

/// Completion state shared between poller callback and waiting handler.
pub struct IoCompletion {
    inner: Mutex<IoCompletionInner>,
    cond: Condvar,
}

struct IoCompletionInner {
    done: bool,
    status: i32,
}

impl IoCompletion {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(IoCompletionInner {
                done: false,
                status: 0,
            }),
            cond: Condvar::new(),
        })
    }

    /// Called by C callback on completion.
    /// Returns true if this call actually completed the I/O (first call wins).
    pub fn complete(&self, status: i32) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.done {
            return false;
        }
        inner.done = true;
        inner.status = status;
        self.cond.notify_one();
        true
    }

    /// Block until complete or timeout. Returns NVMe status.
    pub fn wait(&self, timeout_us: u64) -> i32 {
        let mut inner = self.inner.lock().unwrap();
        if timeout_us == 0 {
            while !inner.done {
                inner = self.cond.wait(inner).unwrap();
            }
        } else {
            let timeout = Duration::from_micros(timeout_us);
            let deadline = Instant::now() + timeout;
            while !inner.done {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return -libc::ETIMEDOUT;
                }
                let (guard, result) = self.cond.wait_timeout(inner, remaining).unwrap();
                inner = guard;
                if result.timed_out() && !inner.done {
                    return -libc::ETIMEDOUT;
                }
            }
        }
        inner.status
    }
}

/// Request sent from handler threads to the poller.
pub struct IoRequest {
    pub op: IoOp,
    pub completion: Arc<IoCompletion>,
    /// Per-bdev in-flight counter. Decremented on completion.
    pub bdev_inflight: std::sync::Arc<AtomicUsize>,
    /// Per-qpair dead flag. Set by the poller when qpair poll fails;
    /// checked by the bdev to fail subsequent I/Os fast.
    pub qpair_dead: std::sync::Arc<AtomicBool>,
}

// SAFETY: exclusive ownership - blocks until completion.
unsafe impl Send for IoRequest {}

/// Poller states
enum PollerState {
    /// Active processing I/O - try_recv loop
    Active,
    /// Idle, blocked on eventfd waiting for work
    Idle,
}

/// SPDK NVMe controller handle — thread-safe.
#[repr(transparent)]
pub struct CtrlHandle(pub *mut spdk_ffi::spdk_nvme_ctrlr);

// SAFETY: opaque SPDK handle; admin completion is thread-safe.
unsafe impl Send for CtrlHandle {}

/// Configuration for the poller thread.
pub struct PollerConfig {
    pub poll_interval_ms: u64,
    pub spin_iter: u32,
    pub io_queue_depth: usize,
    pub ctrlrs: Vec<CtrlHandle>,
}

/// Poller thread handle.
pub struct SpdkPoller {
    /// Channel sender for I/O submissions
    tx: Option<crossbeam::channel::Sender<IoRequest>>,
    /// Eventfd for instant wake signaling
    eventfd: Arc<EventFd>,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    /// Whether poller is blocked on eventfd (idle). Bdevs check this to
    /// skip eventfd write syscall when poller is already active.
    is_sleeping: Arc<AtomicBool>,
}

impl SpdkPoller {
    /// Spawn a new poller thread with the given config.
    pub fn start(config: PollerConfig) -> Self {
        let (tx, rx) = crossbeam::channel::unbounded::<IoRequest>();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        let is_sleeping = Arc::new(AtomicBool::new(false));
        let is_sleeping_clone = is_sleeping.clone();

        // Create eventfd for wake signaling
        let eventfd = EventFd::from_value_and_flags(0, EfdFlags::EFD_NONBLOCK)
            .expect("Failed to create eventfd");
        let eventfd_raw = eventfd.as_raw_fd();
        let eventfd_arc = Arc::new(eventfd);

        let handle = std::thread::Builder::new()
            .name("spdk-poller".to_string())
            .spawn(move || {
                Self::poller_loop(rx, shutdown_clone, is_sleeping_clone, eventfd_raw, config);
            })
            .expect("Failed to spawn SPDK poller thread");

        Self {
            tx: Some(tx),
            eventfd: eventfd_arc,
            shutdown,
            handle: Some(handle),
            is_sleeping,
        }
    }

    /// Get sender for SpdkBdev to hold.
    pub fn sender(&self) -> crossbeam::channel::Sender<IoRequest> {
        self.tx.as_ref().expect("Poller stopped").clone()
    }

    /// Get eventfd for signaling new I/O
    pub fn eventfd(&self) -> RawFd {
        self.eventfd.as_raw_fd()
    }

    /// Get eventfd as Arc for sharing with multiple bdevs
    pub fn eventfd_arc(&self) -> Arc<EventFd> {
        self.eventfd.clone()
    }

    /// Get is_sleeping flag for bdevs to check before eventfd write
    pub fn is_sleeping_arc(&self) -> Arc<AtomicBool> {
        self.is_sleeping.clone()
    }

    /// Unregister qpair from poller, blocking until removed to prevent UAF.
    /// Returns false if poller didn't ack within timeout (likely stuck/dead).
    pub fn unregister_qpair(&self, qpair: *mut spdk_ffi::spdk_nvme_qpair) -> bool {
        let (ack_tx, ack_rx) = mpsc::channel::<()>();
        // TODO: use dedicated control channel instead of IoRequest to avoid dummy allocation (negligible cost)
        let req = IoRequest {
            op: IoOp::UnregisterQpair { qpair, ack: ack_tx },
            completion: IoCompletion::new(),
            bdev_inflight: Arc::new(AtomicUsize::new(0)),
            qpair_dead: Arc::new(AtomicBool::new(false)),
        };
        if let Some(tx) = &self.tx {
            let _ = tx.send(req);
            let _ = self.eventfd.write(1);
            match ack_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(()) => true,
                Err(_) => {
                    error!("Unregister timeout: poller may be stuck, qpair not removed");
                    false
                }
            }
        } else {
            false // Poller stopped
        }
    }

    /// Shut down the poller thread.
    pub fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        let _ = self.eventfd.write(1); // Wake poll(eventfd, timeout) so shutdown is observed promptly
        self.tx.take(); // Drop sender to disconnect the channel during shutdown
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    /// Main poller loop. Runs on dedicated thread.
    fn poller_loop(
        rx: crossbeam::channel::Receiver<IoRequest>,
        shutdown: Arc<AtomicBool>,
        is_sleeping: Arc<AtomicBool>,
        eventfd: RawFd,
        mut config: PollerConfig,
    ) {
        let mut active_qpairs: Vec<*mut spdk_ffi::spdk_nvme_qpair> = Vec::new();
        let active_ctrlrs: Vec<CtrlHandle> = std::mem::take(&mut config.ctrlrs);
        let mut state = PollerState::Idle;
        // Tracks per-qpair state (dead flag + pending Vec) for force-completion.
        let mut dead_qpairs: HashMap<usize, Box<QpairState>> = HashMap::new();

        // Verify curvine_async_ctx buffer fits the C struct.
        debug_assert!(
            unsafe { spdk_ffi::curvine_spdk_async_ctx_sizeof() }
                <= std::mem::size_of::<spdk_ffi::curvine_async_ctx>(),
            "curvine_async_ctx C struct exceeds Rust buffer"
        );

        // Poll interval for keep-alive check (parameterized to detect disconnects)
        let poll_interval = config.poll_interval_ms as i32;
        loop {
            // Check shutdown first
            if shutdown.load(Ordering::Acquire) && rx.is_empty() && active_qpairs.is_empty() {
                break;
            }

            // Active state: drain all pending I/Os and poll completions
            if matches!(state, PollerState::Active) {
                // Drain pending requests (non-blocking) - yield to admin completions every ~128 ops
                // to prevent keep-alive timeout during heavy I/O bursts.
                let delta = Duration::from_millis(config.poll_interval_ms);
                let mut deadline = Instant::now() + delta;
                let mut drain_count: u64 = 0;
                while let Ok(req) = rx.try_recv() {
                    if matches!(req.op, IoOp::UnregisterQpair { .. }) {
                        Self::handle_unregister(&req, &mut active_qpairs, &mut dead_qpairs);
                    } else {
                        Self::submit_one(
                            &req,
                            &mut active_qpairs,
                            &mut dead_qpairs,
                            config.io_queue_depth,
                        );
                    }
                    drain_count += 1;
                    if drain_count & 0x7F == 0 && Instant::now() >= deadline {
                        Self::process_admin_completions(&active_ctrlrs);
                        deadline = Instant::now() + delta;
                    }
                }

                // Poll qpairs for completions and detect failures
                let mut failed_qpairs: Vec<usize> = Vec::new();
                active_qpairs.retain(|qpair| {
                    let rc = unsafe { spdk_ffi::curvine_spdk_qpair_poll(*qpair, 0) };
                    if rc < 0 {
                        error!("qpair {:p} poll error: rc={}", qpair, rc);
                        failed_qpairs.push(*qpair as usize);
                        return false;
                    }
                    true
                });

                // Force-complete stranded I/Os on failed qpairs
                for key in &failed_qpairs {
                    Self::force_complete_qpair(*key, &mut dead_qpairs);
                }
                if !failed_qpairs.is_empty() {
                    error!(
                        "{} qpair(s) failed, removed from active set",
                        failed_qpairs.len()
                    );
                }

                // Process admin completions (keep-alive)
                Self::process_admin_completions(&active_ctrlrs);

                // Spin briefly before Idle to avoid eventfd round-trip for back-to-back I/O
                if rx.is_empty() && active_qpairs.is_empty() {
                    state = PollerState::Idle;
                    for _ in 0..config.spin_iter {
                        std::hint::spin_loop();
                        if !rx.is_empty() {
                            state = PollerState::Active;
                            break;
                        }
                    }
                }
                continue;
            }

            // Idle state: wait for work (eventfd or channel)
            if matches!(state, PollerState::Idle) {
                // Check if channel has pending data (peek)
                let has_pending = !rx.is_empty();

                if has_pending {
                    // Channel has data, transition to Active
                    state = PollerState::Active;
                    continue;
                }

                // Mark as sleeping before blocking — bdevs will write eventfd to wake us
                is_sleeping.store(true, Ordering::SeqCst);

                // Recheck channel after setting flag (closes race with bdev send)
                if !rx.is_empty() {
                    is_sleeping.store(false, Ordering::Release);
                    state = PollerState::Active;
                    continue;
                }

                // Wait on eventfd with timeout for keep-alive check
                let mut eventfd_pollfd = libc::pollfd {
                    fd: eventfd,
                    events: libc::POLLIN,
                    revents: 0,
                };

                let result = unsafe { libc::poll(&mut eventfd_pollfd, 1, poll_interval) };

                match result {
                    n if n > 0 => {
                        // Woken up — clear sleeping flag
                        is_sleeping.store(false, Ordering::Release);

                        // Eventfd signaled - drain it
                        let mut buf = [0u8; EVENTSZ];
                        let _ = unsafe {
                            libc::read(eventfd, buf.as_mut_ptr() as *mut c_void, EVENTSZ)
                        };

                        // Drain any pending channel data — yield to admin completions every ~128 ops.
                        let delta = Duration::from_millis(config.poll_interval_ms);
                        let mut deadline = Instant::now() + delta;
                        let mut drain_count: u64 = 0;
                        while let Ok(req) = rx.try_recv() {
                            if matches!(req.op, IoOp::UnregisterQpair { .. }) {
                                Self::handle_unregister(&req, &mut active_qpairs, &mut dead_qpairs);
                            } else {
                                Self::submit_one(
                                    &req,
                                    &mut active_qpairs,
                                    &mut dead_qpairs,
                                    config.io_queue_depth,
                                );
                            }
                            drain_count += 1;
                            if drain_count & 0x7F == 0 && Instant::now() >= deadline {
                                Self::process_admin_completions(&active_ctrlrs);
                                deadline = Instant::now() + delta;
                            }
                        }

                        // Transition to Active to process work
                        state = PollerState::Active;
                    }
                    0 => {
                        // Timeout - poll active qpairs to check connection health
                        let mut failed_qpairs: Vec<usize> = Vec::new();
                        active_qpairs.retain(|qpair| {
                            let rc = unsafe { spdk_ffi::curvine_spdk_qpair_poll(*qpair, 0) };
                            if rc < 0 {
                                error!("Poller: keep-alive poll error: rc={}, removing qpair", rc);
                                failed_qpairs.push(*qpair as usize);
                                return false;
                            }
                            true
                        });

                        // Force-complete stranded I/Os on failed qpairs
                        for key in &failed_qpairs {
                            Self::force_complete_qpair(*key, &mut dead_qpairs);
                        }
                        if !failed_qpairs.is_empty() {
                            error!(
                                "{} qpair(s) failed during keep-alive, removed from active set",
                                failed_qpairs.len()
                            );
                        }

                        // Process admin completions (keep-alive)
                        Self::process_admin_completions(&active_ctrlrs);

                        state = PollerState::Idle;
                    }
                    -1 => {
                        let errno = unsafe { *libc::__errno_location() };
                        if errno == libc::EINTR {
                            continue;
                        }
                        error!("Poller: poll error: {}", errno);
                        break;
                    }
                    _ => {
                        break;
                    }
                }
            }
        }

        info!("SPDK poller thread exiting");
    }

    /// Handle unregister request, remove qpair from active and dead_qpairs set and ack.
    fn handle_unregister(
        req: &IoRequest,
        active_qpairs: &mut Vec<*mut spdk_ffi::spdk_nvme_qpair>,
        dead_qpairs: &mut HashMap<usize, Box<QpairState>>,
    ) {
        if let IoOp::UnregisterQpair { qpair, ack } = &req.op {
            active_qpairs.retain(|&qp| qp != *qpair);
            let key = *qpair as usize;
            dead_qpairs.remove(&key);
            let _ = ack.send(());
        }
    }

    /// Submit a single I/O request on the poller thread.
    fn submit_one(
        req: &IoRequest,
        active_qpairs: &mut Vec<*mut spdk_ffi::spdk_nvme_qpair>,
        dead_qpairs: &mut HashMap<usize, Box<QpairState>>,
        io_queue_depth: usize,
    ) {
        let qpair = match &req.op {
            IoOp::Read { qpair, .. } => *qpair,
            IoOp::Write { qpair, .. } => *qpair,
            IoOp::Flush { qpair, .. } => *qpair,
            IoOp::UnregisterQpair { .. } => {
                unreachable!("UnregisterQpair handled by handle_unregister")
            }
        };

        let key = qpair as usize;

        // Register/retrieve QpairState on first sight of this qpair.
        let qs = dead_qpairs.entry(key).or_insert_with(|| {
            Box::new(QpairState {
                dead: req.qpair_dead.clone(),
                pending: Vec::with_capacity(io_queue_depth),
            })
        });

        // Fast-fail if this qpair is already known dead.
        if req.qpair_dead.load(Ordering::Acquire) {
            req.bdev_inflight.fetch_sub(1, Ordering::Release);
            req.completion.complete(-libc::ENXIO);
            return;
        }

        let pending_idx = qs.pending.len();
        let qs_ptr = &mut **qs as *mut QpairState;

        // Box::into_raw ensures CallbackCtx survives until poller_callback reclaims it
        let cb_ctx = Box::new(CallbackCtx {
            completion: req.completion.clone(),
            async_ctx: unsafe { std::mem::zeroed() },
            bdev_inflight: req.bdev_inflight.clone(),
            qpair_state: qs_ptr,
            pending_idx,
        });
        let cb_ctx_ptr = Box::into_raw(cb_ctx);

        // Initialize async_ctx via C helper
        unsafe {
            spdk_ffi::curvine_spdk_async_ctx_init(
                &mut (*cb_ctx_ptr).async_ctx,
                poller_callback,
                cb_ctx_ptr as *mut c_void,
            );
        }

        let rc = match &req.op {
            IoOp::Read {
                ns,
                qpair,
                buf,
                offset,
                num_bytes,
            } => unsafe {
                spdk_ffi::curvine_spdk_ns_submit_read(
                    *ns,
                    *qpair,
                    *buf,
                    *offset,
                    *num_bytes,
                    &mut (*cb_ctx_ptr).async_ctx,
                )
            },
            IoOp::Write {
                ns,
                qpair,
                buf,
                offset,
                num_bytes,
            } => unsafe {
                spdk_ffi::curvine_spdk_ns_submit_write(
                    *ns,
                    *qpair,
                    *buf,
                    *offset,
                    *num_bytes,
                    &mut (*cb_ctx_ptr).async_ctx,
                )
            },
            IoOp::Flush { ns, qpair } => unsafe {
                spdk_ffi::curvine_spdk_ns_submit_flush(*ns, *qpair, &mut (*cb_ctx_ptr).async_ctx)
            },
            IoOp::UnregisterQpair { .. } => {
                unreachable!("UnregisterQpair handled by handle_unregister")
            }
        };

        if rc != 0 {
            // Submission failed - reclaim allocation and complete with error
            unsafe { drop(Box::from_raw(cb_ctx_ptr)) };
            req.bdev_inflight.fetch_sub(1, Ordering::Release);
            req.completion.complete(rc);
            return;
        }

        qs.pending.push(cb_ctx_ptr);

        // Track active qpair
        if !active_qpairs.contains(&qpair) {
            active_qpairs.push(qpair);
        }
    }

    /// Force-complete all outstanding I/Os on a failed qpair using the
    /// per-qpair pending Vec, then mark the qpair dead so future submissions fail fast.
    fn force_complete_qpair(key: usize, dead_qpairs: &mut HashMap<usize, Box<QpairState>>) {
        if let Some(qs) = dead_qpairs.get_mut(&key) {
            qs.dead.store(true, Ordering::Release);
            let pending = std::mem::take(&mut qs.pending);
            let count = pending.len();
            for cb_ptr in &pending {
                let ctx = unsafe { Box::from_raw(*cb_ptr as *mut CallbackCtx) };
                if ctx.completion.complete(-libc::EIO) {
                    ctx.bdev_inflight.fetch_sub(1, Ordering::Release);
                }
            }
            if count > 0 {
                error!(
                    "Force-completed {} outstanding I/O(s) on failed qpair 0x{:x} with EIO",
                    count, key
                );
            }
        }
        dead_qpairs.remove(&key);
    }

    /// Process admin completions on all controllers to service keep-alive.
    fn process_admin_completions(ctrlrs: &[CtrlHandle]) {
        for handle in ctrlrs {
            let rc = unsafe { spdk_ffi::spdk_nvme_ctrlr_process_admin_completions(handle.0) };
            if rc < 0 {
                warn!("ctrlr {:p} admin completion error: rc={}", handle.0, rc);
            }
        }
    }
}

/// Per-qpair state tracked on the poller thread. Holds the dead flag and
/// a Vec of all in-flight CallbackCtx pointers for force-completion.
struct QpairState {
    dead: Arc<AtomicBool>,
    pending: Vec<*mut CallbackCtx>,
}

/// C callback context. Heap-allocated for SPDK to hold pointer.
struct CallbackCtx {
    completion: Arc<IoCompletion>,
    async_ctx: spdk_ffi::curvine_async_ctx,
    bdev_inflight: Arc<AtomicUsize>,
    /// Points back to the qpair's QpairState
    qpair_state: *mut QpairState,
    /// Index into QpairState::pending
    pending_idx: usize,
}

/// C callback invoked by SPDK when NVMe command completes.
unsafe extern "C" fn poller_callback(cb_arg: *mut c_void, status: i32) {
    let ctx = Box::from_raw(cb_arg as *mut CallbackCtx);

    let qs = &mut *(ctx.qpair_state as *mut QpairState);

    // Underflow guard only (runs after Box::from_raw + deref, not a UAF guard).
    // TODO: add orphan lifecycle for late-callback safety.
    if qs.pending.is_empty() {
        return;
    }

    let idx = ctx.pending_idx;
    let last = qs.pending.len() - 1;
    if idx != last {
        let last_ptr = qs.pending[last];
        qs.pending.swap_remove(idx);
        (*last_ptr).pending_idx = idx;
    } else {
        qs.pending.pop();
    }

    // Guard against double-decrement on repeated complete()
    if ctx.completion.complete(status) {
        ctx.bdev_inflight.fetch_sub(1, Ordering::Release);
    }
}

impl Drop for SpdkPoller {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const DEAD: usize = 0xDEAD;
    const LIVE: usize = 0xCAFE;

    #[test]
    fn force_complete_reclaims_callback_ctx() {
        // Allocate CallbackCtx as submit_one does, one per simulated I/O.
        let completion_1 = IoCompletion::new();
        let inflight_1 = Arc::new(AtomicUsize::new(1));
        let completion_2 = IoCompletion::new();
        let inflight_2 = Arc::new(AtomicUsize::new(1));
        let completion_3 = IoCompletion::new();
        let inflight_3 = Arc::new(AtomicUsize::new(1));

        // Create QpairState for DEAD qpair.
        let dead_flag = Arc::new(AtomicBool::new(false));
        let mut qs_dead = Box::new(QpairState {
            dead: dead_flag.clone(),
            pending: Vec::new(),
        });

        // Allocate and push 2 CallbackCtx entries into DEAD's pending Vec.
        let ctx_1 = Box::into_raw(Box::new(CallbackCtx {
            completion: completion_1.clone(),
            async_ctx: unsafe { std::mem::zeroed() },
            bdev_inflight: inflight_1.clone(),
            qpair_state: &mut *qs_dead as *mut QpairState,
            pending_idx: 0,
        }));
        qs_dead.pending.push(ctx_1);

        let ctx_2 = Box::into_raw(Box::new(CallbackCtx {
            completion: completion_2.clone(),
            async_ctx: unsafe { std::mem::zeroed() },
            bdev_inflight: inflight_2.clone(),
            qpair_state: &mut *qs_dead as *mut QpairState,
            pending_idx: 1,
        }));
        qs_dead.pending.push(ctx_2);

        // Create QpairState for LIVE qpair (control — should be untouched).
        let live_flag = Arc::new(AtomicBool::new(false));
        let mut qs_live = Box::new(QpairState {
            dead: live_flag,
            pending: Vec::new(),
        });

        let ctx_3 = Box::into_raw(Box::new(CallbackCtx {
            completion: completion_3.clone(),
            async_ctx: unsafe { std::mem::zeroed() },
            bdev_inflight: inflight_3.clone(),
            qpair_state: &mut *qs_live as *mut QpairState,
            pending_idx: 0,
        }));
        qs_live.pending.push(ctx_3);

        // Build dead_qpairs map.
        let mut dead_qpairs: HashMap<usize, Box<QpairState>> = HashMap::new();
        dead_qpairs.insert(DEAD, qs_dead);
        dead_qpairs.insert(LIVE, qs_live);

        // Act.
        SpdkPoller::force_complete_qpair(DEAD, &mut dead_qpairs);

        // Assert: DEAD entries completed with -EIO.
        assert_eq!(completion_1.wait(0), -libc::EIO);
        assert_eq!(completion_2.wait(0), -libc::EIO);

        // Assert: LIVE entry NOT completed (timeout with 1ms).
        assert_eq!(completion_3.wait(1000), -libc::ETIMEDOUT);

        // Assert: DEAD entries' bdev_inflight decremented.
        assert_eq!(inflight_1.load(Ordering::Acquire), 0);
        assert_eq!(inflight_2.load(Ordering::Acquire), 0);

        // Assert: LIVE entry's bdev_inflight unchanged.
        assert_eq!(inflight_3.load(Ordering::Acquire), 1);

        // Assert: dead flag set.
        assert!(dead_flag.load(Ordering::Acquire));

        // Assert: DEAD removed from dead_qpairs, LIVE still present.
        assert!(!dead_qpairs.contains_key(&DEAD));
        assert!(dead_qpairs.contains_key(&LIVE));

        // Clean up LIVE entry (force_complete did not touch it).
        // The raw pointer in qs_live.pending must be reclaimed.
        if let Some(qs) = dead_qpairs.get_mut(&LIVE) {
            for cb_ptr in qs.pending.drain(..) {
                unsafe { drop(Box::from_raw(cb_ptr as *mut CallbackCtx)) };
            }
        }
    }

    #[test]
    fn poller_callback_empty_pending_returns_early() {
        let inflight = Arc::new(AtomicUsize::new(1));
        let completion = IoCompletion::new();
        let qs = Box::new(QpairState {
            dead: Arc::new(AtomicBool::new(false)),
            pending: Vec::new(),
        });
        let ctx = Box::into_raw(Box::new(CallbackCtx {
            completion: completion.clone(),
            async_ctx: unsafe { std::mem::zeroed() },
            bdev_inflight: inflight.clone(),
            qpair_state: &*qs as *const QpairState as *mut QpairState,
            pending_idx: 0,
        }));

        unsafe { poller_callback(ctx as *mut c_void, 0) };

        assert!(qs.pending.is_empty());
        assert_eq!(inflight.load(Ordering::Acquire), 1);
        assert_eq!(completion.wait(1), -libc::ETIMEDOUT);
    }

    #[test]
    fn complete_is_idempotent_first_call_wins() {
        let completion = IoCompletion::new();
        assert!(completion.complete(42));
        assert_eq!(completion.wait(0), 42);

        assert!(!completion.complete(99));
        assert_eq!(completion.wait(0), 42);
    }

    #[test]
    fn poller_callback_fetch_sub_guard_runs_once() {
        let completion = IoCompletion::new();
        let inflight = Arc::new(AtomicUsize::new(1));
        let mut qs = Box::new(QpairState {
            dead: Arc::new(AtomicBool::new(false)),
            pending: Vec::new(),
        });

        // Simulate force_complete_qpair signaling first.
        assert!(completion.complete(42));
        inflight.fetch_sub(1, Ordering::Release);
        assert_eq!(inflight.load(Ordering::Acquire), 0);

        // Now poller_callback fires on the same ctx.
        let ctx = Box::into_raw(Box::new(CallbackCtx {
            completion: completion.clone(),
            async_ctx: unsafe { std::mem::zeroed() },
            bdev_inflight: inflight.clone(),
            qpair_state: &mut *qs as *mut QpairState,
            pending_idx: 0,
        }));
        qs.pending.push(ctx);

        // complete() returns false -> fetch_sub skipped.
        unsafe { poller_callback(ctx as *mut c_void, 99) };

        assert_eq!(completion.wait(0), 42, "first signal wins");
        assert_eq!(inflight.load(Ordering::Acquire), 0, "no double-decrement");
    }
}
