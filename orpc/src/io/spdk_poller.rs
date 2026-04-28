#![cfg(feature = "spdk")]

//! SPDK I/O poller thread - handles NVMe submit/poll on dedicated thread.
/// Qpairs not thread-safe: submit + poll must on same thread.
/// Single poller to demonstrate the correctness work.
/// Uses eventfd for instant wake on new I/O submission.
/// TODO: shard to multiple pollers (one per controller).
///
/// ## Disconnect Detection
/// Detected via periodic keep-alive poll every 1s while idle (~1s latency).
/// TODO: SPDK fabric eventfd for immediate detection.
use log::{error, info};
use nix::sys::eventfd::{EfdFlags, EventFd};
use std::ffi::c_void;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

use crate::io::spdk_ffi;

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
    pub fn complete(&self, status: i32) {
        let mut inner = self.inner.lock().unwrap();
        inner.done = true;
        inner.status = status;
        self.cond.notify_one();
    }

    /// Block until complete or timeout. Returns NVMe status.
    pub fn wait(&self, timeout_us: u64) -> i32 {
        let mut inner = self.inner.lock().unwrap();
        if timeout_us == 0 {
            while !inner.done {
                inner = self.cond.wait(inner).unwrap();
            }
        } else {
            let timeout = std::time::Duration::from_micros(timeout_us);
            let deadline = std::time::Instant::now() + timeout;
            while !inner.done {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
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
    pub bdev_inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
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

/// Poller thread handle.
pub struct SpdkPoller {
    /// Channel sender for I/O submissions
    tx: Option<crossbeam::channel::Sender<IoRequest>>,
    /// Eventfd for instant wake signaling
    eventfd: Arc<EventFd>,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl SpdkPoller {
    /// Spawn a new poller thread with given poll interval (in ms).
    pub fn start(poll_interval_ms: u64) -> Self {
        let (tx, rx) = crossbeam::channel::unbounded::<IoRequest>();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        // Create eventfd for wake signaling
        let eventfd = EventFd::from_value_and_flags(0, EfdFlags::EFD_NONBLOCK)
            .expect("Failed to create eventfd");
        let eventfd_raw = eventfd.as_raw_fd();
        let eventfd_arc = Arc::new(eventfd);

        let handle = std::thread::Builder::new()
            .name("spdk-poller".to_string())
            .spawn(move || {
                Self::poller_loop(rx, shutdown_clone, eventfd_raw, poll_interval_ms);
            })
            .expect("Failed to spawn SPDK poller thread");

        Self {
            tx: Some(tx),
            eventfd: eventfd_arc,
            shutdown,
            handle: Some(handle),
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
        eventfd: RawFd,
        poll_interval_ms: u64,
    ) {
        let mut active_qpairs: Vec<*mut spdk_ffi::spdk_nvme_qpair> = Vec::new();
        let mut state = PollerState::Idle;

        // Verify curvine_async_ctx buffer fits the C struct.
        debug_assert!(
            unsafe { spdk_ffi::curvine_spdk_async_ctx_sizeof() }
                <= std::mem::size_of::<spdk_ffi::curvine_async_ctx>(),
            "curvine_async_ctx C struct exceeds Rust buffer"
        );

        // Poll interval for keep-alive check (parameterized to detect disconnects)
        let poll_interval = poll_interval_ms as i32;
        loop {
            // Check shutdown first
            if shutdown.load(Ordering::Acquire) && rx.is_empty() && active_qpairs.is_empty() {
                break;
            }

            // Active state: drain all pending I/Os and poll completions
            if matches!(state, PollerState::Active) {
                // Drain pending requests (non-blocking)
                while let Ok(req) = rx.try_recv() {
                    Self::submit_one(&req, &mut active_qpairs);
                }

                // Poll qpairs for completions
                active_qpairs.retain(|qpair| {
                    let rc = unsafe { spdk_ffi::curvine_spdk_qpair_poll(*qpair, 0) };
                    if rc < 0 {
                        error!("qpair poll error: {}", rc);
                        return false;
                    }
                    true
                });

                // Transition to Idle if no more work
                if rx.is_empty() && active_qpairs.is_empty() {
                    state = PollerState::Idle;
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

                // Wait on eventfd with timeout for keep-alive check
                let mut eventfd_pollfd = libc::pollfd {
                    fd: eventfd,
                    events: libc::POLLIN,
                    revents: 0,
                };

                let result = unsafe { libc::poll(&mut eventfd_pollfd, 1, poll_interval) };

                match result {
                    n if n > 0 => {
                        // Eventfd signaled - drain it
                        let mut buf = [0u8; 8];
                        let _ = unsafe { libc::read(eventfd, buf.as_mut_ptr() as *mut c_void, 8) };

                        // Drain any pending channel data
                        while let Ok(req) = rx.try_recv() {
                            Self::submit_one(&req, &mut active_qpairs);
                        }

                        // Transition to Active to process work
                        state = PollerState::Active;
                    }
                    0 => {
                        // Timeout - poll active qpairs to check connection health
                        active_qpairs.retain(|qpair| {
                            let rc = unsafe { spdk_ffi::curvine_spdk_qpair_poll(*qpair, 0) };
                            if rc < 0 {
                                error!("Poller: keep-alive poll error: {}, removing qpair", rc);
                                return false;
                            }
                            true
                        });
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

    /// Submit a single I/O request on the poller thread.
    fn submit_one(req: &IoRequest, active_qpairs: &mut Vec<*mut spdk_ffi::spdk_nvme_qpair>) {
        let qpair = match &req.op {
            IoOp::Read { qpair, .. } => *qpair,
            IoOp::Write { qpair, .. } => *qpair,
            IoOp::Flush { qpair, .. } => *qpair,
        };

        // Box::into_raw ensures CallbackCtx survives until poller_callback reclaims it
        let cb_ctx = Box::new(CallbackCtx {
            completion: req.completion.clone(),
            async_ctx: unsafe { std::mem::zeroed() },
            bdev_inflight: req.bdev_inflight.clone(),
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
        };

        if rc != 0 {
            // Submission failed - reclaim allocation and complete with error
            unsafe { drop(Box::from_raw(cb_ctx_ptr)) };
            req.bdev_inflight.fetch_sub(1, Ordering::Release);
            req.completion.complete(rc);
            return;
        }

        // Track active qpair
        if !active_qpairs.contains(&qpair) {
            active_qpairs.push(qpair);
        }
    }
}

/// C callback context. Heap-allocated for SPDK to hold pointer.
struct CallbackCtx {
    completion: Arc<IoCompletion>,
    async_ctx: spdk_ffi::curvine_async_ctx,
    bdev_inflight: Arc<std::sync::atomic::AtomicUsize>,
}

/// C callback invoked by SPDK when NVMe command completes.
unsafe extern "C" fn poller_callback(cb_arg: *mut c_void, status: i32) {
    let ctx = Box::from_raw(cb_arg as *mut CallbackCtx);
    ctx.bdev_inflight.fetch_sub(1, Ordering::Release);
    ctx.completion.complete(status);
}

impl Drop for SpdkPoller {
    fn drop(&mut self) {
        self.stop();
    }
}
