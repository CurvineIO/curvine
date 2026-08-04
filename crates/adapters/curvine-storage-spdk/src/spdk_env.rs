#![cfg(feature = "spdk")]

use crate::spdk_ffi;
use crate::spdk_poller::{CtrlHandle, PollerConfig};
use crate::spdk_poller::{IoRequest, SpdkPoller};
use curvine_core_error::{err_box, err_msg, CommonResult};
use curvine_io::{NvmeTarget, SpdkConf};
use log::{error, info, warn};
use nix::sys::eventfd::EventFd;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Qpair pool - reuse NVMe qpairs across handles
// Lazy allocate, cache on release.
// Per-controller limits derived from negotiated io_queues of spdk's target
// ---------------------------------------------------------------------------

// Per-controller qpair state
pub(crate) struct CtrlQpairState {
    active: AtomicUsize,
    max_active: usize,
}

impl CtrlQpairState {
    fn new(max_active: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            max_active,
        }
    }
}

pub struct QpairPool {
    pub(crate) inner: Mutex<HashMap<usize, Vec<*mut spdk_ffi::spdk_nvme_qpair>>>,
    /// Per-controller active count and max limit, keyed by controller pointer.
    pub(crate) ctrl_state: Mutex<HashMap<usize, CtrlQpairState>>,
    /// Idle cache limit per controller (soft — excess freed on release).
    pub(crate) max_per_ctrlr: usize,
    /// Wakes blocked acquirers when a qpair is released.
    pub(crate) notify: Condvar,
    /// Set by drain_all() to reject new acquires during shutdown.
    pub(crate) shutdown: AtomicBool,
    /// How long `acquire` blocks waiting for a free slot before timing out.
    pub(crate) qpair_acquire_timeout: Duration,
}
// SAFETY: exclusive ownership
unsafe impl Send for QpairPool {}
unsafe impl Sync for QpairPool {}
impl QpairPool {
    /// Default max idle qpairs per controller (soft cache limit).
    const DEFAULT_MAX_PER_CTRLR: usize = 16;
    /// Default qpair acquire timeout, used when SpdkConf does not set `qpair_acquire_timeout`.
    #[cfg(test)]
    const DEFAULT_QPAIR_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::with_qpair_acquire_timeout(Self::DEFAULT_QPAIR_ACQUIRE_TIMEOUT)
    }

    pub(crate) fn with_qpair_acquire_timeout(qpair_acquire_timeout: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            ctrl_state: Mutex::new(HashMap::new()),
            max_per_ctrlr: Self::DEFAULT_MAX_PER_CTRLR,
            notify: Condvar::new(),
            shutdown: AtomicBool::new(false),
            qpair_acquire_timeout,
        }
    }

    /// Register the per-controller qpair limit from the actual negotiated IO queue count.
    /// `actual_io_queues` is queried from SPDK after controller initialization
    pub(crate) fn register_limit(&self, ctrlr_ptr: usize, actual_io_queues: u32) {
        let limit = actual_io_queues as usize;
        let mut state = self.ctrl_state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            !state.contains_key(&ctrlr_ptr),
            "QpairPool: ctrlr {:p} already registered — register_limit is one-shot",
            ctrlr_ptr as *const ()
        );
        state.insert(ctrlr_ptr, CtrlQpairState::new(limit));
        if limit == 0 {
            warn!(
                "QpairPool: ctrlr {:p} registered with max_active=0 (0 negotiated IO queues)",
                ctrlr_ptr as *const ()
            );
        } else {
            info!(
                "QpairPool: registered ctrlr {:p} with max_active={}",
                ctrlr_ptr as *const (), limit
            );
        }
    }

    /// Get (active, max_active) for a controller.
    pub(crate) fn controller_stats(&self, ctrlr_ptr: usize) -> (usize, usize) {
        let state = self.ctrl_state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = state.get(&ctrlr_ptr) {
            (s.active.load(Ordering::Acquire), s.max_active)
        } else {
            (0, 0)
        }
    }

    // TODO: Arc<CtrlQpairState> + #[repr(align(64))] - eliminate ctrl_state Mutex from CAS, prevent false sharing
    /// Atomically reserve a slot for this controller.
    /// Returns true if reserved (active < max_active), false at capacity.
    pub(crate) fn try_reserve(&self, ctrlr_ptr: usize) -> bool {
        let state = self.ctrl_state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = state.get(&ctrlr_ptr) {
            loop {
                let cur = s.active.load(Ordering::Acquire);
                if cur >= s.max_active {
                    return false;
                }
                if s.active
                    .compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    return true;
                }
            }
        } else {
            error!(
                "QpairPool: try_reserve called for unregistered ctrlr {:p}",
                ctrlr_ptr as *const ()
            );
            false
        }
    }

    /// Release a previously reserved slot.
    /// Refuses to decrement when active == 0 to prevent usize::MAX wrap on double-release.
    fn release_reservation(&self, ctrlr_ptr: usize) {
        let state = self.ctrl_state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = state.get(&ctrlr_ptr) {
            let cur = s.active.load(Ordering::Acquire);
            if cur == 0 {
                warn!(
                    "QpairPool: release_reservation called with active=0 for ctrlr {:p} \
                     (no reservation to release — possible double-release)",
                    ctrlr_ptr as *const ()
                );
                return;
            }
            s.active.fetch_sub(1, Ordering::AcqRel);
        } else {
            warn!(
                "QpairPool: release_reservation for unregistered ctrlr {:p} \
                 (no active count to decrement — possible double-release or release without acquire)",
                ctrlr_ptr as *const ()
            );
        }
    }

    /// Acquire qpair - returns cached or allocates new
    pub(crate) fn acquire(
        &self,
        ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
    ) -> CommonResult<*mut spdk_ffi::spdk_nvme_qpair> {
        let key = ctrlr as usize;
        let mut reserved = false;

        // Fast path: check capacity, then try pool without blocking
        if self.try_reserve(key) {
            reserved = true;
            if self.shutdown.load(Ordering::Acquire) {
                self.release_reservation(key);
                return err_box!(
                    "QpairPool: shutdown in progress, acquire rejected for ctrlr {:p}",
                    ctrlr
                );
            }
            let mut pool = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(stack) = pool.get_mut(&key) {
                if let Some(qpair) = stack.pop() {
                    log::trace!(
                        "QpairPool: reusing cached qpair for ctrlr {:p} (pool size now {})",
                        ctrlr,
                        stack.len()
                    );
                    return Ok(qpair);
                }
            }
            // No cached qpair - fall through to slow path with slot already reserved
        } else {
            log::trace!(
                "QpairPool: fast path blocked for ctrlr {:p} (at capacity)",
                ctrlr,
            );
        }

        // Slow path: block if at this controller's capacity, then allocate
        let deadline = Instant::now() + self.qpair_acquire_timeout;
        let mut pool = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            // CAS reserve before pool check — active must increment before returning cached qpair.
            if !reserved {
                if self.try_reserve(key) {
                    reserved = true;
                }
            }

            if reserved {
                // Slot reserved - check pool for a cached qpair to reuse
                if let Some(stack) = pool.get_mut(&key) {
                    if let Some(qpair) = stack.pop() {
                        log::trace!(
                            "QpairPool: reusing cached qpair for ctrlr {:p} after wait",
                            ctrlr,
                        );
                        return Ok(qpair);
                    }
                }
                // No cached qpair - break out and allocate a new one via FFI
                break;
            }

            // at this controller's capacity - wait for a release
            if self.shutdown.load(Ordering::Acquire) {
                return err_box!(
                    "QpairPool: shutdown in progress, acquire rejected for ctrlr {:p}",
                    ctrlr
                );
            }
            let now = Instant::now();
            if now >= deadline {
                return err_box!(
                    "QpairPool: timeout after {:?} waiting for qpair on ctrlr {:p} \
                     This indicates qpair exhaustion under high concurrency. \
                     Check NvmeTarget.io_queues / SpdkConf.qpair_acquire_timeout configuration.",
                    self.qpair_acquire_timeout,
                    ctrlr
                );
            }
            let remaining = deadline.duration_since(now);
            log::trace!("QpairPool: ctrlr {:p} at capacity, waiting...", ctrlr,);
            pool = self
                .notify
                .wait_timeout(pool, remaining)
                .unwrap_or_else(|p| p.into_inner())
                .0;
            // TODO: check shutdown here to avoid a full qpair_acquire_timeout delay if notify_all() was missed
        }

        // Slot reserved (by fast path or slow path) - allocate the qpair
        drop(pool);
        if self.shutdown.load(Ordering::Acquire) {
            self.release_reservation(key);
            return err_box!(
                "QpairPool: shutdown in progress, acquire rejected for ctrlr {:p}",
                ctrlr
            );
        }
        let qpair = unsafe { spdk_ffi::curvine_spdk_alloc_io_qpair(ctrlr) };
        if qpair.is_null() {
            self.release_reservation(key);
            self.notify.notify_one();
            return err_box!(
                "QpairPool: failed to allocate I/O qpair for ctrlr {:p}. \
                 This may indicate qpair exhaustion under high concurrency. \
                 Check NvmeTarget.io_queues configuration.",
                ctrlr
            );
        }
        log::trace!("QpairPool: allocated new qpair for ctrlr {:p}", ctrlr);
        Ok(qpair)
    }
    /// Return qpair to pool for reuse.
    /// Drops the pool lock before notifying to avoid contending with woken threads.
    pub(crate) fn release(
        &self,
        ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
        qpair: *mut spdk_ffi::spdk_nvme_qpair,
    ) {
        let key = ctrlr as usize;
        {
            let mut pool = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let stack = pool.entry(key).or_default();
            if stack.len() >= self.max_per_ctrlr {
                // Pool full — free immediately to bound controller-side memory.
                drop(pool); // release lock before FFI call
                unsafe { spdk_ffi::curvine_spdk_free_io_qpair(qpair) };
                log::trace!(
                    "QpairPool: pool full for ctrlr {:p} (max={}), freed qpair",
                    ctrlr,
                    self.max_per_ctrlr
                );
            } else {
                stack.push(qpair);
                log::trace!(
                    "QpairPool: returned qpair to pool for ctrlr {:p} ({}/{})",
                    ctrlr,
                    stack.len(),
                    self.max_per_ctrlr
                );
            }
        } // inner lock dropped here
        self.release_reservation(key);
        self.notify.notify_one(); // safe to call without holding inner lock
    }
    /// Free all pooled qpairs. Only frees cached (idle) qpairs - active/in-flight
    /// qpairs are tracked by their owners and will be released normally.
    pub(crate) fn drain_all(&self) {
        self.shutdown.store(true, Ordering::Release);
        let mut pool = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let mut total = 0usize;
        for (_ctrlr_key, qpairs) in pool.drain() {
            for qpair in qpairs {
                unsafe { spdk_ffi::curvine_spdk_free_io_qpair(qpair) };
                total += 1;
            }
        }
        if total > 0 {
            info!("QpairPool: freed {} cached qpair(s) during shutdown", total);
        }
        // Warn if there are still active (in-flight) qpairs - these are not freed here,
        // they are tracked by their SpdkBdev owners and will be released via drop().
        for state in self
            .ctrl_state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
        {
            let active = state.active.load(Ordering::Acquire);
            if active > 0 {
                warn!(
                    "QpairPool: drain_all called with {} active (in-flight) qpair(s) remaining",
                    active
                );
                break;
            }
        }
        self.notify.notify_all();
    }
}
// Global singleton

static SPDK_ENV: OnceLock<SpdkEnv> = OnceLock::new();
static INIT_LOCK: Mutex<()> = Mutex::new(());

/// Lifecycle state
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpdkEnvState {
    Created = 0,     // config validated, not init
    Initialized = 1, // controllers attached, bdevs available
    /// spdk_env_fini called; no further ops allowed.
    ShutDown = 2,
}

impl From<u8> for SpdkEnvState {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::Created,
            1 => Self::Initialized,
            2 => Self::ShutDown,
            _ => Self::Created,
        }
    }
}

impl Display for SpdkEnvState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SpdkEnvState::Created => write!(f, "Created"),
            SpdkEnvState::Initialized => write!(f, "Initialized"),
            SpdkEnvState::ShutDown => write!(f, "ShutDown"),
        }
    }
}

// Bdev descriptor (discovered after init)

/// Discovered SPDK block device
#[derive(Debug, Clone)]
pub struct BdevInfo {
    pub name: String,    // e.g. "NVMe0n1"
    pub size_bytes: u64, // total size in bytes
    pub block_size: u32, // typically 512 or 4096
    pub num_blocks: u64,
    pub target_endpoint: String,
    pub ctrlr: usize,       // raw pointer for SpdkBdev
    pub ns: usize,          // raw pointer for SpdkBdev
    pub io_timeout_ms: u64, // per-target I/O timeout in ms
}

impl Display for BdevInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BdevInfo(name={}, size={}B, block_size={}, target={})",
            self.name, self.size_bytes, self.block_size, self.target_endpoint
        )
    }
}

// SpdkEnv - the core struct

/// SPDK env lifecycle (singleton). Send + Sync
pub struct SpdkEnv {
    conf: SpdkConf,
    state: AtomicU8,
    bdevs: Vec<BdevInfo>, // populated during init(), immutable after
    open_handles: AtomicUsize,
    qpair_pool: QpairPool,
    poller: Mutex<Option<SpdkPoller>>,
}

// SAFETY: Fields are either immutable after init (conf, bdevs) or atomic (state).
unsafe impl Send for SpdkEnv {}
unsafe impl Sync for SpdkEnv {}

impl SpdkEnv {
    /// Create in Created state
    pub fn new(mut conf: SpdkConf) -> CommonResult<Self> {
        conf.init()?;
        conf.validate()?;

        info!("SpdkEnv created: {}", conf);

        let qpair_pool = QpairPool::with_qpair_acquire_timeout(Duration::from_millis(
            conf.qpair_acquire_timeout_ms,
        ));

        Ok(Self {
            conf,
            state: AtomicU8::new(SpdkEnvState::Created as u8),
            bdevs: Vec::new(),
            open_handles: AtomicUsize::new(0),
            qpair_pool,
            poller: Mutex::new(None),
        })
    }

    // Global singleton access

    /// Init global SPDK (OnceLock + Mutex)
    pub fn init_global(conf: SpdkConf) -> CommonResult<&'static SpdkEnv> {
        // Fast path: already initialized
        if let Some(env) = SPDK_ENV.get() {
            if env.is_initialized() {
                return Ok(env);
            }
        }

        // Slow path: acquire lock
        let _lock = match INIT_LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("SPDK init lock was poisoned (previous panic), recovering");
                poisoned.into_inner()
            }
        };

        // Double-check after acquiring lock
        if let Some(env) = SPDK_ENV.get() {
            if env.is_initialized() {
                return Ok(env);
            }
        }

        info!("Initializing global SPDK environment: {}", conf);

        let mut env = Self::new(conf)?;
        env.init()?;

        SPDK_ENV.set(env).map_err(|_| {
            err_msg!("Failed to store SpdkEnv instance (concurrent initialization conflict)")
        })?;

        Ok(SPDK_ENV.get().expect("SpdkEnv was just initialized"))
    }

    /// Get global SPDK if initialized
    pub fn global() -> Option<&'static SpdkEnv> {
        SPDK_ENV.get().filter(|env| env.is_initialized())
    }

    /// Get global SPDK regardless of state
    pub fn global_including_shutdown() -> Option<&'static SpdkEnv> {
        SPDK_ENV.get()
    }

    /// Get the global SPDK environment, or return an error.
    pub fn global_or_err() -> CommonResult<&'static SpdkEnv> {
        Self::global().ok_or_else(|| err_msg!("SPDK environment not initialized").into())
    }

    /// Shut down the global SPDK environment singleton.
    /// Safe to call multiple times or when not initialized (no-ops).
    pub fn shutdown_global() {
        if let Some(env) = SPDK_ENV.get() {
            env.shutdown();
        }
    }

    // Initialization (instance-level)

    /// Init SPDK: allocate hugepages, start reactors, attach controllers, discover bdevs.
    pub fn init(&mut self) -> CommonResult<()> {
        let current = self.state();
        if current != SpdkEnvState::Created {
            return err_box!(
                "SpdkEnv::init() called in invalid state: {} (expected Created)",
                current
            );
        }

        info!(
            "SPDK env init: app_name={}, hugepage={}MB, reactor_mask={}, targets={}",
            self.conf.app_name,
            self.conf.hugepage_mb,
            self.conf.reactor_mask,
            self.conf.targets.len()
        );

        // Phase 1: Initialize SPDK environment (hugepages, DPDK, reactors)
        self.env_init()?;

        // Phase 2: Attach NVMe-oF controllers and discover bdevs
        let mut all_bdevs = Vec::new();
        for (i, target) in self.conf.targets.iter().enumerate() {
            match self.attach_controller(target) {
                Ok(bdevs) => {
                    info!(
                        "Target[{}] {}: discovered {} bdev(s): [{}]",
                        i,
                        target.endpoint(),
                        bdevs.len(),
                        bdevs
                            .iter()
                            .map(|b| b.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    // Register per-controller qpair limit from SPDK's negotiated IO queue count.
                    if let Some(first) = bdevs.first() {
                        let actual_io_queues = unsafe {
                            spdk_ffi::curvine_spdk_ctrlr_get_num_io_queues(
                                first.ctrlr as *mut spdk_ffi::spdk_nvme_ctrlr,
                            )
                        };
                        info!(
                            "Target[{}] {}: requested io_queues={}, actual negotiated={}",
                            i,
                            target.endpoint(),
                            target.io_queues,
                            actual_io_queues
                        );
                        self.qpair_pool
                            .register_limit(first.ctrlr, actual_io_queues);
                    }
                    all_bdevs.extend(bdevs);
                }
                Err(e) => {
                    error!("Target[{}] {} attach failed: {}", i, target.endpoint(), e);
                    // Continue to next target — partial success is acceptable
                }
            }
        }

        if all_bdevs.is_empty() {
            // Clean up since we failed
            self.env_fini();
            return err_box!(
                "SpdkEnv::init() failed: no bdevs discovered from {} target(s)",
                self.conf.targets.len()
            );
        }

        info!(
            "SPDK env initialized: {} bdev(s) from {} target(s)",
            all_bdevs.len(),
            self.conf.targets.len()
        );

        self.bdevs = all_bdevs;

        // Collect unique controllers for admin completion polling (keep-alive)
        let mut seen = HashSet::new();
        let mut ctrlrs: Vec<CtrlHandle> = Vec::with_capacity(self.bdevs.len());
        for bdev in &self.bdevs {
            if bdev.ctrlr != 0 && seen.insert(bdev.ctrlr) {
                ctrlrs.push(CtrlHandle(bdev.ctrlr as *mut spdk_ffi::spdk_nvme_ctrlr));
            }
        }

        // Start the dedicated I/O poller thread
        {
            let poller = SpdkPoller::start(PollerConfig {
                poll_interval_ms: self.conf.poll_interval_ms,
                spin_iter: self.conf.spin_iter,
                io_queue_depth: self.conf.io_queue_depth as usize,
                ctrlrs,
            });
            *self.poller.lock().unwrap() = Some(poller);
            info!("SPDK poller thread started");
        }

        self.state
            .store(SpdkEnvState::Initialized as u8, Ordering::Release);

        Ok(())
    }

    // Shutdown

    /// Maximum time `shutdown()` will wait for in-flight handles to close.
    const SHUTDOWN_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    /// Shutdown: detach controllers, free hugepages
    pub fn shutdown(&self) {
        // CAS Initialized => ShutDown, reject new opens after transition
        let prev = self.state.compare_exchange(
            SpdkEnvState::Initialized as u8,
            SpdkEnvState::ShutDown as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        if prev.is_err() {
            warn!(
                "SpdkEnv::shutdown() called in state: {} (expected Initialized)",
                self.state()
            );
            return;
        }

        info!(
            "Shutting down SPDK environment ({} bdevs)",
            self.bdevs.len()
        );

        // Wait for in-flight handles to close (acquire_handle checks state).
        let deadline = std::time::Instant::now() + Self::SHUTDOWN_DRAIN_TIMEOUT;
        let mut logged = false;
        loop {
            let count = self.open_handles.load(Ordering::Acquire);
            if count == 0 {
                break;
            }
            if std::time::Instant::now() >= deadline {
                error!(
                    "Timed out waiting for {} SpdkBdev handle(s) to close after {}s. \
                     Skipping controller detach and env_fini to avoid use-after-free. \
                     Resources will be leaked — this indicates a shutdown ordering bug.",
                    count,
                    Self::SHUTDOWN_DRAIN_TIMEOUT.as_secs()
                );
                // Stop the poller thread (safe — pending I/Os will get channel-closed errors)
                // but do NOT detach controllers or call env_fini.
                if let Some(mut poller) = self.poller.lock().unwrap().take() {
                    poller.stop();
                    info!("SPDK poller thread stopped (timeout path)");
                }
                return;
            }
            if !logged {
                info!(
                    "Waiting for {} outstanding SpdkBdev handle(s) to close before detach",
                    count
                );
                logged = true;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Stop poller before draining qpairs (no in-flight I/Os).
        if let Some(mut poller) = self.poller.lock().unwrap().take() {
            poller.stop();
            info!("SPDK poller thread stopped");
        }

        self.qpair_pool.drain_all();
        self.detach_controllers(); // stop keep-alive, release controller resources
        self.env_fini(); // release hugepages, cleanup DPDK EAL

        info!("SPDK environment shut down successfully");
    }

    /// Current lifecycle state.
    pub fn state(&self) -> SpdkEnvState {
        SpdkEnvState::from(self.state.load(Ordering::Acquire))
    }

    /// Whether the environment is initialized and ready for I/O.
    pub fn is_initialized(&self) -> bool {
        self.state() == SpdkEnvState::Initialized
    }

    /// The validated configuration.
    pub fn conf(&self) -> &SpdkConf {
        &self.conf
    }

    /// Number of configured targets.
    pub fn target_count(&self) -> usize {
        self.conf.targets.len()
    }

    /// Names of all discovered bdevs.
    pub fn bdev_names(&self) -> Vec<String> {
        self.bdevs.iter().map(|b| b.name.clone()).collect()
    }

    /// All discovered bdev metadata.
    pub fn bdevs(&self) -> &[BdevInfo] {
        &self.bdevs
    }

    /// Look up a bdev by name.
    pub fn get_bdev(&self, name: &str) -> Option<&BdevInfo> {
        self.bdevs.iter().find(|b| b.name == name)
    }

    /// Total capacity across all bdevs, in bytes.
    pub fn total_capacity(&self) -> u64 {
        self.bdevs.iter().map(|b| b.size_bytes).sum()
    }

    /// Number of live `SpdkBdev` handles.
    pub fn open_handle_count(&self) -> usize {
        self.open_handles.load(Ordering::Acquire)
    }
    /// Number of idle qpairs currently cached in the pool.
    pub fn pooled_qpair_count(&self) -> usize {
        let pool = self
            .qpair_pool
            .inner
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        pool.values().map(|v| v.len()).sum()
    }

    /// Get sender channel for poller thread (for SpdkIoChannel).
    pub fn poller_sender(&self) -> crossbeam::channel::Sender<IoRequest> {
        self.poller
            .lock()
            .unwrap()
            .as_ref()
            .expect("SpdkPoller not started — was init() called?")
            .sender()
    }

    /// Get eventfd for waking poller on new I/O
    pub fn poller_eventfd(&self) -> std::sync::Arc<EventFd> {
        self.poller
            .lock()
            .unwrap()
            .as_ref()
            .expect("SpdkPoller not started — was init() called?")
            .eventfd_arc()
    }

    /// Get is_sleeping flag for bdevs to skip eventfd write when poller is active
    pub fn poller_is_sleeping(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        self.poller
            .lock()
            .unwrap()
            .as_ref()
            .expect("SpdkPoller not started — was init() called?")
            .is_sleeping_arc()
    }

    // Handle tracking, which is used by SpdkBdev open/drop
    /// Register SpdkBdev handle. Returns Err if not Initialized.
    pub fn acquire_handle(&self) -> CommonResult<()> {
        // Increment first so shutdown() sees us in the drain loop.
        self.open_handles.fetch_add(1, Ordering::AcqRel);
        // Now verify the state — if shutdown already won the CAS, undo.
        if self.state() != SpdkEnvState::Initialized {
            self.open_handles.fetch_sub(1, Ordering::AcqRel);
            return err_box!(
                "Cannot open SpdkBdev: SPDK environment is in state {} (expected Initialized)",
                self.state()
            );
        }
        Ok(())
    }
    /// Unregister SpdkBdev handle.
    pub fn release_handle(&self) {
        self.open_handles.fetch_sub(1, Ordering::AcqRel);
    }

    /// Borrow qpair from pool for controller.
    pub fn acquire_qpair(
        &self,
        ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
    ) -> CommonResult<*mut spdk_ffi::spdk_nvme_qpair> {
        self.qpair_pool.acquire(ctrlr)
    }
    /// Return qpair to pool, or free if at capacity.
    pub fn release_qpair(
        &self,
        ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
        qpair: *mut spdk_ffi::spdk_nvme_qpair,
    ) {
        self.qpair_pool.release(ctrlr, qpair);
    }

    /// Unregister qpair from poller, blocking until removed to prevent UAF.
    /// Returns false if poller didn't ack within timeout (likely stuck/dead).
    pub fn unregister_qpair_from_poller(&self, qpair: *mut spdk_ffi::spdk_nvme_qpair) -> bool {
        let poller = self.poller.lock().unwrap();
        if let Some(poller) = poller.as_ref() {
            poller.unregister_qpair(qpair)
        } else {
            false
        }
    }

    // SPDK FFI — feature-gated

    fn env_init(&self) -> CommonResult<()> {
        use std::ffi::CString;
        let iova_mode_label = if self.conf.iova_mode.is_empty() {
            "auto"
        } else {
            self.conf.iova_mode.as_str()
        };
        info!(
            "SPDK env_init: app={}, hugepage={}MB, mask={}, iova_mode={}",
            self.conf.app_name, self.conf.hugepage_mb, self.conf.reactor_mask, iova_mode_label
        );
        let app_name = CString::new(self.conf.app_name.as_str())
            .map_err(|e| err_msg!(format!("invalid app_name: {}", e)))?;
        let core_mask = CString::new(self.conf.reactor_mask.as_str())
            .map_err(|e| err_msg!(format!("invalid reactor_mask: {}", e)))?;
        let iova_mode = (!self.conf.iova_mode.is_empty())
            .then(|| {
                CString::new(self.conf.iova_mode.as_str())
                    .map_err(|e| err_msg!(format!("invalid iova_mode: {}", e)))
            })
            .transpose()?;
        unsafe {
            // Verify our opaque buffer is large enough.
            let real_size = spdk_ffi::curvine_spdk_env_opts_sizeof();
            let buf_size = std::mem::size_of::<spdk_ffi::spdk_env_opts>();
            if real_size > buf_size {
                return err_box!(
                    "spdk_env_opts is {} bytes but our buffer is only {}",
                    real_size,
                    buf_size
                );
            }

            let mut opts: spdk_ffi::spdk_env_opts = std::mem::zeroed();
            spdk_ffi::curvine_spdk_env_opts_init(&mut opts);
            // Set our fields via C helpers — these write at the correct
            // C struct offsets regardless of SPDK version.
            spdk_ffi::curvine_spdk_env_opts_set_name(&mut opts, app_name.as_ptr());
            spdk_ffi::curvine_spdk_env_opts_set_core_mask(&mut opts, core_mask.as_ptr());
            spdk_ffi::curvine_spdk_env_opts_set_mem_size(&mut opts, self.conf.hugepage_mb as i32);
            if let Some(ref mode) = iova_mode {
                spdk_ffi::curvine_spdk_env_opts_set_iova_mode(&mut opts, mode.as_ptr());
            }
            let rc = spdk_ffi::curvine_spdk_env_init(&mut opts);
            if rc != 0 {
                return err_box!("spdk_env_init failed with rc={}", rc);
            }
            // Register NVMe transports (TCP, RDMA, PCIe).
            // Must be called after env_init but before nvme_connect.
            spdk_ffi::curvine_spdk_register_transports();
        }
        info!("SPDK environment initialized successfully");
        Ok(())
    }

    fn attach_controller(&self, target: &NvmeTarget) -> CommonResult<Vec<BdevInfo>> {
        use std::ffi::CString;
        info!("Attaching NVMe-oF controller: {}", target.endpoint());
        let traddr = CString::new(target.traddr.as_str())
            .map_err(|e| err_msg!(format!("invalid traddr: {}", e)))?;
        let trsvcid = CString::new(target.trsvcid.to_string())
            .map_err(|e| err_msg!(format!("invalid trsvcid: {}", e)))?;
        let subnqn = CString::new(target.subnqn.as_str())
            .map_err(|e| err_msg!(format!("invalid subnqn: {}", e)))?;
        let trtype = match target.trtype.to_lowercase().as_str() {
            "rdma" => spdk_ffi::SPDK_NVME_TRANSPORT_RDMA,
            "tcp" => spdk_ffi::SPDK_NVME_TRANSPORT_TCP,
            _ => return err_box!("unsupported transport type: {}", target.trtype),
        };
        let adrfam = match target.adrfam.to_lowercase().as_str() {
            "ipv4" => spdk_ffi::SPDK_NVMF_ADRFAM_IPV4,
            "ipv6" => spdk_ffi::SPDK_NVMF_ADRFAM_IPV6,
            _ => return err_box!("unsupported address family: {}", target.adrfam),
        };
        unsafe {
            // Verify our opaque buffers are large enough for the installed SPDK version.
            // Same pattern as env_init() for spdk_env_opts.
            let trid_real = spdk_ffi::curvine_spdk_trid_sizeof();
            let trid_buf = std::mem::size_of::<spdk_ffi::spdk_nvme_transport_id>();
            if trid_real > trid_buf {
                return err_box!(
                    "spdk_nvme_transport_id is {} bytes but our buffer is only {}",
                    trid_real,
                    trid_buf
                );
            }
            let opts_real = spdk_ffi::curvine_spdk_ctrlr_opts_sizeof();
            let opts_buf = std::mem::size_of::<spdk_ffi::spdk_nvme_ctrlr_opts>();
            if opts_real > opts_buf {
                return err_box!(
                    "spdk_nvme_ctrlr_opts is {} bytes but our buffer is only {}",
                    opts_real,
                    opts_buf
                );
            }
            // Build transport ID via C helpers
            let mut trid: spdk_ffi::spdk_nvme_transport_id = std::mem::zeroed();
            spdk_ffi::curvine_spdk_trid_set_trtype(&mut trid, trtype);
            spdk_ffi::curvine_spdk_trid_set_adrfam(&mut trid, adrfam);
            spdk_ffi::curvine_spdk_trid_set_traddr(&mut trid, traddr.as_ptr());
            spdk_ffi::curvine_spdk_trid_set_trsvcid(&mut trid, trsvcid.as_ptr());
            spdk_ffi::curvine_spdk_trid_set_subnqn(&mut trid, subnqn.as_ptr());
            // Build controller opts via C helpers
            let mut opts: spdk_ffi::spdk_nvme_ctrlr_opts = std::mem::zeroed();
            spdk_ffi::curvine_spdk_ctrlr_get_default_opts(&mut opts);
            if target.io_queues > 0 {
                spdk_ffi::curvine_spdk_ctrlr_opts_set_num_io_queues(&mut opts, target.io_queues);
            }
            let ka_ms = if target.keep_alive_timeout_ms > 0 {
                target.keep_alive_timeout_ms
            } else {
                self.conf.keep_alive_timeout_ms
            };
            spdk_ffi::curvine_spdk_ctrlr_opts_set_keep_alive_timeout_ms(&mut opts, ka_ms as u32);
            if !target.hostnqn.is_empty() {
                let hostnqn = CString::new(target.hostnqn.as_str())
                    .map_err(|e| err_msg!(format!("invalid hostnqn: {}", e)))?;
                spdk_ffi::curvine_spdk_ctrlr_opts_set_hostnqn(&mut opts, hostnqn.as_ptr());
            }
            // Connect
            let ctrlr = spdk_ffi::curvine_spdk_nvme_connect(&mut trid, &mut opts);
            if ctrlr.is_null() {
                return err_box!("spdk_nvme_connect failed for target {}", target.endpoint());
            }
            // Resolve I/O timeout: per-target override or global default
            let io_timeout_ms = if target.io_timeout_ms > 0 {
                target.io_timeout_ms
            } else {
                self.conf.io_timeout_ms
            };
            // Enumerate active namespaces
            let num_ns = spdk_ffi::spdk_nvme_ctrlr_get_num_ns(ctrlr);
            let mut bdevs = Vec::new();
            for nsid in 1..=num_ns {
                let ns = spdk_ffi::spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
                if ns.is_null() {
                    continue;
                }
                if !spdk_ffi::spdk_nvme_ns_is_active(ns) {
                    continue;
                }
                let sector_size = spdk_ffi::spdk_nvme_ns_get_sector_size(ns);
                let num_sectors = spdk_ffi::spdk_nvme_ns_get_num_sectors(ns);
                let size_bytes = sector_size as u64 * num_sectors;
                let name = format!("NVMe_{}_{}_n{}", target.traddr, target.trsvcid, nsid);
                info!(
                    "  Discovered ns {}: size={}B, sector_size={}, sectors={}",
                    nsid, size_bytes, sector_size, num_sectors
                );
                bdevs.push(BdevInfo {
                    name,
                    size_bytes,
                    block_size: sector_size,
                    num_blocks: num_sectors,
                    target_endpoint: target.endpoint(),
                    ctrlr: ctrlr as usize,
                    ns: ns as usize,
                    io_timeout_ms,
                });
            }
            if bdevs.is_empty() {
                warn!(
                    "Controller {} has no active namespaces, detaching controller",
                    target.endpoint()
                );
                unsafe { spdk_ffi::spdk_nvme_detach(ctrlr) };
            }
            Ok(bdevs)
        }
    }

    fn detach_controllers(&self) {
        let mut detached: HashSet<usize> = HashSet::new();
        for bdev in &self.bdevs {
            if bdev.ctrlr == 0 || !detached.insert(bdev.ctrlr) {
                continue; // skip null or already-detached controllers
            }
            let ctrlr = bdev.ctrlr as *mut spdk_ffi::spdk_nvme_ctrlr;
            info!(
                "Detaching NVMe controller for target '{}' (ctrlr={:p})",
                bdev.target_endpoint, ctrlr
            );
            let rc = unsafe { spdk_ffi::spdk_nvme_detach(ctrlr) };
            if rc != 0 {
                warn!(
                    "spdk_nvme_detach failed for '{}', rc={}",
                    bdev.target_endpoint, rc
                );
            }
        }
        info!("Detached {} NVMe controller(s)", detached.len());
    }

    fn env_fini(&self) {
        info!("Finalizing SPDK environment");
        unsafe { spdk_ffi::spdk_env_fini() };
    }
}

// Display

impl Display for SpdkEnv {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SpdkEnv(state={:?}, targets={}, bdevs={}, capacity={})",
            self.state(),
            self.conf.targets.len(),
            self.bdevs.len(),
            self.total_capacity()
        )
    }
}

// Tests
#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    fn fake(id: usize) -> *mut spdk_ffi::spdk_nvme_qpair {
        id as *mut spdk_ffi::spdk_nvme_qpair
    }
    fn push(pool: &QpairPool, key: usize) {
        pool.inner
            .lock()
            .unwrap()
            .entry(key)
            .or_default()
            .push(fake(key));
    }
    fn pop(pool: &QpairPool, key: usize) -> bool {
        pool.inner
            .lock()
            .unwrap()
            .get_mut(&key)
            .and_then(|v| v.pop())
            .is_some()
    }
    fn cnt(pool: &QpairPool, key: usize) -> usize {
        pool.inner.lock().unwrap().get(&key).map_or(0, |v| v.len())
    }
    fn tot(pool: &QpairPool) -> usize {
        pool.inner.lock().unwrap().values().map(|v| v.len()).sum()
    }
    #[test]
    fn roundtrip() {
        let p = QpairPool::new();
        for _ in 0..3 {
            push(&p, 1);
        }
        assert_eq!(cnt(&p, 1), 3);
        for _ in 0..3 {
            assert!(pop(&p, 1));
        }
    }
    #[test]
    fn isolated() {
        let p = QpairPool::new();
        push(&p, 1);
        push(&p, 1);
        push(&p, 2);
        assert_eq!(cnt(&p, 1), 2);
        assert_eq!(cnt(&p, 2), 1);
    }
    #[test]
    fn concurrent() {
        let p = Arc::new(QpairPool::new());
        let h: Vec<_> = (0..8)
            .map(|_| {
                let x = Arc::clone(&p);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        push(&x, 1);
                    }
                    for _ in 0..100 {
                        pop(&x, 1);
                    }
                })
            })
            .collect();
        for t in h {
            t.join().unwrap();
        }
        assert_eq!(tot(&p), 0);
    }

    #[test]
    #[should_panic(expected = "already registered")]
    fn register_limit_rejects_duplicate() {
        let p = QpairPool::new();
        let ctrlr = 0x1000usize as *mut spdk_ffi::spdk_nvme_qpair;

        p.register_limit(ctrlr as usize, 64);
        p.register_limit(ctrlr as usize, 128);
    }

    #[test]
    fn per_controller_active_tracking() {
        let p = QpairPool::new();
        let ctrlr_a = 0x1000usize as *mut spdk_ffi::spdk_nvme_qpair;
        let ctrlr_b = 0x2000usize as *mut spdk_ffi::spdk_nvme_qpair;

        // Register limits: A=2, B=3
        p.register_limit(ctrlr_a as usize, 2);
        p.register_limit(ctrlr_b as usize, 3);

        // Initially zero
        let (active_a, limit_a) = p.controller_stats(ctrlr_a as usize);
        assert_eq!(active_a, 0);
        assert_eq!(limit_a, 2);

        let (active_b, limit_b) = p.controller_stats(ctrlr_b as usize);
        assert_eq!(active_b, 0);
        assert_eq!(limit_b, 3);

        // Unregistered controller returns (0, 0)
        let (active_c, limit_c) = p.controller_stats(0x3000);
        assert_eq!(active_c, 0);
        assert_eq!(limit_c, 0);
    }

    #[test]
    fn try_reserve_respects_limit() {
        let p = QpairPool::new();
        let ctrlr = 0x1000usize as *mut spdk_ffi::spdk_nvme_qpair;
        p.register_limit(ctrlr as usize, 2);

        // Reserve up to limit
        assert!(p.try_reserve(ctrlr as usize));
        assert!(p.try_reserve(ctrlr as usize));
        let (active, _) = p.controller_stats(ctrlr as usize);
        assert_eq!(active, 2);

        // At limit — should fail
        assert!(!p.try_reserve(ctrlr as usize));

        // Release one — should succeed again
        p.release_reservation(ctrlr as usize);
        assert!(p.try_reserve(ctrlr as usize));
    }

    #[test]
    fn release_returns_qpair_to_pool() {
        let p = QpairPool::new();
        let ctrlr = 0x1000usize as *mut spdk_ffi::spdk_nvme_ctrlr;
        let qpair = 0x2000usize as *mut spdk_ffi::spdk_nvme_qpair;

        p.register_limit(ctrlr as usize, 4);

        // Reserve a slot (simulating acquire path)
        assert!(p.try_reserve(ctrlr as usize)); // active = 1
        let (active, _) = p.controller_stats(ctrlr as usize);
        assert_eq!(active, 1);

        // Release — qpair goes to pool, active decrements
        p.release(ctrlr, qpair);

        assert_eq!(cnt(&p, ctrlr as usize), 1);
        let (active, _) = p.controller_stats(ctrlr as usize);
        assert_eq!(active, 0);
    }

    #[test]
    fn release_active_count_decremented() {
        let p = QpairPool::new();
        let ctrlr = 0x1000usize as *mut spdk_ffi::spdk_nvme_ctrlr;

        p.register_limit(ctrlr as usize, 4);

        // Reserve 3 slots
        assert!(p.try_reserve(ctrlr as usize));
        assert!(p.try_reserve(ctrlr as usize));
        assert!(p.try_reserve(ctrlr as usize));
        let (active, _) = p.controller_stats(ctrlr as usize);
        assert_eq!(active, 3);

        // Release all 3 — release() calls release_reservation()
        for i in 0..3 {
            let qpair = (0x2000 + i) as *mut spdk_ffi::spdk_nvme_qpair;
            p.release(ctrlr, qpair);
        }

        assert_eq!(cnt(&p, ctrlr as usize), 3);
        let (active, _) = p.controller_stats(ctrlr as usize);
        assert_eq!(active, 0);

        // Double-release guard on active=0 should warn and not wrap
        p.release_reservation(ctrlr as usize);
        let (active, _) = p.controller_stats(ctrlr as usize);
        assert_eq!(active, 0); // still 0, not usize::MAX
    }

    #[test]
    fn acquire_fast_path_returns_cached_qpair() {
        let p = QpairPool::new();
        let ctrlr = 0x1000usize as *mut spdk_ffi::spdk_nvme_ctrlr;
        p.register_limit(ctrlr as usize, 4);

        // Push a cached qpair to the pool
        push(&p, ctrlr as usize);
        assert_eq!(cnt(&p, ctrlr as usize), 1);

        // acquire() fast path: try_reserve succeeds, finds cached qpair, returns it
        let result = p.acquire(ctrlr);
        assert!(result.is_ok());

        // Pool should be empty (qpair popped)
        assert_eq!(cnt(&p, ctrlr as usize), 0);
        // Active should be 1 (reserved + returned)
        let (active, _) = p.controller_stats(ctrlr as usize);
        assert_eq!(active, 1);
    }

    #[test]
    fn acquire_slow_path_increments_active() {
        let p = Arc::new(QpairPool::new());
        let ctrlr = 0x1000usize as *mut spdk_ffi::spdk_nvme_ctrlr;
        p.register_limit(ctrlr as usize, 1);

        // Fill capacity directly
        assert!(p.try_reserve(ctrlr as usize)); // active = 1

        // Spawn a thread that releases after a delay
        let p2 = Arc::clone(&p);
        let ctrlr_key = ctrlr as usize;
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            // Push a cached qpair then release the slot
            push(&p2, ctrlr_key);
            p2.release_reservation(ctrlr_key); // active = 0
            p2.notify.notify_one();
        });

        // acquire() enters slow path (fast path fails, at capacity),
        // waits on condvar, wakes up, reserves slot (active=1),
        // pops cached qpair and returns.
        let result = p.acquire(ctrlr);
        assert!(result.is_ok());

        // Verify active count is correct (1, not 0 as with the old bug)
        let (active, _) = p.controller_stats(ctrlr as usize);
        assert_eq!(active, 1);

        handle.join().unwrap();
    }

    #[test]
    fn acquire_rejects_after_shutdown() {
        let p = QpairPool::new();
        let ctrlr = 0x1000usize as *mut spdk_ffi::spdk_nvme_ctrlr;
        p.register_limit(ctrlr as usize, 1);

        // Fill capacity so acquire enters slow path
        assert!(p.try_reserve(ctrlr as usize)); // active = 1

        // Simulate shutdown
        p.drain_all();

        // acquire should fail immediately, not block for 30s
        let result = p.acquire(ctrlr);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("shutdown"),
            "error should mention shutdown: {}",
            msg
        );
    }

    #[test]
    fn acquire_rejects_after_shutdown_with_capacity() {
        let p = QpairPool::new();
        let ctrlr = 0x1000usize as *mut spdk_ffi::spdk_nvme_ctrlr;
        p.register_limit(ctrlr as usize, 4);

        // Shutdown — should reject even though capacity is available (active=0 < max_active=4)
        p.drain_all();

        // Fast path: try_reserve succeeds (active < max_active), but shutdown is set.
        let result = p.acquire(ctrlr);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("shutdown"),
            "error should mention shutdown: {}",
            msg
        );

        // Active should be 0 (reservation was released)
        let (active, _) = p.controller_stats(ctrlr as usize);
        assert_eq!(active, 0);
    }

    #[test]
    fn acquire_times_out_after_configured_timeout() {
        let p = QpairPool::with_qpair_acquire_timeout(Duration::from_millis(200));
        let ctrlr = 0x1000usize as *mut spdk_ffi::spdk_nvme_ctrlr;
        p.register_limit(ctrlr as usize, 1);

        // Fill capacity so acquire blocks on the slow path
        assert!(p.try_reserve(ctrlr as usize)); // active = 1

        // No one releases - acquire must return Err after the configured timeout.
        let start = Instant::now();
        let result = p.acquire(ctrlr);
        let elapsed = start.elapsed();

        let msg = result.expect_err("acquire should time out").to_string();
        assert!(
            msg.contains("timeout"),
            "error should mention timeout: {}",
            msg
        );
        assert!(
            elapsed >= Duration::from_millis(200),
            "acquire returned after {:?}, before the configured timeout",
            elapsed
        );

        // The test's own reservation (try_reserve above) remains held - the
        // timed-out acquire never reserved, so it must not over-release.
        let (active, _) = p.controller_stats(ctrlr as usize);
        assert_eq!(active, 1);
    }

    #[test]
    fn acquire_respects_default_timeout() {
        let p = QpairPool::new();
        assert_eq!(p.qpair_acquire_timeout, Duration::from_secs(30));
    }

    mod config_tests {
        use super::*;

        #[test]
        fn init_parses_global_strings() {
            let mut conf = SpdkConf {
                io_timeout_str: "15s".into(),
                keep_alive_timeout_str: "300s".into(),
                qpair_acquire_timeout_str: "45s".into(),
                ..Default::default()
            };
            conf.init().unwrap();
            assert_eq!(conf.io_timeout_ms, 15_000);
            assert_eq!(conf.keep_alive_timeout_ms, 300_000);
            assert_eq!(conf.qpair_acquire_timeout_ms, 45_000);
        }

        #[test]
        fn init_preserves_defaults_when_strings_empty() {
            let mut conf = SpdkConf::default();
            // io_timeout_str, keep_alive_timeout_str and qpair_acquire_timeout_str are already ""
            conf.init().unwrap();
            assert_eq!(conf.io_timeout_ms, 30_000);
            assert_eq!(conf.keep_alive_timeout_ms, 10_000);
            assert_eq!(conf.qpair_acquire_timeout_ms, 30_000);
        }

        #[test]
        fn init_parses_target_overrides() {
            let mut conf = SpdkConf::default();
            conf.targets.push(NvmeTarget {
                io_timeout_str: "10s".into(),
                keep_alive_timeout_str: "30s".into(),
                ..Default::default()
            });
            conf.init().unwrap();
            assert_eq!(conf.targets[0].io_timeout_ms, 10_000);
            assert_eq!(conf.targets[0].keep_alive_timeout_ms, 30_000);
        }

        #[test]
        fn init_keeps_target_zero_when_not_set() {
            let mut conf = SpdkConf::default();
            conf.targets.push(NvmeTarget::default());
            conf.init().unwrap();
            // 0 means "inherit from global" at runtime
            assert_eq!(conf.targets[0].io_timeout_ms, 0);
            assert_eq!(conf.targets[0].keep_alive_timeout_ms, 0);
        }

        #[test]
        fn validate_rejects_low_keep_alive() {
            let conf = SpdkConf {
                enabled: true,
                poll_interval_ms: 100,
                targets: vec![NvmeTarget {
                    traddr: "10.0.0.1".into(),
                    trsvcid: 4420,
                    subnqn: "nqn.test".into(),
                    keep_alive_timeout_ms: 50, // below poll_interval_ms
                    ..Default::default()
                }],
                ..Default::default()
            };
            let result = conf.validate();
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("keep_alive_timeout_ms"));
        }

        #[test]
        fn validate_accepts_high_keep_alive() {
            let conf = SpdkConf {
                enabled: true,
                poll_interval_ms: 100,
                targets: vec![NvmeTarget {
                    traddr: "10.0.0.1".into(),
                    trsvcid: 4420,
                    subnqn: "nqn.test".into(),
                    keep_alive_timeout_ms: 300, // >= 3 * poll_interval_ms
                    ..Default::default()
                }],
                ..Default::default()
            };
            let result = conf.validate();
            assert!(result.is_ok());
        }

        #[test]
        fn validate_rejects_inherited_low_keep_alive() {
            let conf = SpdkConf {
                enabled: true,
                poll_interval_ms: 100,
                keep_alive_timeout_ms: 50, // global is too low
                targets: vec![NvmeTarget {
                    traddr: "10.0.0.1".into(),
                    trsvcid: 4420,
                    subnqn: "nqn.test".into(),
                    keep_alive_timeout_ms: 0, // inherit global
                    ..Default::default()
                }],
                ..Default::default()
            };
            let result = conf.validate();
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("keep_alive_timeout_ms"));
        }

        #[test]
        fn validate_rejects_zero_qpair_acquire_timeout() {
            let conf = SpdkConf {
                enabled: true,
                qpair_acquire_timeout_ms: 0,
                ..Default::default()
            };
            let result = conf.validate();
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("qpair_acquire_timeout"));
        }

        #[test]
        fn toml_parsing_with_target_overrides() {
            let toml = r#"
                enabled = true
                app_name = "curvine"
                hugepage = "2048MB"
                reactor_mask = "0x3"
                io_queue_depth = 256
                io_queue_requests = 512
                io_timeout = "60s"
                io_retry_count = 4
                keep_alive_timeout = "120s"
                qpair_acquire_timeout = "45s"
                poll_interval = 50
                spin_iter = 2000
                dma_buffer_size = "2MB"
                [[targets]]
                trtype = "rdma"
                traddr = "10.0.0.1"
                trsvcid = 4420
                subnqn = "nqn.test:sub1"
                io_timeout = "15s"
                keep_alive_timeout = "300s"
                io_queues = 1024
                [[targets]]
                trtype = "tcp"
                traddr = "10.0.0.2"
                trsvcid = 4420
                subnqn = "nqn.test:sub2"
            "#;
            let mut conf: SpdkConf = toml::from_str(toml).unwrap();
            conf.init().unwrap();

            assert_eq!(conf.hugepage_mb, 2048);
            assert_eq!(conf.io_timeout_ms, 60_000);
            assert_eq!(conf.keep_alive_timeout_ms, 120_000);
            assert_eq!(conf.qpair_acquire_timeout_ms, 45_000);
            assert_eq!(conf.poll_interval_ms, 50);
            assert_eq!(conf.dma_buffer_bytes, 2 * 1024 * 1024);

            assert_eq!(conf.targets[0].io_timeout_ms, 15_000);
            assert_eq!(conf.targets[0].keep_alive_timeout_ms, 300_000);

            assert_eq!(conf.targets[1].io_timeout_ms, 0);
            assert_eq!(conf.targets[1].keep_alive_timeout_ms, 0);
        }

        #[test]
        fn toml_uses_global_defaults_when_omitted() {
            let mut conf: SpdkConf = toml::from_str("enabled = true").unwrap();
            conf.init().unwrap();
            assert_eq!(conf.io_timeout_ms, 30_000);
            assert_eq!(conf.keep_alive_timeout_ms, 10_000);
            assert_eq!(conf.qpair_acquire_timeout_ms, 30_000);
            assert_eq!(conf.poll_interval_ms, 100);
            assert!(conf.hugepage_mb > 0);
            assert!(conf.iova_mode.is_empty());
        }

        #[test]
        fn iova_mode_empty_means_auto_detect() {
            let conf = SpdkConf::default();
            assert!(conf.iova_mode.is_empty());
        }

        #[test]
        fn validate_rejects_unknown_iova_mode() {
            let mut conf = SpdkConf {
                enabled: true,
                iova_mode: "dc".into(),
                targets: vec![NvmeTarget {
                    traddr: "10.0.0.1".into(),
                    trsvcid: 4420,
                    subnqn: "nqn.test".into(),
                    ..Default::default()
                }],
                ..Default::default()
            };
            conf.init().unwrap();
            assert!(conf.validate().is_err());
        }

        #[test]
        fn validate_accepts_va_and_pa_iova_mode() {
            for mode in ["va", "pa"] {
                let mut conf = SpdkConf {
                    enabled: true,
                    iova_mode: mode.into(),
                    targets: vec![NvmeTarget {
                        traddr: "10.0.0.1".into(),
                        trsvcid: 4420,
                        subnqn: "nqn.test".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                conf.init().unwrap();
                assert!(conf.validate().is_ok(), "mode={mode}");
            }
        }

        #[test]
        fn new_calls_init_before_validate() {
            let conf = SpdkConf {
                enabled: true,
                io_timeout_str: "15s".into(),
                keep_alive_timeout_str: "300s".into(),
                hugepage_str: "512MB".into(),
                targets: vec![NvmeTarget {
                    traddr: "10.0.0.1".into(),
                    trsvcid: 4420,
                    subnqn: "nqn.test".into(),
                    ..Default::default()
                }],
                ..Default::default()
            };
            let env = SpdkEnv::new(conf).unwrap();
            assert_eq!(env.conf.io_timeout_ms, 15_000);
            assert_eq!(env.conf.keep_alive_timeout_ms, 300_000);
            assert!(env.conf.hugepage_mb > 0);
        }
    }
}
