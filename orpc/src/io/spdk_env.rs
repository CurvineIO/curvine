#![cfg(feature = "spdk")]

use crate::err_msg;
use crate::io::spdk_ffi;
use crate::io::spdk_poller::{IoCompletion, IoOp, IoRequest, SpdkPoller};
use crate::{err_box, CommonResult};
use log::{error, info, warn};
use nix::sys::eventfd::EventFd;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

// Qpair pool - reuse NVMe qpairs across handles
/// Lazy allocate, cache on release
pub struct QpairPool {
    inner: Mutex<HashMap<usize, Vec<*mut spdk_ffi::spdk_nvme_qpair>>>,
    max_per_ctrlr: usize,
}
// SAFETY: exclusive ownership
unsafe impl Send for QpairPool {}
unsafe impl Sync for QpairPool {}
impl QpairPool {
    /// Default max idle qpairs per controller
    const DEFAULT_MAX_PER_CTRLR: usize = 16;

    fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            max_per_ctrlr: Self::DEFAULT_MAX_PER_CTRLR,
        }
    }

    /// Acquire qpair - returns cached or allocates new
    fn acquire(
        &self,
        ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
    ) -> CommonResult<*mut spdk_ffi::spdk_nvme_qpair> {
        let key = ctrlr as usize;
        {
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
        }
        // No cached qpair — allocate a new one (outside the lock)
        let qpair = unsafe { spdk_ffi::curvine_spdk_alloc_io_qpair(ctrlr) };
        if qpair.is_null() {
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
    /// Return qpair to pool for reuse
    fn release(
        &self,
        ctrlr: *mut spdk_ffi::spdk_nvme_ctrlr,
        qpair: *mut spdk_ffi::spdk_nvme_qpair,
    ) {
        let key = ctrlr as usize;
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
    }
    /// Free all pooled qpairs
    fn drain_all(&self) {
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

/// Remote NVMe-oF target
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct NvmeTarget {
    pub trtype: String,             // "rdma" or "tcp"
    pub adrfam: String,             // "ipv4" or "ipv6"
    pub traddr: String,             // target IP
    pub trsvcid: u16,               // port
    pub subnqn: String,             // NVMe subsystem NQN
    pub hostnqn: String,            // empty = auto-generated
    pub io_queues: u32,             // 0 = default
    pub keep_alive_timeout_ms: u64, // 0 = use global
}

impl NvmeTarget {
    pub fn new(traddr: &str, trsvcid: u16, subnqn: &str) -> Self {
        Self {
            traddr: traddr.to_string(),
            trsvcid,
            subnqn: subnqn.to_string(),
            ..Default::default()
        }
    }

    /// Validate required fields
    pub fn validate(&self) -> CommonResult<()> {
        if self.traddr.is_empty() {
            return err_box!("NvmeTarget: traddr cannot be empty");
        }
        if self.trsvcid == 0 {
            return err_box!("NvmeTarget: trsvcid cannot be 0");
        }
        if self.subnqn.is_empty() {
            return err_box!("NvmeTarget: subnqn cannot be empty");
        }
        let valid_trtype = ["rdma", "tcp"];
        if !valid_trtype.contains(&self.trtype.to_lowercase().as_str()) {
            return err_box!(
                "NvmeTarget: invalid trtype '{}', expected one of {:?}",
                self.trtype,
                valid_trtype
            );
        }
        Ok(())
    }

    /// Format as `trtype:traddr:trsvcid/subnqn`
    pub fn endpoint(&self) -> String {
        format!(
            "{}://{}:{}/{}",
            self.trtype, self.traddr, self.trsvcid, self.subnqn
        )
    }
}

impl Default for NvmeTarget {
    fn default() -> Self {
        Self {
            trtype: "rdma".to_string(),
            adrfam: "ipv4".to_string(),
            traddr: String::new(),
            trsvcid: 4420,
            subnqn: String::new(),
            hostnqn: String::new(),
            io_queues: 0,
            keep_alive_timeout_ms: 0,
        }
    }
}

impl Display for NvmeTarget {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.endpoint())
    }
}

// SPDK configuration

/// SPDK env config - string fields parsed by init()
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SpdkConf {
    pub enabled: bool, // master switch - false = skip init
    #[serde(default)]
    pub app_name: String, // SPDK EAL name, e.g. "curvine"
    #[serde(alias = "hugepage", default)]
    pub hugepage_str: String, // e.g. "1024MB"
    #[serde(skip)]
    pub hugepage_mb: u32, // parsed by init()
    #[serde(default)]
    pub reactor_mask: String, // hex, e.g. "0x3"
    #[serde(default)]
    pub shm_id: i32, // -1 = single-process
    #[serde(default)]
    pub mem_channel: u32, // 0 = auto-detect
    #[serde(default)]
    pub targets: Vec<NvmeTarget>,
    #[serde(default)]
    pub io_queue_depth: u32,
    #[serde(default)]
    pub io_queue_requests: u32,
    #[serde(alias = "io_timeout", default)]
    pub io_timeout_str: String, // e.g. "30s"
    #[serde(skip)]
    pub io_timeout_us: u64, // parsed by init()
    #[serde(default)]
    pub io_retry_count: u32,
    #[serde(alias = "keep_alive_timeout", default)]
    pub keep_alive_timeout_str: String, // e.g. "10s"
    #[serde(skip)]
    pub keep_alive_timeout_ms: u64, // parsed by init()
    #[serde(alias = "poll_interval", default)]
    pub poll_interval_ms: u64, // default = 1000
    #[serde(alias = "dma_pool_size", default)]
    pub dma_pool_size_str: String, // e.g. "64MB"
    #[serde(skip)]
    pub dma_pool_bytes: u64, // parsed by init()
    pub block_align: u32, // 0 = auto-detect
}

impl SpdkConf {
    /// Parse string fields into computed values
    pub fn init(&mut self) -> CommonResult<()> {
        use crate::common::{ByteUnit, DurationUnit};

        self.hugepage_mb =
            (ByteUnit::from_str(&self.hugepage_str)?.as_byte() / ByteUnit::MB) as u32;

        let io_timeout = DurationUnit::from_str(&self.io_timeout_str)?;
        self.io_timeout_us = io_timeout.as_millis() * 1000;

        let keep_alive = DurationUnit::from_str(&self.keep_alive_timeout_str)?;
        self.keep_alive_timeout_ms = keep_alive.as_millis();

        let dma_pool = ByteUnit::from_str(&self.dma_pool_size_str)?;
        self.dma_pool_bytes = dma_pool.as_byte();

        Ok(())
    }

    /// Validate config
    pub fn validate(&self) -> CommonResult<()> {
        use crate::common::ByteUnit;
        if !self.enabled {
            return Ok(());
        }

        if self.targets.is_empty() {
            return err_box!("SpdkConf: enabled=true but no targets configured");
        }

        // Validate reactor_mask is valid hex
        let mask = self
            .reactor_mask
            .trim_start_matches("0x")
            .trim_start_matches("0X");
        if u64::from_str_radix(mask, 16).is_err() {
            return err_box!(
                "SpdkConf: invalid reactor_mask '{}', expected hex (e.g. '0x3')",
                self.reactor_mask
            );
        }

        // Validate hugepage from source string (skip field is 0 after deserialization)
        let hugepage_mb = if self.hugepage_mb > 0 {
            self.hugepage_mb
        } else {
            // Parse from source string as fallback
            match ByteUnit::from_str(&self.hugepage_str) {
                Ok(v) => (v.as_byte() / ByteUnit::MB) as u32,
                Err(_) => {
                    return err_box!(
                        "SpdkConf: invalid hugepage '{}', expected e.g. '1024MB'",
                        self.hugepage_str
                    );
                }
            }
        };

        if hugepage_mb == 0 {
            return err_box!(
                "SpdkConf: hugepage must be > 0 (got '{}')",
                self.hugepage_str
            );
        }

        if self.io_queue_depth == 0 {
            return err_box!("SpdkConf: io_queue_depth must be > 0");
        }

        if self.io_queue_requests < self.io_queue_depth {
            return err_box!(
                "SpdkConf: io_queue_requests ({}) must be >= io_queue_depth ({})",
                self.io_queue_requests,
                self.io_queue_depth
            );
        }

        for (i, target) in self.targets.iter().enumerate() {
            target.validate().map_err(|e| {
                let msg = format!("SpdkConf: targets[{}]: {}", i, e);
                err_msg!(msg)
            })?;
            if target.keep_alive_timeout_ms > 0
                && target.keep_alive_timeout_ms < self.poll_interval_ms
            {
                return err_box!(
                    "SpdkConf: targets[{}]: keep_alive_timeout_ms ({}) must be >= poll_interval_ms ({})",
                    i,
                    target.keep_alive_timeout_ms,
                    self.poll_interval_ms
                );
            }
        }

        // Validate poller interval is reasonable
        if self.poll_interval_ms == 0 {
            return err_box!("SpdkConf: poll_interval_ms must be > 0");
        }
        if self.poll_interval_ms > i32::MAX as u64 {
            return err_box!(
                "SpdkConf: poll_interval_ms ({}) exceeds i32::MAX ({})",
                self.poll_interval_ms,
                i32::MAX
            );
        }

        Ok(())
    }
}

impl Default for SpdkConf {
    fn default() -> Self {
        Self {
            enabled: false,
            app_name: "curvine".to_string(),
            hugepage_str: "1024MB".to_string(),
            hugepage_mb: 1024,
            reactor_mask: "0x1".to_string(),
            shm_id: -1,
            mem_channel: 0,
            targets: vec![],
            io_queue_depth: 128,
            io_queue_requests: 512,
            io_timeout_str: "30s".to_string(),
            io_timeout_us: 30_000_000,
            io_retry_count: 4,
            keep_alive_timeout_str: "10s".to_string(),
            keep_alive_timeout_ms: 10_000,
            poll_interval_ms: 1000,
            dma_pool_size_str: "64MB".to_string(),
            dma_pool_bytes: 64 * 1024 * 1024,
            block_align: 0,
        }
    }
}

impl Display for SpdkConf {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SpdkConf(enabled={}, hugepage={}MB, reactor_mask={}, targets={})",
            self.enabled,
            self.hugepage_mb,
            self.reactor_mask,
            self.targets.len()
        )
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
    pub ctrlr: usize, // raw pointer for SpdkBdev
    pub ns: usize,    // raw pointer for SpdkBdev
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
    pub fn new(conf: SpdkConf) -> CommonResult<Self> {
        conf.validate()?;

        info!("SpdkEnv created: {}", conf);

        Ok(Self {
            conf,
            state: AtomicU8::new(SpdkEnvState::Created as u8),
            bdevs: Vec::new(),
            open_handles: AtomicUsize::new(0),
            qpair_pool: QpairPool::new(),
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

        // Start the dedicated I/O poller thread
        {
            let poller = SpdkPoller::start(self.conf.poll_interval_ms);
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

    // SPDK FFI — feature-gated

    fn env_init(&self) -> CommonResult<()> {
        use std::ffi::CString;
        info!(
            "SPDK env_init: app={}, hugepage={}MB, mask={}, shm_id={}, mem_ch={}",
            self.conf.app_name,
            self.conf.hugepage_mb,
            self.conf.reactor_mask,
            self.conf.shm_id,
            self.conf.mem_channel
        );
        let app_name = CString::new(self.conf.app_name.as_str())
            .map_err(|e| err_msg!(format!("invalid app_name: {}", e)))?;
        let core_mask = CString::new(self.conf.reactor_mask.as_str())
            .map_err(|e| err_msg!(format!("invalid reactor_mask: {}", e)))?;
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
            spdk_ffi::curvine_spdk_env_opts_set_shm_id(&mut opts, self.conf.shm_id);
            spdk_ffi::curvine_spdk_env_opts_set_mem_channel(
                &mut opts,
                self.conf.mem_channel as i32,
            );
            spdk_ffi::curvine_spdk_env_opts_set_mem_size(&mut opts, self.conf.hugepage_mb as i32);
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
            if target.keep_alive_timeout_ms > 0 {
                spdk_ffi::curvine_spdk_ctrlr_opts_set_keep_alive_timeout_ms(
                    &mut opts,
                    target.keep_alive_timeout_ms as u32,
                );
            }
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
        use std::collections::HashSet;
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
    fn cap() {
        let p = QpairPool {
            inner: Mutex::new(HashMap::new()),
            max_per_ctrlr: 2,
        };
        push(&p, 1);
        push(&p, 1);
        assert!(cnt(&p, 1) >= 2);
    }
    #[test]
    fn drain() {
        let p = QpairPool::new();
        push(&p, 1);
        push(&p, 2);
        p.inner.lock().unwrap().drain();
        assert_eq!(tot(&p), 0);
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
}
