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

use crate::fs::Path;
use curvine_common_macros::ClientCliArgs;
use curvine_core_error::{err_box, try_err, CommonResult};
use curvine_runtime::common::{DurationUnit, FileUtils, LogConf, Utils};
use curvine_sys as sys;
use curvine_sys::{CString, FFIUtils};
use serde::{Deserialize, Serialize};
use std::ffi::c_char;
use std::path::PathBuf;
use std::time::Duration;

// fuse configuration file.
//
// Caching in curvine-fuse spans three layers that are easy to conflate; the
// boundaries are:
//
// 1. Kernel-side caching (the kernel caches on our behalf, controlled via the
//    FUSE reply `entry_valid` / `attr_valid` fields):
//    - `entry_timeout_ms`   -> how long the kernel trusts a name->inode lookup.
//    - `negative_timeout_ms`-> how long the kernel caches a negative lookup (ENOENT).
//    - `attr_timeout_ms`    -> how long the kernel trusts cached file/dir attributes.
//    These trade metadata freshness for fewer upcalls into user space.
//
// 2. User-side caching (maintained inside curvine-fuse itself):
//    - `enable_meta_cache` / `meta_cache_timeout` -> the userspace metadata
//      cache (inode validity / directory-scan results) kept in `NodeState`.
//    - `meta_cache_ttl` (derived from `meta_cache_timeout` in `init()`) -> TTL
//      for metadata cache entries.
//    - `node_cache_timeout` -> TTL-based eviction of the inode/node map.
//
// 3. IO caching / data-path switches (control how file *data* is cached by the
//    page cache, mutually interacting per open):
//    - `direct_io`            -> bypass the page cache for all opens.
//    - `open_direct_on_stale` -> per-open fallback to direct I/O only when the
//      local metadata is detected stale (weaker global impact than `direct_io`).
//    - `write_back_cache`     -> let the kernel buffer writes (write-back) vs
//      write-through; conflicts with `direct_io` and is rejected by `init()`.
//
// Rule of thumb: layer 1 tunes how stale the *kernel* may be, layer 2 tunes the
// process-local metadata caches, and layer 3 decides whether file *data* flows
// through the page cache at all.
#[derive(Debug, Clone, Serialize, Deserialize, ClientCliArgs)]
// `opt_in` keeps the generated `FuseConfCliOverrides` surface limited to the
// fields explicitly annotated below — exactly the flags `curvine-fuse mount`
// has historically exposed. New tunable-by-CLI fields opt in with one
// `#[client_cli]` attribute instead of a new hand-written override branch.
#[client_cli(opt_in)]
#[serde(default)]
pub struct FuseConf {
    // Whether to output the request response log.
    pub debug: bool,

    pub audit_logging_enabled: bool,

    // Master on/off switch for FUSE metrics instrumentation (request/error/
    // latency/notify series). Read once at startup; when false the reply path
    // takes the legacy zero-cost path and emits no per-request metrics.
    // Defaults to true (Phase 1–3 ship everything enabled).
    //
    // Scope: this is a per-request *emission* switch, not a cardinality/footprint
    // downgrade. The metric families are registered unconditionally at startup
    // (`FuseMetrics::ensure_init`), so when disabled they still appear in the
    // scrape as zero-valued series — this keeps a stable scrape schema and lets
    // the switch flip back on without re-registration. "Disabled" means "no
    // emission", not "no registration".
    #[client_cli]
    pub metrics_enabled: bool,

    #[client_cli]
    pub io_threads: usize,

    #[client_cli]
    pub worker_threads: usize,
    // Mounting path
    #[client_cli]
    pub mnt_path: String,

    // Specify the root path of the mount point to access the file system, default "/"
    #[client_cli]
    pub fs_path: String,

    // Number of mount points
    #[client_cli]
    pub mnt_number: usize,

    // How many tasks can be read and write data at each mount point.
    // `mnt_per_task` alias kept for backward compatibility with pre-rename TOML
    // configs (issue #1023 §2); without it `#[serde(default)]` would silently
    // drop the old key and fall back to the default.
    //
    // Raw user input; `0` means "follow io_threads". Consumers must NOT read
    // this directly — use `FuseConf::effective_tasks_per_mnt()` so the `0`
    // fallback is applied against the (possibly CLI-overridden) io_threads.
    #[serde(alias = "mnt_per_task")]
    // Alias kept so existing Fluid/mount scripts do not fail on upgrade.
    #[client_cli(long = "tasks-per-mnt", alias = "mnt-per-task")]
    pub tasks_per_mnt: usize,

    // Whether to enable the clone fd feature
    #[client_cli]
    pub clone_fd: bool,

    // Fuse request queue size, default is 0
    #[client_cli]
    pub fuse_channel_size: usize,

    // Read and write file request queue size, default is 0
    #[client_cli]
    pub stream_channel_size: usize,

    // Mount options for Curvine's direct Linux FUSE backend. Supported VFS
    // pairs are `ro`/`rw`, `nodev`/`dev`, `nosuid`/`suid`, `noexec`/`exec`,
    // `noatime`/`atime`, and `sync`/`async`; `dirsync` is also supported.
    // Supported FUSE-side options are `allow_other`, `default_permissions`, and
    // `big_write` (negotiated through FUSE_INIT). Opposite options conflict, and
    // explicit `rw` also conflicts with `readonly = true`.
    // Legacy libfuse options `auto_unmount`, `allow_root`, and `max_write` are
    // not supported in this field by the direct mount backend.
    pub fuse_opts: Vec<String>,

    // Mount the whole FUSE filesystem read-only at the kernel level.
    pub readonly: bool,

    // Octal umask applied when synthesizing permission bits for `.`/`..` entries.
    // It does NOT mask persisted file modes (getattr reports the stored mode as-is),
    // nor the create path (which applies the per-request umask from the FUSE
    // request). Default value 022.
    pub umask: u32,

    pub uid: u32,

    pub gid: u32,

    #[client_cli]
    pub web_port: u16,

    // Whether to fill the fuse node id when traversing the directory.
    // When executing list_status, if the node id is not filled, the node id returned to the kernel is in curvine and does not exist in the node cache.
    // file attr has cache time. During the cache time, look up will not be executed. If you access this file, an error will be reported (node ​​does not exist)
    // Setting will be true, which is equivalent to executing a lookup for each node before returning data to the kernel, and there will be no node.
    // The default value is true
    #[client_cli]
    pub read_dir_fill_ino: bool,

    // Name search cache time, in milliseconds.
    // After performing a name search, if the same name is requested again, the kernel will check the cache first.
    // If the buffer record is still valid, the cache result will be returned directly, unlike user space for requests.
    // Default 1000ms (1 second). Sub-second granularity is supported (e.g. 500 = 0.5s).
    #[client_cli]
    pub entry_timeout_ms: u64,

    // The timeout (in milliseconds) of cache negative lookups. This means that if the file does not exist (find returns ENOENT)
    // Then the search will only be redone after the timeout, and the file/directory will be assumed to not exist before this.
    // The default value is 0ms, which means cache negative lookup is disabled.
    #[client_cli]
    pub negative_timeout_ms: u64,

    // Cache time for file and directory attributes, in milliseconds.
    // This means that after a file or directory attribute search, if the same attribute is requested again, the kernel will first check the cache.
    // If the record in the cache is still valid (i.e. the timeout time has not exceeded), the cached result will be returned directly without making a request to the user space again
    // Default is 1000ms (1 second). Sub-second granularity is supported (e.g. 500 = 0.5s).
    #[client_cli]
    pub attr_timeout_ms: u64,

    // Parameters are used to specify whether the file system should remember the opened files and directories.
    // By default, the FUSE file system clears the cache when a file or directory is closed.
    #[client_cli]
    pub remember: bool,

    // The maximum number of concurrent execution of backend tasks in the file system.It directly affects the performance and stability of the file system, and is important especially when dealing with high load or asynchronous I/O scenarios.
    #[client_cli]
    pub max_background: u16,

    #[client_cli]
    pub congestion_threshold: u16,

    // Whether to enable metadata cache
    #[client_cli]
    pub enable_meta_cache: bool,

    // Metadata cache TTL string (parsed into `meta_cache_ttl` by `init()`)
    #[client_cli(long = "meta-cache-ttl")]
    pub meta_cache_timeout: String,
    #[client_cli]
    pub node_cache_timeout: String,

    // File and directory related options
    #[client_cli]
    pub direct_io: bool,

    // When the file is opened and the local metadata (mtime/len) differs from the server,
    // fall back to direct I/O for that open instead of letting the kernel serve stale
    // page-cache data.  This gives stronger per-open consistency at the cost of bypassing
    // the page cache entirely for the affected file descriptor.  Default: false.
    pub open_direct_on_stale: bool,

    #[client_cli]
    pub write_back_cache: bool,

    #[client_cli]
    pub cache_readdir: bool,

    #[client_cli]
    pub non_seekable: bool,

    #[client_cli]
    pub check_permission: bool,

    pub state_dir: String,

    /// Override for the FUSE mount BDI `read_ahead_kb` (in KB).
    ///
    /// Defaults to `None` for all construction paths, including TOML `[fuse]`
    /// tables that omit this field: keep the kernel default and do not write
    /// the BDI sysfs file. A large override (e.g. 1 MiB) inflates sequential
    /// prefetch and causes read amplification under mmap, where the kernel
    /// faults one page at a time but readahead still pulls a wide window.
    ///
    /// When `Some(kb)` with `kb > 0`, curvine-fuse writes the value to
    /// `/sys/class/bdi/<major>:<minor>/read_ahead_kb` after each successful
    /// mount and bumps FUSE init `max_readahead` to at least `kb * 1024` bytes
    /// so the kernel can issue larger sequential read requests. Use
    /// [`FuseConf::DEFAULT_MAX_READAHEAD_KB`] (1024 = 1 MiB) only when the
    /// workload is sequential and mmap read amplification is acceptable.
    ///
    /// Linux only; on other platforms the value is accepted but has no effect.
    pub max_readahead_kb: Option<u32>,

    /// The following are some time types, which are initialized only after init is called.
    #[serde(skip_serializing, skip_deserializing)]
    pub attr_ttl: Duration,

    #[serde(skip_serializing, skip_deserializing)]
    pub entry_ttl: Duration,

    #[serde(skip_serializing, skip_deserializing)]
    pub negative_ttl: Duration,

    #[serde(skip_serializing, skip_deserializing)]
    pub node_cache_ttl: Duration,

    #[serde(skip_serializing, skip_deserializing)]
    pub meta_cache_ttl: Duration,

    #[client_cli]
    pub list_limit: usize,

    /// Whether to use splice (zero-copy) for FUSE data transfer.
    /// When enabled, the receiver uses splice(/dev/fuse → pipe → buf) and the
    /// sender uses vmsplice + splice (pipe → /dev/fuse) for large responses.
    /// When disabled, both use plain read/writev (extra memory copy but no
    /// pipe management overhead). Default: true.
    pub enable_splice: bool,

    pub path_lock_stripes: usize,

    pub log: LogConf,
}

impl FuseConf {
    pub const FS_NAME: &'static str = "curvine-fuse";

    /// Default kernel dentry (name lookup) cache timeout, in milliseconds.
    pub const DEFAULT_ENTRY_TIMEOUT_MS: u64 = 1000;

    /// Default kernel negative-lookup (ENOENT) cache timeout, in milliseconds.
    /// `0` disables negative-lookup caching.
    pub const DEFAULT_NEGATIVE_TIMEOUT_MS: u64 = 0;

    /// Default kernel attribute cache timeout, in milliseconds.
    pub const DEFAULT_ATTR_TIMEOUT_MS: u64 = 1000;

    /// Default umask applied to file-system-generated permission bits (octal 022).
    pub const DEFAULT_UMASK: u32 = 0o22;

    /// Suggested FUSE BDI readahead override for sequential reads: 1 MiB
    /// (`1024` KB). Not applied by default — see [`FuseConf::max_readahead_kb`].
    pub const DEFAULT_MAX_READAHEAD_KB: u32 = 1024;

    pub fn init(&mut self) -> CommonResult<()> {
        if self.io_threads == 0 {
            return err_box!("fuse.io_threads must be > 0");
        }
        if self.mnt_number == 0 {
            return err_box!("fuse.mnt_number must be > 0");
        }
        if self.list_limit == 0 {
            return err_box!("fuse.list_limit must be > 0");
        }
        // path_lock_stripes is used as the modulus in NodeState::lock_path
        // (hash % path_locks.len()); 0 stripes => empty vec => divide-by-zero
        // panic on create/release.
        if self.path_lock_stripes == 0 {
            return err_box!("fuse.path_lock_stripes must be > 0");
        }
        // Upper-bound sanity check on the umask (input hygiene, e.g. an octal
        // value mistakenly written as decimal); the low bits are the meaningful
        // permission mask.
        if self.umask > 0o7777 {
            return err_box!("fuse.umask must be <= 0o7777 (octal file-permission bits)");
        }
        // max_background / congestion_threshold are returned verbatim in the
        // FUSE init reply; a zero window or an inverted threshold is nonsensical
        // to the kernel. Check max_background first: the congestion message
        // references it.
        if self.max_background == 0 {
            return err_box!("fuse.max_background must be > 0");
        }
        if self.congestion_threshold == 0 || self.congestion_threshold > self.max_background {
            return err_box!(
                "fuse.congestion_threshold must be > 0 and <= fuse.max_background \
                 (congestion_threshold = {}, max_background = {})",
                self.congestion_threshold,
                self.max_background
            );
        }

        self.attr_ttl = Duration::from_millis(self.attr_timeout_ms);
        self.entry_ttl = Duration::from_millis(self.entry_timeout_ms);
        self.negative_ttl = Duration::from_millis(self.negative_timeout_ms);
        self.node_cache_ttl = DurationUnit::from_str(&self.node_cache_timeout)?.as_duration();
        self.meta_cache_ttl = DurationUnit::from_str(&self.meta_cache_timeout)?.as_duration();

        // NOTE: `tasks_per_mnt == 0` means "follow io_threads". This is resolved
        // at the consumption point (FuseChannel::new via `effective_tasks_per_mnt`),
        // NOT normalized in place here: init() runs twice (once in
        // ClusterConf::from, once after CLI overrides), and mutating the field on
        // the first pass would freeze it so a later `--io-threads` override is not
        // tracked. Keeping the raw value lets the consumer re-resolve every time.

        let fs_path = Path::from_str(&self.fs_path)?;
        self.fs_path = fs_path.path().to_owned();

        let mnt_path = Path::from_str(&self.mnt_path)?;
        self.mnt_path = mnt_path.path().to_owned();

        if let Some(0) = self.max_readahead_kb {
            return err_box!("fuse.max_readahead_kb must be > 0 when set");
        }

        // direct_io bypasses the page cache; write_back_cache relies on the kernel
        // buffering writes in that same page cache. Enabling both is semantically
        // conflicting, so reject it rather than silently letting one win.
        if self.direct_io && self.write_back_cache {
            return err_box!(
                "fuse.direct_io and fuse.write_back_cache cannot both be enabled: \
                 direct I/O bypasses the page cache that write-back caching depends on"
            );
        }

        Ok(())
    }

    /// Effective per-mount IO task count: `tasks_per_mnt`, or `io_threads` when
    /// `tasks_per_mnt == 0` ("follow io_threads"). Resolved on read rather than
    /// stored, so a CLI `--io-threads` override applied after the config is
    /// loaded is always tracked.
    pub fn effective_tasks_per_mnt(&self) -> usize {
        if self.tasks_per_mnt == 0 {
            self.io_threads
        } else {
            self.tasks_per_mnt
        }
    }

    /// Validates that `state_dir` is — or can be made — a writable directory.
    ///
    /// The SIGUSR1 persist/restore state file lives under `state_dir`
    /// (`FuseSession::state_file`). Runtime `init()` stays lenient and never
    /// touches it, so a bad `state_dir` (missing, not a directory, or
    /// non-writable) would otherwise only fail at persist/restore time —
    /// potentially during a graceful upgrade. `validate-config` calls this to
    /// surface the problem before mount.
    pub fn validate_state_dir(&self) -> CommonResult<()> {
        let path = std::path::Path::new(&self.state_dir);

        if path.exists() {
            if !path.is_dir() {
                return err_box!(
                    "fuse.state_dir '{}' exists but is not a directory",
                    self.state_dir
                );
            }
        } else if let Err(e) = std::fs::create_dir_all(path) {
            return err_box!(
                "fuse.state_dir '{}' does not exist and could not be created: {}",
                self.state_dir,
                e
            );
        }

        // Probe writability by creating (then removing) a temp file. Directory
        // permission bits alone can be misleading (e.g. read-only mounts), so a
        // real create is the reliable check.
        let probe = path.join(format!(".curvine_fuse_state_probe_{}", std::process::id()));
        match std::fs::File::create(&probe) {
            Ok(_) => {
                let _ = std::fs::remove_file(&probe);
                Ok(())
            }
            Err(e) => err_box!("fuse.state_dir '{}' is not writable: {}", self.state_dir, e),
        }
    }

    /// Parses raw cluster TOML text and returns the unrecognized keys found
    /// under the `[fuse]` table. Missing `[fuse]` table yields an empty list.
    ///
    /// Delegates to the workspace-wide [`crate::validation`] audit and filters
    /// to this section, so semantics stay in lockstep with full-document
    /// auditing (same skip-field and alias treatment).
    pub fn unrecognized_fuse_keys_from_toml(raw: &str) -> CommonResult<Vec<String>> {
        Ok(crate::validation::audit_unknown_keys(raw)?
            .into_iter()
            .filter_map(|key| key.strip_prefix("fuse.").map(str::to_string))
            .collect())
    }

    pub fn parse_fuse_opts(&self) -> Vec<CString> {
        let mut opts = vec![];
        opts.push(FFIUtils::new_cs_string("curvine-fuse"));

        for opt in &self.fuse_opts {
            opts.push(FFIUtils::new_cs_string("-o"));
            opts.push(FFIUtils::new_cs_string(opt.as_str()))
        }

        opts
    }

    // Get all mount points.
    pub fn get_all_mnt_path(&self) -> CommonResult<Vec<PathBuf>> {
        let base = self.check_mnt()?;
        // There is only 1 mount point.
        if self.mnt_number <= 1 {
            return Ok(vec![base]);
        }

        let mut res = vec![];
        for i in 0..self.mnt_number {
            let path = base.join(format!("mnt-{}", i));
            if !path.exists() {
                FileUtils::create_dir(&path, false)?;
            }

            //let point = CString::new(path.to_string_lossy().to_string())?;
            res.push(path)
        }

        Ok(res)
    }

    // Check the mount point.
    fn check_mnt(&self) -> CommonResult<PathBuf> {
        let path = PathBuf::from(&self.mnt_path);
        if path.exists() {
            if path.is_file() {
                return err_box!("Mnt {} is not a directory", self.mnt_path);
            }
            let mut read_dir = try_err!(path.read_dir());
            if read_dir.next().is_some() {
                return err_box!("Mnt {} is not empty", self.mnt_path);
            }
        } else {
            FileUtils::create_dir(&path, true)?;
        }

        let path = try_err!(path.canonicalize());
        Ok(path)
    }

    pub fn convert_fuse_args(opts: &[CString]) -> Vec<*const c_char> {
        let args = opts.iter().map(|x| x.as_ptr()).collect();

        args
    }

    fn normalized_fuse_opts(opts: &[String]) -> CommonResult<Vec<String>> {
        let mut normalized = Vec::new();

        for entry in opts {
            for raw_opt in entry.split(',') {
                let raw_opt = raw_opt.trim();
                if raw_opt.is_empty() {
                    return err_box!(
                        "FUSE mount option is empty in '{}'; remove empty comma-separated entries",
                        entry
                    );
                }

                let (name, value) = match raw_opt.split_once('=') {
                    Some((name, value)) => (name.trim(), Some(value.trim())),
                    None => (raw_opt, None),
                };

                let option = match name {
                    "ro"
                    | "rw"
                    | "nodev"
                    | "dev"
                    | "nosuid"
                    | "suid"
                    | "noexec"
                    | "exec"
                    | "noatime"
                    | "atime"
                    | "dirsync"
                    | "sync"
                    | "allow_other"
                    | "default_permissions"
                    | "async"
                    | "big_write" => {
                        if value.is_some() {
                            return err_box!(
                                "FUSE mount option '{}' does not accept a value",
                                raw_opt
                            );
                        }
                        name.to_string()
                    }
                    // These names are known FUSE/libfuse options, but the direct
                    // mount backend cannot implement their required semantics.
                    "allow_root" | "max_write" | "auto_unmount" => {
                        return err_box!(
                            "FUSE mount option '{}' is recognized but not supported by the direct mount backend",
                            raw_opt
                        );
                    }
                    _ => return err_box!("Unknown FUSE mount option '{}'", raw_opt),
                };

                let conflicting_option = match option.as_str() {
                    "ro" => Some("rw"),
                    "rw" => Some("ro"),
                    "nodev" => Some("dev"),
                    "dev" => Some("nodev"),
                    "nosuid" => Some("suid"),
                    "suid" => Some("nosuid"),
                    "noexec" => Some("exec"),
                    "exec" => Some("noexec"),
                    "noatime" => Some("atime"),
                    "atime" => Some("noatime"),
                    "sync" => Some("async"),
                    "async" => Some("sync"),
                    _ => None,
                };
                if let Some(conflicting_option) = conflicting_option {
                    if normalized.iter().any(|item| item == conflicting_option) {
                        return err_box!(
                            "FUSE mount options '{}' and '{}' conflict; specify only one",
                            conflicting_option,
                            raw_opt
                        );
                    }
                }

                if !normalized.contains(&option) {
                    normalized.push(option);
                }
            }
        }

        Ok(normalized)
    }

    fn validate_fuse_opts_against_config(&self, opts: &[String]) -> CommonResult<()> {
        if self.readonly && opts.iter().any(|option| option == "rw") {
            return err_box!(
                "FUSE mount option 'rw' conflicts with fuse.readonly=true; remove 'rw' or disable fuse.readonly"
            );
        }

        Ok(())
    }

    /// Normalize and validate mount options after config-file values, CLI
    /// overrides, and defaults have been merged for the FUSE process. Generic
    /// cluster loading deliberately does not call this method because master and
    /// worker processes do not consume FUSE mount options.
    pub fn normalize_fuse_opts(&mut self) -> CommonResult<()> {
        let opts = Self::normalized_fuse_opts(&self.fuse_opts)?;
        self.validate_fuse_opts_against_config(&opts)?;
        self.fuse_opts = opts;
        Ok(())
    }

    /// Appends options that belong in the FUSE mount data string. Linux VFS
    /// options (`ro`/`rw`, `nodev`/`dev`, `nosuid`/`suid`, `noexec`/`exec`,
    /// `noatime`/`atime`, `dirsync`, and `sync`/`async`) are deliberately handled
    /// as `mount(2)` flags, or as the absence of a flag, by the raw backend.
    pub fn set_fuse_opts(&self, mount_options: &mut String) -> CommonResult<()> {
        let opts = Self::normalized_fuse_opts(&self.fuse_opts)?;
        self.validate_fuse_opts_against_config(&opts)?;
        let mut default_permissions_added = false;

        // The kernel can distinguish an executable load from a normal read while
        // FUSE_OPEN only exposes O_RDONLY for both. Keep permission enforcement in
        // the VFS whenever Curvine permission checks are enabled.
        if self.check_permission {
            mount_options.push_str(",default_permissions");
            default_permissions_added = true;
        }

        for opt in opts {
            match opt.as_str() {
                // VFS options are converted to mount(2) flags in fuse_pure.rs;
                // positive/default forms such as `rw` and `async` mean the
                // corresponding restrictive flag is absent. `big_write` is
                // negotiated through FUSE_INIT and is already in
                // SUPPORTED_INIT_FLAGS. None belongs in kernel mount data.
                "ro" | "rw" | "nodev" | "dev" | "nosuid" | "suid" | "noexec" | "exec"
                | "noatime" | "atime" | "dirsync" | "sync" | "async" | "big_write" => {}
                "default_permissions" => {
                    if !default_permissions_added {
                        mount_options.push_str(",default_permissions");
                        default_permissions_added = true;
                    }
                }
                "allow_other" => {
                    mount_options.push(',');
                    mount_options.push_str(&opt);
                }
                // normalized_fuse_opts rejects unsupported and unknown options.
                _ => unreachable!("validated FUSE mount option: {}", opt),
            }
        }

        Ok(())
    }
}

impl Default for FuseConf {
    fn default() -> Self {
        let mut conf = Self {
            debug: false,
            audit_logging_enabled: false,
            metrics_enabled: true,

            io_threads: 32,
            worker_threads: Utils::worker_threads(32),

            mnt_path: "/curvine-fuse".to_string(),
            fs_path: "/".to_string(),
            mnt_number: 1,
            tasks_per_mnt: 0,
            clone_fd: true,
            fuse_channel_size: 0,
            stream_channel_size: 0,
            fuse_opts: vec![],
            readonly: false,
            umask: Self::DEFAULT_UMASK,
            uid: sys::get_uid(),
            gid: sys::get_gid(),
            read_dir_fill_ino: true,
            entry_timeout_ms: FuseConf::DEFAULT_ENTRY_TIMEOUT_MS,
            negative_timeout_ms: FuseConf::DEFAULT_NEGATIVE_TIMEOUT_MS,
            attr_timeout_ms: FuseConf::DEFAULT_ATTR_TIMEOUT_MS,
            remember: false,
            web_port: crate::ClusterConf::DEFAULT_FUSE_WEB_PORT,

            max_background: 256,
            congestion_threshold: 192,

            enable_meta_cache: false,
            meta_cache_timeout: "60s".to_string(),
            node_cache_timeout: "1h".to_string(),

            direct_io: false,
            open_direct_on_stale: false,
            write_back_cache: false,
            cache_readdir: false,
            non_seekable: false,
            check_permission: true,

            state_dir: std::env::temp_dir().to_string_lossy().to_string(),

            max_readahead_kb: None,
            attr_ttl: Default::default(),
            entry_ttl: Default::default(),
            negative_ttl: Default::default(),
            node_cache_ttl: Default::default(),
            meta_cache_ttl: Default::default(),

            list_limit: 1000,
            enable_splice: true,

            path_lock_stripes: 1024,

            log: LogConf::default(),
        };

        conf.init().unwrap();
        conf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_max_readahead_kb_is_none() {
        let conf = FuseConf::default();
        assert_eq!(conf.max_readahead_kb, None);
    }

    #[test]
    fn init_rejects_zero_io_threads() {
        let mut conf = FuseConf {
            io_threads: 0,
            ..Default::default()
        };
        let err = conf.init().expect_err("zero io_threads must be rejected");
        assert!(
            err.to_string().contains("fuse.io_threads must be > 0"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn init_accepts_positive_io_threads() {
        let mut conf = FuseConf {
            io_threads: 1,
            ..Default::default()
        };
        conf.init().expect("positive io_threads must be accepted");
    }

    #[test]
    fn init_rejects_zero_max_readahead_kb() {
        let mut conf = FuseConf {
            max_readahead_kb: Some(0),
            ..Default::default()
        };
        let err = conf.init().expect_err("zero must be rejected");
        assert!(
            err.to_string().contains("max_readahead_kb"),
            "error message should mention the field, got: {}",
            err
        );
    }

    #[test]
    fn init_accepts_positive_max_readahead_kb() {
        let mut conf = FuseConf {
            max_readahead_kb: Some(1024),
            ..Default::default()
        };
        conf.init().expect("positive value must be accepted");
        assert_eq!(conf.max_readahead_kb, Some(1024));
    }

    #[test]
    fn init_rejects_direct_io_with_write_back_cache() {
        let mut conf = FuseConf {
            direct_io: true,
            write_back_cache: true,
            ..Default::default()
        };
        let err = conf
            .init()
            .expect_err("direct_io + write_back_cache must be rejected");
        assert!(
            err.to_string().contains("direct_io") && err.to_string().contains("write_back_cache"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn init_allows_direct_io_alone() {
        let mut conf = FuseConf {
            direct_io: true,
            write_back_cache: false,
            ..Default::default()
        };
        conf.init().expect("direct_io alone must be accepted");
    }

    #[test]
    fn init_allows_write_back_cache_alone() {
        let mut conf = FuseConf {
            direct_io: false,
            write_back_cache: true,
            ..Default::default()
        };
        conf.init()
            .expect("write_back_cache alone must be accepted");
    }

    #[test]
    fn init_rejects_zero_path_lock_stripes() {
        let mut conf = FuseConf {
            path_lock_stripes: 0,
            ..Default::default()
        };
        let err = conf
            .init()
            .expect_err("zero path_lock_stripes must be rejected");
        assert!(
            err.to_string().contains("path_lock_stripes"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn init_rejects_zero_list_limit() {
        let mut conf = FuseConf {
            list_limit: 0,
            ..Default::default()
        };
        let err = conf.init().expect_err("zero list_limit must be rejected");
        assert!(
            err.to_string().contains("list_limit"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn init_rejects_zero_mnt_number() {
        let mut conf = FuseConf {
            mnt_number: 0,
            ..Default::default()
        };
        let err = conf.init().expect_err("zero mnt_number must be rejected");
        assert!(
            err.to_string().contains("mnt_number"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn init_rejects_zero_max_background() {
        let mut conf = FuseConf {
            max_background: 0,
            ..Default::default()
        };
        let err = conf
            .init()
            .expect_err("zero max_background must be rejected");
        assert!(
            err.to_string().contains("max_background"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn init_rejects_zero_congestion_threshold() {
        let mut conf = FuseConf {
            congestion_threshold: 0,
            ..Default::default()
        };
        let err = conf
            .init()
            .expect_err("zero congestion_threshold must be rejected");
        assert!(
            err.to_string().contains("congestion_threshold"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn init_rejects_congestion_threshold_above_max_background() {
        let mut conf = FuseConf {
            max_background: 100,
            congestion_threshold: 200,
            ..Default::default()
        };
        let err = conf
            .init()
            .expect_err("congestion_threshold > max_background must be rejected");
        assert!(
            err.to_string().contains("congestion_threshold"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn init_accepts_congestion_threshold_equal_max_background() {
        let mut conf = FuseConf {
            max_background: 200,
            congestion_threshold: 200,
            ..Default::default()
        };
        conf.init()
            .expect("congestion_threshold == max_background must be accepted");
    }

    #[test]
    fn init_rejects_umask_out_of_range() {
        let mut conf = FuseConf {
            umask: 0o10000,
            ..Default::default()
        };
        let err = conf.init().expect_err("umask > 0o7777 must be rejected");
        assert!(
            err.to_string().contains("umask"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn init_accepts_max_umask() {
        let mut conf = FuseConf {
            umask: 0o7777,
            ..Default::default()
        };
        conf.init().expect("umask == 0o7777 must be accepted");
    }

    #[test]
    fn effective_tasks_per_mnt_follows_io_threads_when_zero() {
        // tasks_per_mnt == 0 means "follow io_threads"; resolution reads the
        // current io_threads (so a CLI --io-threads override after init is tracked).
        let mut conf = FuseConf {
            tasks_per_mnt: 0,
            io_threads: 64,
            ..Default::default()
        };
        conf.init().expect("valid conf");
        assert_eq!(conf.effective_tasks_per_mnt(), 64);

        // Simulate a later CLI override of io_threads; re-init is idempotent and
        // the raw tasks_per_mnt is untouched, so resolution tracks the new value.
        conf.io_threads = 32;
        conf.init().expect("valid conf");
        assert_eq!(conf.effective_tasks_per_mnt(), 32);
    }

    #[test]
    fn effective_tasks_per_mnt_preserves_explicit_value() {
        let mut conf = FuseConf {
            tasks_per_mnt: 5,
            io_threads: 64,
            ..Default::default()
        };
        conf.init().expect("valid conf");
        // Explicit non-zero value is kept regardless of io_threads.
        assert_eq!(conf.effective_tasks_per_mnt(), 5);
        assert_eq!(
            conf.tasks_per_mnt, 5,
            "raw tasks_per_mnt must not be mutated"
        );
    }

    #[test]
    fn toml_round_trip_with_max_readahead_kb() {
        let toml = r#"
max_readahead_kb = 1024
"#;
        let conf: FuseConf = toml::from_str(toml).expect("parse");
        assert_eq!(conf.max_readahead_kb, Some(1024));
    }

    #[test]
    fn toml_omitted_max_readahead_kb_uses_default() {
        let conf: FuseConf = toml::from_str("io_threads = 16").expect("parse partial");
        assert_eq!(conf.max_readahead_kb, None);
    }

    #[test]
    fn normalize_fuse_opts_splits_comma_separated_options() {
        let mut conf = FuseConf {
            fuse_opts: vec![" allow_other , nodev ".to_string()],
            ..Default::default()
        };

        conf.normalize_fuse_opts()
            .expect("supported options must be accepted");

        assert_eq!(conf.fuse_opts, vec!["allow_other", "nodev"]);
    }

    #[test]
    fn normalize_fuse_opts_deduplicates_options() {
        let mut conf = FuseConf {
            fuse_opts: vec!["allow_other,nodev".to_string(), "nodev".to_string()],
            ..Default::default()
        };

        conf.normalize_fuse_opts()
            .expect("supported options must be accepted");

        assert_eq!(conf.fuse_opts, vec!["allow_other", "nodev"]);
    }

    #[test]
    fn normalize_fuse_opts_rejects_conflicting_sync_and_async_options() {
        let cases: &[&[&str]] = &[
            &["sync,async"],
            &["async,sync"],
            &["sync", "async"],
            &["async", "sync"],
        ];

        for options in cases {
            let mut conf = FuseConf {
                fuse_opts: options.iter().map(|option| option.to_string()).collect(),
                ..Default::default()
            };

            let err = conf
                .normalize_fuse_opts()
                .expect_err("sync and async must not be accepted together");
            let message = err.to_string();
            assert!(message.contains("sync"), "unexpected error: {}", err);
            assert!(message.contains("async"), "unexpected error: {}", err);
            assert!(message.contains("conflict"), "unexpected error: {}", err);
        }
    }

    #[test]
    fn normalize_fuse_opts_rejects_rw_when_readonly_is_enabled() {
        let mut conf = FuseConf {
            readonly: true,
            fuse_opts: vec!["rw".to_string()],
            ..Default::default()
        };

        let err = conf
            .normalize_fuse_opts()
            .expect_err("rw must conflict with fuse.readonly=true");
        let message = err.to_string();
        assert!(message.contains("rw"), "unexpected error: {message}");
        assert!(message.contains("readonly"), "unexpected error: {message}");
        assert!(message.contains("conflict"), "unexpected error: {message}");
    }

    #[test]
    fn normalize_fuse_opts_rejects_unknown_option_with_name() {
        let mut conf = FuseConf {
            fuse_opts: vec!["allow_other,unknown_option".to_string()],
            ..Default::default()
        };

        let err = conf
            .normalize_fuse_opts()
            .expect_err("unknown option must be rejected");
        assert!(
            err.to_string().contains("unknown_option"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn normalize_fuse_opts_rejects_auto_unmount_as_unsupported() {
        let mut conf = FuseConf {
            fuse_opts: vec!["auto_unmount".to_string()],
            ..Default::default()
        };

        let err = conf
            .normalize_fuse_opts()
            .expect_err("auto_unmount is not implemented by the direct backend");
        let message = err.to_string();
        assert!(
            message.contains("auto_unmount"),
            "unexpected error: {}",
            err
        );
        assert!(
            message.contains("direct mount backend"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn normalize_fuse_opts_rejects_unimplemented_options_with_name() {
        for option in ["allow_root", "max_write=131072"] {
            let mut conf = FuseConf {
                fuse_opts: vec![option.to_string()],
                ..Default::default()
            };

            let err = conf
                .normalize_fuse_opts()
                .expect_err("unimplemented option must be rejected");
            let message = err.to_string();
            assert!(
                message.contains(option),
                "error must include '{}': {}",
                option,
                err
            );
            assert!(
                message.contains("direct mount backend"),
                "unexpected error: {}",
                err
            );
        }
    }

    #[test]
    fn vfs_options_do_not_enter_fuse_mount_data() {
        let mut conf = FuseConf {
            readonly: true,
            fuse_opts: vec!["ro,nodev,nosuid,noexec,noatime,dirsync,sync".to_string()],
            check_permission: false,
            ..Default::default()
        };
        conf.init().expect("supported VFS options must be accepted");
        let mut mount_options = String::new();

        conf.set_fuse_opts(&mut mount_options)
            .expect("validated options must build mount data");

        assert!(mount_options.is_empty());
    }

    #[test]
    fn supported_fuse_parameters_enter_mount_data() {
        let mut conf = FuseConf {
            fuse_opts: vec!["allow_other,async,big_write".to_string()],
            check_permission: false,
            ..Default::default()
        };
        conf.init()
            .expect("supported FUSE parameters must be accepted");
        let mut mount_options = String::new();

        conf.set_fuse_opts(&mut mount_options)
            .expect("validated options must build mount data");

        assert!(mount_options.split(',').any(|item| item == "allow_other"));
        assert!(!mount_options
            .split(',')
            .any(|item| matches!(item, "async" | "big_write")));
    }

    #[test]
    fn permission_checks_enable_kernel_default_permissions() {
        let conf = FuseConf {
            check_permission: true,
            ..Default::default()
        };
        let mut mount_options = String::new();
        conf.set_fuse_opts(&mut mount_options)
            .expect("default config must build mount data");

        assert_eq!(
            mount_options
                .split(',')
                .filter(|opt| *opt == "default_permissions")
                .count(),
            1
        );
    }

    #[test]
    fn explicit_default_permissions_is_not_duplicated() {
        let conf = FuseConf {
            check_permission: true,
            fuse_opts: vec!["default_permissions".to_string()],
            ..Default::default()
        };
        let mut mount_options = String::new();
        conf.set_fuse_opts(&mut mount_options)
            .expect("supported option must build mount data");

        assert_eq!(
            mount_options
                .split(',')
                .filter(|opt| *opt == "default_permissions")
                .count(),
            1
        );
    }

    #[test]
    fn disabled_permission_checks_do_not_force_default_permissions() {
        let conf = FuseConf {
            check_permission: false,
            ..Default::default()
        };
        let mut mount_options = String::new();
        conf.set_fuse_opts(&mut mount_options)
            .expect("default config must build mount data");

        assert!(!mount_options
            .split(',')
            .any(|opt| opt == "default_permissions"));
    }

    #[test]
    fn toml_readonly_is_parsed() {
        let conf: FuseConf = toml::from_str("readonly = true").expect("parse");
        assert!(conf.readonly);
    }

    #[test]
    fn toml_fuse_section_omitted_max_readahead_kb_uses_default() {
        #[derive(Deserialize)]
        struct FuseOnly {
            fuse: FuseConf,
        }

        let conf: FuseOnly = toml::from_str(
            r#"
[fuse]
io_threads = 16
"#,
        )
        .expect("parse fuse wrapper");
        assert_eq!(conf.fuse.max_readahead_kb, None);
    }

    #[test]
    fn toml_with_removed_node_cache_size_loads_clean() {
        // node_cache_size was removed as a dead param (issue #1023 §1): the node
        // map is evicted by node_cache_timeout (TTL) only, the capacity was never
        // enforced. FuseConf is #[serde(default)] with no deny_unknown_fields, so
        // legacy TOML carrying this key must still deserialize (key ignored).
        let toml = r#"
io_threads = 16
node_cache_size = 200000
"#;
        let conf: FuseConf =
            toml::from_str(toml).expect("legacy node_cache_size key must be ignored, not rejected");
        assert_eq!(conf.io_threads, 16);
    }

    #[test]
    fn toml_legacy_mnt_per_task_alias_preserved() {
        // mnt_per_task was renamed to tasks_per_mnt (issue #1023 §2). FuseConf is
        // #[serde(default)] without deny_unknown_fields, so without a serde alias the
        // old key would be silently dropped and fall back to the default (0 ->
        // io_threads) — a silent behavioral regression. The alias must preserve the
        // user-set value.
        let toml = r#"
mnt_per_task = 7
"#;
        let conf: FuseConf =
            toml::from_str(toml).expect("legacy mnt_per_task key must deserialize via alias");
        assert_eq!(conf.tasks_per_mnt, 7);
    }

    #[test]
    fn validate_state_dir_accepts_writable_dir() {
        let dir = std::env::temp_dir().join(format!("cv_state_ok_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let conf = FuseConf {
            state_dir: dir.to_string_lossy().to_string(),
            ..Default::default()
        };
        conf.validate_state_dir()
            .expect("writable directory must pass");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validate_state_dir_creates_missing_dir() {
        let dir =
            std::env::temp_dir().join(format!("cv_state_missing_{}/nested", std::process::id()));
        // Ensure it does not pre-exist.
        let _ = std::fs::remove_dir_all(&dir);
        let conf = FuseConf {
            state_dir: dir.to_string_lossy().to_string(),
            ..Default::default()
        };
        conf.validate_state_dir()
            .expect("missing state_dir must be created");
        assert!(dir.is_dir(), "state_dir should have been created");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validate_state_dir_rejects_non_directory() {
        let file = std::env::temp_dir().join(format!("cv_state_file_{}", std::process::id()));
        std::fs::write(&file, b"x").unwrap();
        let conf = FuseConf {
            state_dir: file.to_string_lossy().to_string(),
            ..Default::default()
        };
        let err = conf
            .validate_state_dir()
            .expect_err("a regular file must be rejected");
        assert!(
            err.to_string().contains("is not a directory"),
            "unexpected error: {}",
            err
        );
        let _ = std::fs::remove_file(&file);
    }

    #[test]
    fn unrecognized_fuse_keys_flags_typo_and_legacy() {
        // Keys no FuseConf field consumes are flagged so the user can
        // investigate: the typo (direct_iox) and the dropped legacy param
        // (node_cache_size). The renamed-but-aliased key (mnt_per_task) is
        // still consumed via #[serde(alias)], and a current field (io_threads)
        // likewise — neither is flagged. max_readahead_kb (Option<u32>, None
        // default) is also consumed and must not be flagged.
        let raw = r#"
[fuse]
io_threads = 16
node_cache_size = 200000
mnt_per_task = 7
max_readahead_kb = 1024
direct_iox = true
"#;
        let unknown = FuseConf::unrecognized_fuse_keys_from_toml(raw).unwrap();
        assert_eq!(
            unknown,
            vec!["direct_iox".to_string(), "node_cache_size".to_string()]
        );
    }

    #[test]
    fn unrecognized_fuse_keys_empty_when_all_known() {
        let raw = r#"
[fuse]
io_threads = 16
direct_io = true
state_dir = "/tmp"
"#;
        let unknown = FuseConf::unrecognized_fuse_keys_from_toml(raw).unwrap();
        assert!(unknown.is_empty(), "unexpected unknown keys: {:?}", unknown);
    }

    #[test]
    fn unrecognized_fuse_keys_empty_without_fuse_table() {
        let raw = r#"
cluster_id = "test"
"#;
        let unknown = FuseConf::unrecognized_fuse_keys_from_toml(raw).unwrap();
        assert!(unknown.is_empty());
    }
}
