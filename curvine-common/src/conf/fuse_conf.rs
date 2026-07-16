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

use crate::conf::ClusterConf;
use crate::fs::Path;
use orpc::common::{DurationUnit, FileUtils, LogConf, Utils};
use orpc::sys::{CString, FFIUtils};
use orpc::{err_box, sys, try_err, CommonResult};
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
//      write-through; interacts with `direct_io` (direct I/O disables it).
//
// Rule of thumb: layer 1 tunes how stale the *kernel* may be, layer 2 tunes the
// process-local metadata caches, and layer 3 decides whether file *data* flows
// through the page cache at all.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub metrics_enabled: bool,

    pub io_threads: usize,

    pub worker_threads: usize,
    // Mounting path
    pub mnt_path: String,

    // Specify the root path of the mount point to access the file system, default "/"
    pub fs_path: String,

    // Number of mount points
    pub mnt_number: usize,

    // How many tasks can be read and write data at each mount point.
    // `mnt_per_task` alias kept for backward compatibility with pre-rename TOML
    // configs (issue #1023 §2); without it `#[serde(default)]` would silently
    // drop the old key and fall back to the default.
    #[serde(alias = "mnt_per_task")]
    pub tasks_per_mnt: usize,

    // Whether to enable the clone fd feature
    pub clone_fd: bool,

    // Fuse request queue size, default is 0
    pub fuse_channel_size: usize,

    // Read and write file request queue size, default is 0
    pub stream_channel_size: usize,

    // Mount the configuration, needs to be passed to the linux kernel.
    pub fuse_opts: Vec<String>,

    // Mount the whole FUSE filesystem read-only at the kernel level.
    pub readonly: bool,

    // Overwrite the permission bits set by the file system in st_mode.
    // The generated permission bit is the missing permission bit in the given umask value.This value is given in octal representation.
    // Default value 022
    pub umask: u32,

    pub uid: u32,

    pub gid: u32,

    pub web_port: u16,

    // Whether to fill the fuse node id when traversing the directory.
    // When executing list_status, if the node id is not filled, the node id returned to the kernel is in curvine and does not exist in the node cache.
    // file attr has cache time. During the cache time, look up will not be executed. If you access this file, an error will be reported (node ​​does not exist)
    // Setting will be true, which is equivalent to executing a lookup for each node before returning data to the kernel, and there will be no node.
    // The default value is true
    pub read_dir_fill_ino: bool,

    // Name search cache time, in milliseconds.
    // After performing a name search, if the same name is requested again, the kernel will check the cache first.
    // If the buffer record is still valid, the cache result will be returned directly, unlike user space for requests.
    // Default 1000ms (1 second). Sub-second granularity is supported (e.g. 500 = 0.5s).
    pub entry_timeout_ms: u64,

    // The timeout (in milliseconds) of cache negative lookups. This means that if the file does not exist (find returns ENOENT)
    // Then the search will only be redone after the timeout, and the file/directory will be assumed to not exist before this.
    // The default value is 0ms, which means cache negative lookup is disabled.
    pub negative_timeout_ms: u64,

    // Cache time for file and directory attributes, in milliseconds.
    // This means that after a file or directory attribute search, if the same attribute is requested again, the kernel will first check the cache.
    // If the record in the cache is still valid (i.e. the timeout time has not exceeded), the cached result will be returned directly without making a request to the user space again
    // Default is 1000ms (1 second). Sub-second granularity is supported (e.g. 500 = 0.5s).
    pub attr_timeout_ms: u64,

    // Parameters are used to specify whether the file system should remember the opened files and directories.
    // By default, the FUSE file system clears the cache when a file or directory is closed.
    pub remember: bool,

    // The maximum number of concurrent execution of backend tasks in the file system.It directly affects the performance and stability of the file system, and is important especially when dealing with high load or asynchronous I/O scenarios.
    pub max_background: u16,

    pub congestion_threshold: u16,

    // Whether to enable metadata cache
    pub enable_meta_cache: bool,

    // Metadata cache TTL string (parsed into `meta_cache_ttl` by `init()`)
    pub meta_cache_timeout: String,
    pub node_cache_timeout: String,

    // File and directory related options
    pub direct_io: bool,

    // When the file is opened and the local metadata (mtime/len) differs from the server,
    // fall back to direct I/O for that open instead of letting the kernel serve stale
    // page-cache data.  This gives stronger per-open consistency at the cost of bypassing
    // the page cache entirely for the affected file descriptor.  Default: false.
    pub open_direct_on_stale: bool,

    pub write_back_cache: bool,

    pub cache_readdir: bool,

    pub non_seekable: bool,

    pub check_permission: bool,

    pub state_dir: String,

    /// Override for the FUSE mount BDI `max_readahead_kb` (in KB).
    ///
    /// Defaults to [`FuseConf::DEFAULT_MAX_READAHEAD_KB`] (1024 = 1 MiB) for
    /// all construction paths, including TOML `[fuse]` tables that omit this
    /// field. When `Some(kb)` with `kb > 0`, curvine-fuse writes the value to
    /// `/sys/class/bdi/<major>:<minor>/max_readahead_kb` after each successful
    /// mount and bumps FUSE init `max_readahead` to at least `kb * 1024` bytes
    /// so the kernel can issue larger sequential read requests.
    ///
    /// Set to `None` programmatically to keep the kernel default (no BDI
    /// override). Linux only; on other platforms the value is accepted but has
    /// no effect.
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

    /// Default FUSE BDI readahead window: 1 MiB (`1024` KB).
    pub const DEFAULT_MAX_READAHEAD_KB: u32 = 1024;

    pub fn init(&mut self) -> CommonResult<()> {
        if self.io_threads == 0 {
            return err_box!("fuse.io_threads must be > 0");
        }

        self.attr_ttl = Duration::from_millis(self.attr_timeout_ms);
        self.entry_ttl = Duration::from_millis(self.entry_timeout_ms);
        self.negative_ttl = Duration::from_millis(self.negative_timeout_ms);
        self.node_cache_ttl = DurationUnit::from_str(&self.node_cache_timeout)?.as_duration();
        self.meta_cache_ttl = DurationUnit::from_str(&self.meta_cache_timeout)?.as_duration();

        if self.tasks_per_mnt == 0 {
            self.tasks_per_mnt = self.io_threads;
        }

        let fs_path = Path::from_str(&self.fs_path)?;
        self.fs_path = fs_path.path().to_owned();

        let mnt_path = Path::from_str(&self.mnt_path)?;
        self.mnt_path = mnt_path.path().to_owned();

        if let Some(0) = self.max_readahead_kb {
            return err_box!("fuse.max_readahead_kb must be > 0 when set");
        }

        Ok(())
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

    /// Canonical TOML key names for `FuseConf` fields, derived by serializing the
    /// default so it cannot drift from the struct definition. `#[serde(skip_*)]`
    /// runtime-only fields (the `*_ttl` durations) are excluded automatically.
    fn known_field_names() -> Vec<String> {
        match toml::Value::try_from(FuseConf::default()) {
            Ok(toml::Value::Table(t)) => t.keys().cloned().collect(),
            _ => vec![],
        }
    }

    /// Returns keys in a raw `[fuse]` TOML table that do not match any current
    /// `FuseConf` field. These are surfaced as warnings by `validate-config`:
    /// they are silently dropped on load (FuseConf is `#[serde(default)]` with
    /// no `deny_unknown_fields`), so a typo would otherwise never take effect
    /// and never be noticed. Result is sorted for stable output.
    ///
    /// Note: a renamed field kept alive by `#[serde(alias)]` (e.g. the old
    /// `mnt_per_task`) is reported here too — its canonical name is what
    /// `FuseConf` exposes — so the value DOES still take effect via the alias.
    /// The warning is intentionally phrased to cover both cases (typo vs.
    /// deprecated/renamed) rather than claiming the setting was ignored.
    pub fn unrecognized_fuse_keys(fuse_table: &toml::value::Table) -> Vec<String> {
        let known: std::collections::HashSet<String> =
            Self::known_field_names().into_iter().collect();

        let mut unknown: Vec<String> = fuse_table
            .keys()
            .filter(|k| !known.contains(k.as_str()))
            .cloned()
            .collect();
        unknown.sort();
        unknown
    }

    /// Parses raw cluster TOML text and returns the unrecognized keys found
    /// under the `[fuse]` table. Missing `[fuse]` table yields an empty list.
    pub fn unrecognized_fuse_keys_from_toml(raw: &str) -> CommonResult<Vec<String>> {
        let value: toml::Value = try_err!(toml::from_str(raw));
        match value.get("fuse").and_then(|v| v.as_table()) {
            Some(t) => Ok(Self::unrecognized_fuse_keys(t)),
            None => Ok(vec![]),
        }
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

    pub fn set_fuse_opts(&self, mount_options: &mut String) {
        let mut ro_added = false;
        if self.readonly {
            mount_options.push_str(",ro");
            ro_added = true;
        }

        self.fuse_opts.iter().for_each(|opt| match opt.as_str() {
            "ro" => {
                if !ro_added {
                    mount_options.push_str(",ro");
                    ro_added = true;
                }
            }
            "default_permissions" => {
                mount_options.push_str(",default_permissions");
            }
            "allow_other" => {
                mount_options.push_str(",allow_other");
            }
            "allow_root" => {
                mount_options.push_str(",allow_root");
            }
            "async" => {
                mount_options.push_str(",async");
            }
            _ => {}
        });
    }

    pub fn auto_umount(&self) -> bool {
        self.fuse_opts.iter().any(|s| s == "auto_unmount")
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
            web_port: ClusterConf::DEFAULT_FUSE_WEB_PORT,

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

            max_readahead_kb: Some(Self::DEFAULT_MAX_READAHEAD_KB),
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
    fn default_max_readahead_kb_is_one_mib() {
        let conf = FuseConf::default();
        assert_eq!(
            conf.max_readahead_kb,
            Some(FuseConf::DEFAULT_MAX_READAHEAD_KB)
        );
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
        assert_eq!(
            conf.max_readahead_kb,
            Some(FuseConf::DEFAULT_MAX_READAHEAD_KB)
        );
    }

    #[test]
    fn readonly_adds_ro_mount_option() {
        let conf = FuseConf {
            readonly: true,
            ..Default::default()
        };
        let mut mount_options = String::new();
        conf.set_fuse_opts(&mut mount_options);
        assert!(mount_options.split(',').any(|opt| opt == "ro"));
    }

    #[test]
    fn readonly_does_not_duplicate_ro_mount_option() {
        let conf = FuseConf {
            readonly: true,
            fuse_opts: vec!["ro".to_string()],
            ..Default::default()
        };
        let mut mount_options = String::new();
        conf.set_fuse_opts(&mut mount_options);

        assert_eq!(
            mount_options.split(',').filter(|opt| *opt == "ro").count(),
            1
        );
    }

    #[test]
    fn toml_readonly_is_parsed() {
        let conf: FuseConf = toml::from_str("readonly = true").expect("parse");
        assert!(conf.readonly);
    }

    #[test]
    fn toml_fuse_section_omitted_max_readahead_kb_uses_default() {
        use crate::conf::ClusterConf;

        let conf: ClusterConf = toml::from_str(
            r#"
[fuse]
io_threads = 16
"#,
        )
        .expect("parse cluster conf");
        assert_eq!(
            conf.fuse.max_readahead_kb,
            Some(FuseConf::DEFAULT_MAX_READAHEAD_KB)
        );
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
        // Every key not matching a current FuseConf field is flagged so the user
        // can investigate: the typo (direct_iox), the dropped legacy param
        // (node_cache_size), and the renamed key still alive via #[serde(alias)]
        // (mnt_per_task). A current field (io_threads) is NOT flagged.
        let raw = r#"
[fuse]
io_threads = 16
node_cache_size = 200000
mnt_per_task = 7
direct_iox = true
"#;
        let unknown = FuseConf::unrecognized_fuse_keys_from_toml(raw).unwrap();
        assert_eq!(
            unknown,
            vec![
                "direct_iox".to_string(),
                "mnt_per_task".to_string(),
                "node_cache_size".to_string(),
            ]
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
