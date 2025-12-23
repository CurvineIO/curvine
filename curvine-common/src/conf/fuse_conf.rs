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

use orpc::common::{DurationUnit, FileUtils, LogConf, Utils};
use orpc::sys::{CString, FFIUtils};
use orpc::{err_box, sys, try_err, CommonResult};
use serde::{Deserialize, Serialize};
use std::ffi::c_char;
use std::path::PathBuf;
use std::time::Duration;

// fuse configuration file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FuseConf {
    // Whether to output the request response log.
    pub debug: bool,

    pub io_threads: usize,

    pub worker_threads: usize,
    // Mounting path
    pub mnt_path: String,

    // Specify the root path of the mount point to access the file system, default "/"
    pub fs_path: String,

    // Number of mount points
    pub mnt_number: usize,

    // How many tasks can be read and write data at each mount point.
    pub mnt_per_task: usize,

    // Whether to enable the clone fd feature
    pub clone_fd: bool,

    // Fuse request queue size, default is 0
    pub fuse_channel_size: usize,

    // Read and write file request queue size, default is 0
    pub stream_channel_size: usize,

    // Mount the configuration, needs to be passed to the linux kernel.
    pub fuse_opts: Vec<String>,

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

    // Name search cache time.
    // After performing a name search, if the same name is requested again, the kernel will check the cache first.
    // If the buffer record is still valid, the cache result will be returned directly, unlike user space for requests.
    // Default 1.0 seconds
    pub entry_timeout: f64,

    // The timeout (in seconds) of cache negative lookups.This means that if the file does not exist (find returns ENOENT)
    // Then the search will only be redone after the timeout, and the file/directory will be assumed to not exist before this.
    // The default value is 0.0 seconds, which means cache negative lookup is disabled.
    pub negative_timeout: f64,

    // Cache time for file and directory attributes.
    // This means that after a file or directory attribute search, if the same attribute is requested again, the kernel will first check the cache.
    // If the record in the cache is still valid (i.e. the timeout time has not exceeded), the cached result will be returned directly without making a request to the user space again
    // Default is 1.0 seconds.
    pub attr_timeout: f64,

    // Parameters are used to specify the file attribute cache timeout for automatic cache refresh.
    // By default, it is set to the same value as attr_timeout (i.e. 1.0 seconds).
    //
    // When the file is opened, if the automatic cache (auto_cache) needs to refresh the file data, the kernel will check whether the cache of the file attributes has expired.
    // If the cache record of file attributes is still valid (i.e. the timeout time has not exceeded), the automatic cache will refresh the file data.
    pub ac_attr_timeout: f64,

    // Parameters are used to specify the file attribute cache timeout for automatic cache refresh.
    // By default, it is set to the same value as attr_timeout (i.e. 1.0 seconds).
    pub ac_attr_timeout_set: f64,

    // Parameters are used to specify whether the file system should remember the opened files and directories.
    // By default, the FUSE file system clears the cache when a file or directory is closed.
    pub remember: bool,

    // The maximum number of concurrent execution of backend tasks in the file system.It directly affects the performance and stability of the file system, and is important especially when dealing with high load or asynchronous I/O scenarios.
    pub max_background: u16,

    pub congestion_threshold: u16,

    pub node_cache_size: u64,

    pub node_cache_timeout: String,

    // File and directory related options
    pub direct_io: bool,

    pub write_back_cache: bool,

    pub cache_readdir: bool,

    pub non_seekable: bool,

    /// The following are some time types, which are initialized only after init is called.
    #[serde(skip_serializing, skip_deserializing)]
    pub attr_ttl: Duration,

    #[serde(skip_serializing, skip_deserializing)]
    pub entry_ttl: Duration,

    #[serde(skip_serializing, skip_deserializing)]
    pub negative_ttl: Duration,

    #[serde(skip_serializing, skip_deserializing)]
    pub node_cache_ttl: Duration,

    pub log: LogConf,
}

impl FuseConf {
    pub const FS_NAME: &'static str = "curvine-fuse";

    pub const MAX_READ: u32 = 128 * 1024;

    pub const MAX_WRITE: u32 = 128 * 1024;

    pub const MAX_READ_AHEAD: u32 = 128 * 1024;

    pub const TTR_TIMEOUT: f64 = 1.0;

    pub const UMASK: u32 = 0o22;

    pub fn init(&mut self) -> CommonResult<()> {
        self.attr_ttl = Duration::from_secs_f64(self.attr_timeout);
        self.entry_ttl = Duration::from_secs_f64(self.entry_timeout);
        self.negative_ttl = Duration::from_secs_f64(self.negative_timeout);
        self.node_cache_ttl = DurationUnit::from_str(&self.node_cache_timeout)?.as_duration();

        if self.mnt_per_task == 0 {
            self.mnt_per_task = self.io_threads;
        }
        Ok(())
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
        self.fuse_opts.iter().for_each(|opt| match opt.as_str() {
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

            io_threads: 32,
            worker_threads: Utils::worker_threads(32),

            mnt_path: "/curvine-fuse".to_string(),
            fs_path: "/".to_string(),
            mnt_number: 1,
            mnt_per_task: 0,
            clone_fd: true,
            fuse_channel_size: 0,
            stream_channel_size: 0,
            fuse_opts: vec![],
            umask: Self::UMASK,
            uid: sys::get_uid(),
            gid: sys::get_gid(),
            read_dir_fill_ino: true,
            entry_timeout: FuseConf::TTR_TIMEOUT,
            negative_timeout: 0.0,
            attr_timeout: FuseConf::TTR_TIMEOUT,
            ac_attr_timeout: FuseConf::TTR_TIMEOUT,
            ac_attr_timeout_set: FuseConf::TTR_TIMEOUT,
            remember: false,
            web_port: 9002,

            max_background: 256,
            congestion_threshold: 192,

            node_cache_size: 200000,
            node_cache_timeout: "24h".to_string(),

            direct_io: false,
            write_back_cache: false,
            cache_readdir: false,
            non_seekable: false,

            attr_ttl: Default::default(),
            entry_ttl: Default::default(),
            negative_ttl: Default::default(),
            node_cache_ttl: Default::default(),

            log: LogConf::default(),
        };

        conf.init().unwrap();
        conf
    }
}
