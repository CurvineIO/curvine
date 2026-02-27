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

use crate::util::*;
use clap::Parser;
use curvine_client::unified::{UfsFileSystem, UnifiedFileSystem};
use curvine_common::fs::{FileSystem, Path};
use curvine_common::state::{
    ConsistencyStrategy, MountOptions, MountType, Provider, SetAttrOptsBuilder, StorageType,
    TtlAction, WriteType,
};
use curvine_common::utils::ProtoUtils;
use orpc::common::{ByteUnit, DurationUnit};
use orpc::{err_box, CommonResult};
use std::collections::{HashMap, VecDeque};

#[derive(Parser, Debug)]
pub struct MountCommand {
    /// UFS path to mount
    #[arg(default_value = "")]
    ufs_path: String,

    /// Curvine path to mount to
    #[arg(default_value = "")]
    cv_path: String,

    #[arg(short, long)]
    config: Vec<String>,

    /// Update the mount point config if it already exists
    #[arg(long, default_value_t = false)]
    update: bool,

    #[arg(long, default_value = "cst")]
    mnt_type: String,

    #[arg(long, default_value = "none")]
    consistency_strategy: String,

    #[arg(long, default_value = "7d")]
    ttl_ms: String,

    #[arg(
        long,
        default_value = "delete",
        help = "TTL expiration action when file expires:\n  none - No action\n  delete - Delete file\n  persist - Export to UFS (skip if exists), keep CV cache\n  evict - Export to UFS (skip if exists), delete CV cache\n  flush - Force export to UFS (overwrite), delete CV cache"
    )]
    ttl_action: String,

    #[arg(long)]
    replicas: Option<i32>,

    #[arg(long)]
    block_size: Option<String>,

    #[arg(short, long)]
    storage_type: Option<String>,

    #[arg(
        long,
        default_value = "async_through",
        help = "Write type: cache, through, async_through, cache_through"
    )]
    write_type: String,

    #[arg(
        long,
        help = "UFS provider: auto, oss-hdfs, opendal. Controls which implementation to use for the given scheme."
    )]
    provider: Option<String>,

    #[arg(long, default_value_t = false)]
    check: bool,

    /// Metadata-only resync mode: `curvine mount resync <cv_path>`
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// Traverse directories recursively in resync mode
    #[arg(long, default_value_t = false)]
    recursive: bool,

    #[arg(long, default_value_t = false)]
    verbose: bool,
}

#[derive(Default)]
struct ResyncStats {
    scanned: usize,
    skip_same_mtime: usize,
    skip_ufs_time_zero: usize,
    recreated: usize,
    ufs_missing: usize,
    failed: usize,
}

impl MountCommand {
    pub async fn execute(&self, fs: UnifiedFileSystem) -> CommonResult<()> {
        if self.ufs_path.trim() == "resync" {
            return self.execute_resync(fs).await;
        }

        // If no path argument is given, all mount points are listed.
        if self.ufs_path.trim().is_empty() && self.cv_path.trim().is_empty() {
            let rep = handle_rpc_result(fs.fs_client().get_mount_table()).await;
            if self.check {
                if rep.mount_table.is_empty() {
                    println!("Mount Table: (empty)");
                    return Ok(());
                }
                let mut status = vec![];
                let mut max_len = 8;
                for mnt_proto in &rep.mount_table {
                    let mnt = ProtoUtils::mount_info_from_pb(mnt_proto.clone());
                    let ufs_path = Path::from_str(&mnt.ufs_path)?;
                    max_len = max_len.max(ufs_path.to_string().len());
                    max_len = max_len.max(mnt.cv_path.len());
                    max_len = max_len.max(mnt.mount_id.to_string().len());
                    let res = UfsFileSystem::new(&ufs_path, mnt.properties.clone(), mnt.provider);
                    match res {
                        Err(_) => status.push("Invalid"),
                        Ok(ufs) => {
                            if (ufs.list_status(&ufs_path).await).is_err() {
                                status.push("Invalid");
                            } else {
                                status.push("Valid");
                            }
                        }
                    }
                }
                max_len += 2;
                let separator = format!("{:-<1$}", "", max_len * 4 + 9);
                println!("Mount Table:");
                println!("{}", separator);
                println!(
                    "| {:<width$}| {:<width$}| {:<width$}| {:<width$}|",
                    "ID",
                    "CV Path",
                    "UFS Path",
                    "Status",
                    width = max_len
                );
                println!("{}", separator);
                for mnt in &rep.mount_table {
                    let ufs_path = Path::from_str(&mnt.ufs_path)?;
                    println!(
                        "| {:<width$}| {:<width$}| {:<width$}| {:<width$}|",
                        mnt.mount_id.to_string(),
                        mnt.cv_path,
                        ufs_path.to_string(),
                        status.remove(0),
                        width = max_len
                    );
                    println!("{}", separator);
                }
                println!("Total mount points: {}", rep.mount_table.len());
                return Ok(());
            }
            println!("{}", rep);
            return Ok(());
        }

        if self.cv_path.trim().is_empty() {
            eprintln!("Error: Curvine Path cannot be empty");
            std::process::exit(1);
        }

        if self.ufs_path.trim().is_empty() {
            eprintln!("Error: UFS Path cannot be empty");
            std::process::exit(1);
        }

        println!("Ufs path: {}", self.ufs_path);
        println!("Curvine path: {}", self.cv_path);

        let mut configs = match self.get_config_map() {
            Ok(configs) => configs,
            Err(e) => {
                eprintln!("Error: Invalid config format: {}", e);
                std::process::exit(1);
            }
        };

        if let Some(scheme) = extract_scheme(&self.ufs_path) {
            if scheme == "s3" {
                enrich_s3_configs(&self.ufs_path, &mut configs);
            } else if scheme == "hdfs" {
                enrich_hdfs_configs(&self.ufs_path, &mut configs);
            }
        }

        // Check Kerberos configuration for HDFS
        let kerberos_present = configs.keys().any(|k| k.starts_with("hdfs.kerberos."));
        if kerberos_present {
            let has_ccache = configs.contains_key("hdfs.kerberos.ccache");
            let env_ccache = std::env::var("KRB5CCNAME").is_ok();

            if !has_ccache && !env_ccache {
                eprintln!(
                    "Warning: Kerberos configuration detected but no ticket cache specified."
                );
                eprintln!(
                    "  Please provide 'hdfs.kerberos.ccache' via --config or set KRB5CCNAME environment variable."
                );
                eprintln!(
                    "  You can generate a ticket cache using: kinit -kt /path/to/keytab principal@REALM"
                );
                eprintln!();
            }
        }

        let validation_result = validate_path_and_configs(&self.ufs_path, &configs);
        if let Err(error_msg) = validation_result {
            eprintln!("Error: {}", error_msg);
            std::process::exit(1);
        }

        if !configs.is_empty() {
            println!("Configuration:");
            for (key, value) in &configs {
                println!("  {} = {}", key, value);
            }
            println!("\n");
        }

        let ufs_path = Path::from_str(&self.ufs_path)?;
        let cv_path = Path::from_str(&self.cv_path)?;

        let mnt_opts = self.to_mnt_opts()?;

        let ufs = UfsFileSystem::new(
            &ufs_path,
            mnt_opts.add_properties.clone(),
            mnt_opts.provider,
        )?;
        if let Err(e) = ufs.list_status(&ufs_path).await {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }

        handle_rpc_result(fs.mount(&ufs_path, &cv_path, mnt_opts)).await;
        println!("│ ✅️ mount success.");
        Ok(())
    }

    async fn execute_resync(&self, fs: UnifiedFileSystem) -> CommonResult<()> {
        let cv_root = Path::from_str(&self.cv_path)?;
        if !cv_root.is_cv() {
            return err_box!("resync requires a curvine path, got: {}", self.cv_path);
        }

        let mount = match fs.fs_client().get_mount_info(&cv_root).await? {
            Some(v) => v,
            None => return err_box!("mount info not found for {}", self.cv_path),
        };

        let ufs_root = Path::from_str(&mount.ufs_path)?;
        let ufs = UfsFileSystem::new(&ufs_root, mount.properties.clone(), mount.provider)?;

        let mut stats = ResyncStats::default();
        let mut queue = VecDeque::from([cv_root.clone()]);

        while let Some(path) = queue.pop_front() {
            let status = match fs.cv().get_status(&path).await {
                Ok(v) => v,
                Err(e) => {
                    stats.failed += 1;
                    eprintln!("[resync] failed to get cv status {}: {}", path, e);
                    continue;
                }
            };

            if status.is_dir {
                if self.recursive || path == cv_root {
                    let children = match fs.cv().list_status(&path).await {
                        Ok(v) => v,
                        Err(e) => {
                            stats.failed += 1;
                            eprintln!("[resync] failed to list {}: {}", path, e);
                            continue;
                        }
                    };

                    for child in children {
                        queue.push_back(Path::from_str(child.path)?);
                    }
                }
                continue;
            }

            stats.scanned += 1;
            let cv_ufs_mtime = status.storage_policy.ufs_mtime;

            if cv_ufs_mtime == 0 {
                stats.skip_ufs_time_zero += 1;
                if self.verbose {
                    println!("[resync] skip (ufs_time=0): {}", path);
                }
                continue;
            }

            let ufs_path = mount.get_ufs_path(&path)?;
            let ufs_status = match ufs.get_status(&ufs_path).await {
                Ok(v) => v,
                Err(e) => {
                    stats.ufs_missing += 1;
                    if self.verbose {
                        eprintln!("[resync] ufs missing or unavailable {}: {}", ufs_path, e);
                    }
                    continue;
                }
            };

            if cv_ufs_mtime == ufs_status.mtime {
                stats.skip_same_mtime += 1;
                if self.verbose {
                    println!("[resync] skip (same mtime): {}", path);
                }
                continue;
            }

            if self.verbose || self.dry_run {
                println!(
                    "[resync] recreate {} (cv_ufs_mtime={}, ufs_mtime={})",
                    path, cv_ufs_mtime, ufs_status.mtime
                );
            }

            if self.dry_run {
                stats.recreated += 1;
                continue;
            }

            if let Err(e) = fs.cv().delete(&path, false).await {
                stats.failed += 1;
                eprintln!("[resync] failed to delete {}: {}", path, e);
                continue;
            }

            let mut create_opts = mount.get_create_opts(&fs.conf().client);
            create_opts.storage_policy.ufs_mtime = ufs_status.mtime;

            if let Err(e) = fs.cv().create_with_opts(&path, create_opts, true).await {
                stats.failed += 1;
                eprintln!("[resync] failed to create {}: {}", path, e);
                continue;
            }

            let attr_opts = SetAttrOptsBuilder::new()
                .mtime(ufs_status.mtime)
                .ufs_mtime(ufs_status.mtime)
                .build();

            if let Err(e) = fs.cv().set_attr(&path, attr_opts).await {
                stats.failed += 1;
                eprintln!("[resync] failed to set attr {}: {}", path, e);
                continue;
            }

            stats.recreated += 1;
        }

        println!(
            "resync summary: scanned={}, skip_same_mtime={}, skip_ufs_time_zero={}, recreated={}, ufs_missing={}, failed={}",
            stats.scanned,
            stats.skip_same_mtime,
            stats.skip_ufs_time_zero,
            stats.recreated,
            stats.ufs_missing,
            stats.failed
        );

        Ok(())
    }

    pub fn get_config_map(&self) -> CommonResult<HashMap<String, String>> {
        let mut configs = HashMap::new();
        for pair in &self.config {
            let parts: Vec<&str> = pair.split('=').collect();
            if parts.len() == 2 {
                configs.insert(parts[0].trim().to_string(), parts[1].trim().to_string());
            } else {
                return err_box!("Invalid config format: {}", pair);
            }
        }

        Ok(configs)
    }

    pub fn to_mnt_opts(&self) -> CommonResult<MountOptions> {
        let mnt_type = MountType::try_from(self.mnt_type.as_str())?;
        let consistency_strategy =
            ConsistencyStrategy::try_from(self.consistency_strategy.as_str())?;
        let ttl_ms = DurationUnit::from_str(self.ttl_ms.as_str())?.as_millis() as i64;
        let ttl_action = TtlAction::try_from(self.ttl_action.as_str())?;
        let conf_map = self.get_config_map()?;

        let mut opts = MountOptions::builder()
            .update(self.update)
            .set_properties(conf_map)
            .mount_type(mnt_type)
            .consistency_strategy(consistency_strategy)
            .ttl_ms(ttl_ms)
            .ttl_action(ttl_action);

        if let Some(replicas) = self.replicas {
            opts = opts.replicas(replicas);
        }
        if let Some(block_size) = self.block_size.as_ref() {
            opts = opts.block_size(ByteUnit::from_str(block_size.as_str())?.as_byte() as i64);
        }
        if let Some(storage_type) = self.storage_type.as_ref() {
            opts = opts.storage_type(StorageType::try_from(storage_type.as_str())?);
        }

        let write_type = WriteType::try_from(self.write_type.as_str())?;
        opts = opts.write_type(write_type);

        if let Some(provider_str) = self.provider.as_ref() {
            let provider = Provider::try_from(provider_str.as_str())?;
            opts = opts.provider(provider);
        }

        Ok(opts.build())
    }
}
