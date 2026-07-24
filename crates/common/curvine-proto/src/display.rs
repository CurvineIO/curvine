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

use crate::{
    GetMountTableResponse, MountResponse, TtlActionProto, UnMountResponse, WriteTypeProto,
};
use std::fmt;
use std::fmt::Display;

fn write_type_to_str(write_type: WriteTypeProto) -> &'static str {
    match write_type {
        WriteTypeProto::CacheMode => "cache_mode",
        WriteTypeProto::FsMode => "fs_mode",
    }
}

fn storage_type_to_str(storage_type: Option<i32>) -> &'static str {
    match storage_type {
        Some(0) => "mem",
        Some(1) => "ssd",
        Some(2) => "hdd",
        Some(3) => "ufs",
        Some(4) => "disk",
        Some(5) => "spdk",
        _ => "-",
    }
}

fn ttl_action_to_str(ttl_action: TtlActionProto) -> &'static str {
    match ttl_action {
        TtlActionProto::None => "none",
        TtlActionProto::Delete => "delete",
        TtlActionProto::Free => "free",
    }
}

fn provider_to_str(provider: Option<i32>) -> &'static str {
    match provider {
        Some(0) => "auto",
        Some(1) => "oss-hdfs",
        Some(2) => "opendal",
        _ => "-",
    }
}

fn access_mode_to_str(access_mode: Option<i32>) -> &'static str {
    match access_mode {
        Some(1) => "read_write",
        _ => "read_only",
    }
}

impl Display for MountResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "│ ✅️ mount success.")?;
        Ok(())
    }
}

impl Display for UnMountResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "│ ✅️ unmount success.")?;
        Ok(())
    }
}

impl Display for GetMountTableResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.mount_table.is_empty() {
            return writeln!(f, "Mount Table: (empty)");
        }

        let mut id_width = 2;
        let mut curvine_width = 12;
        let mut ufs_width = 8;
        let mut write_type_width = 10;
        let mut read_verify_ufs_width = 14;
        let mut auto_cache_width = 10;
        let mut access_mode_width = 11;
        let mut storage_width = 7;
        let mut ttl_action_width = 10;
        let mut provider_width = 8;

        for mnt in &self.mount_table {
            id_width = id_width.max(mnt.mount_id.to_string().len());
            curvine_width = curvine_width.max(mnt.cv_path.len());
            ufs_width = ufs_width.max(mnt.ufs_path.len());
            write_type_width = write_type_width.max(write_type_to_str(mnt.write_type()).len());
            read_verify_ufs_width =
                read_verify_ufs_width.max(if mnt.read_verify_ufs { "yes" } else { "no" }.len());
            auto_cache_width = auto_cache_width.max(
                if mnt.auto_cache.unwrap_or(true) {
                    "yes"
                } else {
                    "no"
                }
                .len(),
            );
            access_mode_width = access_mode_width.max(access_mode_to_str(mnt.access_mode).len());
            storage_width = storage_width.max(storage_type_to_str(mnt.storage_type).len());
            ttl_action_width = ttl_action_width.max(ttl_action_to_str(mnt.ttl_action()).len());
            provider_width = provider_width.max(provider_to_str(mnt.provider).len());
        }

        id_width += 2;
        curvine_width += 2;
        ufs_width += 2;
        write_type_width += 2;
        read_verify_ufs_width += 2;
        auto_cache_width += 2;
        access_mode_width += 2;
        storage_width += 2;
        ttl_action_width += 2;
        provider_width += 2;

        writeln!(f, "Mount Table:")?;

        write!(f, "+")?;
        write!(f, "{:-^width$}+", "", width = id_width)?;
        write!(f, "{:-^width$}+", "", width = curvine_width)?;
        write!(f, "{:-^width$}+", "", width = ufs_width)?;
        write!(f, "{:-^width$}+", "", width = write_type_width)?;
        write!(f, "{:-^width$}+", "", width = read_verify_ufs_width)?;
        write!(f, "{:-^width$}+", "", width = auto_cache_width)?;
        write!(f, "{:-^width$}+", "", width = access_mode_width)?;
        write!(f, "{:-^width$}+", "", width = storage_width)?;
        write!(f, "{:-^width$}+", "", width = ttl_action_width)?;
        writeln!(f, "{:-^width$}+", "", width = provider_width)?;

        write!(f, "|")?;
        write!(f, " {:<width$}|", "ID", width = id_width - 1)?;
        write!(f, " {:<width$}|", "Curvine Path", width = curvine_width - 1)?;
        write!(f, " {:<width$}|", "UFS Path", width = ufs_width - 1)?;
        write!(
            f,
            " {:<width$}|",
            "Write Type",
            width = write_type_width - 1
        )?;
        write!(
            f,
            " {:<width$}|",
            "Read Verify UFS",
            width = read_verify_ufs_width - 1
        )?;
        write!(
            f,
            " {:<width$}|",
            "Auto Cache",
            width = auto_cache_width - 1
        )?;
        write!(
            f,
            " {:<width$}|",
            "Access Mode",
            width = access_mode_width - 1
        )?;
        write!(f, " {:<width$}|", "Storage", width = storage_width - 1)?;
        write!(
            f,
            " {:<width$}|",
            "TTL Action",
            width = ttl_action_width - 1
        )?;
        writeln!(f, " {:<width$}|", "Provider", width = provider_width - 1)?;

        write!(f, "+")?;
        write!(f, "{:-^width$}+", "", width = id_width)?;
        write!(f, "{:-^width$}+", "", width = curvine_width)?;
        write!(f, "{:-^width$}+", "", width = ufs_width)?;
        write!(f, "{:-^width$}+", "", width = write_type_width)?;
        write!(f, "{:-^width$}+", "", width = read_verify_ufs_width)?;
        write!(f, "{:-^width$}+", "", width = auto_cache_width)?;
        write!(f, "{:-^width$}+", "", width = access_mode_width)?;
        write!(f, "{:-^width$}+", "", width = storage_width)?;
        write!(f, "{:-^width$}+", "", width = ttl_action_width)?;
        writeln!(f, "{:-^width$}+", "", width = provider_width)?;

        for mnt in &self.mount_table {
            write!(f, "|")?;
            write!(f, " {:<width$}|", mnt.mount_id, width = id_width - 1)?;
            write!(f, " {:<width$}|", mnt.cv_path, width = curvine_width - 1)?;
            write!(f, " {:<width$}|", mnt.ufs_path, width = ufs_width - 1)?;
            write!(
                f,
                " {:<width$}|",
                write_type_to_str(mnt.write_type()),
                width = write_type_width - 1
            )?;
            write!(
                f,
                " {:<width$}|",
                if mnt.read_verify_ufs { "yes" } else { "no" },
                width = read_verify_ufs_width - 1
            )?;
            write!(
                f,
                " {:<width$}|",
                if mnt.auto_cache.unwrap_or(true) {
                    "yes"
                } else {
                    "no"
                },
                width = auto_cache_width - 1
            )?;
            write!(
                f,
                " {:<width$}|",
                access_mode_to_str(mnt.access_mode),
                width = access_mode_width - 1
            )?;
            write!(
                f,
                " {:<width$}|",
                storage_type_to_str(mnt.storage_type),
                width = storage_width - 1
            )?;
            write!(
                f,
                " {:<width$}|",
                ttl_action_to_str(mnt.ttl_action()),
                width = ttl_action_width - 1
            )?;
            writeln!(
                f,
                " {:<width$}|",
                provider_to_str(mnt.provider),
                width = provider_width - 1
            )?;
        }

        write!(f, "+")?;
        write!(f, "{:-^width$}+", "", width = id_width)?;
        write!(f, "{:-^width$}+", "", width = curvine_width)?;
        write!(f, "{:-^width$}+", "", width = ufs_width)?;
        write!(f, "{:-^width$}+", "", width = write_type_width)?;
        write!(f, "{:-^width$}+", "", width = read_verify_ufs_width)?;
        write!(f, "{:-^width$}+", "", width = auto_cache_width)?;
        write!(f, "{:-^width$}+", "", width = access_mode_width)?;
        write!(f, "{:-^width$}+", "", width = storage_width)?;
        write!(f, "{:-^width$}+", "", width = ttl_action_width)?;
        writeln!(f, "{:-^width$}+", "", width = provider_width)?;

        writeln!(f, "Total mount points: {}", self.mount_table.len())?;

        Ok(())
    }
}
