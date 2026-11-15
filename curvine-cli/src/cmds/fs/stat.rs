use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem};
use curvine_model::FileStatus;
use curvine_runtime::common::{ByteUnit, DurationUnit};
use curvine_unified_fs::UnifiedFileSystem;

#[derive(Subcommand, Debug)]
pub enum StatCommand {
    /// Stat file or directory
    Stat {
        #[clap(help = "Path of the file or directory to stat")]
        path: String,
    },
}

impl StatCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            StatCommand::Stat { path } => {
                let path = CurvineURI::new(path)?;

                match client.get_status(&path).await {
                    Ok(status) => {
                        for line in stat_lines(path.full_path(), &status) {
                            println!("{}", line);
                        }
                        Ok(())
                    }
                    Err(e) => Err(format!("Cannot stat '{}': {}", path.full_path(), e).into()),
                }
            }
        }
    }
}

const TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// The lines `cv fs stat` prints, each value under one label only. A size shows its raw number
/// and its human-readable form on the same line, and a time shows its epoch and formatted form
/// together, so no value is printed under two labels.
fn stat_lines(full_path: &str, status: &FileStatus) -> Vec<String> {
    let policy = &status.storage_policy;

    vec![
        format!("Path: {}", full_path),
        format!("Name: {}", status.name),
        format!("Id: {}", status.id),
        format!("Mtime: {}", epoch_with_time(status.mtime)),
        format!("Atime: {}", epoch_with_time(status.atime)),
        format!("Children num: {}", status.children_num),
        format!("Is complete: {}", status.is_complete),
        format!("Len: {}", bytes_with_unit(status.len)),
        format!("Replicas: {}", status.replicas),
        format!("Block size: {}", bytes_with_unit(status.block_size)),
        format!("File type: {:?}", status.file_type),
        format!("Xattr: {:?}", status.x_attr),
        // Only the policy fields without a line of their own: ttl_ms, ttl_action and ufs_mtime
        // are printed below as Ttl, Ttl action and Ufs Modification time.
        format!(
            "Storage policy: type={:?} state={:?}",
            policy.storage_type, policy.state
        ),
        format!("Owner: {}", status.owner),
        format!("Ttl: {}", DurationUnit::new(policy.ttl_ms as u64)),
        format!("Ttl action: {:?}", policy.ttl_action),
        format!(
            "Ufs Modification time: {}",
            crate::cmds::fs::common::format_epoch_ms_local(policy.ufs_mtime, TIME_FORMAT)
        ),
        format!(
            "Permission: {}{}",
            if status.is_dir { "d" } else { "-" },
            crate::cmds::fs::common::format_permission(status.mode)
        ),
        format!("Group: {}", status.group),
        format!("Nlink: {}", status.nlink),
    ]
}

/// `<epoch ms> (<local time>)`, e.g. `1700000000000 (2023-11-14 22:13:20)`.
fn epoch_with_time(epoch_ms: i64) -> String {
    format!(
        "{} ({})",
        epoch_ms,
        crate::cmds::fs::common::format_epoch_ms_local(epoch_ms, TIME_FORMAT)
    )
}

/// `<bytes> (<human readable>)`, e.g. `1024 (1.0KB)`. Zero reads `0 (0B)` rather than `0 (0)`.
fn bytes_with_unit(bytes: i64) -> String {
    let human = if bytes == 0 {
        "0B".to_string()
    } else {
        ByteUnit::byte_to_string(bytes as u64)
    };
    format!("{} ({})", bytes, human)
}

#[cfg(test)]
mod tests {
    use super::{stat_lines, TIME_FORMAT};
    use crate::cmds::fs::common::format_epoch_ms_local;
    use curvine_model::{
        FileStatus, FileType, StoragePolicy, StorageState, StorageType, TtlAction,
    };

    const MTIME: i64 = 1_700_000_000_000;
    const ATIME: i64 = 1_600_000_000_000;
    const UFS_MTIME: i64 = 1_650_000_000_000;

    /// Times are printed in local time, so the expected text uses the same formatter.
    fn local(epoch_ms: i64) -> String {
        format_epoch_ms_local(epoch_ms, TIME_FORMAT)
    }

    fn file() -> FileStatus {
        FileStatus {
            id: 11,
            path: "/data/a".to_string(),
            name: "a".to_string(),
            is_dir: false,
            mtime: MTIME,
            atime: ATIME,
            children_num: 0,
            is_complete: true,
            len: 1024,
            replicas: 3,
            block_size: 134_217_728,
            file_type: FileType::File,
            storage_policy: StoragePolicy {
                storage_type: StorageType::Ssd,
                ttl_ms: 86_400_000,
                ttl_action: TtlAction::Delete,
                ufs_mtime: UFS_MTIME,
                state: StorageState::Both,
            },
            mode: 0o640,
            owner: "alice".to_string(),
            group: "staff".to_string(),
            nlink: 1,
            ..Default::default()
        }
    }

    fn dir() -> FileStatus {
        FileStatus {
            id: 12,
            path: "/data".to_string(),
            name: "data".to_string(),
            is_dir: true,
            mtime: MTIME,
            atime: ATIME,
            children_num: 5,
            is_complete: true,
            len: 0,
            replicas: 0,
            block_size: 0,
            file_type: FileType::Dir,
            mode: 0o755,
            owner: "bob".to_string(),
            group: "staff".to_string(),
            nlink: 2,
            ..Default::default()
        }
    }

    #[test]
    fn file_output_is_exact() {
        assert_eq!(
            stat_lines("/data/a", &file()),
            vec![
                "Path: /data/a".to_string(),
                "Name: a".to_string(),
                "Id: 11".to_string(),
                format!("Mtime: {MTIME} ({})", local(MTIME)),
                format!("Atime: {ATIME} ({})", local(ATIME)),
                "Children num: 0".to_string(),
                "Is complete: true".to_string(),
                "Len: 1024 (1.0KB)".to_string(),
                "Replicas: 3".to_string(),
                "Block size: 134217728 (128.0MB)".to_string(),
                "File type: File".to_string(),
                "Xattr: {}".to_string(),
                "Storage policy: type=Ssd state=Both".to_string(),
                "Owner: alice".to_string(),
                "Ttl: 1d".to_string(),
                "Ttl action: Delete".to_string(),
                format!("Ufs Modification time: {}", local(UFS_MTIME)),
                "Permission: -rw-r-----".to_string(),
                "Group: staff".to_string(),
                "Nlink: 1".to_string(),
            ]
        );
    }

    #[test]
    fn dir_output_is_exact() {
        assert_eq!(
            stat_lines("/data", &dir()),
            vec![
                "Path: /data".to_string(),
                "Name: data".to_string(),
                "Id: 12".to_string(),
                format!("Mtime: {MTIME} ({})", local(MTIME)),
                format!("Atime: {ATIME} ({})", local(ATIME)),
                "Children num: 5".to_string(),
                "Is complete: true".to_string(),
                "Len: 0 (0B)".to_string(),
                "Replicas: 0".to_string(),
                "Block size: 0 (0B)".to_string(),
                "File type: Dir".to_string(),
                "Xattr: {}".to_string(),
                "Storage policy: type=Disk state=Cv".to_string(),
                "Owner: bob".to_string(),
                "Ttl: 0ms".to_string(),
                "Ttl action: None".to_string(),
                "Ufs Modification time: -".to_string(),
                "Permission: drwxr-xr-x".to_string(),
                "Group: staff".to_string(),
                "Nlink: 2".to_string(),
            ]
        );
    }

    /// The labels #2138 reported as duplicates must not come back, for files or directories.
    #[test]
    fn removed_duplicate_labels_stay_removed() {
        let removed = [
            "File size",
            "Modification time",
            "Replication",
            "Is dir",
            "Type",
        ];
        for status in [file(), dir()] {
            for line in stat_lines(&status.path, &status) {
                let label = line
                    .split_once(": ")
                    .map_or(line.as_str(), |(label, _)| label);
                assert!(!removed.contains(&label), "obsolete label in {line:?}");
            }
        }
    }
}
