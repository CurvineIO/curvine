use clap::Subcommand;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{CurvineURI, FileSystem};
use orpc::common::{ByteUnit, DurationUnit};
use orpc::CommonResult;

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
                        // Format similar to HDFS stat output
                        println!("Path: {}", path.full_path());
                        println!("Name: {}", status.name);
                        println!("Id: {}", status.id);
                        println!("Is dir: {}", status.is_dir);
                        println!("Mtime: {}", status.mtime);
                        println!("Atime: {}", status.atime);
                        println!("Children num: {}", status.children_num);
                        println!("Is complete: {}", status.is_complete);
                        println!("Len: {}", status.len);
                        println!("Replicas: {}", status.replicas);
                        println!("Block size: {}", status.block_size);
                        println!("File type: {:?}", status.file_type);
                        println!("Xattr: {:?}", status.x_attr);
                        println!("Storage policy: {:?}", status.storage_policy);
                        println!("Owner: {}", status.owner);
                        println!("File size: {}", ByteUnit::byte_to_string(status.len as u64));
                        println!(
                            "Block size: {}",
                            ByteUnit::byte_to_string(status.block_size as u64)
                        );
                        if !status.is_dir {
                            println!("Replication: {}", status.replicas);
                        }

                        println!(
                            "Ttl: {}",
                            DurationUnit::new(status.storage_policy.ttl_ms as u64)
                        );
                        println!("Ttl action: {:?}", status.storage_policy.ttl_action);

                        // Format modification time
                        let formatted_mtime = crate::cmds::fs::common::format_epoch_ms_local(
                            status.mtime,
                            "%Y-%m-%d %H:%M:%S",
                        );
                        let formatted_ufs_mtime = crate::cmds::fs::common::format_epoch_ms_local(
                            status.storage_policy.ufs_mtime,
                            "%Y-%m-%d %H:%M:%S",
                        );

                        println!("Type: {}", if status.is_dir { "directory" } else { "file" });
                        println!("Modification time: {}", formatted_mtime);
                        println!("Ufs Modification time: {}", formatted_ufs_mtime);
                        println!(
                            "Permission: {}rwxr-xr-x",
                            if status.is_dir { "d" } else { "-" }
                        );
                        println!("Owner: {}", status.owner);
                        println!("Group: {}", status.group);
                        println!("Nlink: {}", status.nlink);

                        Ok(())
                    }
                    Err(e) => {
                        eprintln!("stat: Cannot stat '{}': {}", path.full_path(), e);
                        Err(e.into())
                    }
                }
            }
        }
    }
}
