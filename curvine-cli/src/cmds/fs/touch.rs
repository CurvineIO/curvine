use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_error::FsError;
use curvine_fs_api::{CurvineURI, FileSystem, Writer};
use curvine_model::{OpenFlags, SetAttrOpts};
use curvine_runtime::common::LocalTime;
use curvine_unified_fs::UnifiedFileSystem;

use super::common::current_process_acl;

#[derive(Subcommand, Debug)]
pub enum TouchCommand {
    /// Create an empty file or update timestamp
    Touch {
        #[clap(help = "Path of the file to create or touch")]
        path: String,
    },
}

impl TouchCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            TouchCommand::Touch { path } => {
                let path = CurvineURI::new(path)?;

                // Check if the path exists
                match client.get_status(&path).await {
                    Ok(_) => {
                        // Path exists, set its access and modification times to now
                        let now = LocalTime::mills() as i64;
                        let opts = SetAttrOpts {
                            atime: Some(now),
                            mtime: Some(now),
                            ..Default::default()
                        };
                        if let Err(e) = client.set_attr(&path, opts).await {
                            return Err(format!(
                                "Cannot update timestamp of '{}': {}",
                                path.full_path(),
                                e
                            )
                            .into());
                        }

                        println!("Updated timestamp: {}", path.full_path());
                        Ok(())
                    }
                    Err(FsError::FileNotFound(_)) => {
                        // File doesn't exist, create an empty file
                        let (owner, group, mode) = current_process_acl(&client);
                        let opts = client
                            .cv()
                            .create_opts_builder()
                            .create_parent(true)
                            .owner(owner)
                            .group(group)
                            .mode(mode)
                            .build();
                        let flags = OpenFlags::new_write_only().set_create(true);
                        match client.open_with_opts(&path, opts, flags).await {
                            Ok(mut writer) => {
                                // Complete the write to create an empty file
                                if let Err(e) = writer.complete().await {
                                    return Err(format!(
                                        "Cannot create empty file '{}': {}",
                                        path.full_path(),
                                        e
                                    )
                                    .into());
                                }

                                println!("Created empty file: {}", path.full_path());
                                Ok(())
                            }
                            Err(e) => {
                                Err(format!("Cannot create file '{}': {}", path.full_path(), e)
                                    .into())
                            }
                        }
                    }
                    Err(e) => Err(format!("Cannot access '{}': {}", path.full_path(), e).into()),
                }
            }
        }
    }
}
