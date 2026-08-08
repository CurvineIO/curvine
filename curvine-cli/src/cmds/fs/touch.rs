use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem, Writer};
use curvine_model::OpenFlags;
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
                        // File exists, update its timestamp (not implemented yet)
                        println!("touch: Timestamp update not implemented yet");
                        Ok(())
                    }
                    Err(_) => {
                        // File doesn't exist, create an empty file
                        let (uid, gid, mode) = current_process_acl(&client);
                        let opts = client
                            .cv()
                            .create_opts_builder()
                            .create_parent(true)
                            .acl(uid, gid, mode)
                            .build();
                        let flags = OpenFlags::new_write_only().set_create(true);
                        match client.open_with_opts(&path, opts, flags).await {
                            Ok(mut writer) => {
                                // Complete the write to create an empty file
                                if let Err(e) = writer.complete().await {
                                    eprintln!("Error creating empty file: {}", e);
                                    return Err(e.into());
                                }

                                println!("Created empty file: {}", path.full_path());
                                Ok(())
                            }
                            Err(e) => {
                                eprintln!("Error creating file: {}", e);
                                Err(e.into())
                            }
                        }
                    }
                }
            }
        }
    }
}
