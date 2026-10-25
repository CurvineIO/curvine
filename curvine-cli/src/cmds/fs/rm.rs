use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem};
use curvine_unified_fs::UnifiedFileSystem;

#[derive(Subcommand, Debug)]
pub enum RmCommand {
    /// Remove file or directory
    Rm {
        #[clap(help = "Path of the file or directory to remove")]
        path: String,
        #[clap(
            short,
            long,
            help = "Remove directories and their contents recursively"
        )]
        recursive: bool,
        #[clap(
            long,
            help = "Delete permanently, bypassing the recycle bin (if enabled)"
        )]
        skip_trash: bool,
    },
}

impl RmCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            RmCommand::Rm {
                path,
                recursive,
                skip_trash,
            } => {
                let path = CurvineURI::new(path)?;

                // Check if the path exists
                match client.get_status(&path).await {
                    Ok(status) => {
                        // If it's a directory and recursive is not set, return error
                        if status.is_dir && !recursive {
                            eprintln!("rm: Cannot remove '{}': Is a directory", path.full_path());
                            return Ok(());
                        }

                        // Delete the file or directory
                        match client
                            .delete_with_opts(&path, *recursive, *skip_trash)
                            .await
                        {
                            Ok(res) => {
                                if res.trashed {
                                    println!("Moved to /.Trash: {}", path.full_path());
                                } else {
                                    println!("Deleted permanently: {}", path.full_path());
                                }
                                Ok(())
                            }
                            Err(e) => {
                                eprintln!("rm: Cannot remove '{}': {}", path.full_path(), e);
                                Err(e.into())
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("rm: Cannot remove '{}': {}", path.full_path(), e);
                        Err(e.into())
                    }
                }
            }
        }
    }
}
