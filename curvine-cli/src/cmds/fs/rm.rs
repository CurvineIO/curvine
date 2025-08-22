use clap::Subcommand;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{CurvineURI, FileSystem};
use orpc::CommonResult;

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
    },
}

impl RmCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            RmCommand::Rm { path, recursive } => {
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
                        match client.delete(&path, *recursive).await {
                            Ok(_) => {
                                println!("Deleted {}", path.full_path());
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
