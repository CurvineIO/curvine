use super::common::copy_to;
use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem, Reader};
use curvine_unified_fs::UnifiedFileSystem;

#[derive(Subcommand, Debug)]
pub enum CatCommand {
    /// Display file contents
    Cat {
        #[clap(help = "Path of the file to display")]
        path: String,
    },
}

impl CatCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            CatCommand::Cat { path } => {
                let path = CurvineURI::new(path)?;

                // Check if the path exists and is a file
                match client.get_status(&path).await {
                    Ok(status) => {
                        if status.is_dir {
                            eprintln!("cat: Cannot display '{}': Is a directory", path.full_path());
                            return Err(format!(
                                "Cannot display '{}': Is a directory",
                                path.full_path()
                            )
                            .into());
                        }

                        // Open the file for reading
                        match client.open(&path).await {
                            Ok(mut reader) => {
                                let chunk_size = client.conf().client.read_chunk_size;
                                let mut stdout = tokio::io::stdout();
                                if let Err(e) = copy_to(&mut reader, &mut stdout, chunk_size).await
                                {
                                    eprintln!("cat: Cannot display '{}': {}", path.full_path(), e);
                                    return Err(e);
                                }

                                // Complete the read
                                if let Err(e) = reader.complete().await {
                                    eprintln!("Error completing read: {}", e);
                                    return Err(e.into());
                                }

                                Ok(())
                            }
                            Err(e) => {
                                eprintln!("Error opening file: {}", e);
                                Err(e.into())
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("cat: Cannot access '{}': {}", path.full_path(), e);
                        Err(e.into())
                    }
                }
            }
        }
    }
}
