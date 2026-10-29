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
                                    return Err(format!(
                                        "Cannot display '{}': {}",
                                        path.full_path(),
                                        e
                                    )
                                    .into());
                                }

                                // Complete the read
                                if let Err(e) = reader.complete().await {
                                    return Err(format!(
                                        "Cannot complete read of '{}': {}",
                                        path.full_path(),
                                        e
                                    )
                                    .into());
                                }

                                Ok(())
                            }
                            Err(e) => {
                                Err(format!("Cannot open '{}': {}", path.full_path(), e).into())
                            }
                        }
                    }
                    Err(e) => Err(format!("Cannot access '{}': {}", path.full_path(), e).into()),
                }
            }
        }
    }
}
