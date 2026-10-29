use super::common::copy_to;
use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::{CurvineURI, FileSystem, Reader};
use curvine_unified_fs::UnifiedFileSystem;
use std::path::PathBuf;
use tokio::fs;

#[derive(Subcommand, Debug)]
pub enum GetCommand {
    /// Get file to local
    Get {
        #[clap(help = "Path of the file to get")]
        path: String,
        #[clap(help = "Local destination path")]
        local_path: PathBuf,
    },
}

impl GetCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            GetCommand::Get { path, local_path } => {
                let path = CurvineURI::new(path)?;

                // Check if the path exists and is a file
                match client.get_status(&path).await {
                    Ok(status) => {
                        if status.is_dir {
                            return Err(format!(
                                "Cannot download '{}': Is a directory",
                                path.full_path()
                            )
                            .into());
                        }

                        // Determine the final local path
                        let mut final_local_path = if local_path.exists() && local_path.is_dir()
                            || local_path
                                .to_string_lossy()
                                .ends_with(std::path::MAIN_SEPARATOR)
                        {
                            // If local_path is a directory or ends with a path separator, append the filename from the remote path
                            let filename = path.name();
                            local_path.join(filename)
                        } else if !local_path
                            .to_string_lossy()
                            .contains(std::path::MAIN_SEPARATOR)
                        {
                            // If local_path is just a filename without path, use current directory
                            let filename = local_path.to_string_lossy().to_string();
                            std::env::current_dir()
                                .unwrap_or_else(|_| PathBuf::from("."))
                                .join(filename)
                        } else {
                            local_path.clone()
                        };

                        // Check if parent directory exists, refuse to execute if it doesn't
                        if let Some(parent) = final_local_path.parent() {
                            if !parent.exists() {
                                return Err(format!(
                                    "Parent directory '{}' does not exist. Please create it first.",
                                    parent.display()
                                )
                                .into());
                            }
                        }

                        // If the final_local_path itself is a directory (ends with separator), create it
                        if final_local_path
                            .to_string_lossy()
                            .ends_with(std::path::MAIN_SEPARATOR)
                        {
                            if let Err(e) = fs::create_dir_all(&final_local_path).await {
                                return Err(format!(
                                    "Cannot create directory '{}': {}",
                                    final_local_path.display(),
                                    e
                                )
                                .into());
                            }
                            // Append the filename to the directory path
                            let filename = path.name();
                            final_local_path = final_local_path.join(filename);
                        }

                        // Open local file for writing
                        let mut file = match fs::File::create(&final_local_path).await {
                            Ok(file) => file,
                            Err(e) => {
                                return Err(format!(
                                    "Cannot create local file '{}': {}",
                                    final_local_path.display(),
                                    e
                                )
                                .into());
                            }
                        };

                        println!("Downloading to {}", final_local_path.display());

                        // Open the remote file and read its content
                        match client.open(&path).await {
                            Ok(mut reader) => {
                                let chunk_size = client.conf().client.read_chunk_size;
                                let copied = copy_to(&mut reader, &mut file, chunk_size).await;
                                let bytes_read = match copied {
                                    Ok(bytes_read) => bytes_read,
                                    Err(e) => {
                                        // Do not leave a truncated file behind.
                                        drop(file);
                                        let _ = fs::remove_file(&final_local_path).await;
                                        return Err(format!(
                                            "Cannot download '{}': {}",
                                            path.full_path(),
                                            e
                                        )
                                        .into());
                                    }
                                };

                                if let Err(e) = reader.complete().await {
                                    return Err(format!(
                                        "Cannot complete read of '{}': {}",
                                        path.full_path(),
                                        e
                                    )
                                    .into());
                                }

                                println!("Successfully downloaded {} bytes", bytes_read);
                                Ok(())
                            }
                            Err(e) => Err(format!(
                                "Cannot open remote file '{}': {}",
                                path.full_path(),
                                e
                            )
                            .into()),
                        }
                    }
                    Err(e) => Err(format!("Cannot access '{}': {}", path.full_path(), e).into()),
                }
            }
        }
    }
}
