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

use super::content_summary::ContentSummary;
use bytes::BytesMut;
use clap::{Parser, Subcommand};
use curvine_client::file::CurvineFileSystem;
use curvine_common::fs::{CurvineURI, Reader, Writer};
use orpc::CommonResult;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

#[derive(Parser, Debug)]
pub struct FsCommand {
    #[clap(subcommand)]
    action: FsSubCommand,
}

#[derive(Subcommand, Debug)]
pub enum FsSubCommand {
    /// List files and directories
    Ls {
        #[clap(value_name = "path", default_value = "/")]
        path: String,
    },

    /// Create a directory
    Mkdir {
        #[clap(help = "Directory path to create")]
        path: String,

        #[clap(short, long, help = "Create parent directories as needed")]
        parents: bool,
    },

    /// Upload a file to the filesystem
    Put {
        #[clap(help = "Local file path")]
        local_path: PathBuf,

        #[clap(help = "Remote destination path")]
        remote_path: String,
    },

    /// Display file contents
    Cat {
        #[clap(help = "Path of the file to display")]
        path: String,
    },

    /// Show disk usage statistics
    Df {
        #[clap(short, long, help = "Show human-readable sizes")]
        human_readable: bool,
    },

    /// Create an empty file or update timestamp
    Touch {
        #[clap(help = "Path of the file to create or touch")]
        path: String,
    },

    /// Calculate directory space usage
    Du {
        #[clap(help = "Path of the directory to calculate")]
        path: String,

        #[clap(short, long, help = "Show human-readable sizes")]
        human_readable: bool,

        #[clap(short, long, help = "Display the names of columns as a header line")]
        verbose: bool,
    },

    /// Get file to local
    Get {
        #[clap(help = "Path of the file to get")]
        path: String,
        #[clap(help = "Local destination path")]
        local_path: PathBuf,
    },

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

    /// Stat file or directory
    Stat {
        #[clap(help = "Path of the file or directory to stat")]
        path: String,
    },

    /// Count files and directories
    Count {
        #[clap(help = "Path of the directory to count")]
        path: String,
    },
}

impl FsCommand {
    pub async fn execute(&self, client: CurvineFileSystem) -> CommonResult<()> {
        match &self.action {
            FsSubCommand::Ls { path } => {
                let path = CurvineURI::new(path)?;
                println!("List Path {}", path.full_path());
                let files = client.list_status(&path).await?;

                if files.is_empty() {
                    return Ok(());
                }

                // Print header similar to HDFS ls output
                println!("Found {} items", files.len());

                for file in files {
                    // Format similar to HDFS: permissions owner group size date time name
                    let file_type = if file.is_dir { "d" } else { "-" };
                    let permissions = "rwxr-xr-x"; // Default permissions for now
                    let replicas = if file.is_dir {
                        "-"
                    } else {
                        &file.replicas.to_string()
                    };
                    let owner = "curvine"; // Default owner
                    let group = "curvine"; // Default group

                    // Format file size
                    let size = if file.is_dir {
                        "-".to_string()
                    } else {
                        file.len.to_string()
                    };

                    // Format modification time (convert from timestamp)
                    let datetime = chrono::DateTime::from_timestamp(file.mtime / 1000, 0)
                        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
                    let formatted_time = datetime.format("%Y-%m-%d %H:%M").to_string();

                    // Print in HDFS-like format
                    println!(
                        "{}{} {} {} {} {:>12} {} {}",
                        file_type,
                        permissions,
                        replicas,
                        owner,
                        group,
                        size,
                        formatted_time,
                        file.name
                    );
                }
                Ok(())
            }

            FsSubCommand::Mkdir { path, parents } => {
                println!("Creating directory: {} (parents: {})", path, parents);
                let path = CurvineURI::new(path)?;
                client.mkdir(&path, *parents).await?;
                println!("Directory created successfully");
                Ok(())
            }

            FsSubCommand::Put {
                local_path,
                remote_path,
            } => {
                // Check if local file exists
                if !local_path.exists() {
                    eprintln!(
                        "Error: Local file '{}' does not exist",
                        local_path.display()
                    );
                    return Ok(());
                }

                // Check if local path is a file (not directory)
                if !local_path.is_file() {
                    eprintln!("Error: '{}' is not a file", local_path.display());
                    return Ok(());
                }

                // Get file size for progress tracking
                let file_size = match local_path.metadata() {
                    Ok(metadata) => metadata.len(),
                    Err(e) => {
                        eprintln!(
                            "Error getting file metadata '{}': {}",
                            local_path.display(),
                            e
                        );
                        return Ok(());
                    }
                };

                println!(
                    "Uploading {} ({} bytes) to {}",
                    local_path.display(),
                    file_size,
                    remote_path
                );

                // Open local file for streaming read
                let file = match fs::File::open(local_path).await {
                    Ok(file) => file,
                    Err(e) => {
                        eprintln!("Error opening local file '{}': {}", local_path.display(), e);
                        return Ok(());
                    }
                };
                let mut reader = BufReader::new(file);

                // Determine the final remote path
                let final_remote_path = if remote_path.ends_with('/') {
                    // If remote path ends with '/', it's definitely a directory
                    let local_filename = local_path.file_name().unwrap().to_string_lossy();
                    format!("{}{}", remote_path, local_filename)
                } else {
                    // Check if remote path exists and is a directory
                    match CurvineURI::new(remote_path) {
                        Ok(remote_check_uri) => {
                            match client.get_status(&remote_check_uri).await {
                                Ok(status) if status.is_dir => {
                                    // Remote path exists and is a directory
                                    let local_filename =
                                        local_path.file_name().unwrap().to_string_lossy();
                                    format!("{}/{}", remote_path, local_filename)
                                }
                                _ => {
                                    // Remote path doesn't exist or is a file, use as-is
                                    remote_path.to_string()
                                }
                            }
                        }
                        Err(_) => {
                            // If URI parsing fails, treat as file path
                            remote_path.to_string()
                        }
                    }
                };

                // Create remote path URI
                let remote_uri = match CurvineURI::new(&final_remote_path) {
                    Ok(uri) => uri,
                    Err(e) => {
                        eprintln!("Error parsing remote path '{}': {}", final_remote_path, e);
                        return Ok(());
                    }
                };

                // Create writer for streaming upload
                match client.create(&remote_uri, true).await {
                    Ok(mut writer) => {
                        let chunk_size = client.conf().client.write_chunk_size;
                        let mut buffer = vec![0; chunk_size];
                        let mut total_bytes = 0u64;
                        let mut last_progress = 0u64;

                        loop {
                            match reader.read(&mut buffer).await {
                                Ok(0) => break, // EOF
                                Ok(bytes_read) => {
                                    // Write chunk to curvine
                                    match writer.write(&buffer[..bytes_read]).await {
                                        Ok(_) => {
                                            total_bytes += bytes_read as u64;

                                            // Show progress every 10MB or at completion
                                            if total_bytes - last_progress >= 10 * 1024 * 1024
                                                || total_bytes == file_size
                                            {
                                                let progress =
                                                    (total_bytes as f64 / file_size as f64 * 100.0)
                                                        as u32;
                                                println!(
                                                    "Progress: {}% ({}/{} bytes)",
                                                    progress, total_bytes, file_size
                                                );
                                                last_progress = total_bytes;
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("Error writing chunk: {}", e);
                                            return Ok(());
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Error reading from local file: {}", e);
                                    return Ok(());
                                }
                            }
                        }

                        // Complete the upload
                        match writer.complete().await {
                            Ok(_) => {
                                println!(
                                    "Successfully uploaded {} bytes to {}",
                                    total_bytes, final_remote_path
                                );
                            }
                            Err(e) => {
                                eprintln!("Error completing upload: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error creating remote file '{}': {}", final_remote_path, e);
                    }
                }

                Ok(())
            }

            FsSubCommand::Cat { path } => {
                let path = CurvineURI::new(path)?;

                // Check if the path exists and is a file
                match client.get_status(&path).await {
                    Ok(status) => {
                        if status.is_dir {
                            eprintln!("cat: Cannot display '{}': Is a directory", path.full_path());
                            return Ok(());
                        }

                        // Open the file for reading
                        match client.open(&path).await {
                            Ok(mut reader) => {
                                let total_size = reader.len();
                                let chunk_size = client.conf().client.read_chunk_size;
                                let mut buffer = BytesMut::zeroed(chunk_size);
                                let mut bytes_read: i64 = 0;

                                // Read in chunks and print to stdout
                                while bytes_read < total_size {
                                    let remaining = total_size - bytes_read;
                                    let to_read = std::cmp::min(remaining as usize, chunk_size);
                                    let buf_slice = &mut buffer[..to_read];

                                    if let Err(e) = reader.read_full(buf_slice).await {
                                        eprintln!("Error reading from file: {}", e);
                                        return Err(e.into());
                                    }

                                    // Convert bytes to string and print
                                    print!("{}", String::from_utf8_lossy(buf_slice));

                                    bytes_read += to_read as i64;
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

            FsSubCommand::Df { human_readable } => {
                // Get master info from the filesystem
                match client.get_master_info().await {
                    Ok(master_info) => {
                        // Format similar to HDFS df output
                        println!(
                            "Filesystem                Size             Used  Available  Use%"
                        );

                        // Calculate total used space (fs_used + non_fs_used)
                        let used = master_info.fs_used + master_info.non_fs_used;

                        // Calculate usage percentage
                        let usage_percent = if master_info.capacity > 0 {
                            (used as f64 / master_info.capacity as f64) * 100.0
                        } else {
                            0.0
                        };

                        // Format sizes based on human_readable flag
                        let (capacity, used_str, available) = if *human_readable {
                            (
                                format_size(master_info.capacity as u64),
                                format_size(used as u64),
                                format_size(master_info.available as u64),
                            )
                        } else {
                            (
                                master_info.capacity.to_string(),
                                used.to_string(),
                                master_info.available.to_string(),
                            )
                        };

                        println!(
                            "curvine://{}  {:>15}  {:>15}  {:>15}  {:.1}%",
                            master_info.active_master, capacity, used_str, available, usage_percent
                        );

                        Ok(())
                    }
                    Err(e) => {
                        eprintln!("Error getting master info: {}", e);
                        Err(e.into())
                    }
                }
            }

            FsSubCommand::Touch { path } => {
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
                        match client.create(&path, false).await {
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

            FsSubCommand::Du {
                path,
                human_readable,
                verbose,
            } => {
                let path = CurvineURI::new(path)?;

                // Print header similar to HDFS du -v output, but only if verbose is true
                if *verbose {
                    println!(
                        "{:>15}  {:>15}  {:>15}  PATH",
                        "SIZE", "SINGLE_REPLICA", "ALL_REPLICAS"
                    );
                }

                match calculate_content_summary(&client, &path).await {
                    Ok(summary) => {
                        // Get file status to determine replication factor
                        let status = client.get_status(&path).await;
                        // Default replication factor is 3 if we can't get the status
                        let replicas = status.map(|s| s.replicas).unwrap_or(1) as i64;
                        // Calculate disk space consumed with all replicas
                        let disk_space = summary.length * replicas;

                        // Format similar to HDFS du output with single replica data added
                        if *human_readable {
                            println!(
                                "{:>15}  {:>15}  {:>15}  {}",
                                format_size(summary.length as u64),
                                format_size(summary.length as u64),
                                format_size(disk_space as u64),
                                path.full_path()
                            );
                        } else {
                            println!(
                                "{:>15}  {:>15}  {:>15}  {}",
                                summary.length,
                                summary.length,
                                disk_space,
                                path.full_path()
                            );
                        }
                        Ok(())
                    }
                    Err(e) => {
                        eprintln!("du: Cannot access '{}': {}", path.full_path(), e);
                        Err(e)
                    }
                }
            }

            FsSubCommand::Get { path, local_path } => {
                let path = CurvineURI::new(path)?;

                // Check if the path exists and is a file
                match client.get_status(&path).await {
                    Ok(status) => {
                        if status.is_dir {
                            eprintln!(
                                "get: Cannot download '{}': Is a directory",
                                path.full_path()
                            );
                            return Ok(());
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
                                eprintln!("Error: Parent directory '{}' does not exist. Please create it first.", parent.display());
                                return Ok(());
                            }
                        }

                        // If the final_local_path itself is a directory (ends with separator), create it
                        if final_local_path
                            .to_string_lossy()
                            .ends_with(std::path::MAIN_SEPARATOR)
                        {
                            if let Err(e) = fs::create_dir_all(&final_local_path).await {
                                eprintln!("Error creating directory: {}", e);
                                return Err(e.into());
                            }
                            // Append the filename to the directory path
                            let filename = path.name();
                            final_local_path = final_local_path.join(filename);
                        }

                        // Open local file for writing
                        let mut file = match fs::File::create(&final_local_path).await {
                            Ok(file) => file,
                            Err(e) => {
                                eprintln!("Error creating local file: {}", e);
                                return Ok(());
                            }
                        };

                        println!("Downloading to {}", final_local_path.display());

                        // Open the remote file and read its content
                        match client.open(&path).await {
                            Ok(mut reader) => {
                                let total_size = reader.len();
                                let mut bytes_read: i64 = 0;
                                let chunk_size = client.conf().client.read_chunk_size;
                                let mut buffer = BytesMut::zeroed(chunk_size);

                                // Read in chunks and write to local file
                                while bytes_read < total_size {
                                    let remaining = total_size - bytes_read;
                                    let to_read = std::cmp::min(remaining as usize, chunk_size);
                                    let buf_slice = &mut buffer[..to_read];

                                    if let Err(e) = reader.read_full(buf_slice).await {
                                        eprintln!("Error reading from remote file: {}", e);
                                        return Err(e.into());
                                    }

                                    if let Err(e) = file.write_all(buf_slice).await {
                                        eprintln!("Error writing to local file: {}", e);
                                        return Err(e.into());
                                    }

                                    bytes_read += to_read as i64;
                                }

                                // Flush and complete
                                if let Err(e) = file.flush().await {
                                    eprintln!("Error flushing local file: {}", e);
                                    return Err(e.into());
                                }

                                if let Err(e) = reader.complete().await {
                                    eprintln!("Error completing read: {}", e);
                                    return Err(e.into());
                                }

                                println!("Successfully downloaded {} bytes", bytes_read);
                                Ok(())
                            }
                            Err(e) => {
                                eprintln!("Error opening remote file: {}", e);
                                Err(e.into())
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        Err(e.into())
                    }
                }
            }

            FsSubCommand::Rm { path, recursive } => {
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

            FsSubCommand::Stat { path } => {
                let path = CurvineURI::new(path)?;

                match client.get_status(&path).await {
                    Ok(status) => {
                        // Format similar to HDFS stat output
                        println!("File: {}", path.full_path());
                        println!("Size: {}", status.len);

                        // Format modification time
                        let mtime = chrono::DateTime::from_timestamp(status.mtime / 1000, 0)
                            .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
                        let formatted_mtime = mtime.format("%Y-%m-%d %H:%M:%S").to_string();

                        println!("Type: {}", if status.is_dir { "directory" } else { "file" });
                        println!("Modification time: {}", formatted_mtime);
                        println!(
                            "Permission: {}rwxr-xr-x",
                            if status.is_dir { "d" } else { "-" }
                        );
                        println!("Owner: curvine");
                        println!("Group: curvine");

                        if !status.is_dir {
                            println!("Replication: {}", status.replicas);
                        }

                        Ok(())
                    }
                    Err(e) => {
                        eprintln!("stat: Cannot stat '{}': {}", path.full_path(), e);
                        Err(e.into())
                    }
                }
            }

            FsSubCommand::Count { path } => {
                let path = CurvineURI::new(path)?;

                match calculate_content_summary(&client, &path).await {
                    Ok(summary) => {
                        // Format similar to HDFS count output
                        println!("   DIR_COUNT    FILE_COUNT       CONTENT_SIZE PATHNAME");
                        println!(
                            "{:>12} {:>12} {:>18} {}",
                            summary.directory_count,
                            summary.file_count,
                            summary.length,
                            path.full_path()
                        );
                        Ok(())
                    }
                    Err(e) => {
                        eprintln!("count: Cannot count '{}': {}", path.full_path(), e);
                        Err(e)
                    }
                }
            }
        }
    }
}

/// Calculates content summary (directory size, file count, directory count) on the client side
async fn calculate_content_summary(
    client: &CurvineFileSystem,
    path: &CurvineURI,
) -> CommonResult<ContentSummary> {
    calculate_content_summary_impl(client, path).await
}

/// Implementation of calculate_content_summary that handles recursion properly
async fn calculate_content_summary_impl(
    client: &CurvineFileSystem,
    path: &CurvineURI,
) -> CommonResult<ContentSummary> {
    // First check if the path exists and get its status
    let status = client.get_status(path).await?;

    if !status.is_dir {
        // For a file, return a simple summary with just the file's length
        return Ok(ContentSummary::for_file(status.len));
    }

    // For a directory, we need to recursively calculate the summary
    let mut summary = ContentSummary::for_empty_dir();
    let children = client.list_status(path).await?;

    for child in children {
        if child.is_dir {
            // Recursively get summary for subdirectories
            let child_path = CurvineURI::new(&child.path)?;
            // Use Box::pin to handle recursive async call
            let child_summary =
                Box::pin(calculate_content_summary_impl(client, &child_path)).await?;
            summary.merge(&child_summary);
        } else {
            // For files, just add their length and count
            summary.length += child.len;
            summary.file_count += 1;
        }
    }

    Ok(summary)
}

/// Formats a size in bytes to a human-readable string (KB, MB, GB, etc.)
fn format_size(size: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if size >= TB {
        format!("{:.1} TB", size as f64 / TB as f64)
    } else if size >= GB {
        format!("{:.1} GB", size as f64 / GB as f64)
    } else if size >= MB {
        format!("{:.1} MB", size as f64 / MB as f64)
    } else if size >= KB {
        format!("{:.1} KB", size as f64 / KB as f64)
    } else {
        format!("{} B", size)
    }
}
