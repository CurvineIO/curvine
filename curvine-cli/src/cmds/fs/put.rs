use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{CurvineURI, FileSystem, Writer};
use curvine_model::OpenFlags;
use curvine_unified_fs::UnifiedFileSystem;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncReadExt, BufReader};

use super::common::current_process_acl;

#[derive(Subcommand, Debug)]
pub enum PutCommand {
    /// Upload a file to the filesystem
    Put {
        #[clap(help = "Local file path")]
        local_path: PathBuf,

        #[clap(help = "Remote destination path")]
        remote_path: String,

        #[clap(
            short = 'n',
            long,
            help = "Do not overwrite the destination if it already exists"
        )]
        no_clobber: bool,
    },
}

/// Destination of the upload. The local file name is appended when the remote
/// path names a directory, either by a trailing `/` or because it exists as one.
fn resolve_remote_path(local_path: &Path, remote_path: &str, remote_is_dir: bool) -> String {
    if !remote_path.ends_with('/') && !remote_is_dir {
        return remote_path.to_string();
    }

    let local_filename = local_path.file_name().unwrap().to_string_lossy();
    if remote_path.ends_with('/') {
        format!("{}{}", remote_path, local_filename)
    } else {
        format!("{}/{}", remote_path, local_filename)
    }
}

/// Decides whether a `--no-clobber` upload may proceed, from the status lookup
/// of the resolved destination. Only a not-found lookup allows the upload: an
/// existing destination is skipped, and any other failure is reported because
/// it does not prove that the destination is absent.
fn check_no_clobber<T>(remote_path: &str, lookup: FsResult<T>) -> CommonResult<()> {
    match lookup {
        Ok(_) => Err(format!(
            "Remote path '{}' already exists, not overwritten (--no-clobber)",
            remote_path
        )
        .into()),
        Err(FsError::FileNotFound(_)) => Ok(()),
        Err(e) => Err(format!("Cannot check remote path '{}': {}", remote_path, e).into()),
    }
}

impl PutCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            PutCommand::Put {
                local_path,
                remote_path,
                no_clobber,
            } => {
                // Check if local file exists
                if !local_path.exists() {
                    return Err(
                        format!("Local file '{}' does not exist", local_path.display()).into(),
                    );
                }

                // Check if local path is a file (not directory)
                if !local_path.is_file() {
                    return Err(format!("'{}' is not a file", local_path.display()).into());
                }

                // Get file size for progress tracking
                let file_size = match local_path.metadata() {
                    Ok(metadata) => metadata.len(),
                    Err(e) => {
                        return Err(format!(
                            "Cannot read metadata of local file '{}': {}",
                            local_path.display(),
                            e
                        )
                        .into());
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
                        return Err(format!(
                            "Cannot open local file '{}': {}",
                            local_path.display(),
                            e
                        )
                        .into());
                    }
                };
                let mut reader = BufReader::new(file);

                // Determine the final remote path. A trailing '/' already names a
                // directory; otherwise check whether the remote path exists as one.
                let remote_is_dir = !remote_path.ends_with('/')
                    && match CurvineURI::new(remote_path) {
                        Ok(remote_check_uri) => matches!(
                            client.get_status(&remote_check_uri).await,
                            Ok(status) if status.is_dir
                        ),
                        // If URI parsing fails, treat as file path
                        Err(_) => false,
                    };
                let final_remote_path = resolve_remote_path(local_path, remote_path, remote_is_dir);

                // Create remote path URI
                let remote_uri = match CurvineURI::new(&final_remote_path) {
                    Ok(uri) => uri,
                    Err(e) => {
                        return Err(
                            format!("Invalid remote path '{}': {}", final_remote_path, e).into(),
                        );
                    }
                };

                // With --no-clobber, never open (and so never truncate) a destination
                // that exists. The lookup uses the same resolved path as the upload.
                if *no_clobber {
                    check_no_clobber(&final_remote_path, client.get_status(&remote_uri).await)?;
                }

                // Create writer for streaming upload
                let (owner, group, mode) = current_process_acl(&client);
                let opts = client
                    .cv()
                    .create_opts_builder()
                    .create_parent(true)
                    .owner(owner)
                    .group(group)
                    .mode(mode)
                    .build();
                // Without overwrite the master rejects a create over an existing file,
                // which also covers a destination created after the lookup above.
                let flags = OpenFlags::new_write_only()
                    .set_create(true)
                    .set_overwrite(!*no_clobber);
                match client.open_with_opts(&remote_uri, opts, flags).await {
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
                                            return Err(format!(
                                                "Cannot write to '{}': {}",
                                                final_remote_path, e
                                            )
                                            .into());
                                        }
                                    }
                                }
                                Err(e) => {
                                    return Err(format!(
                                        "Cannot read local file '{}': {}",
                                        local_path.display(),
                                        e
                                    )
                                    .into());
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
                                return Err(format!(
                                    "Cannot complete upload to '{}': {}",
                                    final_remote_path, e
                                )
                                .into());
                            }
                        }
                    }
                    Err(e) => {
                        return Err(format!(
                            "Cannot create remote file '{}': {}",
                            final_remote_path, e
                        )
                        .into());
                    }
                }

                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{check_no_clobber, resolve_remote_path, PutCommand};
    use clap::Parser;
    use curvine_error::FsError;
    use std::path::Path;

    #[derive(Parser, Debug)]
    struct Cli {
        #[clap(subcommand)]
        command: PutCommand,
    }

    fn parse_no_clobber(args: &[&str]) -> bool {
        let cli = Cli::try_parse_from(args).unwrap();
        // `PutCommand` has a single variant, so the pattern is irrefutable.
        let PutCommand::Put { no_clobber, .. } = cli.command;
        no_clobber
    }

    #[test]
    fn parses_short_no_clobber_flag() {
        assert!(parse_no_clobber(&["put", "put", "-n", "a.bin", "/a.bin"]));
    }

    #[test]
    fn parses_long_no_clobber_flag() {
        assert!(parse_no_clobber(&["put", "put", "a.bin", "/a.bin", "--no-clobber"]));
    }

    #[test]
    fn no_clobber_defaults_to_off() {
        assert!(!parse_no_clobber(&["put", "put", "a.bin", "/a.bin"]));
    }

    #[test]
    fn resolves_plain_remote_path_as_is() {
        let local = Path::new("dir/model.bin");
        assert_eq!(resolve_remote_path(local, "/models/new.bin", false), "/models/new.bin");
    }

    #[test]
    fn resolves_trailing_slash_to_local_file_name() {
        let local = Path::new("dir/model.bin");
        assert_eq!(resolve_remote_path(local, "/models/", false), "/models/model.bin");
    }

    #[test]
    fn resolves_existing_directory_to_local_file_name() {
        let local = Path::new("dir/model.bin");
        assert_eq!(resolve_remote_path(local, "/models", true), "/models/model.bin");
    }

    #[test]
    fn no_clobber_allows_upload_when_destination_is_missing() {
        let lookup: Result<(), FsError> = Err(FsError::file_not_found("/models/model.bin"));
        assert!(check_no_clobber("/models/model.bin", lookup).is_ok());
    }

    #[test]
    fn no_clobber_rejects_existing_destination() {
        let err = check_no_clobber("/models/model.bin", Ok(())).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("'/models/model.bin' already exists"), "{}", msg);
        assert!(msg.contains("--no-clobber"), "{}", msg);
    }

    #[test]
    fn no_clobber_reports_other_lookup_errors() {
        let lookup: Result<(), FsError> = Err(FsError::common("permission denied"));
        let err = check_no_clobber("/models/model.bin", lookup).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Cannot check remote path '/models/model.bin'"), "{}", msg);
        assert!(msg.contains("permission denied"), "{}", msg);
    }
}
