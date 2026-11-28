use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_error::FsError;
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
        #[clap(
            long = "async",
            help = "Return immediately and let the master reclaim the entry in \
                    the background (Curvine paths only)"
        )]
        async_delete: bool,
        #[clap(
            short,
            long,
            help = "Ignore a path that does not exist; other errors still fail"
        )]
        force: bool,
    },
}

/// With `--force`, a path that does not exist is not an error. Every other
/// failure (permissions, RPC errors, timeouts) is still reported.
fn ignore_error(force: bool, err: &FsError) -> bool {
    force && matches!(err, FsError::FileNotFound(_))
}

impl RmCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            RmCommand::Rm {
                path,
                recursive,
                skip_trash,
                async_delete,
                force,
            } => {
                let path = CurvineURI::new(path)?;

                // Check if the path exists
                match client.get_status(&path).await {
                    Ok(status) => {
                        // If it's a directory and recursive is not set, return error
                        if status.is_dir && !recursive {
                            return Err(format!(
                                "Cannot remove '{}': Is a directory",
                                path.full_path()
                            )
                            .into());
                        }

                        let deleted = if *async_delete && !*skip_trash {
                            client.delete_async(&path, *recursive).await
                        } else {
                            client
                                .delete_with_opts(&path, *recursive, *skip_trash)
                                .await
                        };

                        match deleted {
                            Ok(res) => {
                                if res.purge_pending {
                                    println!("Staged for background purge: {}", path.full_path());
                                } else if res.trashed {
                                    println!("Moved to /.Trash: {}", path.full_path());
                                } else {
                                    println!("Deleted permanently: {}", path.full_path());
                                }
                                Ok(())
                            }
                            // The path may have been removed after it was looked up.
                            Err(e) if ignore_error(*force, &e) => Ok(()),
                            Err(e) => {
                                Err(format!("Cannot remove '{}': {}", path.full_path(), e).into())
                            }
                        }
                    }
                    Err(e) if ignore_error(*force, &e) => Ok(()),
                    Err(e) => Err(format!("Cannot remove '{}': {}", path.full_path(), e).into()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ignore_error, RmCommand};
    use clap::Parser;
    use curvine_error::FsError;
    use std::io;

    #[derive(Parser, Debug)]
    struct Cli {
        #[clap(subcommand)]
        command: RmCommand,
    }

    #[test]
    fn parses_async_flag() {
        let cli = Cli::try_parse_from(["rm", "rm", "-r", "--async", "/data"]).unwrap();
        // `RmCommand` has a single variant, so the pattern is irrefutable and an
        // `else { panic!(..) }` clause would be unreachable (`irrefutable_let_patterns`).
        let RmCommand::Rm {
            recursive,
            skip_trash,
            async_delete,
            ..
        } = cli.command;
        assert!(recursive);
        assert!(async_delete);
        assert!(!skip_trash, "--async must not imply --skip-trash");
    }

    #[test]
    fn async_defaults_to_off() {
        let cli = Cli::try_parse_from(["rm", "rm", "/data"]).unwrap();
        let RmCommand::Rm { async_delete, .. } = cli.command;
        assert!(!async_delete);
    }

    fn parse(args: &[&str]) -> (bool, bool) {
        let mut argv = vec!["rm", "rm"];
        argv.extend_from_slice(args);
        let RmCommand::Rm {
            recursive, force, ..
        } = Cli::try_parse_from(argv).unwrap().command;
        (recursive, force)
    }

    #[test]
    fn parses_force_flag() {
        assert_eq!(parse(&["-f", "/data"]), (false, true));
        assert_eq!(parse(&["--force", "/data"]), (false, true));
        assert_eq!(parse(&["-rf", "/data"]), (true, true));
        assert_eq!(parse(&["-fr", "/data"]), (true, true));
        assert_eq!(parse(&["/data", "-r", "--force"]), (true, true));
    }

    #[test]
    fn force_defaults_to_off() {
        assert_eq!(parse(&["/data"]), (false, false));
        assert_eq!(parse(&["-r", "/data"]), (true, false));
    }

    #[test]
    fn force_ignores_only_missing_paths() {
        assert!(ignore_error(true, &FsError::file_not_found("/data")));
        assert!(!ignore_error(false, &FsError::file_not_found("/data")));

        let others = [
            FsError::from(io::Error::from(io::ErrorKind::PermissionDenied)),
            FsError::from(io::Error::from(io::ErrorKind::TimedOut)),
            FsError::not_leader("master changed"),
            FsError::invalid_argument("bad path"),
        ];
        for err in &others {
            assert!(!ignore_error(true, err), "{err}");
        }
    }
}
