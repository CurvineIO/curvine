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
        #[clap(
            long = "async",
            help = "Return immediately and let the master reclaim the entry in \
                    the background (Curvine paths only)"
        )]
        async_delete: bool,
    },
}

impl RmCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            RmCommand::Rm {
                path,
                recursive,
                skip_trash,
                async_delete,
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

#[cfg(test)]
mod tests {
    use super::RmCommand;
    use clap::Parser;

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
}
