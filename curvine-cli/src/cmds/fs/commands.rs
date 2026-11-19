use clap::{Parser, Subcommand};
use curvine_core_error::CommonResult;
use curvine_unified_fs::UnifiedFileSystem;
use std::path::PathBuf;

use crate::cmds::fs::{
    blocks::BlocksCommand, cat::CatCommand, chmod::ChmodCommand, chown::ChownCommand,
    count::CountCommand, df::DfCommand, du::DuCommand, free::FreeCommand, get::GetCommand,
    ls::LsCommand, mkdir::MkdirCommand, mv::MvCommand, put::PutCommand, rm::RmCommand,
    stat::StatCommand, tail::TailCommand, touch::TouchCommand, trash::TrashCommand,
};
use crate::cmds::transfer::OutputFormat;

#[derive(Parser, Debug)]
pub struct FsCommand {
    #[clap(long, global = true, help = "Query only Curvine cached data")]
    pub cache_only: bool,

    #[clap(subcommand)]
    action: FsSubCommand,
}

#[derive(Subcommand, Debug)]
pub enum FsSubCommand {
    /// List files and directories
    ///
    /// The entries are written to stdout and nothing else is, so the output can be
    /// piped into `wc`, `xargs` or `sort`. The `Found N items` summary is
    /// informational and goes to stderr.
    Ls {
        #[clap(value_name = "path", default_value = "/")]
        path: String,

        #[clap(
            short = 'C',
            long,
            help = "Display the paths of files and directories only"
        )]
        path_only: bool,

        #[clap(short = 'd', long, help = "Directories are listed as plain files")]
        directory: bool,

        #[clap(
            short = 'H',
            long,
            help = "Formats the sizes of files in a human-readable fashion"
        )]
        human_readable: bool,

        #[clap(
            short = 'q',
            long,
            help = "Print ? instead of non-printable characters"
        )]
        hide_non_printable: bool,

        #[clap(
            short = 'R',
            long,
            help = "Recursively list the contents of directories"
        )]
        recursive: bool,

        #[clap(short = 'r', long, help = "Reverse the order of the sort")]
        reverse: bool,

        #[clap(
            short = 't',
            long,
            help = "Sort files by modification time (most recent first)"
        )]
        mtime: bool,

        #[clap(short = 'S', long, help = "Sort files by size")]
        size: bool,

        #[clap(
            short = 'u',
            long,
            help = "Use time of last access instead of modification for display and sorting"
        )]
        atime: bool,

        #[clap(short = 'l', long, help = "Use a long listing format")]
        long_format: bool,
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

        #[clap(
            short = 'n',
            long,
            help = "Do not overwrite the destination if it already exists"
        )]
        no_clobber: bool,
    },

    /// Display file contents
    Cat {
        #[clap(help = "Path of the file to display")]
        path: String,

        #[clap(long, default_value_t = 0, help = "Byte offset to start reading at")]
        offset: u64,

        #[clap(
            long,
            value_parser = clap::value_parser!(u64).range(1..),
            help = "Maximum number of bytes to print [default: the rest of the file]"
        )]
        length: Option<u64>,
    },

    /// Display the last bytes of a file
    Tail {
        #[clap(help = "Path of the file to display")]
        path: String,

        #[clap(
            short = 'n',
            long,
            default_value_t = super::tail::DEFAULT_TAIL_BYTES,
            value_parser = clap::value_parser!(u64).range(1..),
            help = "Number of trailing bytes to print"
        )]
        bytes: u64,
    },

    /// Show disk usage statistics
    #[command(disable_help_flag = true)]
    Df {
        #[clap(short, long, help = "Show human-readable sizes")]
        human_readable: bool,
        #[clap(long, action = clap::ArgAction::Help, help = "Print help")]
        help: Option<bool>,
    },

    /// Create an empty file or update timestamp
    Touch {
        #[clap(help = "Path of the file to create or touch")]
        path: String,
    },

    /// Calculate directory space usage
    #[command(disable_help_flag = true)]
    Du {
        #[clap(
            required = true,
            num_args = 1..,
            help = "Paths of the files or directories to calculate"
        )]
        paths: Vec<String>,

        #[clap(short, long, help = "Show human-readable sizes")]
        human_readable: bool,

        #[clap(short, long, help = "Display the names of columns as a header line")]
        verbose: bool,

        #[clap(long, action = clap::ArgAction::Help, help = "Print help")]
        help: Option<bool>,
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
        #[clap(
            long,
            help = "Delete permanently, bypassing the recycle bin (if enabled)"
        )]
        skip_trash: bool,
        #[clap(
            long = "async",
            help = "Return immediately and let the master reclaim the entry in the background (Curvine paths only)"
        )]
        async_delete: bool,
        #[clap(
            short,
            long,
            help = "Ignore a path that does not exist; other errors still fail"
        )]
        force: bool,
    },

    /// Recycle bin operations
    Trash {
        #[clap(subcommand)]
        command: TrashCommand,
    },

    /// Stat file or directory
    Stat {
        #[clap(help = "Path of the file or directory to stat")]
        path: String,

        #[clap(long, help = "Print the metadata as JSON")]
        json: bool,
    },

    /// Count files and directories
    Count {
        #[clap(help = "Path of the directory to count")]
        path: String,
    },

    /// Move file or directory
    Mv {
        #[clap(help = "Source path to move")]
        src_path: String,
        #[clap(help = "Destination path")]
        dst_path: String,
    },

    /// chmod file or directory
    Chmod {
        #[clap(
            allow_hyphen_values = true,
            help = "Permissions to set: octal (755, 0755, 0o755) or symbolic \
                    [ugoa]*[+-=][rwx]*, comma-separated (u+x, go-w, a=r,u+w)"
        )]
        mode: String,
        #[clap(help = "Path of the file/directory to modify")]
        path: String,
        #[clap(
            long,
            help = "Recursively apply permissions to all files and directories"
        )]
        recursive: bool,
    },

    /// chown file or directory
    Chown {
        #[clap(help = "Owner/Group to set in format owner:group")]
        owner_group: String,
        #[clap(help = "Path of the file/directory to modify")]
        path: String,
        #[clap(
            long,
            help = "Recursively apply ownership to all files and directories"
        )]
        recursive: bool,
    },

    /// Get block location information for a file
    Blocks {
        #[clap(help = "Path to get block location information")]
        path: String,

        /// Output format
        #[clap(long, value_enum, default_value_t = OutputFormat::Table)]
        format: OutputFormat,
    },

    /// Free path: release Curvine cache for UFS-synced data (outputs inode count and released bytes)
    Free {
        #[clap(help = "Path of the file or directory to free")]
        path: String,
        #[clap(short, long, help = "Recursively free all files under the directory")]
        recursive: bool,
    },
}

impl FsCommand {
    pub async fn execute(&self, mut client: UnifiedFileSystem) -> CommonResult<()> {
        if self.cache_only {
            client.disable_unified();
        }
        match &self.action {
            FsSubCommand::Ls {
                path,
                path_only,
                directory,
                human_readable,
                hide_non_printable,
                recursive,
                reverse,
                mtime,
                size,
                atime,
                long_format,
            } => {
                let ls_cmd = LsCommand::Ls {
                    path: path.clone(),
                    path_only: *path_only,
                    directory: *directory,
                    human_readable: *human_readable,
                    hide_non_printable: *hide_non_printable,
                    recursive: *recursive,
                    reverse: *reverse,
                    mtime: *mtime,
                    size: *size,
                    atime: *atime,
                    long_format: *long_format,
                };
                ls_cmd.execute(client).await
            }

            FsSubCommand::Mkdir { path, parents } => {
                let mkdir_cmd = MkdirCommand::Mkdir {
                    path: path.clone(),
                    parents: *parents,
                };
                mkdir_cmd.execute(client).await
            }

            FsSubCommand::Put {
                local_path,
                remote_path,
                no_clobber,
            } => {
                let put_cmd = PutCommand::Put {
                    local_path: local_path.clone(),
                    remote_path: remote_path.clone(),
                    no_clobber: *no_clobber,
                };
                put_cmd.execute(client).await
            }

            FsSubCommand::Cat {
                path,
                offset,
                length,
            } => {
                let cat_cmd = CatCommand::Cat {
                    path: path.clone(),
                    offset: *offset,
                    length: *length,
                };
                cat_cmd.execute(client).await
            }

            FsSubCommand::Tail { path, bytes } => {
                let tail_cmd = TailCommand::Tail {
                    path: path.clone(),
                    bytes: *bytes,
                };
                tail_cmd.execute(client).await
            }

            FsSubCommand::Df { human_readable, .. } => {
                let df_cmd = DfCommand::Df {
                    human_readable: *human_readable,
                    help: None,
                };
                df_cmd.execute(client).await
            }

            FsSubCommand::Touch { path } => {
                let touch_cmd = TouchCommand::Touch { path: path.clone() };
                touch_cmd.execute(client).await
            }

            FsSubCommand::Du {
                paths,
                human_readable,
                verbose,
                ..
            } => {
                let du_cmd = DuCommand::Du {
                    paths: paths.clone(),
                    human_readable: *human_readable,
                    verbose: *verbose,
                    help: None,
                };
                du_cmd.execute(client).await
            }

            FsSubCommand::Get { path, local_path } => {
                let get_cmd = GetCommand::Get {
                    path: path.clone(),
                    local_path: local_path.clone(),
                };
                get_cmd.execute(client).await
            }

            FsSubCommand::Rm {
                path,
                recursive,
                skip_trash,
                async_delete,
                force,
            } => {
                let rm_cmd = RmCommand::Rm {
                    path: path.clone(),
                    recursive: *recursive,
                    skip_trash: *skip_trash,
                    async_delete: *async_delete,
                    force: *force,
                };
                rm_cmd.execute(client).await
            }

            FsSubCommand::Trash { command } => command.execute(client).await,

            FsSubCommand::Stat { path, json } => {
                let stat_cmd = StatCommand::Stat {
                    path: path.clone(),
                    json: *json,
                };
                stat_cmd.execute(client).await
            }

            FsSubCommand::Count { path } => {
                let count_cmd = CountCommand::Count { path: path.clone() };
                count_cmd.execute(client).await
            }

            FsSubCommand::Mv { src_path, dst_path } => {
                let mv_cmd = MvCommand::Mv {
                    source: src_path.clone(),
                    destination: dst_path.clone(),
                };
                mv_cmd.execute(client).await
            }

            FsSubCommand::Chmod {
                mode,
                path,
                recursive,
            } => {
                let chmod_cmd = ChmodCommand::Chmod {
                    mode: mode.clone(),
                    path: path.clone(),
                    recursive: *recursive,
                };
                chmod_cmd.execute(client).await
            }

            FsSubCommand::Chown {
                owner_group,
                path,
                recursive,
            } => {
                let chown_cmd = ChownCommand::Chown {
                    owner_group: owner_group.clone(),
                    path: path.clone(),
                    recursive: *recursive,
                };
                chown_cmd.execute(client).await
            }

            FsSubCommand::Blocks { path, format } => {
                let blocks_cmd = BlocksCommand::Blocks {
                    path: path.clone(),
                    format: *format,
                };
                blocks_cmd.execute(client).await
            }

            FsSubCommand::Free { path, recursive } => {
                let free_cmd = FreeCommand::Free {
                    path: path.clone(),
                    recursive: *recursive,
                };
                free_cmd.execute(client).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FsCommand, FsSubCommand, OutputFormat};
    use clap::error::ErrorKind;
    use clap::Parser;

    fn parse_chmod(args: &[&str]) -> (String, String, bool) {
        let mut argv = vec!["fs", "chmod"];
        argv.extend_from_slice(args);
        let cmd = FsCommand::try_parse_from(argv).unwrap();
        match cmd.action {
            FsSubCommand::Chmod {
                mode,
                path,
                recursive,
            } => (mode, path, recursive),
            other => panic!("parsed as {other:?}"),
        }
    }

    #[test]
    fn chmod_accepts_symbolic_modes() {
        assert_eq!(
            parse_chmod(&["go-w", "/a"]),
            ("go-w".to_string(), "/a".to_string(), false)
        );
        assert_eq!(
            parse_chmod(&["u+x,g=rx", "/a", "--recursive"]),
            ("u+x,g=rx".to_string(), "/a".to_string(), true)
        );
    }

    #[test]
    fn chmod_accepts_modes_starting_with_a_hyphen() {
        assert_eq!(
            parse_chmod(&["-x", "/a"]),
            ("-x".to_string(), "/a".to_string(), false)
        );
        assert_eq!(
            parse_chmod(&["--recursive", "-w", "/a"]),
            ("-w".to_string(), "/a".to_string(), true)
        );
        assert_eq!(
            parse_chmod(&["-w", "/a", "--recursive"]),
            ("-w".to_string(), "/a".to_string(), true)
        );
    }

    #[test]
    fn chmod_keeps_octal_modes_and_the_recursive_flag() {
        assert_eq!(
            parse_chmod(&["--recursive", "755", "/a"]),
            ("755".to_string(), "/a".to_string(), true)
        );
    }

    fn blocks_format(args: &[&str]) -> Result<OutputFormat, clap::Error> {
        let mut argv = vec!["fs", "blocks", "/a"];
        argv.extend_from_slice(args);
        match FsCommand::try_parse_from(argv)?.action {
            FsSubCommand::Blocks { format, .. } => Ok(format),
            other => panic!("parsed as {other:?}"),
        }
    }

    #[test]
    fn blocks_format_defaults_to_table_and_accepts_json() {
        assert_eq!(blocks_format(&[]).unwrap(), OutputFormat::Table);
        let json = blocks_format(&["--format", "json"]).unwrap();
        assert_eq!(json, OutputFormat::Json);
        let table = blocks_format(&["--format", "table"]).unwrap();
        assert_eq!(table, OutputFormat::Table);
    }

    #[test]
    fn blocks_rejects_an_unknown_format() {
        // These used to fall back to the table silently and exit 0.
        for bad in ["jsno", "yaml"] {
            let err = blocks_format(&["--format", bad]).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidValue, "{bad}");
            assert!(err.to_string().contains("table, json"), "{err}");
        }
    }
}
