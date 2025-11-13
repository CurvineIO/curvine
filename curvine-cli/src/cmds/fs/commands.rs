use clap::{Parser, Subcommand};
use curvine_client::unified::UnifiedFileSystem;
use orpc::CommonResult;
use std::path::PathBuf;

use crate::cmds::fs::{
    blocks::BlocksCommand, cat::CatCommand, chmod::ChmodCommand, count::CountCommand,
    df::DfCommand, du::DuCommand, get::GetCommand, ls::LsCommand, mkdir::MkdirCommand,
    mv::MvCommand, put::PutCommand, rm::RmCommand, stat::StatCommand, touch::TouchCommand,
};

#[derive(Parser, Debug)]
pub struct FsCommand {
    #[clap(
        short = 'c',
        long,
        global = true,
        help = "Query only Curvine cached data"
    )]
    pub cache_only: bool,

    #[clap(subcommand)]
    action: FsSubCommand,
}

#[derive(Subcommand, Debug)]
pub enum FsSubCommand {
    /// List files and directories
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

    /// Move file or directory
    Mv {
        #[clap(help = "Source path to move")]
        src_path: String,
        #[clap(help = "Destination path")]
        dst_path: String,
    },

    /// chmod file or directory
    Chmod {
        #[clap(help = "Permissions to set (e.g., 755)")]
        mode: String,
        #[clap(help = "Path of the file/directory to modify")]
        path: String,
        #[clap(help = "Recursively apply permissions to all files and directories")]
        recursive: bool,
    },

    /// Get block location information for a file
    Blocks {
        #[clap(help = "Path to get block location information")]
        path: String,

        /// Output format: table, json
        #[clap(long, default_value = "table")]
        format: String,
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
            } => {
                let put_cmd = PutCommand::Put {
                    local_path: local_path.clone(),
                    remote_path: remote_path.clone(),
                };
                put_cmd.execute(client).await
            }

            FsSubCommand::Cat { path } => {
                let cat_cmd = CatCommand::Cat { path: path.clone() };
                cat_cmd.execute(client).await
            }

            FsSubCommand::Df { human_readable } => {
                let df_cmd = DfCommand::Df {
                    human_readable: *human_readable,
                };
                df_cmd.execute(client).await
            }

            FsSubCommand::Touch { path } => {
                let touch_cmd = TouchCommand::Touch { path: path.clone() };
                touch_cmd.execute(client).await
            }

            FsSubCommand::Du {
                path,
                human_readable,
                verbose,
            } => {
                let du_cmd = DuCommand::Du {
                    path: path.clone(),
                    human_readable: *human_readable,
                    verbose: *verbose,
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

            FsSubCommand::Rm { path, recursive } => {
                let rm_cmd = RmCommand::Rm {
                    path: path.clone(),
                    recursive: *recursive,
                };
                rm_cmd.execute(client).await
            }

            FsSubCommand::Stat { path } => {
                let stat_cmd = StatCommand::Stat { path: path.clone() };
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

            FsSubCommand::Blocks { path, format } => {
                let blocks_cmd = BlocksCommand::Blocks {
                    path: path.clone(),
                    format: format.clone(),
                };
                blocks_cmd.execute(client).await
            }
        }
    }
}
