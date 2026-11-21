use super::common::format_size;
use super::content_summary::ContentSummary;
use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::CurvineURI;
use curvine_unified_fs::UnifiedFileSystem;

#[derive(Subcommand, Debug)]
pub enum DuCommand {
    /// Calculate directory space usage
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
}

impl DuCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            DuCommand::Du {
                paths,
                human_readable,
                verbose,
                ..
            } => {
                // The header describes the columns, not the path, so it is
                // printed once however many paths follow.
                if *verbose {
                    println!(
                        "{:>15}  {:>15}  {:>15}  PATH",
                        "SIZE", "SINGLE_REPLICA", "ALL_REPLICAS"
                    );
                }

                // One failing path must not hide the sizes of the others, so the
                // error is reported per path and the command fails at the end.
                let mut failed = 0usize;
                for raw in paths {
                    let path = match CurvineURI::new(raw) {
                        Ok(path) => path,
                        Err(e) => {
                            eprintln!("{}", failure_line(raw, &e));
                            failed += 1;
                            continue;
                        }
                    };
                    match crate::cmds::fs::common::calculate_content_summary(&client, &path).await {
                        Ok(summary) => {
                            println!(
                                "{}",
                                format_du_row(&summary, path.full_path(), *human_readable)
                            );
                        }
                        Err(e) => {
                            eprintln!("{}", failure_line(path.full_path(), &e));
                            failed += 1;
                        }
                    }
                }

                match batch_error(failed, paths.len()) {
                    Some(message) => Err(message.into()),
                    None => Ok(()),
                }
            }
        }
    }
}

/// The stderr line for one path that could not be measured.
///
/// One line per operand, in the order the caller supplied them, so a script can
/// pair the failures with its inputs.
fn failure_line(path: &str, error: &impl std::fmt::Display) -> String {
    format!("du: {}: {}", path, error)
}

/// The closing error, or `None` when every path succeeded.
///
/// Extracted from the command so the count and the exit condition are testable
/// without a cluster.
fn batch_error(failed: usize, total: usize) -> Option<String> {
    (failed > 0).then(|| format!("{} of {} paths failed", failed, total))
}

/// Formats one `du` output row: size, single replica size, size of all replicas, path.
fn format_du_row(summary: &ContentSummary, path: &str, human_readable: bool) -> String {
    // Format similar to HDFS du output with single replica data added
    if human_readable {
        format!(
            "{:>15}  {:>15}  {:>15}  {}",
            format_size(summary.length as u64),
            format_size(summary.length as u64),
            format_size(summary.space_consumed as u64),
            path
        )
    } else {
        format!(
            "{:>15}  {:>15}  {:>15}  {}",
            summary.length, summary.length, summary.space_consumed, path
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{batch_error, failure_line, format_du_row, ContentSummary};
    use crate::cmds::fs::commands::FsSubCommand;
    use clap::Parser;
    use curvine_model::FileStatus;

    /// Parses the command the CLI actually exposes: `cv fs du` dispatches on
    /// `FsSubCommand`, which is where `-h`/`--help` are negotiated.
    #[derive(Parser)]
    struct Cli {
        #[command(subcommand)]
        cmd: FsSubCommand,
    }

    fn paths(args: &[&str]) -> Vec<String> {
        let cli = Cli::try_parse_from(args).unwrap_or_else(|e| panic!("{args:?}: {e}"));
        match cli.cmd {
            FsSubCommand::Du { paths, .. } => paths,
            other => panic!("expected du, got {other:?}"),
        }
    }

    /// The aggregation is what decides the exit status, and it is the part a
    /// cluster-free test can still pin: every operand contributes one line, and
    /// the command only fails when at least one did.
    #[test]
    fn failures_are_counted_per_path_and_decide_the_exit() {
        assert_eq!(batch_error(0, 3), None);
        assert_eq!(batch_error(2, 3).as_deref(), Some("2 of 3 paths failed"));
        assert_eq!(batch_error(3, 3).as_deref(), Some("3 of 3 paths failed"));

        assert_eq!(
            failure_line("/a", &"Access denied"),
            "du: /a: Access denied"
        );
    }

    #[test]
    fn one_or_more_paths_are_accepted_in_order() {
        assert_eq!(paths(&["cv", "du", "/a"]), vec!["/a".to_string()]);
        assert_eq!(
            paths(&["cv", "du", "-h", "-v", "/a", "/b", "/c"]),
            vec!["/a".to_string(), "/b".to_string(), "/c".to_string()]
        );
    }

    #[test]
    fn no_path_is_a_usage_error() {
        assert!(Cli::try_parse_from(["cv", "du"]).is_err());
    }

    fn file(len: i64, replicas: i32) -> FileStatus {
        FileStatus {
            len,
            replicas,
            ..Default::default()
        }
    }

    fn mixed_replica_dir() -> ContentSummary {
        let mut summary = ContentSummary::for_empty_dir();
        summary.merge(&ContentSummary::for_file(&file(1024, 1)));
        summary.merge(&ContentSummary::for_file(&file(512, 3)));
        summary
    }

    #[test]
    fn dir_row_reports_replicated_size_of_files() {
        let row = format_du_row(&mixed_replica_dir(), "/du", false);
        let expected = format!("{:>15}  {:>15}  {:>15}  /du", 1536, 1536, 2560);
        assert_eq!(row, expected);
    }

    #[test]
    fn dir_row_human_readable_reports_replicated_size_of_files() {
        let row = format_du_row(&mixed_replica_dir(), "/du", true);
        let expected = format!("{:>15}  {:>15}  {:>15}  /du", "1.5 KB", "1.5 KB", "2.5 KB");
        assert_eq!(row, expected);
    }
}
