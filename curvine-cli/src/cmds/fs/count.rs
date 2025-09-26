use clap::Subcommand;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::CurvineURI;
use orpc::CommonResult;

#[derive(Subcommand, Debug)]
pub enum CountCommand {
    /// Count files and directories
    Count {
        #[clap(help = "Path of the directory to count")]
        path: String,
    },
}

impl CountCommand {
    pub async fn execute(&self, mut client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            CountCommand::Count { path } => {
                client.disable_unified();
                let path = CurvineURI::new(path)?;

                match crate::cmds::fs::common::calculate_content_summary(&client, &path).await {
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
