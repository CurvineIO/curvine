use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::CurvineURI;
use curvine_unified_fs::UnifiedFileSystem;

#[derive(Subcommand, Debug)]
pub enum CountCommand {
    /// Count files and directories
    Count {
        #[clap(help = "Path of the directory to count")]
        path: String,

        #[clap(long, help = "Force live recursive scan instead of dir-space snapshot")]
        live: bool,
    },
}

impl CountCommand {
    pub async fn execute(&self, mut client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            CountCommand::Count { path, live } => {
                client.disable_unified();
                let path = CurvineURI::new(path)?;
                let mut warn_fallback = true;

                match crate::cmds::fs::common::resolve_content_summary_for_path(
                    &client,
                    &path,
                    *live,
                    &mut warn_fallback,
                )
                .await
                {
                    Ok(summary) => {
                        println!("   DIR_COUNT    FILE_COUNT       CONTENT_SIZE PATHNAME");
                        println!(
                            "{:>12} {:>12} {:>18} {}",
                            summary.directory_count,
                            summary.file_count,
                            summary.logical_bytes,
                            path.full_path()
                        );
                        Ok(())
                    }
                    Err(e) => Err(format!("Cannot count '{}': {}", path.full_path(), e).into()),
                }
            }
        }
    }
}
