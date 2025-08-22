use clap::Subcommand;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{CurvineURI, FileSystem};
use orpc::CommonResult;

#[derive(Subcommand, Debug)]
pub enum MkdirCommand {
    /// Create a directory
    Mkdir {
        #[clap(help = "Directory path to create")]
        path: String,

        #[clap(short, long, help = "Create parent directories as needed")]
        parents: bool,
    },
}

impl MkdirCommand {
    pub async fn execute(&self, client: UnifiedFileSystem) -> CommonResult<()> {
        match self {
            MkdirCommand::Mkdir { path, parents } => {
                println!("Creating directory: {} (parents: {})", path, parents);
                let path = CurvineURI::new(path)?;
                client.mkdir(&path, *parents).await?;
                println!("Directory created successfully");
                Ok(())
            }
        }
    }
}
