use clap::Subcommand;
use curvine_core_error::CommonResult;
use curvine_fs_api::CurvineURI;
use curvine_model::MkdirOptsBuilder;
use curvine_unified_fs::UnifiedFileSystem;

use super::common::current_process_acl;

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
                let (uid, gid, mode) = current_process_acl(&client);
                let opts = MkdirOptsBuilder::with_conf(&client.conf().client)
                    .create_parent(*parents)
                    .acl(uid, gid, mode)
                    .build();
                let _ = client.mkdir_with_opts(&path, opts).await?;

                println!("Directory created successfully");
                Ok(())
            }
        }
    }
}
