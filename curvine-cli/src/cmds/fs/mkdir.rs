use clap::Subcommand;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{CurvineURI, FileSystem};
use curvine_common::state::SetAttrOpts;
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
                let _ = client.mkdir(&path, *parents).await?;
                let uid = orpc::sys::get_uid();
                let gid = orpc::sys::get_gid();
                let owner = orpc::sys::get_username_by_uid(uid);
                let group = orpc::sys::get_groupname_by_gid(gid);
                let opts = SetAttrOpts {
                    owner,
                    group,
                    ..Default::default()
                };
                client.set_attr(&path, opts).await?;

                println!("Directory created successfully");
                Ok(())
            }
        }
    }
}
