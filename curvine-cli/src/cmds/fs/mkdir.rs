use clap::Subcommand;
use curvine_fs_api::{CurvineURI, FileSystem};
use curvine_model::SetAttrOpts;
use curvine_unified_fs::UnifiedFileSystem;
use curvine_core_error::CommonResult;

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
                let uid = curvine_sys::get_uid();
                let gid = curvine_sys::get_gid();
                let owner = curvine_sys::get_username_by_uid(uid);
                let group = curvine_sys::get_groupname_by_gid(gid);
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
