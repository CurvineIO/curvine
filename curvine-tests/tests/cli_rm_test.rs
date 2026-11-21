// Copyright 2026 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![recursion_limit = "256"]

use clap::Parser;
use curvine_cli::cmds::FsCommand;
use curvine_client::unified::UnifiedFileSystem;
use curvine_core_error::CommonResult;
use curvine_fs_api::{FileSystem, Path};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_tests::Testing;
use std::sync::Arc;

async fn rm(fs: &UnifiedFileSystem, args: &[&str]) -> CommonResult<()> {
    let mut argv = vec!["fs", "rm"];
    argv.extend_from_slice(args);
    FsCommand::try_parse_from(argv)?.execute(fs.clone()).await
}

#[test]
fn rm_force_ignores_only_missing_paths() -> CommonResult<()> {
    let testing = Testing::builder().workers(1).build()?;
    let _cluster = testing.start_cluster()?;
    let rt = Arc::new(Runtime::new("cli-rm-force-test", 2, 2));
    let fs = testing.get_unified_fs_with_rt(rt.clone())?;

    rt.block_on(async move {
        // A missing path fails without --force and succeeds with it.
        let missing = Path::from_str("/cli-rm/missing")?;
        let msg = rm(&fs, &[missing.path()]).await.unwrap_err().to_string();
        assert!(msg.contains("Cannot remove"), "{msg}");
        rm(&fs, &["-f", missing.path()]).await?;
        rm(&fs, &["-rf", missing.path(), "--skip-trash"]).await?;
        assert!(!fs.exists(&missing).await?);

        // An existing file is still removed.
        let file = Path::from_str("/cli-rm/file")?;
        fs.cv().write_string(&file, "hello").await?;
        rm(&fs, &["--force", file.path(), "--skip-trash"]).await?;
        assert!(!fs.exists(&file).await?);

        // --force does not imply --recursive.
        let dir = Path::from_str("/cli-rm/dir")?;
        let child = Path::from_str("/cli-rm/dir/child")?;
        fs.cv().write_string(&child, "hello").await?;
        let msg = rm(&fs, &["-f", dir.path()]).await.unwrap_err().to_string();
        assert!(msg.contains("Is a directory"), "{msg}");
        assert!(fs.exists(&child).await?);

        rm(&fs, &["-rf", dir.path(), "--skip-trash"]).await?;
        assert!(!fs.exists(&dir).await?);

        Ok(())
    })
}
