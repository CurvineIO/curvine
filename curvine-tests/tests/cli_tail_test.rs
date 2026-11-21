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

async fn tail(fs: &UnifiedFileSystem, args: &[&str]) -> CommonResult<()> {
    let mut argv = vec!["fs", "tail"];
    argv.extend_from_slice(args);
    let cmd = FsCommand::try_parse_from(argv)?;
    cmd.execute(fs.clone()).await
}

#[test]
fn tail_reads_files_and_rejects_directories() -> CommonResult<()> {
    let testing = Testing::builder().workers(1).build()?;
    let _cluster = testing.start_cluster()?;
    let rt = Arc::new(Runtime::new("cli-tail-test", 2, 2));
    let fs = testing.get_unified_fs_with_rt(rt.clone())?;

    rt.block_on(async move {
        // The printed bytes go to stdout; the unit tests of `copy_tail` check
        // them. Here the command only has to succeed on real files.
        let file = Path::from_str("/cli-tail/dir/file.log")?;
        fs.cv().write_string(&file, "hello world").await?;
        tail(&fs, &[file.path()]).await?;
        tail(&fs, &["-n", "5", file.path()]).await?;
        tail(&fs, &["--bytes", "4096", file.path()]).await?;

        // An empty file prints nothing and succeeds.
        let empty = Path::from_str("/cli-tail/dir/empty.log")?;
        fs.cv().write_string(&empty, "").await?;
        assert_eq!(fs.get_status(&empty).await?.len, 0);
        tail(&fs, &[empty.path()]).await?;

        // A directory is rejected like `cv fs cat` rejects it.
        let res = tail(&fs, &["/cli-tail/dir"]).await;
        let msg = res.unwrap_err().to_string();
        assert!(msg.contains("Is a directory"), "{msg}");

        // A missing path is an error.
        let res = tail(&fs, &["/cli-tail/dir/missing.log"]).await;
        assert!(res.is_err(), "tail of a missing path must fail");

        // A zero size is rejected before any request is sent.
        let res = tail(&fs, &["-n", "0", file.path()]).await;
        assert!(res.is_err(), "tail -n 0 must be rejected");

        Ok(())
    })
}
