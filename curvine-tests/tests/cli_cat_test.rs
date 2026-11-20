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
use curvine_fs_api::Path;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_tests::Testing;
use std::sync::Arc;

async fn cat(fs: &UnifiedFileSystem, args: &[&str]) -> CommonResult<()> {
    let mut argv = vec!["fs", "cat"];
    argv.extend_from_slice(args);
    let cmd = FsCommand::try_parse_from(argv)?;
    cmd.execute(fs.clone()).await
}

#[test]
fn cat_reads_byte_ranges() -> CommonResult<()> {
    let testing = Testing::builder().workers(1).build()?;
    let _cluster = testing.start_cluster()?;
    let rt = Arc::new(Runtime::new("cli-cat-range-test", 2, 2));
    let fs = testing.get_unified_fs_with_rt(rt.clone())?;

    rt.block_on(async move {
        // The printed bytes go to stdout; the unit tests of `copy_range` check
        // them. Here the command only has to succeed on a real file.
        let file = Path::from_str("/cli-cat/range.txt")?;
        fs.cv().write_string(&file, "0123456789abcdefghij").await?;
        let path = file.path();
        cat(&fs, &[path]).await?;
        cat(&fs, &["--offset", "0", "--length", "10", path]).await?;
        cat(&fs, &["--offset", "5", "--length", "3", path]).await?;
        cat(&fs, &["--offset", "15", path]).await?;

        // A length past the end and an offset past the end both succeed.
        cat(&fs, &["--offset", "18", "--length", "4096", path]).await?;
        cat(&fs, &["--offset", "20", path]).await?;
        cat(&fs, &["--offset", "1000", "--length", "5", path]).await?;

        // Invalid values are rejected before any request is sent.
        let res = cat(&fs, &["--length", "0", path]).await;
        assert!(res.is_err(), "cat --length 0 must be rejected");
        let res = cat(&fs, &["--offset", "-1", path]).await;
        assert!(res.is_err(), "cat --offset -1 must be rejected");

        // A directory is still rejected.
        let res = cat(&fs, &["--offset", "1", "/cli-cat"]).await;
        let msg = res.unwrap_err().to_string();
        assert!(msg.contains("Is a directory"), "{msg}");

        Ok(())
    })
}
