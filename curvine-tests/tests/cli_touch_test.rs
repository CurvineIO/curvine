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
use curvine_model::SetAttrOpts;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_tests::Testing;
use std::sync::Arc;

/// A timestamp far enough in the past that any update by touch is visible.
const OLD_TIME_MS: i64 = 1_000_000;

async fn touch(fs: &UnifiedFileSystem, path: &str) -> CommonResult<()> {
    FsCommand::try_parse_from(["fs", "touch", path])?
        .execute(fs.clone())
        .await
}

async fn set_old_times(fs: &UnifiedFileSystem, path: &Path) -> CommonResult<()> {
    let opts = SetAttrOpts {
        atime: Some(OLD_TIME_MS),
        mtime: Some(OLD_TIME_MS),
        ..Default::default()
    };
    fs.set_attr(path, opts).await?;
    let status = fs.get_status(path).await?;
    assert_eq!(status.atime, OLD_TIME_MS);
    assert_eq!(status.mtime, OLD_TIME_MS);
    Ok(())
}

#[test]
fn touch_updates_timestamps_of_existing_paths() -> CommonResult<()> {
    let testing = Testing::builder().workers(1).build()?;
    let _cluster = testing.start_cluster()?;
    let rt = Arc::new(Runtime::new("cli-touch-test", 2, 2));
    let fs = testing.get_unified_fs_with_rt(rt.clone())?;

    rt.block_on(async move {
        let file = Path::from_str("/cli-touch/existing/file")?;
        fs.cv().write_string(&file, "hello").await?;
        set_old_times(&fs, &file).await?;

        touch(&fs, file.path()).await?;
        let status = fs.get_status(&file).await?;
        assert!(status.atime > OLD_TIME_MS, "atime not updated: {status:?}");
        assert!(status.mtime > OLD_TIME_MS, "mtime not updated: {status:?}");
        assert_eq!(status.len, 5, "touch must not change the file content");
        assert_eq!(fs.cv().read_string(&file).await?, "hello");

        let dir = Path::from_str("/cli-touch/existing")?;
        set_old_times(&fs, &dir).await?;

        touch(&fs, dir.path()).await?;
        let status = fs.get_status(&dir).await?;
        assert!(status.is_dir);
        assert!(status.atime > OLD_TIME_MS, "atime not updated: {status:?}");
        assert!(status.mtime > OLD_TIME_MS, "mtime not updated: {status:?}");

        Ok(())
    })
}

#[test]
fn touch_creates_missing_file() -> CommonResult<()> {
    let testing = Testing::builder().workers(1).build()?;
    let _cluster = testing.start_cluster()?;
    let rt = Arc::new(Runtime::new("cli-touch-create-test", 2, 2));
    let fs = testing.get_unified_fs_with_rt(rt.clone())?;

    rt.block_on(async move {
        let file = Path::from_str("/cli-touch/missing/parent/file")?;
        assert!(!fs.exists(&file).await?);

        touch(&fs, file.path()).await?;
        let status = fs.get_status(&file).await?;
        assert!(!status.is_dir);
        assert!(status.is_complete);
        assert_eq!(status.len, 0);

        Ok(())
    })
}
