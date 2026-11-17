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
use curvine_error::FsError;
use curvine_fs_api::Path;
use curvine_model::OpenFlags;
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_tests::Testing;
use std::sync::Arc;

async fn put(fs: &UnifiedFileSystem, args: &[&str]) -> CommonResult<()> {
    let mut argv = vec!["fs", "put"];
    argv.extend_from_slice(args);
    let cmd = FsCommand::try_parse_from(argv)?;
    cmd.execute(fs.clone()).await
}

/// Writes a local file to upload and returns its path.
fn local_file(name: &str, content: &str) -> String {
    let dir_name = format!("curvine-cli-put-{}", std::process::id());
    let dir = std::env::temp_dir().join(dir_name);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    path.to_str().unwrap().to_owned()
}

#[test]
fn put_no_clobber_keeps_existing_destination() -> CommonResult<()> {
    let testing = Testing::builder().workers(1).build()?;
    let _cluster = testing.start_cluster()?;
    let rt = Arc::new(Runtime::new("cli-put-no-clobber-test", 2, 2));
    let fs = testing.get_unified_fs_with_rt(rt.clone())?;
    let local = local_file("no-clobber.txt", "uploaded");

    rt.block_on(async move {
        let local = local.as_str();

        // A new destination is uploaded.
        let created = Path::from_str("/cli-put/new/file")?;
        put(&fs, &["-n", local, created.path()]).await?;
        assert_eq!(fs.cv().read_string(&created).await?, "uploaded");

        // An existing destination is left untouched and the command fails.
        let existing = Path::from_str("/cli-put/existing/file")?;
        fs.cv().write_string(&existing, "original").await?;
        let res = put(&fs, &["-n", local, existing.path()]).await;
        let msg = res.unwrap_err().to_string();
        assert!(msg.contains("already exists"), "{msg}");
        assert_eq!(fs.cv().read_string(&existing).await?, "original");

        // Without -n the same destination is still overwritten.
        put(&fs, &[local, existing.path()]).await?;
        assert_eq!(fs.cv().read_string(&existing).await?, "uploaded");

        Ok(())
    })
}

#[test]
fn put_no_clobber_checks_resolved_directory_destination() -> CommonResult<()> {
    let testing = Testing::builder().workers(1).build()?;
    let _cluster = testing.start_cluster()?;
    let rt = Arc::new(Runtime::new("cli-put-directory-test", 2, 2));
    let fs = testing.get_unified_fs_with_rt(rt.clone())?;
    let local = local_file("in-dir.txt", "uploaded");

    rt.block_on(async move {
        let local = local.as_str();

        // The directory already holds a file with the local file name.
        let existing = Path::from_str("/cli-put/dir/in-dir.txt")?;
        fs.cv().write_string(&existing, "original").await?;

        for remote in ["/cli-put/dir", "/cli-put/dir/"] {
            let res = put(&fs, &["-n", local, remote]).await;
            assert!(res.is_err(), "put -n to {remote} must fail");
            assert_eq!(fs.cv().read_string(&existing).await?, "original");
        }

        // A trailing '/' on a missing directory uploads under the local name.
        put(&fs, &["-n", local, "/cli-put/fresh/"]).await?;
        let created = Path::from_str("/cli-put/fresh/in-dir.txt")?;
        assert_eq!(fs.cv().read_string(&created).await?, "uploaded");

        Ok(())
    })
}

#[test]
fn exclusive_create_keeps_a_file_created_first() -> CommonResult<()> {
    let testing = Testing::builder().workers(1).build()?;
    let _cluster = testing.start_cluster()?;
    let rt = Arc::new(Runtime::new("cli-put-exclusive-test", 2, 2));
    let fs = testing.get_unified_fs_with_rt(rt.clone())?;

    rt.block_on(async move {
        // What `put -n` meets when another client creates the destination after
        // its lookup: the open, with the flags `put -n` sends, must fail by
        // itself and leave that file as it is.
        let path = Path::from_str("/cli-put/race/file")?;
        fs.cv().write_string(&path, "winner").await?;

        let opts = fs.cv().create_opts_builder().create_parent(true).build();
        let flags = OpenFlags::new_create().set_exclusive(true);
        let res = fs.open_with_opts(&path, opts, flags).await;
        assert!(matches!(res, Err(FsError::FileAlreadyExists(_))));
        assert_eq!(fs.cv().read_string(&path).await?, "winner");

        Ok(())
    })
}
