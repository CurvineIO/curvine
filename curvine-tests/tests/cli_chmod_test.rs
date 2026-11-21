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

async fn chmod(fs: &UnifiedFileSystem, args: &[&str]) -> CommonResult<()> {
    let mut argv = vec!["fs", "chmod"];
    argv.extend_from_slice(args);
    FsCommand::try_parse_from(argv)?.execute(fs.clone()).await
}

async fn set_mode(fs: &UnifiedFileSystem, path: &Path, mode: u32) -> CommonResult<()> {
    let opts = SetAttrOpts {
        mode: Some(mode),
        ..Default::default()
    };
    fs.set_attr(path, opts).await?;
    assert_eq!(mode_of(fs, path).await?, mode);
    Ok(())
}

async fn mode_of(fs: &UnifiedFileSystem, path: &Path) -> CommonResult<u32> {
    Ok(fs.get_status(path).await?.mode & 0o7777)
}

#[test]
fn chmod_applies_symbolic_modes_to_the_current_mode() -> CommonResult<()> {
    let testing = Testing::builder().workers(1).build()?;
    let _cluster = testing.start_cluster()?;
    let rt = Arc::new(Runtime::new("cli-chmod-symbolic-test", 2, 2));
    let fs = testing.get_unified_fs_with_rt(rt.clone())?;

    rt.block_on(async move {
        let file = Path::from_str("/cli-chmod/symbolic/file")?;
        fs.cv().write_string(&file, "hello").await?;

        set_mode(&fs, &file, 0o644).await?;
        chmod(&fs, &["u+x", file.path()]).await?;
        assert_eq!(mode_of(&fs, &file).await?, 0o744);

        set_mode(&fs, &file, 0o666).await?;
        chmod(&fs, &["go-w", file.path()]).await?;
        assert_eq!(mode_of(&fs, &file).await?, 0o644);

        set_mode(&fs, &file, 0o777).await?;
        chmod(&fs, &["a=r", file.path()]).await?;
        assert_eq!(mode_of(&fs, &file).await?, 0o444);

        chmod(&fs, &["-r,u+rw", file.path()]).await?;
        assert_eq!(mode_of(&fs, &file).await?, 0o600);

        // Octal modes still replace the whole mode.
        chmod(&fs, &["0755", file.path()]).await?;
        assert_eq!(mode_of(&fs, &file).await?, 0o755);

        // An invalid mode is rejected before the path is touched.
        for mode in ["u+z", "x+r", "u", "u+x,"] {
            assert!(chmod(&fs, &[mode, file.path()]).await.is_err(), "{mode}");
            assert_eq!(mode_of(&fs, &file).await?, 0o755, "{mode}");
        }

        Ok(())
    })
}

#[test]
fn chmod_recursive_symbolic_mode_is_relative_to_each_entry() -> CommonResult<()> {
    let testing = Testing::builder().workers(1).build()?;
    let _cluster = testing.start_cluster()?;
    let rt = Arc::new(Runtime::new("cli-chmod-recursive-test", 2, 2));
    let fs = testing.get_unified_fs_with_rt(rt.clone())?;

    rt.block_on(async move {
        let root = Path::from_str("/cli-chmod/tree")?;
        let sub = Path::from_str("/cli-chmod/tree/sub")?;
        let file = Path::from_str("/cli-chmod/tree/file")?;
        let nested = Path::from_str("/cli-chmod/tree/sub/nested")?;
        fs.cv().write_string(&file, "a").await?;
        fs.cv().write_string(&nested, "b").await?;

        set_mode(&fs, &root, 0o755).await?;
        set_mode(&fs, &sub, 0o700).await?;
        set_mode(&fs, &file, 0o644).await?;
        set_mode(&fs, &nested, 0o600).await?;

        chmod(&fs, &["--recursive", "g+w", root.path()]).await?;
        assert_eq!(mode_of(&fs, &root).await?, 0o775);
        assert_eq!(mode_of(&fs, &sub).await?, 0o720);
        assert_eq!(mode_of(&fs, &file).await?, 0o664);
        assert_eq!(mode_of(&fs, &nested).await?, 0o620);

        chmod(&fs, &["-w", root.path(), "--recursive"]).await?;
        assert_eq!(mode_of(&fs, &root).await?, 0o555);
        assert_eq!(mode_of(&fs, &sub).await?, 0o500);
        assert_eq!(mode_of(&fs, &file).await?, 0o444);
        assert_eq!(mode_of(&fs, &nested).await?, 0o400);

        // Without --recursive only the named directory changes.
        chmod(&fs, &["u+w", root.path()]).await?;
        assert_eq!(mode_of(&fs, &root).await?, 0o755);
        assert_eq!(mode_of(&fs, &sub).await?, 0o500);

        // Octal modes keep the single master-side recursive set_attr.
        chmod(&fs, &["--recursive", "750", root.path()]).await?;
        for path in [&root, &sub, &file, &nested] {
            assert_eq!(mode_of(&fs, path).await?, 0o750, "{path}");
        }

        Ok(())
    })
}
