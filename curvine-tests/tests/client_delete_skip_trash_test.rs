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

use curvine_client::unified::UnifiedFileSystem;
use curvine_core_error::CommonResult;
use curvine_fs_api::{FileSystem, Path};
use curvine_runtime::runtime::{RpcRuntime, Runtime};
use curvine_tests::Testing;
use std::sync::Arc;

fn fs_with(master_trash: bool, client_skip_trash: Option<bool>) -> UnifiedFileSystem {
    let testing = Testing::builder()
        .workers(1)
        .mutate_conf(move |conf| {
            conf.master.trash_enabled = master_trash;
            if let Some(skip) = client_skip_trash {
                conf.client.skip_trash = skip;
            }
        })
        .build()
        .unwrap();
    testing.start_cluster().unwrap();
    let rt = Arc::new(Runtime::new("client-delete-skip-trash-test", 2, 2));
    testing.get_unified_fs_with_rt(rt).unwrap()
}

/// The client config defaults to honouring the recycle bin, so a delete must
/// leave a restorable entry while the master's trash is enabled. The config is
/// left untouched here so the shipped default is what gets exercised.
#[test]
fn client_delete_uses_the_trash_by_default() -> CommonResult<()> {
    let fs = fs_with(true, None);
    let path = Path::from_str("/skip-trash/default")?;

    fs.clone_runtime().block_on(async {
        fs.create(&path, true).await?;
        let res = fs.delete(&path, false).await?;
        assert!(res.trashed, "the master must move the entry into /.Trash");
        assert!(!fs.exists(&path).await?);
        let listed = fs.list_trash().await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].origin, path.path());
        Ok(())
    })
}

/// Opting in purges permanently even while the master's recycle bin is enabled.
#[test]
fn client_delete_skips_the_trash_when_configured() -> CommonResult<()> {
    let fs = fs_with(true, Some(true));
    let path = Path::from_str("/skip-trash/opt-in")?;

    fs.clone_runtime().block_on(async {
        fs.create(&path, true).await?;
        let res = fs.delete(&path, false).await?;
        assert!(!res.trashed, "the client must not use the recycle bin");
        assert!(!fs.exists(&path).await?);
        assert!(fs.list_trash().await?.is_empty());
        Ok(())
    })
}

/// The bare `CurvineFileSystem` API is flag-driven: it takes an explicit
/// `skip_trash` argument and does not consult the client-wide setting.
#[test]
fn curvine_filesystem_delete_ignores_the_client_setting() -> CommonResult<()> {
    let fs = fs_with(true, Some(true));
    let path = Path::from_str("/skip-trash/cv")?;

    fs.clone_runtime().block_on(async {
        fs.create(&path, true).await?;
        let res = fs.cv().delete(&path, false).await?;
        assert!(res.trashed, "the low-level API must still honour the trash");
        assert!(!fs.exists(&path).await?);
        assert_eq!(fs.list_trash().await?.len(), 1);
        Ok(())
    })
}
