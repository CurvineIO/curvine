// Copyright 2025 OPPO.
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

use curvine_error::FsResult;
use curvine_fs_api::{Path, Writer};
use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
use curvine_tests::Testing;
use std::sync::Arc;

#[test]
fn create_file_uses_4mb_default_block_size() -> FsResult<()> {
    let testing = Testing::builder().default().build()?;
    testing.start_cluster()?;

    let conf = testing.get_active_cluster_conf()?;
    assert_eq!(conf.client.block_size, 4 * 1024 * 1024);

    let rt = Arc::new(AsyncRuntime::single());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        let path = Path::from_str("/default_block_size/file.log")?;
        let mut writer = fs.create(&path, true).await?;
        writer.write(&[1u8; 1024]).await?;
        writer.complete().await?;

        let status = fs.get_status(&path).await?;
        assert_eq!(status.block_size, 4 * 1024 * 1024);
        assert_eq!(status.len, 1024);
        Ok(())
    })
}
