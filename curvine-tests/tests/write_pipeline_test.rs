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

use curvine_core_error::CommonResult;
use curvine_fs_api::{Path, Reader, Writer};
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;

#[test]
fn queued_writes_preserve_flush_seek_and_block_boundaries() -> CommonResult<()> {
    let testing = Testing::builder().default().build()?;
    let _cluster = testing.start_cluster()?;
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;
    conf.client.block_size = 64 * 1024;
    conf.client.write_chunk_size = 4096;
    conf.client.write_batch_chunks = 4;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;

    rt.block_on(async move {
        let path = Path::from_str("/window/payload")?;
        let mut writer = fs.create(&path, true).await?;
        let mut expected: Vec<u8> = (0..147_777u32).map(|i| (i ^ (i >> 11)) as u8).collect();
        writer.write(&expected).await?;
        writer.flush().await?;
        let mut reader = fs.open(&path).await?;
        let mut actual = vec![0; expected.len()];
        reader.read_full(&mut actual).await?;
        reader.complete().await?;
        assert_eq!(actual, expected);
        writer.seek(65_503).await?;
        expected[65_503..90_503].fill(123);
        writer.write(&expected[65_503..90_503]).await?;
        writer.complete().await?;
        let mut reader = fs.open(&path).await?;
        reader.read_full(&mut actual).await?;
        reader.complete().await?;
        assert_eq!(actual, expected);
        Ok(())
    })
}
