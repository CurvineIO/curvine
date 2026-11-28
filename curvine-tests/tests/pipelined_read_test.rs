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

// Kept in its own test binary: it asserts on the process-global
// `client_read_discarded_bytes` counter, which other pipelined read tests
// running in the same process would also move.

use bytes::BytesMut;
use curvine_client::file::FsContext;
use curvine_core_error::CommonResult;
use curvine_error::FsError;
use curvine_fs_api::{Path, Reader, Writer};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;

// Each parallel lane reads a slice and then seeks to its next one. Requests
// must stop at the slice end, or each seek drops up to depth - 1 chunks.
#[test]
fn parallel_pipelined_reads_do_not_prefetch_past_slices() -> CommonResult<()> {
    let testing = Testing::default();

    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;
    conf.client.block_size = 1024 * 1024;
    // 256KB slices, so each 1MB block holds four slices and a lane's slice
    // ends mid-block.
    conf.client.read_chunk_size = 65536;
    conf.client.read_chunk_num = 4;
    conf.client.read_slice_size = 256 * 1024;
    conf.client.read_slice_size_str = "256KB".to_owned();
    conf.client.read_parallel = 4;
    conf.client.read_pipeline_depth = 4;
    let path = Path::from_str("/file_pipelined_parallel_no_waste.data")?;

    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
    rt.block_on(async move {
        let data = Utils::rand_str(4 * 1024 * 1024);
        let mut writer = fs.create(&path, true).await?;
        writer.write(data.as_bytes()).await?;
        writer.complete().await?;

        let discarded = FsContext::get_metrics().read_discarded_bytes.get();
        let mut reader = fs.open(&path).await?;
        let mut content = Vec::with_capacity(data.len());
        let mut buf = BytesMut::zeroed(65536);
        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            content.extend_from_slice(&buf[..n]);
        }
        reader.complete().await?;

        assert_eq!(content, data.as_bytes());
        assert_eq!(
            FsContext::get_metrics().read_discarded_bytes.get(),
            discarded
        );
        Ok::<(), FsError>(())
    })
    .unwrap();

    Ok(())
}
