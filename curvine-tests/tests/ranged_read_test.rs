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
use curvine_client::file::{FsContext, FsReader};
use curvine_core_error::CommonResult;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{Path, Reader, Writer};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;

async fn read_range(reader: &mut FsReader, start: i64, end: i64) -> FsResult<Vec<u8>> {
    reader.seek(start).await?;
    let mut content = Vec::with_capacity((end - start) as usize);
    let mut buf = BytesMut::zeroed(65536);
    while (content.len() as i64) < end - start {
        let want = buf.len().min((end - start) as usize - content.len());
        let n = reader.read(&mut buf[..want]).await?;
        if n == 0 {
            break;
        }
        content.extend_from_slice(&buf[..n]);
    }
    Ok(content)
}

// A seek carries no read length. Without a read-end hint, a pipelined reader
// keeps requesting chunks past the range and drops them on the next seek.
#[test]
fn ranged_reads_with_read_end_do_not_prefetch_past_range() -> CommonResult<()> {
    let testing = Testing::default();

    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;
    conf.client.block_size = 1024 * 1024;
    conf.client.read_chunk_size = 65536;
    // No background buffer and one lane, so every request comes from the
    // caller's reads.
    conf.client.read_chunk_num = 1;
    conf.client.read_parallel = 1;
    conf.client.read_pipeline_depth = 4;
    let path = Path::from_str("/file_ranged_read_end.data")?;

    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
    rt.block_on(async move {
        let data = Utils::rand_str(4 * 1024 * 1024);
        let data = data.as_bytes();
        let mut writer = fs.create(&path, true).await?;
        writer.write(data).await?;
        writer.complete().await?;

        let discarded = FsContext::get_metrics().read_discarded_bytes.get();
        let mut reader = fs.open(&path).await?;

        // Unaligned ranges inside one block, and in a later block.
        for (start, end) in [(100_000, 200_000), (1_500_000, 1_700_000)] {
            reader.set_read_end(end);
            let content = read_range(&mut reader, start, end).await?;
            assert_eq!(content, &data[start as usize..end as usize]);
        }

        // Reading past the hinted end still returns the data.
        reader.set_read_end(2_600_000);
        let content = read_range(&mut reader, 2_500_000, 2_900_000).await?;
        assert_eq!(content, &data[2_500_000..2_900_000]);

        reader.complete().await?;
        assert_eq!(
            FsContext::get_metrics().read_discarded_bytes.get(),
            discarded
        );
        Ok::<(), FsError>(())
    })
    .unwrap();

    Ok(())
}
