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

use bytes::BytesMut;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::{MountOptionsBuilder, WriteType};
use curvine_tests::Testing;
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::sys::DataSlice;
use std::env;
use std::sync::Arc;

#[test]
fn test_mount_write_cache() {
    // Check if UFS configuration is available, if not, skip the test
    if env::var("UFS_TEST_PATH").is_err() {
        println!("⚠️  UFS_TEST_PATH is not set, skipping test");
        println!(
            "   Set UFS_TEST_PATH and UFS_TEST_PROPERTIES environment variables to run this test"
        );
        println!("   Example: export UFS_TEST_PATH=hdfs://127.0.0.1:9000");
        println!("   Example: export UFS_TEST_PROPERTIES=\"hdfs.namenode=hdfs://127.0.0.1:9000,hdfs.user=root\"");
        return;
    }

    let testing = Testing::default();
    let rt = Arc::new(AsyncRuntime::single());
    let fs = testing.get_unified_fs_with_rt(rt.clone()).unwrap();

    rt.block_on(async move {
        mount(&fs, WriteType::CacheMode).await;
        mount(&fs, WriteType::FsMode).await;

        write(&fs, WriteType::CacheMode, false).await;
        write(&fs, WriteType::FsMode, false).await;
    })
}

async fn write(fs: &UnifiedFileSystem, write_type: WriteType, random_write: bool) {
    let chunk_size = 64 * 1024;
    let total_size = 1024 * 1024;
    let num_chunks = total_size / chunk_size;

    let dir = format!("write_cache_{:?}", write_type);
    let path = Path::from_str(format!("/{}/test.log", dir)).unwrap();
    let mut writer = fs.create(&path, true).await.unwrap();

    let mut written_data = vec![0u8; total_size];

    // Sequential write all chunks
    for _ in 0..num_chunks {
        let data_str = Utils::rand_str(chunk_size);
        let data = DataSlice::from_str(data_str.clone()).freeze();

        let write_pos = writer.pos() as usize;
        writer.async_write(data.clone()).await.unwrap();
        written_data[write_pos..write_pos + chunk_size].copy_from_slice(data_str.as_bytes());
    }

    if random_write {
        let random_chunk_data = Utils::rand_str(chunk_size);
        let random_data = DataSlice::from_str(random_chunk_data.clone()).freeze();

        let random_pos = (num_chunks / 2 * chunk_size) as i64;
        writer.seek(random_pos).await.unwrap();

        let write_pos = writer.pos() as usize;
        writer.async_write(random_data.clone()).await.unwrap();
        written_data[write_pos..write_pos + chunk_size]
            .copy_from_slice(random_chunk_data.as_bytes());
    }

    writer.complete().await.unwrap();

    verify_read_data(fs, &path, &written_data, write_type).await;

    verify_cv_ufs_consistency(fs, &path).await;
}

async fn verify_read_data(
    fs: &UnifiedFileSystem,
    path: &Path,
    expected_data: &[u8],
    _write_type: WriteType,
) {
    let mut reader = fs.open(path).await.unwrap();

    let mut read_data = BytesMut::zeroed(reader.len() as usize);
    reader.read_full(&mut read_data).await.unwrap();
    reader.complete().await.unwrap();

    assert_eq!(
        Utils::crc32(&read_data),
        Utils::crc32(expected_data),
        "Read data does not match written data"
    );
}

async fn verify_cv_ufs_consistency(fs: &UnifiedFileSystem, path: &Path) {
    let (ufs_path, mnt) = fs.get_mount(path).await.unwrap().unwrap();

    let mut cv_reader = fs.cv().open(path).await.unwrap();
    let mut ufs_reader = mnt.ufs.open(&ufs_path).await.unwrap();

    assert!(cv_reader.status().is_complete);

    assert_eq!(
        cv_reader.status().storage_policy.ufs_mtime,
        ufs_reader.status().mtime
    );

    let mut cv_data = BytesMut::zeroed(cv_reader.len() as usize);
    cv_reader.read_full(&mut cv_data).await.unwrap();

    let mut ufs_data = BytesMut::zeroed(ufs_reader.len() as usize);
    ufs_reader.read_full(&mut ufs_data).await.unwrap();

    assert_eq!(Utils::crc32(&cv_data), Utils::crc32(&ufs_data))
}

async fn mount(fs: &UnifiedFileSystem, write_type: WriteType) {
    let ufs_base = env::var("UFS_TEST_PATH").unwrap();

    let dir = format!("write_cache_{:?}", write_type);
    let ufs_path = Path::from_str(format!("{}/{}", ufs_base, dir)).unwrap();
    let cv_path = Path::from_str(format!("/{}", dir)).unwrap();

    let mut opts_builder = MountOptionsBuilder::new().write_type(write_type);

    // Add properties from environment variable if set
    if let Ok(props_str) = env::var("UFS_TEST_PROPERTIES") {
        for pair in props_str.split(',') {
            if let Some((key, value)) = pair.split_once('=') {
                opts_builder = opts_builder.add_property(key.trim(), value.trim());
            }
        }
    }

    let opts = opts_builder.build();
    fs.mount(&ufs_path, &cv_path, opts).await.unwrap();
}
