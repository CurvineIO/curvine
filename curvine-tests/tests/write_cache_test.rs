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
use curvine_client::unified::{UfsFileSystem, UnifiedFileSystem, UnifiedReader};
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::{MountOptionsBuilder, MountType, WriteType};
use curvine_tests::Testing;
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::sys::DataSlice;
use std::env;
use std::sync::Arc;
use std::time::Duration;

fn get_fs() -> UnifiedFileSystem {
    env::set_var("UFS_TEST_PATH", "s3://cv-test/ufs-test");
    env::set_var("UFS_TEST_PROPERTIES", "s3.credentials.access=minioadmin,s3.credentials.secret=minioadmin ,s3.region_name=cn,s3.endpoint_url=http://192.168.108.129:9000");

    // Check if UFS configuration is available, if not, skip the test
    if env::var("UFS_TEST_PATH").is_err() {
        println!("⚠️  UFS_TEST_PATH is not set, skipping test");
        println!(
            "   Set UFS_TEST_PATH and UFS_TEST_PROPERTIES environment variables to run this test"
        );
        println!("   Example: export UFS_TEST_PATH=hdfs://127.0.0.1:9000");
        println!("   Example: export UFS_TEST_PROPERTIES=\"hdfs.namenode=hdfs://127.0.0.1:9000,hdfs.user=root\"");
        panic!("UFS_TEST_PATH is not set")
    }

    let testing = Testing::default();
    let rt = Arc::new(AsyncRuntime::single());
    testing.get_unified_fs_with_rt(rt.clone()).unwrap()
}
#[test]
fn test_cache_mode() {
    let fs = get_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::CacheMode).await;

        let path = format!("/write_cache_{:?}/test.log", WriteType::CacheMode).into();

        // Test 1: verify data write is correct
        write(&fs, &path, false).await;

        // Test 2: resubmit async task (skipped if data already synced); then check UFS mtime unchanged
        let (ufs_path, mnt) = fs.get_mount(&path).await.unwrap().unwrap();
        let ufs_reader_before = mnt.ufs.open(&ufs_path).await.unwrap();
        let mtime_before = ufs_reader_before.status().mtime;
        drop(ufs_reader_before);

        fs.async_cache(&path).await.unwrap();
        fs.wait_job_complete(&path, false).await.unwrap();

        let ufs_reader_after = mnt.ufs.open(&ufs_path).await.unwrap();
        let mtime_after = ufs_reader_after.status().mtime;
        drop(ufs_reader_after);
        assert_eq!(
            mtime_before, mtime_after,
            "resubmit should skip, UFS mtime should be unchanged ({} vs {})",
            mtime_before, mtime_after
        );


        // Test 3: read cache test
        let path = format!("/write_cache_{:?}/read_cache.log", WriteType::CacheMode).into();

        // Write file to UFS, then test read
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string(Utils::rand_str(1024)).await.unwrap();
        writer.complete().await.unwrap();
        test_cache_read(&fs, &path).await;

        // Delete curvine file to simulate expiry
        fs.cv().delete(&path, false).await.unwrap();
        test_cache_read(&fs, &path).await;
    });
}

async fn test_cache_read(fs: &UnifiedFileSystem, path: &Path) {
    let mut reader1 = fs.open(path).await.unwrap();
    assert!(!matches!(reader1, UnifiedReader::Cv(_)), "first read should be from ufs");

    let str1 = reader1.read_as_string().await.unwrap();

    let (ufs_path, _) = fs.get_mount(path).await.unwrap().unwrap();
    fs.wait_job_complete(&ufs_path, false).await.unwrap();

    let mut reader2 = fs.open(path).await.unwrap();
    assert!(matches!(reader2, UnifiedReader::Cv(_)), "second read should be from curvine");

    let str2 = reader2.read_as_string().await.unwrap();
    assert_eq!(str1, str2);
}

#[test]
fn test_fs_mode() {
    let fs = get_fs();
    let rt = fs.clone_runtime();
    rt.block_on(async move {
        mount(&fs, WriteType::FsMode).await;
        let path = format!("/write_cache_{:?}/test.log", WriteType::FsMode).into();
        write(&fs, &path, false).await;

        let (_, mnt) = fs.get_mount(&path).await.unwrap().unwrap();

        // Test rename
        let path = format!("/write_cache_{:?}/meta.log", WriteType::FsMode).into();
        let mut writer = fs.create(&path, true).await.unwrap();
        writer.write_string(Utils::rand_str(1024)).await.unwrap();
        writer.complete().await.unwrap();

        let dst_path = format!("/write_cache_{:?}/meta_rename.log", WriteType::FsMode).into();
        fs.rename(&path, &dst_path).await.unwrap();

        tokio::time::sleep(Duration::from_secs(5)).await;

        // Check UFS file
        let ufs_path = mnt.get_ufs_path(&dst_path).unwrap();
        let mut reader1 = mnt.ufs.open(&ufs_path).await.unwrap();
        let str1 = reader1.read_as_string().await.unwrap();

        let mut reader2 = fs.open(&dst_path).await.unwrap();
        let str2 = reader2.read_as_string().await.unwrap();

        assert_eq!(str1, str2);

        // Test delete
        fs.delete(&dst_path, false).await.unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(!mnt.ufs.exists(&ufs_path).await.unwrap());
    });
}

async fn write(fs: &UnifiedFileSystem, path: &Path, random_write: bool) {
    let chunk_size = 64 * 1024;
    let total_size = 1024 * 1024;
    let num_chunks = total_size / chunk_size;

    let mut writer = fs.create(path, true).await.unwrap();
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

    verify_read_data(fs, path, &written_data).await;

    fs.wait_job_complete(path, false).await.unwrap();

    verify_cv_ufs_consistency(fs, path).await;
}

async fn verify_read_data(
    fs: &UnifiedFileSystem,
    path: &Path,
    expected_data: &[u8],
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

    let opts = opts_builder.mount_type(MountType::Orch).build();
    fs.mount(&ufs_path, &cv_path, opts.clone()).await.unwrap();

    let ufs = UfsFileSystem::new(&ufs_path, opts.add_properties, None).unwrap();
    if ufs.exists(&ufs_path).await.unwrap() {
        ufs.delete(&ufs_path, true).await.unwrap();
    }
}
