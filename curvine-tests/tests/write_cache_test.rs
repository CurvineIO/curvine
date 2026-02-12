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
use curvine_client::unified::{UnifiedFileSystem, UnifiedReader, UnifiedWriter};
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::{
    CreateFileOptsBuilder, MountOptionsBuilder, MountType, OpenFlags, WriteType,
};
use curvine_tests::Testing;
use orpc::common::Utils;
use orpc::runtime::{AsyncRuntime, RpcRuntime};
use orpc::sys::DataSlice;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

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
        mount(&fs, WriteType::Cache).await;
        mount(&fs, WriteType::Through).await;
        mount(&fs, WriteType::CacheThrough).await;
        mount(&fs, WriteType::AsyncThrough).await;

        write(&fs, WriteType::Cache, false).await;
        write(&fs, WriteType::Through, false).await;
        write(&fs, WriteType::CacheThrough, false).await;
        write(&fs, WriteType::CacheThrough, true).await;
        write(&fs, WriteType::AsyncThrough, false).await;
        write(&fs, WriteType::AsyncThrough, true).await;
        write(&fs, WriteType::CacheThrough, true).await;
        write(&fs, WriteType::AsyncThrough, true).await;
    })
}

#[test]
fn test_async_through_reopen_truncate_missing_cv_cache() {
    if env::var("UFS_TEST_PATH").is_err() {
        println!("⚠️  UFS_TEST_PATH is not set, skipping test");
        return;
    }

    let testing = Testing::builder().workers(3).build().unwrap();
    testing.start_cluster().unwrap();
    let rt = Arc::new(AsyncRuntime::single());
    let fs = testing.get_unified_fs_with_rt(rt.clone()).unwrap();
    let dir = format!("write_cache_AsyncThrough_reopen_{}", Utils::rand_str(8));

    rt.block_on(async move {
        mount_with_dir(&fs, WriteType::AsyncThrough, &dir).await;
        reopen_truncate_missing_cv_cache(&fs, &dir).await;
    })
}

#[test]
fn test_through_read_and_status_ignore_stale_cv_cache() {
    if env::var("UFS_TEST_PATH").is_err() {
        println!("⚠️  UFS_TEST_PATH is not set, skipping test");
        return;
    }

    let testing = Testing::builder().workers(3).build().unwrap();
    testing.start_cluster().unwrap();
    let rt = Arc::new(AsyncRuntime::single());
    let fs = testing.get_unified_fs_with_rt(rt.clone()).unwrap();
    let dir = format!("write_cache_Through_stale_cv_{}", Utils::rand_str(8));

    rt.block_on(async move {
        mount_with_dir(&fs, WriteType::Through, &dir).await;

        let path = Path::from_str(format!("/{dir}/through_stale_cache.txt")).unwrap();

        // 1) Write real source data to UFS through mount path.
        let mut ufs_writer = fs.create(&path, true).await.unwrap();
        ufs_writer.write(b"ufs-data").await.unwrap();
        ufs_writer.complete().await.unwrap();

        // 2) Inject stale CV cache content for the same logical path.
        let mut cv_writer = fs.cv().create(&path, true).await.unwrap();
        cv_writer.write(b"stale-cv-cache").await.unwrap();
        cv_writer.complete().await.unwrap();

        // 3) Through-mode open must still read from UFS, not stale CV cache.
        let mut reader = fs.open(&path).await.unwrap();
        assert!(
            matches!(reader, UnifiedReader::Opendal(_)),
            "through-mode should open UFS reader instead of CV cache reader"
        );

        let mut read_data = BytesMut::zeroed(reader.len() as usize);
        reader.read_full(&mut read_data).await.unwrap();
        reader.complete().await.unwrap();
        assert_eq!(read_data.as_ref(), b"ufs-data");

        // 4) Through-mode status must also reflect UFS truth.
        let status = fs.get_status(&path).await.unwrap();
        assert_eq!(status.len, b"ufs-data".len() as i64);
    })
}

#[test]
fn test_async_through_delete_should_not_leave_ufs_residue() {
    if env::var("UFS_TEST_PATH").is_err() {
        println!("⚠️  UFS_TEST_PATH is not set, skipping test");
        return;
    }

    let testing = Testing::builder().workers(3).build().unwrap();
    testing.start_cluster().unwrap();
    let rt = Arc::new(AsyncRuntime::single());
    let fs = testing.get_unified_fs_with_rt(rt.clone()).unwrap();
    let dir = format!("write_cache_async_delete_residue_{}", Utils::rand_str(8));

    rt.block_on(async move {
        mount_with_dir(&fs, WriteType::AsyncThrough, &dir).await;

        let loops = 40;
        for i in 0..loops {
            let path = Path::from_str(format!("/{dir}/delete_residue_{i:04}.bin")).unwrap();
            let mut writer = fs.create(&path, true).await.unwrap();
            writer.write(b"delete-race-data").await.unwrap();
            writer.complete().await.unwrap();

            // Reproduce user workload: delete immediately after close without waiting async sync.
            fs.delete(&path, false).await.unwrap();
        }

        // Give background sync jobs enough time to finish if they were already submitted.
        sleep(Duration::from_secs(4)).await;

        let mut leaked = vec![];
        for i in 0..loops {
            let path = Path::from_str(format!("/{dir}/delete_residue_{i:04}.bin")).unwrap();
            let (ufs_path, mount) = fs.get_mount(&path).await.unwrap().unwrap();
            if mount.ufs.exists(&ufs_path).await.unwrap() {
                leaked.push(ufs_path.full_path().to_string());
            }
        }

        assert!(
            leaked.is_empty(),
            "async_through delete left {} ufs objects, examples: {:?}",
            leaked.len(),
            leaked.iter().take(5).collect::<Vec<_>>()
        );
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

    // If async write, wait for job to complete
    if matches!(write_type, WriteType::AsyncThrough) {
        match &writer {
            UnifiedWriter::CacheSync(r) => {
                r.wait_job_complete().await.unwrap();
            }

            _ => panic!("Invalid writer type"),
        };
    }

    verify_read_data(fs, &path, &written_data, write_type).await;

    verify_cv_ufs_consistency(fs, &path).await;
}

async fn verify_read_data(
    fs: &UnifiedFileSystem,
    path: &Path,
    expected_data: &[u8],
    write_type: WriteType,
) {
    let mut reader = fs.open(path).await.unwrap();

    // Check reader type
    match write_type {
        WriteType::Cache | WriteType::CacheThrough | WriteType::AsyncThrough => {
            assert!(matches!(reader, UnifiedReader::Cv(_)));
        }

        WriteType::Through => {
            assert!(matches!(reader, UnifiedReader::Opendal(_)));
        }
    }

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
    if matches!(mnt.info.write_type, WriteType::Cache | WriteType::Through) {
        return;
    }

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
    let dir = format!("write_cache_{:?}", write_type);
    mount_with_dir(fs, write_type, &dir).await;
}

async fn mount_with_dir(fs: &UnifiedFileSystem, write_type: WriteType, dir: &str) {
    let ufs_base = env::var("UFS_TEST_PATH").unwrap();
    let ufs_path = Path::from_str(format!("{}/{}", ufs_base, dir)).unwrap();
    let cv_path = Path::from_str(format!("/{}", dir)).unwrap();

    let mut opts_builder = MountOptionsBuilder::new()
        .mount_type(MountType::Orch)
        .write_type(write_type);

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

async fn reopen_truncate_missing_cv_cache(fs: &UnifiedFileSystem, dir: &str) {
    // Regression case:
    // 1) UFS object exists
    // 2) CV cache inode is removed
    // 3) Re-open with WT (truncate, no create bit) should still succeed for mounted async-through path
    let path = Path::from_str(format!("/{dir}/reopen_missing_cache.log")).unwrap();

    let mut writer = fs.create(&path, true).await.unwrap();
    writer.write(b"seed-data").await.unwrap();
    writer.complete().await.unwrap();
    if let UnifiedWriter::CacheSync(sync_writer) = &writer {
        sync_writer.wait_job_complete().await.unwrap();
    }

    let (ufs_path, mount) = fs.get_mount(&path).await.unwrap().unwrap();
    assert!(
        mount.ufs.exists(&ufs_path).await.unwrap(),
        "UFS object must exist before clearing CV cache"
    );

    fs.cv().delete(&path, false).await.unwrap();

    let opts = CreateFileOptsBuilder::with_conf(&fs.conf().client)
        .create_parent(true)
        .build();
    let flags = OpenFlags::new_write_only().set_overwrite(true);

    let mut writer = fs.open_with_opts(&path, opts, flags).await.unwrap();
    writer.write(b"rewritten-data").await.unwrap();
    writer.complete().await.unwrap();
    if let UnifiedWriter::CacheSync(sync_writer) = &writer {
        sync_writer.wait_job_complete().await.unwrap();
    }

    let mut reader = fs.open(&path).await.unwrap();
    let mut data = BytesMut::zeroed(reader.len() as usize);
    reader.read_full(&mut data).await.unwrap();
    reader.complete().await.unwrap();
    assert_eq!(data.as_ref(), b"rewritten-data");
}
