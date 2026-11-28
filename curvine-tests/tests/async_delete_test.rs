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
use curvine_server::test::MiniCluster;
use curvine_tests::Testing;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn get_fs(trash_enabled: bool) -> UnifiedFileSystem {
    get_fs_and_cluster(trash_enabled).0
}

fn get_fs_and_cluster(trash_enabled: bool) -> (UnifiedFileSystem, Arc<MiniCluster>) {
    let testing = Testing::builder()
        .workers(1)
        .mutate_conf(move |conf| conf.master.trash_enabled = trash_enabled)
        .build()
        .unwrap();
    let cluster = testing.start_cluster().unwrap();
    let rt = Arc::new(Runtime::new("async-delete-test", 2, 2));
    (testing.get_unified_fs_with_rt(rt).unwrap(), cluster)
}

/// True once `/.Trash/.pending` holds no staged entry. The staging tree is
/// invisible to client RPCs (`get_status` always returns `FileNotFound`), so
/// ask the active master's purger scan instead; a failed scan is an error,
/// not proof of a purge.
fn pending_drained(cluster: &MiniCluster) -> CommonResult<bool> {
    let page = cluster
        .get_active_master_fs()
        .scan_pending_trash(usize::MAX, None)?;
    Ok(page.paths.is_empty())
}

fn seed(fs: &UnifiedFileSystem, root: &str, dirs: usize, per_dir: usize) {
    fs.clone_runtime().block_on(async {
        for dir in 0..dirs {
            for file in 0..per_dir {
                let path = Path::from_str(format!("{root}/d{dir}/f{file}")).unwrap();
                fs.create(&path, true).await.unwrap();
            }
        }
    });
}

/// The client must be able to retire a large tree without waiting for the master
/// to walk it, with and without the recycle bin.
#[test]
fn client_async_delete_stages_instead_of_walking() -> CommonResult<()> {
    for trash_enabled in [false, true] {
        let fs = get_fs(trash_enabled);
        let root = format!("/async-client-trash-{trash_enabled}");
        seed(&fs, &root, 4, 50);
        let path = Path::from_str(root.clone())?;

        let started = Instant::now();
        let res = fs
            .clone_runtime()
            .block_on(async { fs.delete_async(&path, true).await })?;
        let elapsed = started.elapsed();

        assert!(res.purge_pending, "the delete must report staging");
        assert_eq!(res.inodes, 0, "staging walks no inode");
        assert!(
            elapsed < Duration::from_secs(5),
            "async delete must not pay for the subtree, took {elapsed:?}"
        );

        // Namespace effect is immediate, so a client can retire its work and exit.
        let exists = fs
            .clone_runtime()
            .block_on(async { fs.exists(&path).await })?;
        assert!(!exists, "the path is gone as soon as the call returns");

        // The staged entry is not a restorable recycle-bin entry.
        let listed = fs
            .clone_runtime()
            .block_on(async { fs.list_trash().await })?;
        assert!(listed.is_empty(), "staged entries stay invisible");
    }
    Ok(())
}

/// Waiting a few cycles must reclaim the space, proving the purge lane runs on a
/// live cluster and not only in unit tests.
#[test]
fn async_delete_space_is_reclaimed_by_the_purger() -> CommonResult<()> {
    let (fs, cluster) = get_fs_and_cluster(false);
    let path = Path::from_str("/async-client-reclaim")?;
    seed(&fs, "/async-client-reclaim", 2, 20);

    let res = fs
        .clone_runtime()
        .block_on(async { fs.delete_async(&path, true).await })?;
    assert!(res.purge_pending);

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = fs
            .clone_runtime()
            .block_on(async { fs.get_status(&path).await });
        assert!(status.is_err(), "the path must stay deleted");

        // The purger removes the entry, so the staging directory drains.
        if pending_drained(&cluster)? {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the purger must reclaim the staged entry"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    Ok(())
}

/// Manual comparison of the two lanes. Run with:
/// `cargo test -p curvine-tests --test async_delete_test bench_ -- --ignored --nocapture`
#[test]
#[ignore = "manual benchmark: measures sync vs async recursive delete"]
fn bench_sync_vs_async_delete() -> CommonResult<()> {
    // Override with BENCH_INODES to measure a different tree size.
    let inodes: usize = std::env::var("BENCH_INODES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5_000);
    let (fs, cluster) = get_fs_and_cluster(false);

    // Warm the client and the master connection so the first call is not skewed.
    let warm = Path::from_str("/bench-warm")?;
    fs.clone_runtime()
        .block_on(async { fs.create(&warm, true).await })?;
    fs.clone_runtime()
        .block_on(async { fs.delete(&warm, true).await })?;

    let sync_root = Path::from_str("/bench-sync")?;
    seed(&fs, "/bench-sync", 50, inodes / 50);
    let started = Instant::now();
    fs.clone_runtime()
        .block_on(async { fs.delete(&sync_root, true).await })?;
    let sync_delete = started.elapsed();

    let async_root = Path::from_str("/bench-async")?;
    seed(&fs, "/bench-async", 50, inodes / 50);
    let started = Instant::now();
    let res = fs
        .clone_runtime()
        .block_on(async { fs.delete_async(&async_root, true).await })?;
    let async_delete = started.elapsed();

    // Purge lag: how long until the master has reclaimed the staged tree.
    let started = Instant::now();
    let mut purge_lag = None;
    while started.elapsed() < Duration::from_secs(120) {
        if pending_drained(&cluster)? {
            purge_lag = Some(started.elapsed());
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    println!(
        "inodes={inodes} sync_delete={sync_delete:?} async_delete={async_delete:?} \
         purge_pending={} purge_lag={:?}",
        res.purge_pending, purge_lag
    );
    assert!(res.purge_pending);
    Ok(())
}
