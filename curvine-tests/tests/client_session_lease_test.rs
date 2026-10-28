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

//! End-to-end client session lease suite (#1882 for #1867).
//!
//! These cases drive the whole chain — client task, master session map, reaper,
//! journalled release — against a real mini cluster with a short lease, so a
//! crash is observed the way production would observe it: heartbeats stop and the
//! master reclaims what the client owned.

use curvine_client::file::CurvineFileSystem;
use curvine_fs_api::{Path, Writer};
use curvine_model::{FileLock, LockFlags, LockType};
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::test::MiniCluster;
use curvine_tests::Testing;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 200ms beats inside a 1s lease: five missed beats before a session expires.
const HEARTBEAT_MS: u64 = 200;
const SESSION_TIMEOUT: &str = "1s";

fn start_cluster() -> (Testing, Arc<MiniCluster>) {
    let testing = Testing::builder()
        .workers(1)
        .mutate_conf(|conf| {
            conf.client.heartbeat_interval_ms = HEARTBEAT_MS;
            conf.master.client_session_timeout = SESSION_TIMEOUT.to_string();
            // Duration strings are parsed by `init`; mutating alone leaves the
            // previously parsed window in place.
            conf.master.init().expect("master conf must stay valid");
        })
        .build()
        .unwrap();

    let cluster = testing.start_cluster().unwrap();
    (testing, cluster)
}

fn client_fs(testing: &Testing) -> CurvineFileSystem {
    testing.get_fs(None, None).unwrap()
}

fn client_id(fs: &CurvineFileSystem) -> String {
    fs.fs_context_ref().clone_client_name()
}

fn wait_until(what: &str, timeout: Duration, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out waiting for {what}");
}

fn session_registered(cluster: &MiniCluster, client_id: &str) -> bool {
    cluster
        .get_active_master_fs()
        .session_map
        .read()
        .get(client_id)
        .is_some()
}

fn get_status(fs: &CurvineFileSystem, path: &Path) -> curvine_model::FileStatus {
    fs.clone_runtime()
        .block_on(async { fs.get_status(path).await })
        .unwrap()
}

fn create_file(fs: &CurvineFileSystem, path: &Path) {
    fs.clone_runtime().block_on(async {
        let mut writer = fs.create(path, true).await.unwrap();
        writer.complete().await.unwrap();
    });
}

fn lock(client_id: &str, owner_id: u64, lock_type: LockType, flags: LockFlags) -> FileLock {
    FileLock {
        client_id: client_id.to_string(),
        owner_id,
        pid: std::process::id(),
        acquire_time: 0,
        lock_type,
        lock_flags: flags,
        start: 0,
        end: u64::MAX,
        session_owned: false,
    }
}

fn set_lock(fs: &CurvineFileSystem, path: &Path, lock: FileLock) -> Option<FileLock> {
    fs.clone_runtime()
        .block_on(async { fs.set_lock(path, lock).await })
        .unwrap()
}

fn find_conflict(fs: &CurvineFileSystem, path: &Path, probe: FileLock) -> Option<FileLock> {
    fs.clone_runtime()
        .block_on(async { fs.get_lock(path, probe).await })
        .unwrap()
}

fn probe() -> FileLock {
    // A different owner, so it conflicts with anything the crashed client held.
    lock("probe-client", 999, LockType::WriteLock, LockFlags::Plock)
}

/// Crash a client holding a POSIX lock and a BSD lock. Both must be released once
/// its lease expires, and another client must then be able to take them.
#[test]
fn crashed_client_locks_are_released_after_its_lease_expires() {
    let (testing, cluster) = start_cluster();
    let crashed = client_fs(&testing);
    let crashed_id = client_id(&crashed);
    wait_until(
        "the first session to register",
        Duration::from_secs(20),
        || session_registered(&cluster, &crashed_id),
    );

    let path = Path::from_str(format!("/crashed-locks-{}", std::process::id())).unwrap();
    create_file(&crashed, &path);
    assert!(set_lock(
        &crashed,
        &path,
        lock(&crashed_id, 11, LockType::WriteLock, LockFlags::Plock)
    )
    .is_none());
    assert!(set_lock(
        &crashed,
        &path,
        lock(&crashed_id, 12, LockType::WriteLock, LockFlags::Flock)
    )
    .is_none());

    let survivor = client_fs(&testing);
    let survivor_id = client_id(&survivor);
    wait_until(
        "the second session to register",
        Duration::from_secs(20),
        || session_registered(&cluster, &survivor_id),
    );
    assert!(
        find_conflict(&survivor, &path, probe()).is_some(),
        "while the owner is alive its lock must be enforced"
    );

    // Crash: dropping the last FsContext stops the heartbeat task (#1878).
    drop(crashed);

    wait_until(
        "the crashed client's locks to be released",
        Duration::from_secs(40),
        || find_conflict(&survivor, &path, probe()).is_none(),
    );

    // And the survivor can take both of them.
    assert!(set_lock(
        &survivor,
        &path,
        lock(&survivor_id, 21, LockType::WriteLock, LockFlags::Plock)
    )
    .is_none());
    assert!(set_lock(
        &survivor,
        &path,
        lock(&survivor_id, 22, LockType::WriteLock, LockFlags::Flock)
    )
    .is_none());
}

/// Crash a client that opened a file for write and never completed it. The reaper
/// must drop its writer marker, which makes the file complete again and lets
/// another client append and complete.
#[test]
fn crashed_writer_is_reaped_and_the_file_becomes_completable() {
    let (testing, cluster) = start_cluster();
    let crashed = client_fs(&testing);
    let crashed_id = client_id(&crashed);
    wait_until(
        "the writer session to register",
        Duration::from_secs(20),
        || session_registered(&cluster, &crashed_id),
    );

    let path = Path::from_str(format!("/crashed-writer-{}", std::process::id())).unwrap();
    // Open for write and never finish: this leaves a writer marker behind.
    crashed
        .clone_runtime()
        .block_on(async { crashed.create(&path, true).await })
        .unwrap();

    let status = get_status(&crashed, &path);
    assert!(
        !status.is_complete,
        "an open writer leaves the file incomplete"
    );

    // Crash mid-write.
    drop(crashed);

    let rescuer = client_fs(&testing);
    wait_until(
        "the crashed writer's marker to be reaped",
        Duration::from_secs(40),
        || get_status(&rescuer, &path).is_complete,
    );

    // The file is writable again: append and complete through a healthy client.
    let written = rescuer.clone_runtime().block_on(async {
        let mut writer = rescuer.append(&path).await.unwrap();
        writer.write(&vec![0xAB_u8; 1024]).await.unwrap();
        writer.complete().await.unwrap();
        1024
    });
    let status = get_status(&rescuer, &path);
    assert!(status.is_complete);
    assert_eq!(status.len, written, "the append must be durable");
}

/// Idle clients must not accumulate reclamation work: every registered session is
/// live, and the reaper has nothing to do while they keep heartbeating.
#[test]
fn idle_clients_stay_live_without_reclamation_work() {
    let (testing, cluster) = start_cluster();
    let clients: Vec<CurvineFileSystem> = (0..5).map(|_| client_fs(&testing)).collect();
    let ids: Vec<String> = clients.iter().map(client_id).collect();

    wait_until(
        "all idle sessions to register",
        Duration::from_secs(30),
        || ids.iter().all(|id| session_registered(&cluster, id)),
    );

    // Several lease windows later nothing may have expired: a heartbeat inside the
    // lease must be enough to keep a session (and everything it owns) alive.
    std::thread::sleep(Duration::from_millis(3 * SESSION_TIMEOUT_MS));

    let master = cluster.get_active_master_fs();
    let summary = master.session_summary().unwrap();
    // The cluster itself may own a client session, so only assert on ours.
    let ours: Vec<_> = summary
        .iter()
        .filter(|entry| ids.contains(&entry.client_id))
        .collect();
    assert_eq!(ours.len(), ids.len(), "every idle client must be listed");
    assert!(
        ours.iter().all(|entry| entry.live),
        "every idle client must still hold a live lease"
    );
    for id in &ids {
        assert!(
            session_registered(&cluster, id),
            "session {} disappeared while its client was heartbeating",
            id
        );
    }
    assert!(
        ours.iter()
            .all(|entry| entry.lock_hints == 0 && entry.writer_hints == 0),
        "idle clients own nothing, so there is no reclamation work to do"
    );
}

/// `client_session_timeout` is a string in the config; make the milliseconds
/// conversion used above explicit so a change of units fails loudly here.
const SESSION_TIMEOUT_MS: u64 = 1_000;
