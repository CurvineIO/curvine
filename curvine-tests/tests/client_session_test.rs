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

//! Client session lease, end to end (#1878): a client registers a session with
//! the master, renews it, stops renewing when its `FsContext` is dropped, and
//! stamps session ownership onto the locks it acquires.

use curvine_client::file::CurvineFileSystem;
use curvine_fs_api::{Path, Writer};
use curvine_model::{FileLock, LockFlags, LockType};
use curvine_runtime::runtime::RpcRuntime;
use curvine_server::master::fs::state::monotonic_ms;
use curvine_server::test::MiniCluster;
use curvine_tests::Testing;
use std::sync::Arc;
use std::time::{Duration, Instant};

const HEARTBEAT_MS: u64 = 200;
const SESSION_TIMEOUT: &str = "2s";

fn start_cluster(heartbeat_interval_ms: u64) -> (Testing, Arc<MiniCluster>) {
    let testing = Testing::builder()
        .workers(1)
        .mutate_conf(move |conf| {
            conf.client.heartbeat_interval_ms = heartbeat_interval_ms;
            conf.master.client_session_timeout = SESSION_TIMEOUT.to_string();
            // The duration unit is parsed by `init`; mutating the string alone
            // leaves the previously parsed window in place.
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

/// Poll `check` until it returns true or the deadline passes.
fn wait_until(what: &str, timeout: Duration, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for {what}");
}

fn session_of(
    cluster: &MiniCluster,
    client_id: &str,
) -> Option<(u64, Option<u32>, Option<String>)> {
    let master = cluster.get_active_master_fs();
    let sessions = master.session_map.read();
    sessions
        .get(client_id)
        .map(|s| (s.last_heartbeat, s.pid, s.client_version.clone()))
}

fn session_is_live(cluster: &MiniCluster, client_id: &str) -> bool {
    let master = cluster.get_active_master_fs();
    let sessions = master.session_map.read();
    sessions.is_live(client_id, monotonic_ms())
}

fn create_file(fs: &CurvineFileSystem, path: &Path) {
    fs.clone_runtime().block_on(async {
        let mut writer = fs.create(path, true).await.unwrap();
        writer.complete().await.unwrap();
    });
}

fn probe_lock() -> FileLock {
    FileLock {
        client_id: "probe".to_string(),
        owner_id: 999,
        pid: 1,
        acquire_time: 0,
        lock_type: LockType::WriteLock,
        lock_flags: LockFlags::Plock,
        start: 0,
        end: 100,
        session_owned: false,
    }
}

fn write_lock(client_id: String, owner_id: u64) -> FileLock {
    FileLock {
        client_id,
        owner_id,
        pid: std::process::id(),
        acquire_time: 0,
        lock_type: LockType::WriteLock,
        lock_flags: LockFlags::Plock,
        start: 0,
        end: 100,
        // Deliberately left false: the SetLock boundary must stamp it.
        session_owned: false,
    }
}

#[test]
fn client_registers_renews_and_stops_its_session() {
    let (testing, cluster) = start_cluster(HEARTBEAT_MS);
    let fs = client_fs(&testing);
    let client_id = fs.fs_context_ref().clone_client_name();

    wait_until(
        "the client session to register",
        Duration::from_secs(15),
        || session_of(&cluster, &client_id).is_some(),
    );

    let (first_heartbeat, pid, version) = session_of(&cluster, &client_id).unwrap();
    assert_eq!(
        pid,
        Some(std::process::id()),
        "the client identifies its pid"
    );
    // The client reports the version of its own crate, which the test crate cannot
    // name without pinning a bump-prone constant.
    assert!(
        version.as_deref().is_some_and(|v| !v.is_empty()),
        "the client identifies its build"
    );

    // The lease must move forward without re-registering.
    wait_until(
        "the session lease to be renewed",
        Duration::from_secs(10),
        || session_of(&cluster, &client_id).is_some_and(|(now, _, _)| now > first_heartbeat),
    );
    assert!(session_is_live(&cluster, &client_id));

    // Dropping the last FsContext stops the task: the lease stops moving and
    // expires. Nothing is reaped yet (that is #1884/T5a), but liveness must
    // already flip, otherwise a crashed client would look alive forever.
    drop(fs);
    wait_until(
        "the dropped client session to expire",
        Duration::from_secs(20),
        || !session_is_live(&cluster, &client_id),
    );
}

#[test]
fn disabled_heartbeat_leaves_no_session_and_legacy_locks() {
    let (testing, cluster) = start_cluster(0);
    let fs = client_fs(&testing);
    let client_id = fs.fs_context_ref().clone_client_name();

    let path = Path::from_str(format!("/session-disabled-{}", std::process::id())).unwrap();
    create_file(&fs, &path);
    fs.clone_runtime()
        .block_on(async { fs.set_lock(&path, write_lock(client_id.clone(), 7)).await })
        .unwrap();

    // Give the (disabled) task more than a few would-be intervals to prove it is
    // not running.
    std::thread::sleep(Duration::from_millis(3 * HEARTBEAT_MS));
    assert!(
        session_of(&cluster, &client_id).is_none(),
        "client.heartbeat_interval_ms = 0 must not register a session"
    );

    // A lock from a client without a session keeps the legacy TTL path.
    let stored = cluster
        .get_active_master_fs()
        .get_lock(path.full_path(), probe_lock())
        .unwrap()
        .expect("the probe must see the client's conflicting lock");
    assert_eq!(stored.client_id, client_id);
    assert!(
        !stored.session_owned,
        "a client without heartbeats must not claim a session-owned lock"
    );
}

#[test]
fn heartbeat_client_stamps_session_ownership_on_its_locks() {
    let (testing, cluster) = start_cluster(HEARTBEAT_MS);
    let fs = client_fs(&testing);
    let client_id = fs.fs_context_ref().clone_client_name();

    // The client stamps only after it has seen the master's acknowledgement,
    // which lands just after the master-side registration.
    wait_until(
        "the client to confirm its session lease",
        Duration::from_secs(15),
        || fs.fs_context_ref().session_active(),
    );
    assert!(session_of(&cluster, &client_id).is_some());

    let path = Path::from_str(format!("/session-owned-{}", std::process::id())).unwrap();
    create_file(&fs, &path);
    fs.clone_runtime()
        .block_on(async { fs.set_lock(&path, write_lock(client_id.clone(), 11)).await })
        .unwrap();

    let stored = cluster
        .get_active_master_fs()
        .get_lock(path.full_path(), probe_lock())
        .unwrap()
        .expect("the probe must see the client's conflicting lock");

    assert_eq!(stored.client_id, client_id);
    assert!(
        stored.session_owned,
        "a heartbeat client's locks must be marked session-owned so #1879 can \
         judge them by session liveness"
    );
}

#[test]
fn foreign_owner_lock_keeps_the_legacy_path_under_a_live_session() {
    // curvine-lancedb takes locks as `curvine-lancedb:<pid>` through a context
    // whose session is registered under the context UUID. Such a lock has no
    // session that could ever renew it, so it must stay on `lock_expire_time`
    // even when the caller asks for session ownership.
    let (testing, cluster) = start_cluster(HEARTBEAT_MS);
    let fs = client_fs(&testing);
    wait_until(
        "the client to confirm its session lease",
        Duration::from_secs(15),
        || fs.fs_context_ref().session_active(),
    );

    let path = Path::from_str(format!("/session-foreign-{}", std::process::id())).unwrap();
    create_file(&fs, &path);
    let foreign = format!("curvine-lancedb:{}", std::process::id());
    let mut lock = write_lock(foreign.clone(), 13);
    lock.session_owned = true;
    fs.clone_runtime()
        .block_on(async { fs.set_lock(&path, lock).await })
        .unwrap();

    let stored = cluster
        .get_active_master_fs()
        .get_lock(path.full_path(), probe_lock())
        .unwrap()
        .expect("the probe must see the foreign owner's conflicting lock");
    assert_eq!(stored.client_id, foreign);
    assert!(
        !stored.session_owned,
        "a lock whose client_id is not the registered session must keep the legacy TTL path"
    );
}
