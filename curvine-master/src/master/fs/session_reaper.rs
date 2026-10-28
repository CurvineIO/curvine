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

use crate::master::fs::MasterFilesystem;
use crate::master::meta::store::OwnerKind;
use crate::master::MasterMonitor;
use curvine_error::FsResult;
use curvine_runtime::common::LocalTime;
use curvine_runtime::runtime::LoopTask;
use log::{info, warn};

/// What one bounded reaper tick achieved.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReapReport {
    /// Sessions fully drained and removed from the session map.
    pub sessions: u64,
    /// Lock records released through `SetLocks`.
    pub locks: u64,
    /// Writer markers released through `ReleaseWriter`.
    pub writers: u64,
    /// Ownership hints dropped because the inode or the ownership was already
    /// gone.
    pub stale_hints: u64,
    /// Whether the tick stopped on its work budget with sessions still pending.
    pub budget_exhausted: bool,
}

impl ReapReport {
    pub fn is_empty(&self) -> bool {
        self.sessions == 0 && self.locks == 0 && self.writers == 0 && self.stale_hints == 0
    }
}

/// Reclaims what an expired client session owns (#1884).
///
/// The reaper is the *only* owner of the leader-change grace window: a session
/// that expires is only acted on once `SessionMap::reclaim_after` has passed, and
/// the lock path in #1879 trusts the very same deadline, so the two can never
/// disagree about whether an owner-less lock is still protected.
///
/// Cost per tick is `O(expired sessions + ownership hints processed)` through the
/// owner index (#1880); it never scans the namespace. Work is bounded by a budget
/// so a large backlog is drained over several ticks instead of holding `fs_dir`
/// for a long time in one go.
pub struct SessionReaper {
    fs: MasterFilesystem,
    monitor: MasterMonitor,
    /// Maximum sessions examined per tick.
    session_batch: usize,
    /// Maximum inode mutations (lock sets released + writers released) per tick.
    work_budget: usize,
}

impl SessionReaper {
    /// Default number of sessions examined per tick.
    pub const DEFAULT_SESSION_BATCH: usize = 64;
    /// Default number of inode mutations per tick. Each one is a journalled
    /// metadata write, so the budget keeps a tick's `fs_dir` occupancy bounded.
    pub const DEFAULT_WORK_BUDGET: usize = 256;

    pub fn new(fs: MasterFilesystem, monitor: MasterMonitor) -> Self {
        Self {
            fs,
            monitor,
            session_batch: Self::DEFAULT_SESSION_BATCH,
            work_budget: Self::DEFAULT_WORK_BUDGET,
        }
    }

    pub fn with_limits(
        fs: MasterFilesystem,
        monitor: MasterMonitor,
        session_batch: usize,
        work_budget: usize,
    ) -> Self {
        Self {
            fs,
            monitor,
            session_batch: session_batch.max(1),
            work_budget: work_budget.max(1),
        }
    }

    /// One bounded pass at `now`.
    ///
    /// `now` is a parameter rather than a wall-clock read so the expiry and grace
    /// boundaries are testable without sleeping.
    pub fn reap_once(&self, now: u64) -> FsResult<ReapReport> {
        let mut report = ReapReport::default();

        // A demoted master must not reclaim anything: its session map is about to be
        // cleared and its journal append would be rejected anyway.
        if !self.monitor.is_active() {
            return Ok(report);
        }

        // Leader-change grace window. Sessions are leader-local, so right after a
        // failover every client looks owner-less until its next heartbeat; acting
        // now would strip the locks of healthy clients (#1879).
        let reclaim_after = self.fs.session_map.read().reclaim_after();
        if now < reclaim_after {
            return Ok(report);
        }

        // Snapshot the expired ids and drop the session lock immediately: sessions
        // must never be held while `fs_dir` is taken (see `SessionMap`).
        let expired = {
            let sessions = self.fs.session_map.read();
            let mut expired = sessions.expired_at(now);
            expired.truncate(self.session_batch);
            expired
        };

        for client_id in expired {
            if !self.monitor.is_active() {
                break;
            }

            match self.reap_session(&client_id, &mut report) {
                Ok(true) => report.sessions += 1,
                Ok(false) => {
                    // Budget exhausted mid-session: keep the session so the next tick
                    // resumes where this one stopped.
                    report.budget_exhausted = true;
                    break;
                }
                Err(e) => {
                    warn!("reap client session {} failed: {}", client_id, e);
                }
            }
        }

        if !report.is_empty() {
            self.publish_metrics(&report);
            info!(
                "session reaper: {} sessions, {} locks, {} writer markers, {} stale hints \
                 released (budget exhausted: {})",
                report.sessions,
                report.locks,
                report.writers,
                report.stale_hints,
                report.budget_exhausted
            );
        }

        Ok(report)
    }

    /// Publish the tick's outcome plus the current session count (#1885).
    fn publish_metrics(&self, report: &ReapReport) {
        let Ok(metrics) = crate::master::Master::get_metrics() else {
            return;
        };
        metrics
            .expired_sessions_total
            .inc_by(report.sessions as i64);
        metrics.reaped_locks_total.inc_by(report.locks as i64);
        metrics.reaped_writers_total.inc_by(report.writers as i64);
        metrics
            .reaped_stale_hints_total
            .inc_by(report.stale_hints as i64);
        metrics
            .active_sessions
            .set(self.fs.session_map.read().len() as i64);
    }

    /// Release everything `client_id` owns. Returns `false` when the work budget ran
    /// out before the session was fully drained.
    ///
    /// The session is only forgotten once it is drained, so an interrupted pass is
    /// resumed by the next tick: every hint released is gone from the index, so the
    /// remaining work shrinks monotonically.
    fn reap_session(&self, client_id: &str, report: &mut ReapReport) -> FsResult<bool> {
        let owners = self.fs.get_client_owners(client_id)?;
        let mut budget = self.work_budget;
        let mut drained = true;

        for (kind, ino) in owners {
            if budget == 0 {
                drained = false;
                break;
            }

            let outcome = match kind {
                OwnerKind::Lock => self.release_locks(client_id, ino)?,
                OwnerKind::Writer => self.release_writer(client_id, ino)?,
            };

            budget -= 1;
            match outcome {
                ReleaseOutcome::Released => match kind {
                    OwnerKind::Lock => report.locks += 1,
                    OwnerKind::Writer => report.writers += 1,
                },
                ReleaseOutcome::Stale => {
                    report.stale_hints += 1;
                    // Hint is worthless: drop it so it is never revisited. The apply
                    // path only removes hints it actually acted on.
                    self.fs.remove_client_owner(client_id, kind, ino)?;
                }
            }
        }

        if drained {
            // Fully drained: forget the session so it is never revisited.
            self.fs.session_map.write().remove(client_id);
        }

        Ok(drained)
    }

    /// Drop one client's session-owned locks on `ino`, journalled through `SetLocks`.
    fn release_locks(&self, client_id: &str, ino: i64) -> FsResult<ReleaseOutcome> {
        let changed = self.fs.release_client_locks(ino, client_id)?;
        Ok(if changed {
            ReleaseOutcome::Released
        } else {
            ReleaseOutcome::Stale
        })
    }

    /// Drop one client's writer marker on `ino`, journalled through `ReleaseWriter`.
    fn release_writer(&self, client_id: &str, ino: i64) -> FsResult<ReleaseOutcome> {
        let changed = self.fs.release_writer(ino, client_id)?;
        Ok(if changed {
            ReleaseOutcome::Released
        } else {
            ReleaseOutcome::Stale
        })
    }
}

enum ReleaseOutcome {
    Released,
    /// The hint no longer matches any state: the inode is gone, or the ownership
    /// was already released or never session-owned.
    Stale,
}

impl LoopTask for SessionReaper {
    type Error = curvine_error::FsError;

    fn run(&self) -> FsResult<()> {
        let report = self.reap_once(LocalTime::mills())?;
        if report.budget_exhausted {
            warn!(
                "session reaper work budget ({} inode mutations) exhausted with sessions \
                 still pending; the remaining work resumes on the next tick",
                self.work_budget
            );
        }
        Ok(())
    }

    fn terminate(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::fs::MasterFilesystem;
    use crate::master::journal::JournalSystem;
    use crate::master::meta::inode::InodePath;
    use crate::master::meta::store::OwnerKind;
    use crate::master::{Master, MasterHandler};
    use curvine_config::ClusterConf;
    use curvine_model::{CreateFileOpts, FileLock, LockFlags, LockType};
    use curvine_proto::{ClientAddressProto, ClientHeartbeatRequest};
    use curvine_raft::raft::RoleState;
    use curvine_runtime::common::Utils;
    use std::sync::mpsc;
    use std::time::Duration;

    fn test_fs(name: &str) -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.client_session_timeout = "60s".to_string();
        conf.master.meta_dir = Utils::test_sub_dir(format!(
            "session-reaper/meta-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.journal.journal_dir = Utils::test_sub_dir(format!(
            "session-reaper/journal-{}-{}",
            name,
            Utils::rand_str(6)
        ));
        conf.master.init().unwrap();
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        fs.master_monitor.set_role_for_test(RoleState::Leader);
        fs
    }

    fn register(fs: &MasterFilesystem, client_id: &str, now: u64) {
        MasterHandler::process_client_heartbeat(
            fs,
            ClientHeartbeatRequest {
                client: ClientAddressProto {
                    client_name: client_id.to_string(),
                    hostname: "host".to_string(),
                    ip_addr: "127.0.0.1".to_string(),
                    port: 0,
                },
                heartbeat_interval_ms: 12_000,
                pid: Some(1),
                mount_point: None,
                client_version: Some("test".to_string()),
            },
            now,
        )
        .unwrap();
    }

    fn create_file(fs: &MasterFilesystem, path: &str, writer: &str) -> i64 {
        let mut fs_dir = fs.fs_dir.write();
        let inp = InodePath::resolve(fs_dir.root_ptr(), path, &fs_dir.store).unwrap();
        let mut opts = CreateFileOpts::with_create(false);
        opts.client_name = writer.to_string();
        let inp = fs_dir.create_file(inp, opts).unwrap();
        inp.get_last_inode().unwrap().id()
    }

    fn set_lock(fs: &MasterFilesystem, path: &str, client: &str, session_owned: bool) {
        fs.set_lock(
            path,
            FileLock {
                client_id: client.to_string(),
                owner_id: 1,
                pid: 1,
                acquire_time: 0,
                lock_type: LockType::WriteLock,
                lock_flags: LockFlags::Plock,
                start: 0,
                end: 100,
                session_owned,
            },
        )
        .unwrap();
    }

    fn owners(fs: &MasterFilesystem, client: &str) -> Vec<(OwnerKind, i64)> {
        fs.get_client_owners(client).unwrap()
    }

    fn stored_locks(fs: &MasterFilesystem, ino: i64) -> Vec<FileLock> {
        fs.fs_dir.read().store.get_locks(ino).unwrap().to_vec()
    }

    fn stored_writers(fs: &MasterFilesystem, ino: i64) -> Vec<String> {
        fs.fs_dir
            .read()
            .store
            .get_inode(ino, None)
            .unwrap()
            .map(|inode| inode.writer_clients())
            .unwrap_or_default()
    }

    /// A `now` far beyond both the session lease and the leader-change grace window.
    fn far_future(fs: &MasterFilesystem) -> u64 {
        fs.session_map.read().reclaim_after() + 1
    }

    #[test]
    fn reaps_locks_and_writer_markers_of_an_expired_session() {
        let fs = test_fs("full");
        let ino = create_file(&fs, "/owned.log", "dead-client");
        set_lock(&fs, "/owned.log", "dead-client", true);
        register(&fs, "dead-client", 1_000);

        assert_eq!(owners(&fs, "dead-client").len(), 2, "lock + writer hints");
        assert_eq!(stored_locks(&fs, ino).len(), 1);
        assert_eq!(stored_writers(&fs, ino), vec!["dead-client".to_string()]);

        let report = SessionReaper::new(fs.clone(), fs.master_monitor.clone())
            .reap_once(far_future(&fs))
            .unwrap();

        assert_eq!(report.sessions, 1);
        assert_eq!(report.locks, 1);
        assert_eq!(report.writers, 1);
        assert_eq!(report.stale_hints, 0);
        assert!(!report.budget_exhausted);

        assert!(stored_locks(&fs, ino).is_empty(), "locks must be released");
        assert!(
            stored_writers(&fs, ino).is_empty(),
            "writer markers must be released, so the file is complete again"
        );
        assert!(owners(&fs, "dead-client").is_empty(), "hints must be gone");
        assert!(
            fs.session_map.read().get("dead-client").is_none(),
            "a drained session must be forgotten"
        );
    }

    #[test]
    fn leader_change_grace_window_blocks_reaping() {
        let fs = test_fs("grace");
        create_file(&fs, "/grace.log", "dead-client");
        let now = 1_000;
        register(&fs, "dead-client", now);

        let reclaim_after = fs.session_map.read().reclaim_after();
        let timeout = fs.session_map.read().session_timeout_ms();
        // Past the session lease but inside the grace window.
        let inside_grace = (now + timeout + 1).max(reclaim_after - 1);
        assert!(inside_grace < reclaim_after);

        let report = SessionReaper::new(fs.clone(), fs.master_monitor.clone())
            .reap_once(inside_grace)
            .unwrap();
        assert!(
            report.is_empty(),
            "the grace window must suppress all reaping"
        );
        assert!(
            fs.session_map.read().get("dead-client").is_some(),
            "the session must survive the grace window"
        );
        assert_eq!(owners(&fs, "dead-client").len(), 1);

        // Past the window it proceeds.
        let report = SessionReaper::new(fs.clone(), fs.master_monitor.clone())
            .reap_once(reclaim_after)
            .unwrap();
        assert_eq!(report.writers, 1);
        assert!(owners(&fs, "dead-client").is_empty());
    }

    #[test]
    fn legacy_owners_are_never_reaped() {
        let fs = test_fs("legacy");
        let ino = create_file(&fs, "/legacy.log", "legacy-client");
        set_lock(&fs, "/legacy.log", "legacy-client", false);

        let report = SessionReaper::new(fs.clone(), fs.master_monitor.clone())
            .reap_once(far_future(&fs))
            .unwrap();
        assert!(report.is_empty());
        assert_eq!(
            stored_locks(&fs, ino).len(),
            1,
            "a legacy lock keeps the TTL path and must not be reaped per client"
        );
        assert_eq!(
            stored_writers(&fs, ino),
            vec!["legacy-client".to_string()],
            "the writer marker is released only through complete/reap of a session"
        );
    }

    #[test]
    fn active_session_is_not_reaped() {
        let fs = test_fs("alive");
        let ino = create_file(&fs, "/alive.log", "alive-client");
        register(&fs, "alive-client", 1_000);

        let report = SessionReaper::new(fs.clone(), fs.master_monitor.clone())
            .reap_once(1_000 + 1)
            .unwrap();
        assert!(report.is_empty());
        assert_eq!(owners(&fs, "alive-client"), vec![(OwnerKind::Writer, ino)]);
    }

    #[test]
    fn work_budget_bounds_one_tick_and_resumes_on_the_next() {
        let fs = test_fs("budget");
        for index in 0..3 {
            create_file(&fs, &format!("/budget-{index}.log"), "dead-client");
        }
        register(&fs, "dead-client", 1_000);
        assert_eq!(owners(&fs, "dead-client").len(), 3);

        let reaper = SessionReaper::with_limits(fs.clone(), fs.master_monitor.clone(), 8, 2);

        let first = reaper.reap_once(far_future(&fs)).unwrap();
        assert!(first.budget_exhausted);
        assert_eq!(first.writers, 2, "one tick must stop at its work budget");
        assert_eq!(first.sessions, 0, "the session is not drained yet");
        assert_eq!(owners(&fs, "dead-client").len(), 1);

        let second = reaper.reap_once(far_future(&fs)).unwrap();
        assert!(!second.budget_exhausted);
        assert_eq!(second.writers, 1);
        assert_eq!(second.sessions, 1);
        assert!(owners(&fs, "dead-client").is_empty());
        assert!(fs.session_map.read().get("dead-client").is_none());
    }

    #[test]
    fn stale_hints_are_dropped_instead_of_retried() {
        let fs = test_fs("stale");
        let ino = create_file(&fs, "/stale.log", "ghost-client");
        register(&fs, "ghost-client", 1_000);

        // Someone already cleared the marker behind the index's back: the hint no
        // longer matches any state.
        {
            let fs_dir = fs.fs_dir.write();
            let mut inode = fs_dir.store.get_inode(ino, None).unwrap().unwrap();
            inode.as_file_mut().unwrap().remove_writer("ghost-client");
            fs_dir.store.apply_reopen_file(&inode).unwrap();
        }
        // Re-add the hint the apply path just removed, to simulate an index entry
        // that outlived its ownership.
        {
            let fs_dir = fs.fs_dir.read();
            fs_dir
                .store
                .add_client_owner("ghost-client", OwnerKind::Writer, ino)
                .unwrap();
        }
        assert_eq!(owners(&fs, "ghost-client"), vec![(OwnerKind::Writer, ino)]);

        let report = SessionReaper::new(fs.clone(), fs.master_monitor.clone())
            .reap_once(far_future(&fs))
            .unwrap();

        assert_eq!(report.writers, 0);
        assert_eq!(report.stale_hints, 1);
        assert!(
            owners(&fs, "ghost-client").is_empty(),
            "a stale hint must be dropped so it is never revisited"
        );
        assert!(fs.session_map.read().get("ghost-client").is_none());
    }

    #[test]
    fn inactive_master_does_not_reap() {
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.meta_dir =
            Utils::test_sub_dir(format!("session-reaper/standby-{}", Utils::rand_str(6)));
        conf.journal.journal_dir =
            Utils::test_sub_dir(format!("session-reaper/standby-j-{}", Utils::rand_str(6)));
        conf.master.init().unwrap();
        Master::init_test_metrics();
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        // Default monitor state is not the active master.
        create_file(&fs, "/standby.log", "dead-client");
        register(&fs, "dead-client", 1_000);

        let report = SessionReaper::new(fs.clone(), fs.master_monitor.clone())
            .reap_once(far_future(&fs))
            .unwrap();

        assert!(
            report.is_empty(),
            "a standby master must not reclaim anything"
        );
        assert!(fs.session_map.read().get("dead-client").is_some());
    }

    /// Reaping must interleave with the hottest lock path without deadlocking:
    /// the reaper snapshots sessions before `fs_dir` and never holds both.
    #[test]
    fn concurrent_lock_traffic_and_reaping_do_not_deadlock() {
        let fs = test_fs("concurrent");
        for index in 0..4 {
            create_file(&fs, &format!("/concurrent-{index}.log"), "dead-client");
        }
        register(&fs, "dead-client", 1_000);

        let (tx, rx) = mpsc::channel();
        let mut handles = Vec::new();

        for worker in 0..4 {
            let fs = fs.clone();
            let tx = tx.clone();
            handles.push(std::thread::spawn(move || {
                for index in 0..200 {
                    let client = format!("client-{worker}");
                    let path = format!("/concurrent-{}.log", index % 4);
                    set_lock(&fs, &path, &client, true);
                }
                let _ = tx.send(());
            }));
        }

        {
            let fs = fs.clone();
            let tx = tx.clone();
            handles.push(std::thread::spawn(move || {
                let reaper = SessionReaper::new(fs.clone(), fs.master_monitor.clone());
                for _ in 0..200 {
                    let _ = reaper.reap_once(far_future(&fs));
                }
                let _ = tx.send(());
            }));
        }
        drop(tx);

        for _ in 0..handles.len() {
            rx.recv_timeout(Duration::from_secs(60))
                .expect("lock traffic and reaping deadlocked");
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn reaping_publishes_session_metrics() {
        let fs = test_fs("metrics");
        create_file(&fs, "/metrics.log", "dead-client");
        set_lock(&fs, "/metrics.log", "dead-client", true);
        register(&fs, "dead-client", 1_000);

        let metrics = Master::get_metrics().unwrap();
        let expired_before = metrics.expired_sessions_total.get();
        let locks_before = metrics.reaped_locks_total.get();
        let writers_before = metrics.reaped_writers_total.get();
        assert_eq!(
            metrics.active_sessions.get(),
            1,
            "registering a session must publish the active count"
        );

        let report = SessionReaper::new(fs.clone(), fs.master_monitor.clone())
            .reap_once(far_future(&fs))
            .unwrap();
        assert_eq!(report.sessions, 1);

        assert_eq!(
            metrics.expired_sessions_total.get(),
            expired_before + 1,
            "an expired session must be counted"
        );
        assert_eq!(metrics.reaped_locks_total.get(), locks_before + 1);
        assert_eq!(metrics.reaped_writers_total.get(), writers_before + 1);
        assert_eq!(
            metrics.active_sessions.get(),
            0,
            "the gauge must drop back once the session is gone"
        );
    }

    #[test]
    fn session_summary_reports_ownership_footprint() {
        let fs = test_fs("summary");
        let ino = create_file(&fs, "/summary.log", "watched-client");
        set_lock(&fs, "/summary.log", "watched-client", true);
        // The summary judges liveness against the real clock, so register now.
        register(&fs, "watched-client", LocalTime::mills());

        let summary = fs.session_summary().unwrap();
        assert_eq!(summary.len(), 1);
        let entry = &summary[0];
        assert_eq!(entry.client_id, "watched-client");
        assert_eq!(entry.hostname, "host");
        assert_eq!(entry.mount_point, None);
        assert!(entry.live, "the lease was just renewed");
        assert_eq!(entry.lock_hints, 1);
        assert_eq!(entry.writer_hints, 1);
        assert_eq!(entry.lock_hints + entry.writer_hints, 2);
        assert!(entry.expire_at > entry.last_heartbeat);
        assert_eq!(
            owners(&fs, "watched-client"),
            vec![(OwnerKind::Lock, ino), (OwnerKind::Writer, ino)]
        );

        // A legacy lock is not part of the footprint: it is never reclaimed per
        // client, so it must not inflate the number an operator reads.
        set_lock(&fs, "/summary.log", "legacy-client", false);
        assert!(fs.session_summary().unwrap()[0].lock_hints == 1);
    }
}
