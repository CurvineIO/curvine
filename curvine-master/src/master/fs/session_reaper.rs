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

use crate::master::fs::state::monotonic_ms;
use crate::master::fs::{MasterFilesystem, ReapClaim, ReapStep};
use crate::master::meta::store::OwnerKind;
use crate::master::MasterMonitor;
use curvine_error::FsResult;
use curvine_runtime::runtime::LoopTask;
use log::{info, warn};
use std::sync::Mutex;

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
    /// Hints not acted on because the claim no longer held at release time: the
    /// client renewed or re-registered, this master stepped down, or a new epoch
    /// started. Nothing was changed for them.
    pub skipped: u64,
    /// Whether the tick stopped on its work budget with work still pending.
    pub budget_exhausted: bool,
}

impl ReapReport {
    pub fn is_empty(&self) -> bool {
        self.sessions == 0 && self.locks == 0 && self.writers == 0 && self.stale_hints == 0
    }

    fn count(&mut self, kind: OwnerKind, step: ReapStep) {
        match (step, kind) {
            (ReapStep::Released, OwnerKind::Lock) => self.locks += 1,
            (ReapStep::Released, OwnerKind::Writer) => self.writers += 1,
            (ReapStep::Stale, _) => self.stale_hints += 1,
            (ReapStep::Skipped, _) => self.skipped += 1,
        }
    }
}

/// Reclaims what an expired client session owns (#1884).
///
/// The reaper is the *only* owner of the leader-change grace window: a session
/// that expires is only acted on once `SessionMap::reclaim_after` has passed,
/// measured from the lease epoch this master adopted for its current term
/// (`MasterFilesystem::adopt_session_epoch`). The lock path no longer consults
/// liveness (#1879), so this is the one place the window is enforced.
///
/// Two sources of work, both through the owner index (#1880), never a namespace
/// scan:
///
/// * **Expired sessions** in the session map: every lock and writer hint of the
///   client is released.
/// * **Orphaned locks** after a failover: a client that died before it ever
///   heartbeated this leader is absent from the map, so it never expires there.
///   Once per epoch, after the grace window, the reaper walks the lock hints of
///   every client and releases those whose owner still has no session. Writer
///   hints are not covered: legacy writers without a session leave the same
///   hints, so an absent session does not prove a writer marker is reclaimable.
///
/// Every release re-validates its claim inside the `fs_dir` critical section
/// (`MasterFilesystem::reap_client_owner`), so a heartbeat or a step-down that
/// lands mid-tick stops the reaper before it touches the client. One work budget
/// is shared by the whole tick, so a tick's `fs_dir` occupancy is bounded no
/// matter how many sessions are pending.
pub struct SessionReaper {
    fs: MasterFilesystem,
    monitor: MasterMonitor,
    /// Maximum sessions examined per tick.
    session_batch: usize,
    /// Maximum inode mutations (released or stale hints) per tick, across all
    /// sessions and the orphan pass.
    work_budget: usize,
    /// Resumable position of the once-per-epoch orphan pass.
    orphan_scan: Mutex<OrphanScan>,
}

/// Progress of the orphan pass for one epoch.
#[derive(Debug, Default)]
struct OrphanScan {
    /// Epoch (raft term) the pass belongs to; a new term restarts it.
    term: Option<u64>,
    /// Index key to resume from; `None` = from the start.
    cursor: Option<Vec<u8>>,
    /// Whole index walked for `term`.
    done: bool,
}

/// How a session drain ended.
enum Drain {
    /// No hint left: the session may be forgotten.
    Done,
    /// The tick's budget ran out first; resume next tick.
    OutOfBudget,
    /// The claim stopped holding (renewal, re-registration, demotion, new
    /// epoch): leave the session alone.
    Skipped,
}

impl SessionReaper {
    /// Default number of sessions examined per tick.
    pub const DEFAULT_SESSION_BATCH: usize = 64;
    /// Default number of inode mutations per tick. Each one is a journalled
    /// metadata write, so the budget keeps a tick's `fs_dir` occupancy bounded.
    pub const DEFAULT_WORK_BUDGET: usize = 256;
    /// Index keys the orphan pass visits per budget unit. Visiting a key is a
    /// read; only acting on one costs budget.
    const ORPHAN_KEYS_PER_UNIT: usize = 4;

    pub fn new(fs: MasterFilesystem, monitor: MasterMonitor) -> Self {
        Self::with_limits(
            fs,
            monitor,
            Self::DEFAULT_SESSION_BATCH,
            Self::DEFAULT_WORK_BUDGET,
        )
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
            orphan_scan: Mutex::new(OrphanScan::default()),
        }
    }

    /// Whether lock records are reclaimed at all. With
    /// `client_session_lock_lease` off, session-owned locks keep the legacy
    /// `lock_expire_time` and the reaper only releases writer markers.
    fn reaps_locks(&self) -> bool {
        self.fs.conf.client_session_lock_lease
    }

    /// One bounded pass at `now`.
    ///
    /// `now` is a parameter rather than a wall-clock read so the expiry and grace
    /// boundaries are testable without sleeping.
    pub fn reap_once(&self, now: u64) -> FsResult<ReapReport> {
        let mut report = ReapReport::default();

        // A demoted master must not reclaim anything. This is only the cheap early
        // exit; every release re-checks leadership under `fs_dir`.
        if !self.monitor.is_active() {
            // The gauge is bound to this master's term: a standby rejects heartbeats
            // and never reaps, so without an explicit reset it would keep serving
            // its last leader count on `/metrics` (#1885 review). No heartbeat can
            // race this publication: the RPC rejects standbys before they reach
            // the session map.
            if let Ok(metrics) = crate::master::Master::get_metrics() {
                metrics.active_sessions.set(0);
            }
            return Ok(report);
        }

        // Leader-change grace window. Sessions are leader-local, so right after a
        // failover every client looks owner-less until its next heartbeat; acting
        // now would strip the locks of healthy clients (#1879). Adopt the current
        // term first, so the window is measured from this master's election and not
        // from process start (#1883 review).
        self.fs.adopt_session_epoch(now);
        let (term, reclaim_after, expired) = {
            let sessions = self.fs.session_map.read();
            // Snapshot and drop the guard: sessions are never held while `fs_dir`
            // is taken (see `SessionMap`).
            (
                sessions.term(),
                sessions.reclaim_after(),
                sessions.expired_sessions(now, self.session_batch),
            )
        };
        let Some(term) = term else {
            return Ok(report);
        };
        if now < reclaim_after {
            return Ok(report);
        }

        let mut budget = self.work_budget;
        for (client_id, started_at) in expired {
            if budget == 0 {
                report.budget_exhausted = true;
                break;
            }
            if !self.monitor.is_active() {
                break;
            }

            let claim = ReapClaim::Expired {
                client_id,
                started_at,
                term,
            };
            match self.reap_session(&claim, now, &mut budget, &mut report) {
                Ok(Drain::Done) => {
                    // Forget the session only if it is still the drained, expired
                    // generation; a client that renewed or re-registered mid-tick
                    // keeps its fresh session.
                    if self.fs.session_map.write().remove_expired(
                        claim.client_id(),
                        started_at,
                        now,
                    ) {
                        report.sessions += 1;
                    }
                }
                Ok(Drain::OutOfBudget) => {
                    // Keep the session so the next tick resumes where this one
                    // stopped.
                    report.budget_exhausted = true;
                    break;
                }
                Ok(Drain::Skipped) => {}
                Err(e) => {
                    warn!("reap client session {} failed: {}", claim.client_id(), e);
                }
            }
        }

        if self.reaps_locks() && !report.budget_exhausted && budget > 0 {
            if let Err(e) = self.reap_orphans(term, now, &mut budget, &mut report) {
                warn!("session reaper orphan pass failed: {}", e);
            }
        }

        if !report.is_empty() || report.skipped > 0 {
            self.publish_metrics(&report);
            info!(
                "session reaper: {} sessions, {} locks, {} writer markers, {} stale hints \
                 released, {} skipped (budget exhausted: {})",
                report.sessions,
                report.locks,
                report.writers,
                report.stale_hints,
                report.skipped,
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
        // The `.read()` temporary lives to the end of the statement, so the gauge
        // is set while the session map is still read-locked: a concurrent heartbeat
        // cannot publish a different count between the read and the set (#1885
        // review).
        metrics
            .active_sessions
            .set(self.fs.session_map.read().len() as i64);
    }

    /// Release what `claim`'s client owns, spending from the tick's `budget`.
    ///
    /// Reads at most `budget + 1` hints: one more than can be acted on tells
    /// "drained" apart from "out of budget" without reading the whole backlog.
    /// The session is forgotten by the caller only once it is drained, so an
    /// interrupted pass resumes next tick; every hint acted on leaves the index,
    /// so the remaining work shrinks monotonically.
    fn reap_session(
        &self,
        claim: &ReapClaim,
        now: u64,
        budget: &mut usize,
        report: &mut ReapReport,
    ) -> FsResult<Drain> {
        // With the lock lease off, lock hints are left to `lock_expire_time`, so
        // only writer hints decide whether the session is drained.
        let kind = if self.reaps_locks() {
            None
        } else {
            Some(OwnerKind::Writer)
        };
        let owners =
            self.fs
                .get_client_owners_limited(claim.client_id(), kind, budget.saturating_add(1))?;

        for (kind, ino) in owners {
            if *budget == 0 {
                return Ok(Drain::OutOfBudget);
            }
            let step = self.fs.reap_client_owner(claim, kind, ino, now)?;
            report.count(kind, step);
            if step == ReapStep::Skipped {
                return Ok(Drain::Skipped);
            }
            *budget -= 1;
        }
        Ok(Drain::Done)
    }

    /// Release lock hints whose owner has no session in this epoch (#1884).
    ///
    /// Runs once per epoch after the grace window and is resumable across ticks.
    /// A client absent from the map that late either died before or during the
    /// failover or never had a session; with the lock lease on, nothing else will
    /// ever release its session-owned locks. Legacy (non-session) locks are never
    /// indexed as `Lock` hints, so they are not touched.
    fn reap_orphans(
        &self,
        term: u64,
        now: u64,
        budget: &mut usize,
        report: &mut ReapReport,
    ) -> FsResult<()> {
        let mut scan = self.orphan_scan.lock().unwrap();
        if scan.term != Some(term) {
            *scan = OrphanScan {
                term: Some(term),
                ..OrphanScan::default()
            };
        }
        if scan.done {
            return Ok(());
        }

        let keys = budget.saturating_mul(Self::ORPHAN_KEYS_PER_UNIT);
        let (hints, next) = self.fs.scan_lock_owners(scan.cursor.as_deref(), keys)?;

        for (client_id, ino, key) in hints {
            // Cheap pre-filter without `fs_dir`; `reap_client_owner` re-checks.
            if self.fs.session_map.read().get(&client_id).is_some() {
                continue;
            }
            if *budget == 0 || !self.monitor.is_active() {
                report.budget_exhausted |= *budget == 0;
                scan.cursor = Some(key);
                return Ok(());
            }

            let claim = ReapClaim::Orphan { client_id, term };
            let step = self
                .fs
                .reap_client_owner(&claim, OwnerKind::Lock, ino, now)?;
            report.count(OwnerKind::Lock, step);
            if step == ReapStep::Skipped {
                // Map busy or epoch changed: retry this hint next tick.
                scan.cursor = Some(key);
                return Ok(());
            }
            *budget -= 1;
        }

        match next {
            Some(key) => scan.cursor = Some(key),
            None => scan.done = true,
        }
        Ok(())
    }
}

impl LoopTask for SessionReaper {
    type Error = curvine_error::FsError;

    fn run(&self) -> FsResult<()> {
        // Session deadlines are `monotonic_ms` values; a wall-clock `now` would never
        // line up with them.
        let report = self.reap_once(monotonic_ms())?;
        if report.budget_exhausted {
            warn!(
                "session reaper work budget ({} inode mutations) exhausted with work \
                 still pending; the remaining work resumes on the next tick",
                self.work_budget
            );
        }
        Ok(())
    }

    fn terminate(&self) -> bool {
        self.monitor.is_stop()
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
        test_fs_with(name, true)
    }

    fn test_fs_with(name: &str, lock_lease: bool) -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.client_session_timeout = "60s".to_string();
        conf.master.client_session_lock_lease = lock_lease;
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
        let _metric_serial = Master::metric_test_serial();
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
        let _metric_serial = Master::metric_test_serial();
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
        let _metric_serial = Master::metric_test_serial();
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
        let _metric_serial = Master::metric_test_serial();
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
        let _metric_serial = Master::metric_test_serial();
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
        let _metric_serial = Master::metric_test_serial();
        let fs = test_fs("stale");
        let ino = create_file(&fs, "/stale.log", "ghost-client");
        register(&fs, "ghost-client", 1_000);

        // Someone already cleared the marker behind the index's back: the hint no
        // longer matches any state.
        {
            let fs_dir = fs.fs_dir.write();
            let mut inode = fs_dir.store.get_inode(ino, None).unwrap().unwrap();
            let file = inode.as_file_mut().unwrap();
            let writers_before = file.writer_clients();
            file.remove_writer("ghost-client");
            fs_dir
                .store
                .apply_reopen_file(&inode, &writers_before)
                .unwrap();
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
        let _metric_serial = Master::metric_test_serial();
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
        // Heartbeats are only accepted by the active master, so register while
        // leading, then step down.
        fs.master_monitor.set_role_for_test(RoleState::Leader);
        create_file(&fs, "/standby.log", "dead-client");
        register(&fs, "dead-client", 1_000);
        fs.master_monitor.set_role_for_test(RoleState::Follower);

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
        let _metric_serial = Master::metric_test_serial();
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

    fn expired_claim(fs: &MasterFilesystem, client: &str) -> ReapClaim {
        let sessions = fs.session_map.read();
        ReapClaim::Expired {
            client_id: client.to_string(),
            started_at: sessions.get(client).unwrap().started_at,
            term: sessions.term().unwrap(),
        }
    }

    /// #1884: a re-election restarts the grace window, measured from the new
    /// epoch, and drops the previous term's sessions.
    #[test]
    fn re_election_restarts_the_grace_window() {
        let _metric_serial = Master::metric_test_serial();
        let fs = test_fs("re-elect");
        fs.master_monitor.set_term_for_test(1);
        create_file(&fs, "/re-elect.log", "dead-client");
        register(&fs, "dead-client", 1_000);
        let timeout = fs.session_map.read().session_timeout_ms();

        // Step down, then win a later election at `promoted`, long after the old
        // epoch's grace window ended.
        fs.master_monitor.set_role_for_test(RoleState::Follower);
        let promoted = fs.session_map.read().reclaim_after() + 10 * timeout;
        fs.master_monitor.set_term_for_test(2);
        fs.master_monitor.set_role_for_test(RoleState::Leader);

        let reaper = SessionReaper::new(fs.clone(), fs.master_monitor.clone());
        let report = reaper.reap_once(promoted).unwrap();
        assert!(
            report.is_empty(),
            "a fresh epoch starts inside its grace window"
        );
        assert_eq!(fs.session_map.read().term(), Some(2));
        assert_eq!(
            fs.session_map.read().reclaim_after(),
            promoted + 2 * timeout
        );
        assert!(
            fs.session_map.read().get("dead-client").is_none(),
            "the previous term's sessions are dropped"
        );
        assert_eq!(owners(&fs, "dead-client").len(), 1);

        let report = reaper.reap_once(promoted + 2 * timeout - 1).unwrap();
        assert!(report.is_empty(), "still inside the new epoch's window");
        assert_eq!(owners(&fs, "dead-client").len(), 1);
    }

    /// #1884: a heartbeat that lands between the reaper's snapshot and its release
    /// re-registers the client; the stale claim must not release anything, and
    /// the fresh session must survive.
    #[test]
    fn re_registration_after_the_snapshot_keeps_locks_and_session() {
        let _metric_serial = Master::metric_test_serial();
        let fs = test_fs("renewal");
        let ino = create_file(&fs, "/renewal.log", "busy-client");
        register(&fs, "busy-client", 1_000);
        let now = far_future(&fs);

        // The reaper's snapshot: the old generation, expired at `now`.
        let stale = expired_claim(&fs, "busy-client");

        // The client heartbeats before the release and takes a new lock.
        register(&fs, "busy-client", now);
        set_lock(&fs, "/renewal.log", "busy-client", true);

        for kind in [OwnerKind::Lock, OwnerKind::Writer] {
            let step = fs.reap_client_owner(&stale, kind, ino, now).unwrap();
            assert_eq!(
                step,
                ReapStep::Skipped,
                "{kind:?} of a re-registered client"
            );
        }
        assert_eq!(stored_locks(&fs, ino).len(), 1, "the new lock survives");
        assert_eq!(stored_writers(&fs, ino), vec!["busy-client".to_string()]);
        assert!(!fs.session_map.write().remove_expired(
            "busy-client",
            stale_started_at(&stale),
            now
        ));
        assert!(fs.session_map.read().get("busy-client").is_some());

        // A full tick at the same `now` also leaves the live client alone.
        let report = SessionReaper::new(fs.clone(), fs.master_monitor.clone())
            .reap_once(now)
            .unwrap();
        assert!(report.is_empty());
        assert_eq!(owners(&fs, "busy-client").len(), 2);
    }

    fn stale_started_at(claim: &ReapClaim) -> u64 {
        match claim {
            ReapClaim::Expired { started_at, .. } => *started_at,
            ReapClaim::Orphan { .. } => unreachable!(),
        }
    }

    /// #1884: leadership is re-checked before every release, not once per session.
    #[test]
    fn demotion_between_two_releases_stops_the_second() {
        let _metric_serial = Master::metric_test_serial();
        let fs = test_fs("demote");
        fs.master_monitor.set_term_for_test(1);
        let first = create_file(&fs, "/demote-1.log", "dead-client");
        let second = create_file(&fs, "/demote-2.log", "dead-client");
        register(&fs, "dead-client", 1_000);
        let now = far_future(&fs);
        let claim = expired_claim(&fs, "dead-client");

        let step = fs
            .reap_client_owner(&claim, OwnerKind::Writer, first, now)
            .unwrap();
        assert_eq!(step, ReapStep::Released);

        fs.master_monitor.set_role_for_test(RoleState::Follower);
        let step = fs
            .reap_client_owner(&claim, OwnerKind::Writer, second, now)
            .unwrap();
        assert_eq!(
            step,
            ReapStep::Skipped,
            "a demoted master must stop mid-session"
        );
        assert_eq!(stored_writers(&fs, second), vec!["dead-client".to_string()]);
        assert_eq!(
            owners(&fs, "dead-client"),
            vec![(OwnerKind::Writer, second)]
        );

        // Re-elected: the claim belongs to the old epoch and stays void.
        fs.master_monitor.set_term_for_test(2);
        fs.master_monitor.set_role_for_test(RoleState::Leader);
        fs.adopt_session_epoch(now);
        let much_later = fs.session_map.read().reclaim_after() + 1;
        let step = fs
            .reap_client_owner(&claim, OwnerKind::Writer, second, much_later)
            .unwrap();
        assert_eq!(
            step,
            ReapStep::Skipped,
            "a claim from an earlier epoch is void"
        );
        assert_eq!(stored_writers(&fs, second), vec!["dead-client".to_string()]);
    }

    /// #1884: one budget bounds the whole tick, not each session.
    #[test]
    fn work_budget_is_shared_across_sessions() {
        let _metric_serial = Master::metric_test_serial();
        let fs = test_fs("shared-budget");
        for client in ["dead-a", "dead-b", "dead-c"] {
            for index in 0..2 {
                create_file(&fs, &format!("/{client}-{index}.log"), client);
            }
            register(&fs, client, 1_000);
        }
        let total = |fs: &MasterFilesystem| {
            ["dead-a", "dead-b", "dead-c"]
                .iter()
                .map(|c| owners(fs, c).len())
                .sum::<usize>()
        };
        assert_eq!(total(&fs), 6);

        let reaper = SessionReaper::with_limits(fs.clone(), fs.master_monitor.clone(), 8, 3);
        let now = far_future(&fs);

        let first = reaper.reap_once(now).unwrap();
        assert!(first.budget_exhausted);
        assert_eq!(first.writers, 3, "three sessions share one budget of 3");
        assert_eq!(first.sessions, 1);
        assert_eq!(total(&fs), 3);

        let second = reaper.reap_once(now).unwrap();
        assert_eq!(second.writers, 3);
        assert_eq!(second.sessions, 2);
        assert_eq!(total(&fs), 0);
        assert!(fs.session_map.read().is_empty());
    }

    /// #1884: a tick examines at most `session_batch` expired sessions.
    #[test]
    fn session_batch_bounds_expired_sessions_per_tick() {
        let _metric_serial = Master::metric_test_serial();
        let fs = test_fs("batch");
        for client in ["dead-a", "dead-b", "dead-c"] {
            register(&fs, client, 1_000);
        }
        let reaper = SessionReaper::with_limits(fs.clone(), fs.master_monitor.clone(), 2, 64);
        let now = far_future(&fs);

        assert_eq!(reaper.reap_once(now).unwrap().sessions, 2);
        assert_eq!(fs.session_map.read().len(), 1);
        assert_eq!(reaper.reap_once(now).unwrap().sessions, 1);
        assert!(fs.session_map.read().is_empty());
    }

    /// #1884: the reaper reads O(budget) hints of a client, not its whole backlog.
    #[test]
    fn owner_reads_are_bounded() {
        let _metric_serial = Master::metric_test_serial();
        let fs = test_fs("bounded-read");
        for index in 0..5 {
            create_file(&fs, &format!("/bounded-{index}.log"), "busy-client");
        }
        set_lock(&fs, "/bounded-0.log", "busy-client", true);

        let limited = |kind, limit| {
            fs.get_client_owners_limited("busy-client", kind, limit)
                .unwrap()
        };
        assert_eq!(limited(None, 2).len(), 2);
        assert_eq!(limited(None, 64).len(), 6);
        assert_eq!(limited(Some(OwnerKind::Lock), 64).len(), 1);
        assert_eq!(limited(Some(OwnerKind::Writer), 64).len(), 5);
        assert_eq!(limited(None, 64), owners(&fs, "busy-client"));
    }

    /// #1884: a client that died before the failover never heartbeats the new
    /// leader, so it is absent from the map; its session-owned locks are released
    /// by the orphan pass once the new epoch's grace window has passed.
    #[test]
    fn locks_of_a_client_missing_after_failover_are_released() {
        let _metric_serial = Master::metric_test_serial();
        let fs = test_fs("orphan");
        fs.master_monitor.set_term_for_test(1);
        let dead = create_file(&fs, "/orphan.log", "other-writer");
        let alive = create_file(&fs, "/alive.log", "other-writer");
        register(&fs, "dead-client", 1_000);
        set_lock(&fs, "/orphan.log", "dead-client", true);

        // Failover: the new epoch's map does not know `dead-client`.
        let promoted = fs.session_map.read().reclaim_after() + 1;
        fs.master_monitor.set_term_for_test(2);
        fs.adopt_session_epoch(promoted);
        let reclaim_after = fs.session_map.read().reclaim_after();
        assert!(fs.session_map.read().get("dead-client").is_none());

        // A healthy client re-registers in the new epoch and holds a lock.
        register(&fs, "live-client", reclaim_after - 1);
        set_lock(&fs, "/alive.log", "live-client", true);

        let reaper = SessionReaper::new(fs.clone(), fs.master_monitor.clone());
        let report = reaper.reap_once(reclaim_after - 1).unwrap();
        assert!(report.is_empty(), "orphans wait for the grace window too");
        assert_eq!(stored_locks(&fs, dead).len(), 1);

        let report = reaper.reap_once(reclaim_after).unwrap();
        assert_eq!(report.locks, 1);
        assert!(stored_locks(&fs, dead).is_empty());
        assert!(owners(&fs, "dead-client").is_empty());
        assert_eq!(stored_locks(&fs, alive).len(), 1, "a live owner is kept");
        assert_eq!(owners(&fs, "live-client"), vec![(OwnerKind::Lock, alive)]);

        // The pass runs once per epoch.
        set_lock(&fs, "/orphan.log", "dead-client", true);
        assert!(reaper.reap_once(reclaim_after + 1).unwrap().is_empty());
        assert_eq!(stored_locks(&fs, dead).len(), 1);
    }

    /// #1884: the orphan pass is resumable under a small budget.
    #[test]
    fn orphan_pass_resumes_across_ticks() {
        let _metric_serial = Master::metric_test_serial();
        let fs = test_fs("orphan-resume");
        fs.master_monitor.set_term_for_test(1);
        for index in 0..3 {
            let path = format!("/orphan-{index}.log");
            create_file(&fs, &path, "other-writer");
            set_lock(&fs, &path, &format!("dead-{index}"), true);
        }
        fs.master_monitor.set_term_for_test(2);
        fs.adopt_session_epoch(1_000);
        let now = fs.session_map.read().reclaim_after();

        let reaper = SessionReaper::with_limits(fs.clone(), fs.master_monitor.clone(), 8, 2);
        let first = reaper.reap_once(now).unwrap();
        assert_eq!(first.locks, 2);
        assert!(first.budget_exhausted);
        let second = reaper.reap_once(now).unwrap();
        assert_eq!(second.locks, 1);
        assert!(!second.budget_exhausted);
        for index in 0..3 {
            assert!(owners(&fs, &format!("dead-{index}")).is_empty());
        }
    }

    /// #1884: with `client_session_lock_lease` off, locks stay on
    /// `lock_expire_time`; the reaper releases only writer markers and runs no
    /// orphan pass.
    #[test]
    fn lock_lease_off_leaves_locks_to_the_legacy_ttl() {
        let _metric_serial = Master::metric_test_serial();
        let fs = test_fs_with("lease-off", false);
        let ino = create_file(&fs, "/lease-off.log", "dead-client");
        let orphan = create_file(&fs, "/lease-off-orphan.log", "other-writer");
        set_lock(&fs, "/lease-off.log", "dead-client", true);
        set_lock(&fs, "/lease-off-orphan.log", "orphan-client", true);
        register(&fs, "dead-client", 1_000);

        let report = SessionReaper::new(fs.clone(), fs.master_monitor.clone())
            .reap_once(far_future(&fs))
            .unwrap();
        assert_eq!(report.writers, 1);
        assert_eq!(report.locks, 0);
        assert_eq!(report.sessions, 1);
        assert!(stored_writers(&fs, ino).is_empty());
        assert_eq!(stored_locks(&fs, ino).len(), 1, "locks keep the TTL path");
        assert_eq!(stored_locks(&fs, orphan).len(), 1, "no orphan pass");
        assert_eq!(owners(&fs, "dead-client"), vec![(OwnerKind::Lock, ino)]);
    }

    #[test]
    fn terminates_only_when_the_master_stops() {
        let _metric_serial = Master::metric_test_serial();
        let fs = test_fs("terminate");
        let reaper = SessionReaper::new(fs.clone(), fs.master_monitor.clone());
        assert!(!reaper.terminate());
        fs.master_monitor.set_role_for_test(RoleState::Follower);
        assert!(
            !reaper.terminate(),
            "a standby keeps the task for its next term"
        );
        fs.master_monitor.set_role_for_test(RoleState::Exit);
        assert!(reaper.terminate());
    }

    #[test]
    fn standby_publishes_a_zero_gauge() {
        let _metric_serial = Master::metric_test_serial();
        let fs = test_fs("standby-gauge");
        create_file(&fs, "/standby.log", "client-a");
        set_lock(&fs, "/standby.log", "client-a", true);
        register(&fs, "client-a", 1_000);
        assert_eq!(
            Master::get_metrics().unwrap().active_sessions.get(),
            1,
            "precondition: the leader published its count"
        );

        // A demotion must not leave the previous count on /metrics: a standby
        // rejects heartbeats and never reaps, so nothing else would reset the
        // gauge (#1885 review).
        fs.master_monitor.set_role_for_test(RoleState::Follower);
        let report = SessionReaper::new(fs.clone(), fs.master_monitor.clone())
            .reap_once(2_000)
            .unwrap();

        assert!(report.is_empty(), "a standby must not reclaim anything");
        assert_eq!(
            Master::get_metrics().unwrap().active_sessions.get(),
            0,
            "the standby must publish a zero gauge"
        );
    }

    #[test]
    fn reaping_publishes_session_metrics() {
        let _metric_serial = Master::metric_test_serial();
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
        let _metric_serial = Master::metric_test_serial();
        let fs = test_fs("summary");
        let ino = create_file(&fs, "/summary.log", "watched-client");
        set_lock(&fs, "/summary.log", "watched-client", true);
        // Register on the clock the heartbeat RPC uses. A wall-clock `now` here hid
        // a bug where `live` compared epoch ms with monotonic deadlines (#1885
        // review).
        register(&fs, "watched-client", monotonic_ms());

        let wall_before = curvine_runtime::common::LocalTime::mills();
        let summary = fs.session_summary().unwrap();
        let wall_after = curvine_runtime::common::LocalTime::mills();
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
        // Times are reported as Unix epoch ms, not process-local monotonic ms.
        assert!(
            entry.last_heartbeat + 5_000 >= wall_before && entry.last_heartbeat <= wall_after,
            "last_heartbeat {} must be the wall clock of the renewal ({}..={})",
            entry.last_heartbeat,
            wall_before,
            wall_after
        );
        assert!(entry.started_at <= entry.last_heartbeat);
        assert!(
            entry.expire_at > wall_after,
            "the lease runs into the future"
        );
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
