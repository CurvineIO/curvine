use curvine_core_error::CommonResult;
use curvine_model::{FileLock, LockFlags, LockType};
use curvine_runtime::common::SerdeUtils as Serde;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// `FileLock` as it was persisted before session ownership (#1878).
///
/// Lock records are written to RocksDB and to the journal with bincode, which is
/// positional and not self-describing: a record written by an older master has no
/// trailing `session_owned` byte, and the current `FileLock` layout cannot decode
/// it. Keeping the old mirror type lets both readers fall back instead of losing
/// the record (RocksDB) or refusing to replay the journal, which is fatal on a
/// master restart after an upgrade.
#[derive(Serialize, Deserialize)]
pub(crate) struct LegacyFileLock {
    pub(crate) client_id: String,
    pub(crate) owner_id: u64,
    pub(crate) pid: u32,
    pub(crate) acquire_time: u64,
    pub(crate) lock_type: LockType,
    pub(crate) lock_flags: LockFlags,
    pub(crate) start: u64,
    pub(crate) end: u64,
}

impl From<LegacyFileLock> for FileLock {
    fn from(lock: LegacyFileLock) -> Self {
        Self {
            client_id: lock.client_id,
            owner_id: lock.owner_id,
            pid: lock.pid,
            acquire_time: lock.acquire_time,
            lock_type: lock.lock_type,
            lock_flags: lock.lock_flags,
            start: lock.start,
            end: lock.end,
            // Locks persisted before #1878 were acquired by clients that never
            // sent a heartbeat, so they belong to the legacy TTL path.
            session_owned: false,
        }
    }
}

/// Decode a persisted lock list in either the current or the pre-#1878 layout.
pub(crate) fn decode_persisted_locks(bytes: &[u8]) -> CommonResult<Vec<FileLock>> {
    match Serde::deserialize::<Vec<FileLock>>(bytes) {
        Ok(locks) => Ok(locks),
        Err(current_err) => match Serde::deserialize::<Vec<LegacyFileLock>>(bytes) {
            Ok(legacy) => Ok(legacy.into_iter().map(Into::into).collect()),
            // Report the current-schema error: a genuinely corrupt record is the
            // more useful signal than "legacy layout did not match either".
            Err(_) => Err(current_err),
        },
    }
}

/// How a lock's lifetime is judged at one instant (#1879).
///
/// Session ownership replaces the fixed TTL for clients that keep a lease alive:
/// a lock must not be stolen merely because it is old, while a client that
/// stopped heartbeating must lose its locks in bounded time.
///
/// * `session_owned` lock, owner live -> never expired, however old it is.
/// * `session_owned` lock, owner unknown or expired -> reclaimable only after
///   [`LockValidity::reclaim_after`]. That window covers a leader change, where
///   every session is unknown until the clients re-register; without it a
///   failover would steal the locks of healthy clients.
/// * legacy lock (`session_owned == false`) -> historical `lock_expire_time` from
///   `acquire_time`, unchanged, so pre-#1878 clients and their persisted locks
///   keep working.
#[derive(Debug, Clone, Default)]
pub struct LockValidity {
    /// TTL applied to legacy (non session-owned) locks.
    pub legacy_expire_ms: u64,
    /// Client ids whose session lease is valid at the instant of the snapshot.
    pub live_clients: HashSet<String>,
    /// Earliest instant (master clock, ms) a session-owned lock may be reclaimed
    /// when its owner is not live. See [`SessionMap::reclaim_after`].
    pub reclaim_after: u64,
}

impl LockValidity {
    pub fn new(legacy_expire_ms: u64, live_clients: HashSet<String>, reclaim_after: u64) -> Self {
        Self {
            legacy_expire_ms,
            live_clients,
            reclaim_after,
        }
    }

    /// A validity view that treats every session-owned lock as live. Used by
    /// callers that have no session state at hand (tests, replay paths).
    pub fn legacy_only(legacy_expire_ms: u64) -> Self {
        Self::new(legacy_expire_ms, HashSet::new(), u64::MAX)
    }

    /// Whether `lock` may be reclaimed at `now`.
    pub fn is_expired(&self, lock: &FileLock, now: u64) -> bool {
        if lock.session_owned {
            if self.live_clients.contains(&lock.client_id) {
                return false;
            }
            return now >= self.reclaim_after;
        }

        if lock.acquire_time == 0 {
            return true;
        }
        now.saturating_sub(lock.acquire_time) > self.legacy_expire_ms
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct LockMeta {
    plock: HashMap<String, Vec<FileLock>>,
    flock: HashMap<String, FileLock>,
}

impl LockMeta {
    /// Load persisted lock state, tolerating records written before #1878.
    pub fn decode_persisted(bytes: &[u8]) -> CommonResult<Self> {
        Ok(Self::with_vec(decode_persisted_locks(bytes)?))
    }

    pub fn with_vec(vec: Vec<FileLock>) -> Self {
        let mut plock = HashMap::new();
        let mut flock = HashMap::new();

        for lock in vec {
            let key = Self::key(&lock);
            match lock.lock_flags {
                LockFlags::Plock => {
                    plock.entry(key).or_insert_with(Vec::new).push(lock);
                }

                LockFlags::Flock => {
                    flock.insert(key, lock);
                }
            }
        }

        Self { plock, flock }
    }

    pub fn to_vec(&self) -> Vec<FileLock> {
        let mut result = Vec::new();

        for locks in self.plock.values() {
            result.extend(locks.iter().cloned());
        }
        result.extend(self.flock.values().cloned());

        result
    }

    fn key(lock: &FileLock) -> String {
        format!("{}{}", lock.client_id, lock.owner_id)
    }

    pub fn len(&self) -> usize {
        self.plock.values().map(|x| x.len()).sum::<usize>() + self.flock.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn locks(&self) -> (&HashMap<String, Vec<FileLock>>, &HashMap<String, FileLock>) {
        (&self.plock, &self.flock)
    }

    fn regions_overlap(a_start: u64, a_end: u64, b_start: u64, b_end: u64) -> bool {
        a_start <= b_end && b_start <= a_end
    }

    fn subtract_region(
        region_start: u64,
        region_end: u64,
        remove_start: u64,
        remove_end: u64,
    ) -> Vec<(u64, u64)> {
        if !Self::regions_overlap(region_start, region_end, remove_start, remove_end) {
            return vec![(region_start, region_end)];
        }

        let mut out = Vec::new();
        if region_start < remove_start {
            out.push((region_start, remove_start.saturating_sub(1)));
        }
        if region_end > remove_end {
            out.push((remove_end.saturating_add(1), region_end));
        }
        out
    }

    fn same_owner(a: &FileLock, b: &FileLock) -> bool {
        a.client_id == b.client_id && a.owner_id == b.owner_id
    }

    fn types_conflict(query: LockType, existing: LockType) -> bool {
        matches!(
            (query, existing),
            (LockType::WriteLock, _) | (_, LockType::WriteLock)
        )
    }

    /// Drop expired POSIX ranges. Returns whether anything was removed.
    fn purge_expired_plocks(&mut self, validity: &LockValidity, now: u64) -> bool {
        if self.plock.is_empty() {
            return false;
        }

        let before = self.len();
        self.plock.retain(|_, owner_locks| {
            owner_locks.retain(|lock| !validity.is_expired(lock, now));
            !owner_locks.is_empty()
        });
        self.len() != before
    }

    fn subtract_owner_regions(owner_locks: &mut Vec<FileLock>, remove_start: u64, remove_end: u64) {
        let mut next = Vec::new();
        for existing in owner_locks.drain(..) {
            for (start, end) in
                Self::subtract_region(existing.start, existing.end, remove_start, remove_end)
            {
                next.push(FileLock {
                    start,
                    end,
                    ..existing.clone()
                });
            }
        }
        *owner_locks = next;
    }

    /// Pure conflict query for POSIX ranges: expired locks are skipped, never
    /// removed.
    fn find_plock_conflict(
        &self,
        lock: &FileLock,
        validity: &LockValidity,
        now: u64,
    ) -> Option<FileLock> {
        let mut conflict_lock = None;
        for existing_lock in self.plock.values().flatten() {
            if Self::same_owner(lock, existing_lock) {
                continue;
            }
            if !Self::types_conflict(lock.lock_type, existing_lock.lock_type) {
                continue;
            }
            if !Self::regions_overlap(lock.start, lock.end, existing_lock.start, existing_lock.end)
            {
                continue;
            }
            if validity.is_expired(existing_lock, now) {
                continue;
            }

            let replace = conflict_lock
                .as_ref()
                .map(|best: &FileLock| existing_lock.start < best.start)
                .unwrap_or(true);
            if replace {
                conflict_lock = Some(existing_lock.clone());
            }
        }

        conflict_lock
    }

    /// Pure conflict query for BSD locks: expired owners are skipped, never
    /// removed.
    fn find_flock_conflict(
        &self,
        lock: &FileLock,
        validity: &LockValidity,
        now: u64,
    ) -> Option<FileLock> {
        self.flock
            .values()
            .find(|existing| {
                !Self::same_owner(lock, existing)
                    && !validity.is_expired(existing, now)
                    && matches!(
                        (lock.lock_type, existing.lock_type),
                        (LockType::WriteLock, _) | (_, LockType::WriteLock)
                    )
            })
            .cloned()
    }

    /// Conflict query without side effects (#1879): `F_GETLK` must not write.
    ///
    /// Expired locks are skipped when the answer is computed and left for
    /// [`LockMeta::set_lock_with_change`] or the session reaper to reclaim. There
    /// is deliberately no mutating variant: a query that reclaims cannot report
    /// the change, so the caller would neither persist nor journal it.
    pub fn find_conflict(
        &self,
        lock: &FileLock,
        validity: &LockValidity,
        now: u64,
    ) -> Option<FileLock> {
        if lock.lock_type == LockType::UnLock {
            return None;
        }

        match lock.lock_flags {
            LockFlags::Plock => self.find_plock_conflict(lock, validity, now),
            LockFlags::Flock => self.find_flock_conflict(lock, validity, now),
        }
    }

    fn set_plock(
        &mut self,
        mut lock: FileLock,
        validity: &LockValidity,
        now: u64,
    ) -> (Option<FileLock>, bool) {
        let key = Self::key(&lock);

        if lock.lock_type == LockType::UnLock {
            // No-op unlock on an empty map (common for FUSE_FLUSH without prior
            // fcntl) must not pay for a full expire scan.
            if self.plock.is_empty() {
                return (None, false);
            }

            let mut changed = self.purge_expired_plocks(validity, now);

            if lock.start == 0 && lock.end == u64::MAX {
                changed |= self.plock.remove(&key).is_some();
                return (None, changed);
            }

            if let Some(owner_locks) = self.plock.get_mut(&key) {
                if owner_locks
                    .iter()
                    .any(|l| Self::regions_overlap(l.start, l.end, lock.start, lock.end))
                {
                    Self::subtract_owner_regions(owner_locks, lock.start, lock.end);
                    changed = true;
                    if owner_locks.is_empty() {
                        self.plock.remove(&key);
                    }
                }
            }
            return (None, changed);
        }

        // The persisted state changes whenever expiry reclamation drops something,
        // even if the new lock itself conflicts.
        let mut changed = self.purge_expired_plocks(validity, now);
        if let Some(conflict) = self.find_plock_conflict(&lock, validity, now) {
            return (Some(conflict), changed);
        }

        if let Some(owner_locks) = self.plock.get_mut(&key) {
            Self::subtract_owner_regions(owner_locks, lock.start, lock.end);
        }

        lock.acquire_time = now;
        self.plock.entry(key).or_default().push(lock);

        changed = true;
        (None, changed)
    }

    fn set_flock(
        &mut self,
        mut lock: FileLock,
        validity: &LockValidity,
        now: u64,
    ) -> (Option<FileLock>, bool) {
        let key = Self::key(&lock);

        if lock.lock_type == LockType::UnLock {
            let changed = self.flock.remove(&key).is_some();
            return (None, changed);
        }

        let len_before = self.flock.len();
        self.flock
            .retain(|_, existing| !validity.is_expired(existing, now));
        let mut changed = self.flock.len() != len_before;

        if let Some(conflict) = self.find_flock_conflict(&lock, validity, now) {
            return (Some(conflict), changed);
        }

        lock.acquire_time = now;
        self.flock.insert(key, lock);

        changed = true;
        (None, changed)
    }

    /// Set a lock (acquire or release).
    ///
    /// Locks whose owner is judged reclaimable by `validity` are dropped as part
    /// of the update, so the caller persists and journals the reclaimed state.
    ///
    /// # Arguments
    /// * `lock` - The lock to set. If lock_type is UnLock, the lock will be released.
    /// * `validity` - Session liveness view used to judge existing locks.
    /// * `now` - Master clock (ms) the decision is made at.
    ///
    /// # Returns
    /// * `Some(FileLock)` - If setting the lock would conflict with an existing lock, returns the conflicting lock
    /// * `None` - If the lock was successfully set (or released)
    pub fn set_lock(
        &mut self,
        lock: FileLock,
        validity: &LockValidity,
        now: u64,
    ) -> Option<FileLock> {
        self.set_lock_with_change(lock, validity, now).0
    }

    /// Like [`Self::set_lock`], but also reports whether persistent lock state changed.
    pub fn set_lock_with_change(
        &mut self,
        lock: FileLock,
        validity: &LockValidity,
        now: u64,
    ) -> (Option<FileLock>, bool) {
        match lock.lock_flags {
            LockFlags::Plock => self.set_plock(lock, validity, now),
            LockFlags::Flock => self.set_flock(lock, validity, now),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_runtime::common::LocalTime;
    use std::collections::HashSet;
    use std::thread;
    use std::time::Duration;

    /// Validity view with no sessions at all: reproduces the pre-#1879 TTL rules.
    fn legacy(expire_ms: u64) -> LockValidity {
        LockValidity::legacy_only(expire_ms)
    }

    fn create_plock(
        client_id: &str,
        owner_id: u64,
        lock_type: LockType,
        start: u64,
        end: u64,
    ) -> FileLock {
        FileLock {
            client_id: client_id.to_string(),
            owner_id,
            pid: 1000,
            acquire_time: LocalTime::mills(),
            lock_type,
            lock_flags: LockFlags::Plock,
            start,
            end,
            session_owned: false,
        }
    }

    fn create_flock(client_id: &str, owner_id: u64, lock_type: LockType) -> FileLock {
        FileLock {
            client_id: client_id.to_string(),
            owner_id,
            pid: 1000,
            acquire_time: LocalTime::mills(),
            lock_type,
            lock_flags: LockFlags::Flock,
            start: 0,
            end: u64::MAX,
            session_owned: false,
        }
    }

    #[test]
    fn test_set_lock_plock_read() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_plock("client1", 100, LockType::ReadLock, 0, 100);
        let result = meta.set_lock(lock1.clone(), &legacy(expire_ms), LocalTime::mills());
        assert!(result.is_none(), "should successfully set read lock");

        let locks = meta.to_vec();
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].client_id, "client1");
        assert_eq!(locks[0].lock_flags, LockFlags::Plock);
    }

    #[test]
    fn test_set_lock_plock_write() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_plock("client1", 100, LockType::WriteLock, 0, 100);
        let result = meta.set_lock(lock1.clone(), &legacy(expire_ms), LocalTime::mills());
        assert!(result.is_none(), "should successfully set write lock");

        let locks = meta.to_vec();
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].lock_type, LockType::WriteLock);
    }

    #[test]
    fn test_set_lock_flock_read() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_flock("client1", 100, LockType::ReadLock);
        let result = meta.set_lock(lock1.clone(), &legacy(expire_ms), LocalTime::mills());
        assert!(result.is_none(), "should successfully set BSD read lock");

        let locks = meta.to_vec();
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].lock_flags, LockFlags::Flock);
    }

    #[test]
    fn test_set_lock_flock_write() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_flock("client1", 100, LockType::WriteLock);
        let result = meta.set_lock(lock1.clone(), &legacy(expire_ms), LocalTime::mills());
        assert!(result.is_none(), "should successfully set BSD write lock");

        let locks = meta.to_vec();
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].lock_type, LockType::WriteLock);
    }

    #[test]
    fn test_set_lock_conflict_plock() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_plock("client1", 100, LockType::WriteLock, 0, 100);
        assert!(meta
            .set_lock(lock1, &legacy(expire_ms), LocalTime::mills())
            .is_none());

        let lock2 = create_plock("client2", 200, LockType::WriteLock, 50, 150);
        let conflict = meta.set_lock(lock2, &legacy(expire_ms), LocalTime::mills());
        assert!(conflict.is_some(), "should detect conflict");
        assert_eq!(conflict.unwrap().client_id, "client1");
    }

    #[test]
    fn test_set_lock_conflict_flock() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_flock("client1", 100, LockType::WriteLock);
        assert!(meta
            .set_lock(lock1, &legacy(expire_ms), LocalTime::mills())
            .is_none());

        let lock2 = create_flock("client2", 200, LockType::WriteLock);
        let conflict = meta.set_lock(lock2, &legacy(expire_ms), LocalTime::mills());
        assert!(conflict.is_some(), "should detect conflict");
        assert_eq!(conflict.unwrap().client_id, "client1");
    }

    #[test]
    fn test_check_conflict_plock_expired() {
        let expire_ms = 3000;

        let mut expired_lock = create_plock("client1", 100, LockType::WriteLock, 0, 100);
        expired_lock.acquire_time = LocalTime::mills().saturating_sub(5000);
        let mut meta = LockMeta::with_vec(vec![expired_lock]);

        let locks_before = meta.to_vec();
        assert_eq!(locks_before.len(), 1);

        let new_lock = create_plock("client2", 200, LockType::WriteLock, 0, 100);
        let validity = legacy(expire_ms);
        assert!(
            meta.find_conflict(&new_lock, &validity, LocalTime::mills())
                .is_none(),
            "an expired lock must not conflict"
        );
        assert_eq!(
            meta.to_vec().len(),
            1,
            "a query must not reclaim: F_GETLK is a read path"
        );

        let (conflict, changed) =
            meta.set_lock_with_change(new_lock, &validity, LocalTime::mills());
        assert!(conflict.is_none());
        assert!(
            changed,
            "the reclaim must be reported so it can be journaled"
        );
        let locks_after = meta.to_vec();
        assert_eq!(locks_after.len(), 1);
        assert_eq!(locks_after[0].client_id, "client2");
    }

    #[test]
    fn test_check_conflict_flock_expired() {
        let expire_ms = 3000;

        let mut expired_lock = create_flock("client1", 100, LockType::WriteLock);
        expired_lock.acquire_time = LocalTime::mills().saturating_sub(5000);
        let mut meta = LockMeta::with_vec(vec![expired_lock]);

        let locks_before = meta.to_vec();
        assert_eq!(locks_before.len(), 1);

        let new_lock = create_flock("client2", 200, LockType::WriteLock);
        let validity = legacy(expire_ms);
        assert!(
            meta.find_conflict(&new_lock, &validity, LocalTime::mills())
                .is_none(),
            "an expired flock must not conflict"
        );
        assert_eq!(
            meta.to_vec().len(),
            1,
            "a query must not reclaim: F_GETLK is a read path"
        );

        let (conflict, changed) =
            meta.set_lock_with_change(new_lock, &validity, LocalTime::mills());
        assert!(conflict.is_none());
        assert!(
            changed,
            "the reclaim must be reported so it can be journaled"
        );
        let locks_after = meta.to_vec();
        assert_eq!(locks_after.len(), 1);
        assert_eq!(locks_after[0].client_id, "client2");
    }

    #[test]
    fn test_check_conflict_plock_not_expired() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_plock("client1", 100, LockType::WriteLock, 0, 100);
        assert!(meta
            .set_lock(lock1, &legacy(expire_ms), LocalTime::mills())
            .is_none());

        let lock2 = create_plock("client2", 200, LockType::WriteLock, 50, 150);
        let conflict = meta.find_conflict(&lock2, &legacy(expire_ms), LocalTime::mills());
        assert!(conflict.is_some(), "non-expired lock should conflict");
        assert_eq!(conflict.unwrap().client_id, "client1");
    }

    #[test]
    fn test_check_conflict_flock_not_expired() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_flock("client1", 100, LockType::WriteLock);
        assert!(meta
            .set_lock(lock1, &legacy(expire_ms), LocalTime::mills())
            .is_none());

        let lock2 = create_flock("client2", 200, LockType::WriteLock);
        let conflict = meta.find_conflict(&lock2, &legacy(expire_ms), LocalTime::mills());
        assert!(conflict.is_some(), "non-expired lock should conflict");
        assert_eq!(conflict.unwrap().client_id, "client1");
    }

    #[test]
    fn test_check_conflict_plock_read_read() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_plock("client1", 100, LockType::ReadLock, 0, 100);
        assert!(meta
            .set_lock(lock1, &legacy(expire_ms), LocalTime::mills())
            .is_none());

        let lock2 = create_plock("client2", 200, LockType::ReadLock, 50, 150);
        let conflict = meta.find_conflict(&lock2, &legacy(expire_ms), LocalTime::mills());
        assert!(conflict.is_none(), "read locks should not conflict");
    }

    #[test]
    fn test_check_conflict_flock_read_read() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_flock("client1", 100, LockType::ReadLock);
        assert!(meta
            .set_lock(lock1, &legacy(expire_ms), LocalTime::mills())
            .is_none());

        let lock2 = create_flock("client2", 200, LockType::ReadLock);
        let conflict = meta.find_conflict(&lock2, &legacy(expire_ms), LocalTime::mills());
        assert!(conflict.is_none(), "read locks should not conflict");
    }

    #[test]
    fn test_getlk_reports_earliest_conflicting_plock() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        assert!(meta
            .set_lock(
                create_plock("client1", 100, LockType::WriteLock, 10, 14),
                &legacy(expire_ms),
                LocalTime::mills(),
            )
            .is_none());
        assert!(meta
            .set_lock(
                create_plock("client1", 100, LockType::ReadLock, 1, 5),
                &legacy(expire_ms),
                LocalTime::mills(),
            )
            .is_none());

        let query = create_plock("client2", 200, LockType::WriteLock, 0, u64::MAX);
        let conflict = meta
            .find_conflict(&query, &legacy(expire_ms), LocalTime::mills())
            .expect("conflict");
        assert_eq!(conflict.lock_type, LockType::ReadLock);
        assert_eq!(conflict.start, 1);
        assert_eq!(conflict.end, 5);
        assert_eq!(conflict.pid, 1000);
    }

    #[test]
    fn test_partial_unlock_splits_plock_region() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        assert!(meta
            .set_lock(
                create_plock("client1", 100, LockType::WriteLock, 10, 14),
                &legacy(expire_ms),
                LocalTime::mills(),
            )
            .is_none());
        assert!(meta
            .set_lock(
                create_plock("client1", 100, LockType::UnLock, 5, 10),
                &legacy(expire_ms),
                LocalTime::mills(),
            )
            .is_none());

        let locks = meta.to_vec();
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].start, 11);
        assert_eq!(locks[0].end, 14);
        assert_eq!(locks[0].lock_type, LockType::WriteLock);
    }

    #[test]
    fn test_same_owner_overlapping_plock_replaces_region() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        assert!(meta
            .set_lock(
                create_plock("client1", 100, LockType::WriteLock, 0, 100),
                &legacy(expire_ms),
                LocalTime::mills(),
            )
            .is_none());
        assert!(meta
            .set_lock(
                create_plock("client1", 100, LockType::WriteLock, 50, 60),
                &legacy(expire_ms),
                LocalTime::mills(),
            )
            .is_none());

        let mut locks = meta.to_vec();
        locks.sort_by_key(|lock| lock.start);
        assert_eq!(locks.len(), 3);
        assert_eq!(locks[0].start, 0);
        assert_eq!(locks[0].end, 49);
        assert_eq!(locks[1].start, 50);
        assert_eq!(locks[1].end, 60);
        assert_eq!(locks[2].start, 61);
        assert_eq!(locks[2].end, 100);
    }

    #[test]
    fn test_partial_unlock_splits_plock_interior_hole() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        assert!(meta
            .set_lock(
                create_plock("client1", 100, LockType::WriteLock, 10, 20),
                &legacy(expire_ms),
                LocalTime::mills(),
            )
            .is_none());
        assert!(meta
            .set_lock(
                create_plock("client1", 100, LockType::UnLock, 12, 15),
                &legacy(expire_ms),
                LocalTime::mills(),
            )
            .is_none());

        let mut locks = meta.to_vec();
        locks.sort_by_key(|lock| lock.start);
        assert_eq!(locks.len(), 2);
        assert_eq!(locks[0].start, 10);
        assert_eq!(locks[0].end, 11);
        assert_eq!(locks[1].start, 16);
        assert_eq!(locks[1].end, 20);
    }

    #[test]
    fn test_unlock_empty_plock_is_noop() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let (conflict, changed) = meta.set_lock_with_change(
            create_plock("client1", 100, LockType::UnLock, 0, u64::MAX),
            &legacy(expire_ms),
            LocalTime::mills(),
        );
        assert!(conflict.is_none());
        assert!(!changed);
        assert!(meta.to_vec().is_empty());
    }

    #[test]
    fn test_unlock_missing_owner_does_not_mark_changed() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        assert!(meta
            .set_lock(
                create_plock("client1", 100, LockType::WriteLock, 0, 10),
                &legacy(expire_ms),
                LocalTime::mills(),
            )
            .is_none());

        let (conflict, changed) = meta.set_lock_with_change(
            create_plock("client2", 200, LockType::UnLock, 0, u64::MAX),
            &legacy(expire_ms),
            LocalTime::mills(),
        );
        assert!(conflict.is_none());
        assert!(!changed);
        assert_eq!(meta.to_vec().len(), 1);
    }

    #[test]
    fn test_partial_unlock_marks_changed_when_region_splits() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        assert!(meta
            .set_lock(
                create_plock("client1", 100, LockType::WriteLock, 10, 20),
                &legacy(expire_ms),
                LocalTime::mills(),
            )
            .is_none());

        let (conflict, changed) = meta.set_lock_with_change(
            create_plock("client1", 100, LockType::UnLock, 12, 15),
            &legacy(expire_ms),
            LocalTime::mills(),
        );
        assert!(conflict.is_none());
        assert!(changed);

        let mut locks = meta.to_vec();
        locks.sort_by_key(|lock| lock.start);
        assert_eq!(locks.len(), 2);
        assert_eq!(locks[0].start, 10);
        assert_eq!(locks[0].end, 11);
        assert_eq!(locks[1].start, 16);
        assert_eq!(locks[1].end, 20);
    }

    #[test]
    fn test_partial_unlock_without_overlap_does_not_mark_changed() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        assert!(meta
            .set_lock(
                create_plock("client1", 100, LockType::WriteLock, 10, 20),
                &legacy(expire_ms),
                LocalTime::mills(),
            )
            .is_none());

        let (conflict, changed) = meta.set_lock_with_change(
            create_plock("client1", 100, LockType::UnLock, 30, 40),
            &legacy(expire_ms),
            LocalTime::mills(),
        );
        assert!(conflict.is_none());
        assert!(!changed);
        assert_eq!(meta.to_vec().len(), 1);
        assert_eq!(meta.to_vec()[0].start, 10);
        assert_eq!(meta.to_vec()[0].end, 20);
    }

    #[test]
    fn test_unlock_marks_changed_when_only_purging_expired() {
        let mut meta = LockMeta::default();
        let expire_ms = 50;

        assert!(meta
            .set_lock(
                create_plock("client1", 100, LockType::WriteLock, 0, 10),
                &legacy(expire_ms),
                LocalTime::mills(),
            )
            .is_none());
        thread::sleep(Duration::from_millis(expire_ms + 20));

        // Unlock a different owner so only expiry purge can mutate state.
        let (conflict, changed) = meta.set_lock_with_change(
            create_plock("client2", 200, LockType::UnLock, 0, u64::MAX),
            &legacy(expire_ms),
            LocalTime::mills(),
        );
        assert!(conflict.is_none());
        assert!(changed);
        assert!(meta.to_vec().is_empty());
    }

    #[test]
    fn persisted_locks_without_the_session_flag_still_load() {
        let legacy = vec![LegacyFileLock {
            client_id: "legacy-client".to_string(),
            owner_id: 44,
            pid: 55,
            acquire_time: 66,
            lock_type: LockType::WriteLock,
            lock_flags: LockFlags::Plock,
            start: 0,
            end: 100,
        }];
        let bytes = Serde::serialize(&legacy).unwrap();

        let meta = LockMeta::decode_persisted(&bytes).unwrap();

        let locks = meta.to_vec();
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].client_id, "legacy-client");
        assert_eq!(locks[0].acquire_time, 66);
        assert!(
            !locks[0].session_owned,
            "a lock persisted before #1878 belongs to the legacy TTL path"
        );
    }

    #[test]
    fn persisted_locks_keep_the_session_flag() {
        let mut lock = create_plock("c1", 1, LockType::WriteLock, 0, 10);
        lock.session_owned = true;
        let bytes = Serde::serialize(&vec![lock]).unwrap();

        let meta = LockMeta::decode_persisted(&bytes).unwrap();

        assert!(meta.to_vec()[0].session_owned);
    }

    fn plock(client: &str, owner: u64, lock_type: LockType) -> FileLock {
        create_plock(client, owner, lock_type, 0, 100)
    }

    #[test]
    fn session_owned_lock_outlives_the_legacy_ttl_while_its_owner_stays_live() {
        let mut meta = LockMeta::default();
        let validity = LockValidity::new(1_000, HashSet::from(["live-client".to_string()]), 61_000);

        let mut lock = plock("live-client", 1, LockType::WriteLock);
        lock.session_owned = true;
        assert!(meta.set_lock(lock, &validity, 1_000).is_none());

        // Ten legacy TTLs later the owner is still heartbeating: the lock must not
        // be treated as stale, and a competing writer must be rejected.
        let later = 1_000 + 10_000;
        let query = plock("other-client", 2, LockType::WriteLock);
        assert!(
            meta.find_conflict(&query, &validity, later).is_some(),
            "a live owner's lock must never be expired by the legacy TTL"
        );
        assert!(meta.set_lock(query, &validity, later).is_some());
        assert_eq!(meta.to_vec().len(), 1);
        assert_eq!(meta.to_vec()[0].client_id, "live-client");
    }

    #[test]
    fn session_owned_lock_is_reclaimed_when_its_owner_is_gone_and_grace_passed() {
        let mut meta = LockMeta::default();
        let validity = LockValidity::new(1_000, HashSet::new(), 5_000);

        let mut lock = plock("dead-client", 1, LockType::WriteLock);
        lock.session_owned = true;
        assert!(meta.set_lock(lock, &validity, 1_000).is_none());

        let query = plock("other-client", 2, LockType::WriteLock);
        assert!(
            meta.find_conflict(&query, &validity, 4_999).is_some(),
            "before the grace deadline an owner-less lock is still protected"
        );
        assert!(
            meta.find_conflict(&query, &validity, 5_000).is_none(),
            "past the grace deadline the dead owner's lock is reclaimable"
        );

        let (conflict, changed) = meta.set_lock_with_change(query, &validity, 5_000);
        assert!(conflict.is_none());
        assert!(changed, "reclaiming the stale lock must be journaled");
        let locks = meta.to_vec();
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].client_id, "other-client");
    }

    #[test]
    fn leader_change_grace_window_protects_locks_of_not_yet_reregistered_owners() {
        // After a leader change the session map is empty, so every session-owned
        // lock looks owner-less until its client re-registers.
        let mut meta = LockMeta::default();
        let validity = LockValidity::new(1_000, HashSet::new(), 121_000);

        let mut lock = plock("re-registering-client", 1, LockType::WriteLock);
        lock.session_owned = true;
        assert!(meta.set_lock(lock, &validity, 1_000).is_none());

        let query = plock("other-client", 2, LockType::WriteLock);
        assert!(
            meta.find_conflict(&query, &validity, 120_999).is_some(),
            "a failover must not hand a healthy client's lock to someone else"
        );
        assert!(meta.find_conflict(&query, &validity, 121_000).is_none());
    }

    #[test]
    fn legacy_lock_keeps_the_ttl_behaviour() {
        let mut meta = LockMeta::default();
        let validity = LockValidity::new(1_000, HashSet::new(), u64::MAX);

        let lock = plock("legacy-client", 1, LockType::WriteLock);
        assert!(meta.set_lock(lock, &validity, 1_000).is_none());

        let query = plock("other-client", 2, LockType::WriteLock);
        assert!(meta.find_conflict(&query, &validity, 2_000).is_some());
        // Strictly greater than the TTL, exactly as before #1879.
        assert!(meta.find_conflict(&query, &validity, 2_001).is_none());
    }

    #[test]
    fn find_conflict_does_not_reclaim_anything() {
        let mut meta = LockMeta::default();
        let validity = LockValidity::legacy_only(1_000);

        let lock = plock("dead-client", 1, LockType::WriteLock);
        assert!(meta.set_lock(lock, &validity, 1_000).is_none());

        let query = plock("other-client", 2, LockType::WriteLock);
        assert!(
            meta.find_conflict(&query, &validity, 10_000).is_none(),
            "an expired lock must not be reported as a conflict"
        );
        assert_eq!(
            meta.to_vec().len(),
            1,
            "F_GETLK must not reclaim: the read path has to stay free of journals"
        );

        // The mutating path is what reclaims, and it reports the change so the
        // caller can persist and journal it.
        let (conflict, changed) = meta.set_lock_with_change(query, &validity, 10_000);
        assert!(conflict.is_none());
        assert!(changed);
        assert_eq!(meta.to_vec()[0].client_id, "other-client");
    }
}
