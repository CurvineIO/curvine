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

use curvine_common::state::{FileLock, LockFlags, LockType};
use orpc::common::LocalTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Default, Serialize, Deserialize)]
pub struct LockMeta {
    plock: HashMap<String, Vec<FileLock>>,
    flock: HashMap<String, FileLock>,
}

impl LockMeta {
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

    fn is_lock_expired(lock: &FileLock, expire_ms: u64) -> bool {
        if lock.acquire_time == 0 {
            return true;
        }
        let now = LocalTime::mills();
        now.saturating_sub(lock.acquire_time) > expire_ms
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

    fn check_plock_conflict(&mut self, lock: &FileLock, expire_ms: u64) -> Option<FileLock> {
        let mut conflict_lock = None;

        for owner_locks in self.plock.values_mut() {
            owner_locks.retain(|existing_lock| {
                if Self::is_lock_expired(existing_lock, expire_ms) {
                    return false;
                }

                if conflict_lock.is_none()
                    && (existing_lock.client_id != lock.client_id
                        || existing_lock.owner_id != lock.owner_id)
                {
                    let is_conflict = matches!(
                        (lock.lock_type, existing_lock.lock_type),
                        (LockType::WriteLock, _) | (_, LockType::WriteLock)
                    );

                    if is_conflict {
                        let has_overlap =
                            lock.end >= existing_lock.start && lock.start <= existing_lock.end;
                        if has_overlap {
                            conflict_lock = Some(existing_lock.clone());
                        }
                    }
                }

                true
            });
        }

        conflict_lock
    }

    fn check_flock_conflict(&mut self, lock: &FileLock, expire_ms: u64) -> Option<FileLock> {
        let mut conflict_lock = None;

        self.flock.retain(|_key, existing_lock| {
            if Self::is_lock_expired(existing_lock, expire_ms) {
                return false;
            }

            if conflict_lock.is_none()
                && (existing_lock.client_id != lock.client_id
                    || existing_lock.owner_id != lock.owner_id)
            {
                let is_conflict = matches!(
                    (lock.lock_type, existing_lock.lock_type),
                    (LockType::WriteLock, _) | (_, LockType::WriteLock)
                );

                if is_conflict {
                    conflict_lock = Some(existing_lock.clone());
                }
            }

            true
        });

        conflict_lock
    }

    /// Check if a lock conflicts with existing locks.
    ///
    /// During conflict checking, expired locks are automatically removed.
    /// An expired lock is one where (current_time - acquire_time) > expire_ms.
    ///
    /// # Arguments
    /// * `lock` - The lock to check for conflicts
    /// * `expire_ms` - Lock expiration time in milliseconds
    ///
    /// # Returns
    /// * `Some(FileLock)` - If a conflicting lock is found, returns the conflicting lock
    /// * `None` - If no conflict exists (or if lock is UnLock type)
    pub fn check_conflict(&mut self, lock: &FileLock, expire_ms: u64) -> Option<FileLock> {
        if lock.lock_type == LockType::UnLock {
            return None;
        }

        match lock.lock_flags {
            LockFlags::Plock => self.check_plock_conflict(lock, expire_ms),
            LockFlags::Flock => self.check_flock_conflict(lock, expire_ms),
        }
    }

    fn set_plock(&mut self, mut lock: FileLock, expire_ms: u64) -> Option<FileLock> {
        let key = Self::key(&lock);

        if lock.lock_type == LockType::UnLock {
            if lock.start == 0 && lock.end == u64::MAX {
                self.plock.remove(&key);
                return None;
            }

            if let Some(owner_locks) = self.plock.get_mut(&key) {
                let unlock_start = lock.start;
                let unlock_end = lock.end;

                owner_locks.retain(|existing| {
                    let overlaps = existing.end >= unlock_start && existing.start <= unlock_end;
                    !overlaps
                });

                if owner_locks.is_empty() {
                    self.plock.remove(&key);
                }
            }
            return None;
        }

        if let Some(conflict) = self.check_plock_conflict(&lock, expire_ms) {
            return Some(conflict);
        }

        lock.acquire_time = LocalTime::mills();
        self.plock.entry(key).or_default().push(lock);

        None
    }

    fn set_flock(&mut self, mut lock: FileLock, expire_ms: u64) -> Option<FileLock> {
        let key = Self::key(&lock);

        if lock.lock_type == LockType::UnLock {
            self.flock.remove(&key);
            return None;
        }

        if let Some(conflict) = self.check_flock_conflict(&lock, expire_ms) {
            return Some(conflict);
        }

        lock.acquire_time = LocalTime::mills();
        self.flock.insert(key, lock);

        None
    }

    /// Set a lock (acquire or release).
    ///
    /// This method handles both POSIX locks (Plock) and BSD locks (Flock).
    /// Before setting a new lock, expired locks are automatically cleaned up.
    ///
    /// # Arguments
    /// * `lock` - The lock to set. If lock_type is UnLock, the lock will be released.
    /// * `expire_ms` - Lock expiration time in milliseconds. Used to clean up expired locks.
    ///
    /// # Returns
    /// * `Some(FileLock)` - If setting the lock would conflict with an existing lock, returns the conflicting lock
    /// * `None` - If the lock was successfully set (or released)
    ///
    /// # Behavior
    /// * For UnLock type: Removes the specified lock(s) from the metadata
    /// * For ReadLock/WriteLock: Checks for conflicts, removes expired locks, and sets the new lock
    /// * The acquire_time of the new lock is automatically set to the current time
    pub fn set_lock(&mut self, lock: FileLock, expire_ms: u64) -> Option<FileLock> {
        match lock.lock_flags {
            LockFlags::Plock => self.set_plock(lock, expire_ms),
            LockFlags::Flock => self.set_flock(lock, expire_ms),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

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
        }
    }

    #[test]
    fn test_set_lock_plock_read() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_plock("client1", 100, LockType::ReadLock, 0, 100);
        let result = meta.set_lock(lock1.clone(), expire_ms);
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
        let result = meta.set_lock(lock1.clone(), expire_ms);
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
        let result = meta.set_lock(lock1.clone(), expire_ms);
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
        let result = meta.set_lock(lock1.clone(), expire_ms);
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
        assert!(meta.set_lock(lock1, expire_ms).is_none());

        let lock2 = create_plock("client2", 200, LockType::WriteLock, 50, 150);
        let conflict = meta.set_lock(lock2, expire_ms);
        assert!(conflict.is_some(), "should detect conflict");
        assert_eq!(conflict.unwrap().client_id, "client1");
    }

    #[test]
    fn test_set_lock_conflict_flock() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_flock("client1", 100, LockType::WriteLock);
        assert!(meta.set_lock(lock1, expire_ms).is_none());

        let lock2 = create_flock("client2", 200, LockType::WriteLock);
        let conflict = meta.set_lock(lock2, expire_ms);
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

        thread::sleep(Duration::from_millis(100));

        let new_lock = create_plock("client2", 200, LockType::WriteLock, 0, 100);
        let conflict = meta.check_conflict(&new_lock, expire_ms);
        assert!(conflict.is_none(), "expired lock should not conflict");

        let locks_after = meta.to_vec();
        assert_eq!(locks_after.len(), 0, "expired lock should be removed");
    }

    #[test]
    fn test_check_conflict_flock_expired() {
        let expire_ms = 3000;

        let mut expired_lock = create_flock("client1", 100, LockType::WriteLock);
        expired_lock.acquire_time = LocalTime::mills().saturating_sub(5000);
        let mut meta = LockMeta::with_vec(vec![expired_lock]);

        let locks_before = meta.to_vec();
        assert_eq!(locks_before.len(), 1);

        thread::sleep(Duration::from_millis(100));

        let new_lock = create_flock("client2", 200, LockType::WriteLock);
        let conflict = meta.check_conflict(&new_lock, expire_ms);
        assert!(conflict.is_none(), "expired lock should not conflict");

        let locks_after = meta.to_vec();
        assert_eq!(locks_after.len(), 0, "expired lock should be removed");
    }

    #[test]
    fn test_check_conflict_plock_not_expired() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_plock("client1", 100, LockType::WriteLock, 0, 100);
        assert!(meta.set_lock(lock1, expire_ms).is_none());

        let lock2 = create_plock("client2", 200, LockType::WriteLock, 50, 150);
        let conflict = meta.check_conflict(&lock2, expire_ms);
        assert!(conflict.is_some(), "non-expired lock should conflict");
        assert_eq!(conflict.unwrap().client_id, "client1");
    }

    #[test]
    fn test_check_conflict_flock_not_expired() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_flock("client1", 100, LockType::WriteLock);
        assert!(meta.set_lock(lock1, expire_ms).is_none());

        let lock2 = create_flock("client2", 200, LockType::WriteLock);
        let conflict = meta.check_conflict(&lock2, expire_ms);
        assert!(conflict.is_some(), "non-expired lock should conflict");
        assert_eq!(conflict.unwrap().client_id, "client1");
    }

    #[test]
    fn test_check_conflict_plock_read_read() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_plock("client1", 100, LockType::ReadLock, 0, 100);
        assert!(meta.set_lock(lock1, expire_ms).is_none());

        let lock2 = create_plock("client2", 200, LockType::ReadLock, 50, 150);
        let conflict = meta.check_conflict(&lock2, expire_ms);
        assert!(conflict.is_none(), "read locks should not conflict");
    }

    #[test]
    fn test_check_conflict_flock_read_read() {
        let mut meta = LockMeta::default();
        let expire_ms = 3000;

        let lock1 = create_flock("client1", 100, LockType::ReadLock);
        assert!(meta.set_lock(lock1, expire_ms).is_none());

        let lock2 = create_flock("client2", 200, LockType::ReadLock);
        let conflict = meta.check_conflict(&lock2, expire_ms);
        assert!(conflict.is_none(), "read locks should not conflict");
    }
}
