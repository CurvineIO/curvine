//  Copyright 2025 OPPO.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::FuseResult;
use curvine_runtime::common::{FastHashMap, LocalTime};
use curvine_runtime::sync::channel::CallSender;
use std::collections::VecDeque;
use std::mem;
use std::time::Duration;

#[repr(i8)]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone)]
pub enum Operator {
    Write = 1,
    Complete = 3,
}

impl Operator {
    pub fn is_write(&self) -> bool {
        matches!(self, Operator::Write)
    }

    pub fn is_complete(&self) -> bool {
        matches!(self, Operator::Complete)
    }
}

pub enum DirtyMessage {
    Write(u64),
    Complete(u64),
    CommitAll(bool, CallSender<FuseResult<()>>),
}

impl DirtyMessage {
    pub fn new_write(ino: u64) -> Self {
        DirtyMessage::Write(ino)
    }

    pub fn new_complete(ino: u64) -> Self {
        DirtyMessage::Complete(ino)
    }
}

#[derive(Debug, Clone)]
pub struct DirtyEntry {
    pub ino: u64,
    pub created_at: u64,
    pub complete_num: usize,
}

pub struct DirtyList {
    writes: FastHashMap<u64, DirtyEntry>,
    flush_delay_ms: u64,
}

impl DirtyList {
    pub fn new(flush_delay: Duration) -> Self {
        Self {
            writes: FastHashMap::new(),
            flush_delay_ms: flush_delay.as_millis() as u64,
        }
    }

    pub fn insert(&mut self, ino: u64, operator: Operator) {
        let complete = operator.is_complete();
        self.writes
            .entry(ino)
            .and_modify(|e| {
                if complete {
                    e.complete_num += 1;
                }
            })
            .or_insert_with(|| {
                let created_at = LocalTime::mills();
                DirtyEntry {
                    ino,
                    created_at,
                    complete_num: if complete { 1 } else { 0 },
                }
            });
    }

    pub fn len(&self) -> usize {
        self.writes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn pop_ready(&mut self, max: usize) -> VecDeque<DirtyEntry> {
        if max == 0 || self.writes.is_empty() {
            return VecDeque::new();
        }

        let now_ms = LocalTime::mills();
        let fd = self.flush_delay_ms;
        let ready_keys: Vec<u64> = self
            .writes
            .iter()
            .filter(|(_, e)| e.created_at + fd <= now_ms)
            .map(|(ino, _)| *ino)
            .take(max)
            .collect();

        let mut out = VecDeque::with_capacity(ready_keys.len());
        for ino in ready_keys {
            if let Some(e) = self.writes.remove(&ino) {
                out.push_back(e);
            }
        }

        out
    }

    pub fn pop_all(&mut self) -> VecDeque<DirtyEntry> {
        mem::take(&mut self.writes).take().into_values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn list(delay_ms: u64) -> DirtyList {
        DirtyList::new(Duration::from_millis(delay_ms))
    }

    #[test]
    fn new_list_is_empty() {
        let mut list = list(0);
        assert_eq!(list.len(), 0);
        assert!(list.is_empty());
        assert!(list.pop_ready(10).is_empty());
        assert!(list.pop_all().is_empty());
    }

    #[test]
    fn insert_write_creates_entry() {
        let mut list = list(0);
        list.insert(1, Operator::Write);
        assert_eq!(list.len(), 1);
        let entry = list.pop_all().pop_front().unwrap();
        assert_eq!(entry.ino, 1);
        assert_eq!(entry.complete_num, 0);
        assert!(entry.created_at > 0);
    }

    #[test]
    fn insert_complete_creates_entry_with_count_one() {
        let mut list = list(0);
        list.insert(1, Operator::Complete);
        let entry = list.pop_all().pop_front().unwrap();
        assert_eq!(entry.ino, 1);
        assert_eq!(entry.complete_num, 1);
    }

    #[test]
    fn insert_write_twice_keeps_single_entry() {
        let mut list = list(0);
        list.insert(1, Operator::Write);
        list.insert(1, Operator::Write);
        assert_eq!(list.len(), 1);
        let entry = list.pop_all().pop_front().unwrap();
        assert_eq!(entry.complete_num, 0);
    }

    #[test]
    fn insert_complete_increments_existing_count() {
        let mut list = list(0);
        list.insert(1, Operator::Write);
        list.insert(1, Operator::Complete);
        list.insert(1, Operator::Complete);
        let entry = list.pop_all().pop_front().unwrap();
        assert_eq!(entry.complete_num, 2);
    }

    #[test]
    fn insert_write_after_complete_keeps_count() {
        let mut list = list(0);
        list.insert(1, Operator::Complete);
        list.insert(1, Operator::Write);
        let entry = list.pop_all().pop_front().unwrap();
        assert_eq!(entry.complete_num, 1);
    }

    #[test]
    fn multiple_inos_tracked_independently() {
        let mut list = list(0);
        list.insert(1, Operator::Complete);
        list.insert(2, Operator::Write);
        list.insert(3, Operator::Complete);
        assert_eq!(list.len(), 3);
        let all = list.pop_all();
        let mut counts = all
            .iter()
            .map(|e| (e.ino, e.complete_num))
            .collect::<Vec<_>>();
        counts.sort();
        assert_eq!(counts, vec![(1, 1), (2, 0), (3, 1)]);
    }

    #[test]
    fn pop_ready_zero_max_returns_empty() {
        let mut list = list(0);
        list.insert(1, Operator::Write);
        assert!(list.pop_ready(0).is_empty());
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn pop_ready_with_zero_delay_returns_immediately() {
        let mut list = list(0);
        for ino in 1..=5 {
            list.insert(ino, Operator::Write);
        }
        let ready = list.pop_ready(10);
        assert_eq!(ready.len(), 5);
        assert!(list.is_empty());
    }

    #[test]
    fn pop_ready_limits_by_max() {
        let mut list = list(0);
        for ino in 1..=5 {
            list.insert(ino, Operator::Write);
        }
        let ready = list.pop_ready(2);
        assert_eq!(ready.len(), 2);
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn pop_ready_respects_flush_delay() {
        let mut list = list(100);
        list.insert(1, Operator::Write);
        assert!(list.pop_ready(10).is_empty());

        std::thread::sleep(Duration::from_millis(150));
        let ready = list.pop_ready(10);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].ino, 1);
    }

    #[test]
    fn pop_ready_only_removes_ready_entries() {
        let mut list = list(100);
        list.insert(1, Operator::Write);
        std::thread::sleep(Duration::from_millis(150));
        list.insert(2, Operator::Write);

        let ready = list.pop_ready(10);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].ino, 1);
        assert_eq!(list.len(), 1);
        assert_eq!(list.pop_all().pop_front().unwrap().ino, 2);
    }

    #[test]
    fn pop_all_returns_everything_and_empties() {
        let mut list = list(0);
        list.insert(1, Operator::Write);
        list.insert(2, Operator::Complete);
        list.insert(3, Operator::Write);
        let all = list.pop_all();
        assert_eq!(all.len(), 3);
        assert!(list.is_empty());
        assert!(list.pop_all().is_empty());
    }

    #[test]
    fn operator_helper_methods() {
        assert!(Operator::Write.is_write());
        assert!(!Operator::Write.is_complete());
        assert!(Operator::Complete.is_complete());
        assert!(!Operator::Complete.is_write());
    }

    #[test]
    fn dirty_message_constructors() {
        assert!(matches!(DirtyMessage::new_write(1), DirtyMessage::Write(1)));
        assert!(matches!(
            DirtyMessage::new_complete(2),
            DirtyMessage::Complete(2)
        ));
    }
}
