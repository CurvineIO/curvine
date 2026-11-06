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

use curvine_master::master::meta::inode::ttl::{TtlBucket, TtlBucketList};
use curvine_master::master::meta::inode::{InodeFile, InodeView};
use curvine_model::{CreateFileOpts, TtlAction};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

#[test]
fn concurrent_add_and_reinsert_survive_bucket_drain() {
    const PRODUCERS: usize = 4;
    const PER_PRODUCER: usize = 2000;
    let list = Arc::new(TtlBucketList::new(1000).unwrap());
    let completed = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(Barrier::new(PRODUCERS + 1));
    let producers: Vec<_> = (0..PRODUCERS)
        .map(|producer| {
            let list = list.clone();
            let start = start.clone();
            let completed = completed.clone();
            std::thread::spawn(move || {
                let mut opts = CreateFileOpts::with_create(false);
                opts.storage_policy.ttl_ms = 1;
                opts.storage_policy.ttl_action = TtlAction::Delete;
                start.wait();
                for i in 0..PER_PRODUCER {
                    let id = (producer * PER_PRODUCER + i + 1) as i64;
                    if i % 2 == 0 {
                        let inode = InodeView::new_file(
                            "x".to_string(),
                            InodeFile::with_opts(id, 0, opts.clone()),
                        );
                        list.add(&inode);
                    } else {
                        let bucket = TtlBucket::new(0, 1000);
                        bucket.add(id);
                        list.put_back(&bucket);
                    }
                }
                completed.fetch_add(1, Ordering::Release);
            })
        })
        .collect();
    start.wait();
    let mut seen = HashSet::new();
    while completed.load(Ordering::Acquire) != PRODUCERS {
        for bucket in list.get_expired_buckets_at(1000) {
            seen.extend(bucket.inodes.lock().iter());
        }
        std::thread::yield_now();
    }
    for producer in producers {
        producer.join().unwrap();
    }
    for bucket in list.get_expired_buckets_at(1000) {
        seen.extend(bucket.inodes.lock().iter());
    }
    assert_eq!(seen.len(), PRODUCERS * PER_PRODUCER);
}
