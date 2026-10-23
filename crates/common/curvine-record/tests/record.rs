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

use curvine_record::{InsertOutcome, Layout, Record};
use std::collections::BTreeMap;

/// 1-byte key length, 2-byte value length: keys are u64 big-endian, values can
/// outgrow a single byte so large-record splits stay covered.
const LAYOUT: Layout = Layout::new(1, 2);

fn new_area() -> Record {
    Record::new(LAYOUT)
}

fn insert(area: &mut Record, key: u64, payload: &[u8]) -> InsertOutcome {
    area.insert(&key.to_be_bytes(), payload)
}

fn read_payload(area: &Record, key: u64) -> Option<&[u8]> {
    area.get(&key.to_be_bytes())
}

fn stored_key(buf: &[u8], off: usize) -> u64 {
    u64::from_be_bytes(buf[LAYOUT.key_range(buf, off)].try_into().unwrap())
}

fn walk(area: &Record, mut f: impl FnMut(&[u8], usize)) {
    for chunk in area.chunks() {
        let used = chunk.used();
        let mut off = 0usize;
        while off < used {
            f(chunk.buf(), off);
            off += LAYOUT.record_len(chunk.buf(), off);
        }
    }
}

#[test]
fn empty_area_misses_and_starts_at_origin() {
    let area = new_area();
    assert_eq!(area.find(b"anything"), Err((0, 0)));
    assert_eq!(area.get(b"anything"), None);
    assert_eq!(area.len(), 0);
    assert!(area.is_empty());
    assert_eq!(area.chunk_count(), 0);
}

#[test]
fn unordered_insert_matches_btree_semantics() {
    let mut area = new_area();
    let mut oracle: BTreeMap<u64, Vec<u8>> = BTreeMap::new();

    for i in 0..1_000u64 {
        let key = (i * 7919) % 1_000;
        let payload = format!("v{}", key).into_bytes();
        assert_eq!(insert(&mut area, key, &payload), InsertOutcome::Inserted);
        oracle.insert(key, payload);
    }

    assert_eq!(area.len(), oracle.len());
    for (key, payload) in &oracle {
        assert_eq!(read_payload(&area, *key), Some(payload.as_slice()));
    }

    // Duplicate keys are rejected without changing the area.
    let len_before = area.len();
    let chunks_before = area.chunk_count();
    assert_eq!(insert(&mut area, 42, b"dup"), InsertOutcome::Duplicate);
    assert_eq!(area.len(), len_before);
    assert_eq!(area.chunk_count(), chunks_before);
}

#[test]
fn variable_length_records_split_and_iterate_in_order() {
    let mut area = new_area();
    let mut expected = Vec::new();

    for key in 0..5_000u64 {
        let payload = vec![(key % 251) as u8; (key % 17) as usize];
        expected.push(key);
        assert_eq!(insert(&mut area, key, &payload), InsertOutcome::Inserted);
    }
    assert!(area.chunk_count() > 1, "expected chunk splits");

    let mut seen = Vec::new();
    walk(&area, |buf, off| seen.push(stored_key(buf, off)));
    assert_eq!(seen, expected);

    for key in (0..5_000u64).step_by(37) {
        let payload = vec![(key % 251) as u8; (key % 17) as usize];
        assert_eq!(read_payload(&area, key), Some(payload.as_slice()));
    }
}

#[test]
fn remove_and_rewrite_keep_records_consistent() {
    let mut area = new_area();

    for key in 0..2_000u64 {
        let payload = (key as u32).to_le_bytes();
        let _ = insert(&mut area, key, &payload);
    }

    for key in (0..2_000u64).step_by(2) {
        let kb = key.to_be_bytes();
        let (ci, off) = area.find(&kb).expect("even key present");
        area.remove(ci, off);
    }
    assert_eq!(area.len(), 1_000);

    area.rewrite(|value| {
        for byte in value {
            *byte ^= 0xff;
        }
    });

    for key in (0..2_000u64).step_by(2) {
        assert_eq!(read_payload(&area, key), None);
    }
    for key in (1..2_000u64).step_by(2) {
        let expected = (key as u32).to_le_bytes().map(|b| !b);
        assert_eq!(read_payload(&area, key), Some(expected.as_slice()));
    }
}

#[test]
fn large_records_out_of_order_keep_global_order() {
    // A record larger than half a chunk used to split off an empty tail chunk;
    // its zeroed buffer decoded as the smallest key and misordered inserts.
    let mut area = new_area();
    let big = vec![0xAB; 3_000];
    assert_eq!(insert(&mut area, 5_000, &big), InsertOutcome::Inserted);
    assert_eq!(insert(&mut area, 100, &big), InsertOutcome::Inserted);

    let mut seen = Vec::new();
    walk(&area, |buf, off| seen.push(stored_key(buf, off)));
    assert_eq!(seen, vec![100, 5_000]);
    assert_eq!(read_payload(&area, 100), Some(big.as_slice()));
    assert_eq!(read_payload(&area, 5_000), Some(big.as_slice()));
}

#[test]
fn empty_key_sorts_first_without_spinning() {
    // An empty probe key is smaller than the zeroed buffer of an empty chunk;
    // before the fix this re-split the same chunk forever.
    let big = vec![0u8; 3_000];
    let mut area = new_area();
    assert_eq!(area.insert(b"b", &big), InsertOutcome::Inserted, "insert b");
    assert_eq!(
        area.insert(b"", &big),
        InsertOutcome::Inserted,
        "insert empty key"
    );
    assert_eq!(area.insert(b"a", &big), InsertOutcome::Inserted, "insert a");

    let mut seen: Vec<Vec<u8>> = Vec::new();
    walk(&area, |buf, off| {
        seen.push(buf[LAYOUT.key_range(buf, off)].to_vec());
    });
    assert_eq!(seen, vec![b"".to_vec(), b"a".to_vec(), b"b".to_vec()]);
    assert!(area.find(b"").is_ok());
    assert!(area.find(b"a").is_ok());
    assert!(area.find(b"b").is_ok());
}

#[test]
fn custom_chunk_size_splits_and_finds() {
    // A 256-byte chunk size keeps the ladder but splits far earlier; lookup
    // must stay correct across the extra chunks.
    let mut area = Record::with_chunk_size(LAYOUT, 256);
    for key in 0..1_000u64 {
        assert_eq!(insert(&mut area, key, b"payload"), InsertOutcome::Inserted);
    }
    assert!(area.chunk_count() > 1);
    assert_eq!(area.chunk_size(), 256);

    for key in 0..1_000u64 {
        assert_eq!(read_payload(&area, key), Some(b"payload".as_slice()));
    }
    let mut seen = Vec::new();
    walk(&area, |buf, off| seen.push(stored_key(buf, off)));
    assert_eq!(seen, (0..1_000u64).collect::<Vec<_>>());
}

#[test]
#[should_panic(expected = "value of")]
fn value_over_width_is_rejected() {
    // A 1-byte value length cannot describe 300 bytes; the container refuses
    // instead of truncating the prefix and corrupting the record boundaries.
    let mut area = Record::new(Layout::new(1, 1));
    let _ = area.insert(b"k", &[0u8; 300]);
}

#[test]
fn randomized_insert_remove_matches_btree() {
    // xorshift64: deterministic, keeps the test dependency-free.
    let mut seed = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };

    let mut area = new_area();
    let mut oracle: BTreeMap<u64, Vec<u8>> = BTreeMap::new();

    for _ in 0..5_000 {
        let key = next() % 500;
        if next() % 3 == 0 {
            if let Ok((ci, off)) = area.find(&key.to_be_bytes()) {
                area.remove(ci, off);
                oracle.remove(&key);
            }
        } else {
            let payload = vec![(key % 251) as u8; (next() % 40) as usize];
            if insert(&mut area, key, &payload) == InsertOutcome::Inserted {
                oracle.insert(key, payload);
            }
        }
    }

    assert_eq!(area.len(), oracle.len());
    for (key, payload) in &oracle {
        assert_eq!(read_payload(&area, *key), Some(payload.as_slice()));
    }

    let mut seen = Vec::new();
    walk(&area, |buf, off| seen.push(stored_key(buf, off)));
    assert_eq!(seen, oracle.keys().copied().collect::<Vec<_>>());
}

#[test]
fn chunks_shrink_back_to_empty() {
    let mut area = new_area();
    for key in 0..300u64 {
        let _ = insert(&mut area, key, b"x");
    }
    for key in 0..300u64 {
        let kb = key.to_be_bytes();
        let (ci, off) = area.find(&kb).expect("key present");
        area.remove(ci, off);
    }
    assert_eq!(area.len(), 0);
    assert_eq!(area.chunk_count(), 0);

    // The area stays usable after being drained.
    assert_eq!(insert(&mut area, 7, b"again"), InsertOutcome::Inserted);
    assert_eq!(read_payload(&area, 7), Some(b"again".as_slice()));
}
