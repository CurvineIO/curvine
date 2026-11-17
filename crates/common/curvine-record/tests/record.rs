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

#[test]
fn layout_rejects_an_unsupported_width() {
    // Widths are validated only by new(); the fields are private so a struct
    // literal cannot build a layout that would hit unreachable! while framing.
    let layout = Layout::new(2, 4);
    assert_eq!(layout.key_len_width(), 2);
    assert_eq!(layout.value_len_width(), 4);
    assert_eq!(layout.header_len(), 6);
}

#[test]
#[should_panic(expected = "key length width must be")]
fn layout_rejects_a_three_byte_key_width() {
    let _ = Layout::new(3, 1);
}

#[test]
fn insert_with_writes_every_value_byte() {
    let mut area = new_area();
    for key in 0..500u64 {
        let kb = key.to_be_bytes();
        let outcome = area.insert_with(&kb, 16, |dst| {
            dst[..4].copy_from_slice(&(key as u32).to_le_bytes());
            dst[4..12].copy_from_slice(&key.to_be_bytes());
            dst[12..].fill((key % 251) as u8);
        });
        assert_eq!(outcome, InsertOutcome::Inserted);
    }
    assert!(area.chunk_count() > 1, "expected the run to span chunks");

    for key in 0..500u64 {
        let value = read_payload(&area, key).expect("value present");
        assert_eq!(value.len(), 16);
        assert_eq!(&value[..4], (key as u32).to_le_bytes().as_slice());
        assert_eq!(&value[4..12], key.to_be_bytes().as_slice());
        assert!(value[12..].iter().all(|&b| b == (key % 251) as u8));
    }

    let mut seen = Vec::new();
    walk(&area, |buf, off| seen.push(stored_key(buf, off)));
    assert_eq!(seen, (0..500u64).collect::<Vec<_>>());
}

#[test]
fn insert_with_underfill_leaves_zeroes_not_stale_bytes() {
    // The slot between two existing records still holds the bytes of the record
    // that was shifted right; zeroing it keeps a partial write from reading back
    // as the neighbour's value.
    let mut area = Record::with_chunk_size(Layout::new(1, 1), 64);
    assert_eq!(area.insert(b"a", b"AAAAAAAA"), InsertOutcome::Inserted);
    assert_eq!(area.insert(b"c", b"CCCCCCCC"), InsertOutcome::Inserted);

    assert_eq!(
        area.insert_with(b"b", 8, |dst| dst[0] = b'B'),
        InsertOutcome::Inserted
    );

    assert_eq!(area.get(b"b"), Some(b"B\0\0\0\0\0\0\0".as_slice()));
    assert_eq!(area.get(b"a"), Some(b"AAAAAAAA".as_slice()));
    assert_eq!(area.get(b"c"), Some(b"CCCCCCCC".as_slice()));
}

#[test]
fn insert_with_duplicate_never_runs_the_closure() {
    let mut area = new_area();
    assert_eq!(insert(&mut area, 7, b"payload"), InsertOutcome::Inserted);

    let mut called = false;
    assert_eq!(
        area.insert_with(&7u64.to_be_bytes(), 7, |_| called = true),
        InsertOutcome::Duplicate
    );
    assert!(!called, "a duplicate key must not touch the value slot");
    assert_eq!(read_payload(&area, 7), Some(b"payload".as_slice()));
}

/// Stores `key` through `insert_with` while writing only the first four bytes of
/// a five byte value slot, so a record that crosses a grow or a split still has
/// to read back zero filled rather than holding whatever it was shifted onto.
fn insert_partial(area: &mut Record, oracle: &mut BTreeMap<u64, Vec<u8>>, key: u64) {
    let head = (key as u32).to_le_bytes();
    let key_bytes = key.to_be_bytes();
    assert_eq!(
        area.insert_with(&key_bytes, 5, move |dst| dst[..4].copy_from_slice(&head)),
        InsertOutcome::Inserted
    );

    let mut payload = head.to_vec();
    payload.push(0);
    oracle.insert(key, payload);
}

#[test]
fn insert_with_locates_records_across_grow_and_split() {
    // 16-byte records in a 128-byte chunk: the first chunk starts at CHUNK_MIN and
    // has to grow before it can hold eight of them, and filling it with even keys
    // before inserting odd ones forces a split. insert_with must land in the right
    // slot and keep its own bytes after a reallocation and after a split.
    let record_len = LAYOUT.record_len_of(8, 5);
    assert_eq!(record_len, 16);

    let mut area = Record::with_chunk_size(LAYOUT, 128);
    let mut oracle: BTreeMap<u64, Vec<u8>> = BTreeMap::new();

    // Ascending even keys fill the single chunk, which grows from CHUNK_MIN.
    for key in (0..16u64).step_by(2) {
        insert_partial(&mut area, &mut oracle, key);
    }
    assert_eq!(area.chunk_count(), 1);
    assert_eq!(area.chunks()[0].used(), 8 * record_len);
    assert_eq!(
        area.allocated_bytes(),
        128,
        "the first chunk must have grown from CHUNK_MIN up to the chunk size"
    );

    // An odd key sorts into the middle of a full chunk, which only a split can
    // serve: the append fast path needs the insert point at the very end.
    insert_partial(&mut area, &mut oracle, 1);
    assert_eq!(area.chunk_count(), 2);
    for (ci, chunk) in area.chunks().iter().enumerate() {
        assert!(
            chunk.used() > record_len,
            "split left chunk {} holding only {} bytes, which is the append path",
            ci,
            chunk.used()
        );
    }

    // The remaining odd keys keep landing inside halves that split and grow again.
    for key in (3..16u64).step_by(2) {
        insert_partial(&mut area, &mut oracle, key);
    }

    assert_eq!(area.len(), oracle.len());
    for (key, payload) in &oracle {
        assert_eq!(read_payload(&area, *key), Some(payload.as_slice()));
    }

    let mut seen = Vec::new();
    walk(&area, |buf, off| seen.push(stored_key(buf, off)));
    let expected: Vec<u64> = oracle.keys().copied().collect();
    assert_eq!(
        seen, expected,
        "records must stay globally sorted across every chunk"
    );
}

#[test]
fn ascending_append_keeps_chunks_near_capacity() {
    // Appending past the end of the last chunk must start a new chunk instead of
    // splitting the tail in half, otherwise a sorted load keeps twice as many
    // half-full chunks as the data needs.
    let mut area = new_area();
    let payload = b"payload";
    for key in 0..2_000u64 {
        assert_eq!(insert(&mut area, key, payload), InsertOutcome::Inserted);
    }

    let record_len = LAYOUT.record_len_of(8, payload.len());
    let total = 2_000 * record_len;
    let chunk_size = area.chunk_size();
    let optimal = total.div_ceil(chunk_size);
    assert!(
        area.chunk_count() <= optimal + 1,
        "{} chunks for {} bytes is far above the {} chunk optimum",
        area.chunk_count(),
        total,
        optimal
    );

    for chunk in &area.chunks()[..area.chunk_count() - 1] {
        assert!(
            chunk.used() + record_len > chunk_size,
            "a non-tail chunk kept only {} of {} bytes",
            chunk.used(),
            chunk_size
        );
    }

    let mut seen = Vec::new();
    walk(&area, |buf, off| seen.push(stored_key(buf, off)));
    assert_eq!(seen, (0..2_000u64).collect::<Vec<_>>());
}

#[test]
fn allocated_bytes_reports_every_chunk() {
    let mut area = new_area();
    assert_eq!(area.allocated_bytes(), 0);

    for key in 0..2_000u64 {
        assert_eq!(insert(&mut area, key, b"payload"), InsertOutcome::Inserted);
    }

    let used: usize = area.chunks().iter().map(|chunk| chunk.used()).sum();
    assert_eq!(used, 2_000 * LAYOUT.record_len_of(8, 7));
    assert!(area.allocated_bytes() >= used);
    assert!(
        area.allocated_bytes() < 2 * used,
        "the capacity ladder wasted {} bytes over {} used",
        area.allocated_bytes(),
        used
    );

    for key in 0..2_000u64 {
        let kb = key.to_be_bytes();
        let (ci, off) = area.find(&kb).expect("key present");
        area.remove(ci, off);
    }
    assert_eq!(area.allocated_bytes(), 0);
}

#[test]
#[should_panic(expected = "not a record boundary")]
fn remove_rejects_a_misaligned_offset() {
    // A bare (chunk, offset) pair carries no proof that it points at a record;
    // without the boundary walk an off-by-one silently truncates the chunk.
    let mut area = new_area();
    for key in 0..5u64 {
        assert_eq!(insert(&mut area, key, b"payload"), InsertOutcome::Inserted);
    }
    let (ci, off) = area.find(&3u64.to_be_bytes()).expect("key present");
    area.remove(ci, off + 1);
}

#[test]
#[should_panic(expected = "out of range")]
fn remove_rejects_an_unknown_chunk() {
    let mut area = new_area();
    assert_eq!(insert(&mut area, 1, b"payload"), InsertOutcome::Inserted);
    area.remove(1, 0);
}

#[test]
fn a_clone_keeps_its_bytes_while_the_original_is_written() {
    // Chunks are shared between clones and copied on first write: a reader that
    // holds the clone must never observe a later insert, remove or rewrite.
    let mut area = new_area();
    for key in 0..200u64 {
        assert_eq!(insert(&mut area, key, b"before"), InsertOutcome::Inserted);
    }
    let frozen = area.clone();

    assert_eq!(insert(&mut area, 1_000, b"after"), InsertOutcome::Inserted);
    let (ci, off) = area.find(&7u64.to_be_bytes()).expect("key present");
    area.remove(ci, off);
    area.rewrite(|value| value.fill(b'x'));

    assert_eq!(frozen.len(), 200);
    assert_eq!(read_payload(&frozen, 7), Some(&b"before"[..]));
    assert_eq!(read_payload(&frozen, 1_000), None);
    assert_eq!(read_payload(&area, 7), None);
    assert_eq!(read_payload(&area, 1_000), Some(&b"xxxxx"[..]));
}
