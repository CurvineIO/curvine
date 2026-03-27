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

#![cfg(feature = "heap-trace")]

use orpc::common::heap_trace::{
    normalize_site, normalize_stack_lines, reduce_topn, HeapTraceHotspot, OTHER_HOTSPOT_SITE_NAME,
    UNKNOWN_SITE_NAME,
};

#[test]
fn normalize_prefers_curvine_frames_for_site_name() {
    let frames = vec![
        "0: jemalloc::arena::malloc at /rustc/lib.rs:101".to_string(),
        "1: curvine_worker::cache::allocate at src/cache.rs:88".to_string(),
        "2: rocksdb::DBImpl::Write at db_impl.cc:921".to_string(),
    ];

    let site = normalize_site(&frames);

    assert_eq!(site.site_name, "curvine_worker::cache::allocate");
    assert_eq!(
        site.frames,
        vec![
            "jemalloc::arena::malloc at /rustc/lib.rs".to_string(),
            "curvine_worker::cache::allocate at src/cache.rs".to_string(),
            "rocksdb::DBImpl::Write at db_impl.cc".to_string(),
        ]
    );
    assert!(!site.stable_id.is_empty());
    assert_eq!(site.stable_id.len(), 32);
}

#[test]
fn normalize_falls_back_when_no_curvine_frame_exists() {
    let rocksdb_frames = vec![
        "0: jemalloc::arena::malloc at /rustc/lib.rs:101".to_string(),
        "1: rocksdb::DBImpl::Write at db_impl.cc:921".to_string(),
    ];
    let rocksdb_site = normalize_site(&rocksdb_frames);
    assert_eq!(rocksdb_site.site_name, "rocksdb::DBImpl::Write");

    let generic_frames = vec![
        "0: std::alloc::alloc at library/alloc.rs:44".to_string(),
        "1: tokio::runtime::block_on at runtime.rs:19".to_string(),
    ];
    let generic_site = normalize_site(&generic_frames);
    assert_eq!(generic_site.site_name, "std::alloc::alloc");

    let unknown_site = normalize_site(&Vec::new());
    assert_eq!(unknown_site.site_name, UNKNOWN_SITE_NAME);
}

#[test]
fn reduce_hotspots_with_zero_topn_returns_other_bucket() {
    let reduced = reduce_topn(
        vec![
            hotspot(99, "curvine::write", "aaa", 800),
            hotspot(4, "rocksdb::flush", "bbb", 600),
        ],
        0,
    );

    assert_eq!(reduced.len(), 1);
    assert_eq!(reduced[0].rank, 1);
    assert_eq!(reduced[0].site_name, OTHER_HOTSPOT_SITE_NAME);
    assert_eq!(reduced[0].bytes, 1400);
}

#[test]
fn normalize_stable_id_ignores_addresses_and_line_numbers() {
    let first = vec![
        "0: curvine_worker::cache::allocate at src/cache.rs:88".to_string(),
        "1: jemalloc::arena::malloc+0x1a0".to_string(),
        "2: ?? 0x7ffeefbff5c8".to_string(),
    ];
    let second = vec![
        "0: curvine_worker::cache::allocate at src/cache.rs:144".to_string(),
        "1: jemalloc::arena::malloc+0x2f0".to_string(),
        "2: ?? 0x7ffeefbffaa0".to_string(),
    ];

    let first_site = normalize_site(&first);
    let second_site = normalize_site(&second);

    assert_eq!(first_site.site_name, second_site.site_name);
    assert_eq!(first_site.frames, second_site.frames);
    assert_eq!(first_site.stable_id, second_site.stable_id);
    assert!(!first_site.frames.join("\n").contains("0x"));
    assert!(!first_site.frames.join("\n").contains(":88"));
    assert!(!second_site.frames.join("\n").contains(":144"));
}

#[test]
fn reduce_hotspots_preserves_topn_and_groups_other() {
    let reduced = reduce_topn(
        vec![
            hotspot(99, "curvine::write", "aaa", 800),
            hotspot(4, "rocksdb::flush", "bbb", 600),
            hotspot(7, "tokio::runtime", "ccc", 400),
            hotspot(11, "jemalloc::arena", "ddd", 200),
        ],
        2,
    );

    assert_eq!(reduced.len(), 3);
    assert_eq!(reduced[0].rank, 1);
    assert_eq!(reduced[0].site_name, "curvine::write");
    assert_eq!(reduced[0].bytes, 800);
    assert_eq!(reduced[1].rank, 2);
    assert_eq!(reduced[1].site_name, "rocksdb::flush");
    assert_eq!(reduced[1].bytes, 600);
    assert_eq!(reduced[2].rank, 3);
    assert_eq!(reduced[2].site_name, OTHER_HOTSPOT_SITE_NAME);
    assert_eq!(reduced[2].bytes, 600);
}

#[test]
fn normalize_stack_lines_trims_and_stabilizes_frames() {
    let normalized = normalize_stack_lines(
        "\n  0: curvine_worker::cache::allocate at src/cache.rs:88\n  1: ?? 0x7ffeefbffaa0\n",
    )
    .unwrap();

    assert_eq!(
        normalized,
        "curvine_worker::cache::allocate at src/cache.rs\n??"
    );
}

fn hotspot(rank: usize, site_name: &str, stable_id: &str, bytes: usize) -> HeapTraceHotspot {
    HeapTraceHotspot {
        rank,
        site_name: site_name.to_string(),
        stable_id: stable_id.to_string(),
        bytes,
        objects: 0,
        growth_bytes: 0,
        frames: vec![site_name.to_string()],
    }
}
