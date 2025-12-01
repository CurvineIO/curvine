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

use curvine_server::master::quota::eviction::evictor::{Evictor, LRUEvictor};
use curvine_server::master::quota::eviction::types::{EvictionConf, EvictionMode, EvictionPolicy};
use orpc::CommonResult;

// ============================================================================
// Helper Functions
// ============================================================================

fn create_test_eviction_conf() -> EvictionConf {
    EvictionConf {
        enable_quota_eviction: true,
        eviction_mode: EvictionMode::DeleteFile,
        policy: EvictionPolicy::Lru,
        high_watermark: 0.8,
        low_watermark: 0.6,
        candidate_scan_page: 10,
        dry_run: false,
        capacity: 1_000_000,
    }
}

/// Calculate eviction plan based on watermark logic.
/// Returns (should_trigger, target_free_bytes)
fn calculate_evict_plan(
    used: i64,
    quota: i64,
    high_watermark: f64,
    low_watermark: f64,
) -> (bool, i64) {
    if quota <= 0 || used <= 0 {
        return (false, 0);
    }

    let usage_ratio = used as f64 / quota as f64;
    if usage_ratio < high_watermark {
        return (false, 0);
    }

    let target_ratio = low_watermark.min(high_watermark).min(1.0);
    let target_used = (target_ratio * quota as f64) as i64;
    let target_free_bytes = (used - target_used).max(0);

    (true, target_free_bytes)
}

// ============================================================================
// Unit Tests: LRUEvictor
// ============================================================================

#[test]
fn test_lru_order_basic() -> CommonResult<()> {
    // Verify LRU order: oldest accessed files should be evicted first
    let evictor = LRUEvictor::new(create_test_eviction_conf());

    // Access files in order: 1, 2, 3, 4, 5
    for i in 1..=5 {
        evictor.on_access(i);
    }

    // Select 2 victims - should be oldest: 1, 2
    let victims = evictor.select_victims(2);
    assert_eq!(victims, vec![1, 2], "Should select oldest accessed files");

    Ok(())
}

#[test]
fn test_lru_order_with_reaccess() -> CommonResult<()> {
    // Verify that re-accessing a file moves it to "recently used"
    let evictor = LRUEvictor::new(create_test_eviction_conf());

    // Access files: 1, 2, 3, 4, 5
    for i in 1..=5 {
        evictor.on_access(i);
    }

    // Re-access file 1 and 2 (now they become "recent")
    evictor.on_access(1);
    evictor.on_access(2);

    // LRU order now: 3, 4, 5, 1, 2 (oldest to newest)
    // Select 3 victims - should be: 3, 4, 5
    let victims = evictor.select_victims(3);
    assert_eq!(
        victims,
        vec![3, 4, 5],
        "Re-accessed files should not be victims"
    );

    Ok(())
}

#[test]
fn test_lru_remove_victims() -> CommonResult<()> {
    // Verify that removed victims are no longer selected
    let evictor = LRUEvictor::new(create_test_eviction_conf());

    for i in 1..=5 {
        evictor.on_access(i);
    }

    // Select and remove first 2 victims
    let victims = evictor.select_victims(2);
    assert_eq!(victims, vec![1, 2]);

    evictor.remove_victims(&victims);

    // Now select again - should get 3, 4
    let new_victims = evictor.select_victims(2);
    assert_eq!(
        new_victims,
        vec![3, 4],
        "Removed victims should not appear again"
    );

    Ok(())
}

#[test]
fn test_lru_select_more_than_available() -> CommonResult<()> {
    // Verify behavior when requesting more victims than available
    let evictor = LRUEvictor::new(create_test_eviction_conf());

    evictor.on_access(1);
    evictor.on_access(2);

    // Request 10 victims but only 2 exist
    let victims = evictor.select_victims(10);
    assert_eq!(victims.len(), 2, "Should return all available, not more");
    assert_eq!(victims, vec![1, 2]);

    Ok(())
}

#[test]
fn test_lru_empty_cache() -> CommonResult<()> {
    // Verify behavior with empty cache
    let evictor = LRUEvictor::new(create_test_eviction_conf());

    let victims = evictor.select_victims(5);
    assert!(victims.is_empty(), "Empty cache should return no victims");

    Ok(())
}

#[test]
fn test_lru_disabled_eviction() -> CommonResult<()> {
    // Verify that disabled eviction doesn't track access
    let mut conf = create_test_eviction_conf();
    conf.enable_quota_eviction = false;

    let evictor = LRUEvictor::new(conf);

    // These should be ignored
    for i in 1..=5 {
        evictor.on_access(i);
    }

    let victims = evictor.select_victims(5);
    assert!(
        victims.is_empty(),
        "Disabled eviction should not track files"
    );

    Ok(())
}

// ============================================================================
// Unit Tests: Watermark Logic (Eviction Plan)
// ============================================================================

#[test]
fn test_watermark_trigger_eviction() -> CommonResult<()> {
    // Case: usage 80% with high=0.8, low=0.6 -> should trigger
    // target_free = 800 - (0.6 * 1000) = 200
    let (trigger, target_free) = calculate_evict_plan(800, 1000, 0.8, 0.6);
    assert!(trigger, "Should trigger when usage >= high_watermark");
    assert_eq!(
        target_free, 200,
        "Should free 200 bytes to reach low_watermark"
    );

    Ok(())
}

#[test]
fn test_watermark_no_trigger_below_high() -> CommonResult<()> {
    // Case: usage 70% with high=0.8 -> should NOT trigger
    let (trigger, _) = calculate_evict_plan(700, 1000, 0.8, 0.6);
    assert!(!trigger, "Should not trigger when usage < high_watermark");

    Ok(())
}

#[test]
fn test_watermark_high_usage() -> CommonResult<()> {
    // Case: usage 95% with high=0.8, low=0.6 -> should trigger
    // target_free = 950 - (0.6 * 1000) = 350
    let (trigger, target_free) = calculate_evict_plan(950, 1000, 0.8, 0.6);
    assert!(trigger, "Should trigger at 95% usage");
    assert_eq!(target_free, 350, "Should free 350 bytes");

    Ok(())
}

#[test]
fn test_watermark_full_disk() -> CommonResult<()> {
    // Case: usage 100% with high=0.8, low=0.6 -> should trigger
    // target_free = 1000 - (0.6 * 1000) = 400
    let (trigger, target_free) = calculate_evict_plan(1000, 1000, 0.8, 0.6);
    assert!(trigger, "Should trigger at 100% usage");
    assert_eq!(target_free, 400, "Should free 400 bytes");

    Ok(())
}

#[test]
fn test_watermark_zero_quota() -> CommonResult<()> {
    // Case: quota = 0 -> should NOT trigger (avoid division by zero)
    let (trigger, _) = calculate_evict_plan(100, 0, 0.8, 0.6);
    assert!(!trigger, "Should not trigger with zero quota");

    Ok(())
}

#[test]
fn test_watermark_zero_used() -> CommonResult<()> {
    // Case: used = 0 -> should NOT trigger
    let (trigger, _) = calculate_evict_plan(0, 1000, 0.8, 0.6);
    assert!(!trigger, "Should not trigger with zero usage");

    Ok(())
}

#[test]
fn test_watermark_exact_threshold() -> CommonResult<()> {
    // Case: usage exactly at high_watermark (80%)
    // target_free = 800 - (0.6 * 1000) = 200
    let (trigger, target_free) = calculate_evict_plan(800, 1000, 0.8, 0.6);
    assert!(trigger, "Should trigger at exact high_watermark");
    assert_eq!(target_free, 200);

    Ok(())
}

#[test]
fn test_watermark_just_below_threshold() -> CommonResult<()> {
    // Case: usage just below high_watermark (79.9%)
    let (trigger, _) = calculate_evict_plan(799, 1000, 0.8, 0.6);
    assert!(!trigger, "Should not trigger just below high_watermark");

    Ok(())
}

#[test]
fn test_watermark_narrow_gap() -> CommonResult<()> {
    // Case: high=0.9, low=0.85 (narrow gap)
    // usage 90%, target_free = 900 - (0.85 * 1000) = 50
    let (trigger, target_free) = calculate_evict_plan(900, 1000, 0.9, 0.85);
    assert!(trigger, "Should trigger with narrow gap");
    assert_eq!(target_free, 50, "Should free only 50 bytes with narrow gap");

    Ok(())
}

#[test]
fn test_watermark_large_scale() -> CommonResult<()> {
    // Case: 100GB disk, 80GB used (80%), high=0.8, low=0.6
    // target_free = 80GB - (0.6 * 100GB) = 20GB
    let gb = 1024 * 1024 * 1024_i64; // 1GB in bytes
    let quota = 100 * gb;
    let used = 80 * gb;
    let (trigger, target_free) = calculate_evict_plan(used, quota, 0.8, 0.6);
    assert!(trigger, "Should trigger on large disk");

    let expected_free = 20 * gb; // 20GB
                                 // Allow 1% tolerance for floating point
    let tolerance = gb;
    assert!(
        (target_free - expected_free).abs() < tolerance,
        "Should free ~20GB, got {} bytes",
        target_free
    );

    Ok(())
}
