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

use curvine_common::conf::ClientConf;
use curvine_common::fs::Path;

/// Read pattern enumeration
///
/// Used to identify file read patterns, affecting prefetch strategy:
/// - Sequential: Sequential read mode, enables prefetch to improve performance
/// - Random: Random read mode, disables prefetch to avoid resource waste
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReadPattern {
    Sequential = 1,
    Random = 2,
}

impl ReadPattern {
    pub fn is_random(&self) -> bool {
        matches!(self, ReadPattern::Random)
    }

    pub fn is_sequential(&self) -> bool {
        matches!(self, ReadPattern::Sequential)
    }
}

/// Read pattern detector
///
/// Responsible for detecting file read patterns (sequential/random) and controlling prefetch behavior based on detection results.
///
/// ## Pattern Switching Flow
///
/// ### 1. Initial State
/// - Default pattern: `Sequential` (sequential read)
/// - `seq_count = 0` (sequential read count)
/// - `last_read_pos = -1` (invalid position, indicates read not started)
///
/// ### 2. Seek Operation Triggers Pattern Switch
/// ```
/// seek(pos) → record_seek()
///   ├─ Reset state: seq_count = 0, last_read_pos = -1
///   └─ If current is Sequential → Switch to Random
///      └─ Reason: seek operation usually indicates random access, need to pause prefetch
/// ```
///
/// ### 3. Read Operation Triggers Pattern Switch
/// ```
/// read(start_pos, end_pos) → record_read()
///   ├─ Determine if sequential read:
///   │   ├─ last_read_pos == -1 (first read) → seq_count++
///   │   ├─ start_pos == last_read_pos (continuous read) → seq_count++
///   │   └─ start_pos != last_read_pos (jump read) → seq_count = 0
///   │
///   ├─ Update last_read_pos = end_pos
///   │
///   └─ Pattern switch judgment:
///       ├─ seq_count >= check_threshold → Sequential
///       │   └─ Sequential reads reach threshold, switch to sequential mode, start prefetch
///       │
///       └─ seq_count < check_threshold → Keep current pattern
///           └─ If current is Sequential but threshold not reached, still keep Sequential
///               (Note: In this design, will not switch back from Sequential to Random)
/// ```
///
/// ### 4. Impact of Pattern Switching
/// - **Sequential → Random**:
///   - Trigger: seek operation
///   - Impact: Pause prefetch task, clear prefetch buffer
///
/// - **Random → Sequential**:
///   - Trigger: Continuous `check_threshold` sequential reads
///   - Impact: Start prefetch task, begin background prefetching
///
/// ### 5. Typical Scenario Examples
///
/// **Scenario A: Pure Sequential Read**
/// ```
/// read(0, 100)   → seq_count=1,  Sequential (threshold not reached, keep)
/// read(100, 200) → seq_count=2,  Sequential (threshold not reached, keep)
/// ...
/// read(900, 1000) → seq_count=10, Sequential (threshold reached, switch!)
/// ```
///
/// **Scenario B: Seek Occurs During Sequential Read**
/// ```
/// read(0, 100)   → seq_count=1,  Sequential
/// read(100, 200) → seq_count=2,  Sequential
/// seek(1000)     → seq_count=0,  Random (immediate switch)
/// read(1000, 1100) → seq_count=1, Random (restart counting)
/// ```
///
/// **Scenario C: Mixed Read Pattern**
/// ```
/// read(0, 100)     → seq_count=1,  Sequential
/// read(100, 200)   → seq_count=2,  Sequential
/// read(500, 600)   → seq_count=0,  Sequential (jump, but pattern unchanged)
/// read(600, 700)   → seq_count=1,  Sequential (restart counting)
/// ```
///
/// ## Notes
///
/// 1. **Default Sequential**: Initial state is sequential mode, suitable for most file read scenarios
/// 2. **Threshold Design**: `check_threshold` controls sensitivity of switching from Random to Sequential
/// 3. **State Reset**: Each seek resets detection state, restarting pattern recognition
/// 4. **Unidirectional Switch**: In current implementation, read operation will not switch from Sequential back to Random
///    (Only seek operation triggers Sequential → Random)
pub struct ReadDetector {
    pub enabled: bool,
    last_read_pos: i64,
    seq_count: u64,
    check_threshold: u64,
    read_parallel: i64,
    read_pattern: ReadPattern,
}

impl ReadDetector {
    pub fn with_conf(conf: &ClientConf, file_size: i64) -> Self {
        let mut read_parallel = conf.read_parallel.max(1);
        let check_threshold = conf.sequential_read_threshold;
        if conf.enable_smart_prefetch && file_size >= conf.large_file_size {
            let calculated_parallel = (file_size + conf.large_file_size - 1) / conf.large_file_size;
            read_parallel = conf
                .max_read_parallel
                .max(1)
                .min(1.max(calculated_parallel));
        }

        Self {
            enabled: conf.enable_smart_prefetch,
            last_read_pos: -1,
            seq_count: 0,
            check_threshold,
            read_parallel,
            read_pattern: ReadPattern::Sequential,
        }
    }

    pub fn read_parallel(&self) -> i64 {
        self.read_parallel
    }

    pub fn read_pattern(&self) -> ReadPattern {
        self.read_pattern
    }

    pub fn is_random(&self) -> bool {
        self.read_pattern.is_random()
    }

    pub fn is_sequential(&self) -> bool {
        self.read_pattern.is_sequential()
    }

    pub fn seq_count(&self) -> u64 {
        self.seq_count
    }

    pub fn record_seek(&mut self, path: &Path) {
        if !self.enabled {
            return;
        }

        self.seq_count = 0;
        self.last_read_pos = -1;

        if self.read_pattern.is_sequential() {
            log::debug!(
                "file {} read pattern changed: {:?} -> {:?}",
                path,
                self.read_pattern,
                ReadPattern::Random
            );

            self.read_pattern = ReadPattern::Random;
        }
    }

    pub fn record_read(&mut self, start_pos: i64, end_pos: i64, path: &Path) -> bool {
        if !self.enabled {
            return false;
        }

        if self.last_read_pos == -1 || start_pos == self.last_read_pos {
            self.seq_count += 1;
        } else {
            self.seq_count = 0;
        }
        self.last_read_pos = end_pos;

        let read_pattern = if self.seq_count >= self.check_threshold {
            ReadPattern::Sequential
        } else {
            self.read_pattern
        };

        if self.read_pattern != read_pattern {
            log::debug!(
                "file {} read pattern changed: {:?} -> {:?}",
                path,
                self.read_pattern,
                read_pattern
            );

            self.read_pattern = read_pattern;
            true
        } else {
            false
        }
    }

    pub fn set_last_read_pos(&mut self, pos: i64) {
        self.last_read_pos = pos;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::conf::ClientConf;

    fn create_test_conf(enable_smart_prefetch: bool, threshold: u64) -> ClientConf {
        ClientConf {
            enable_smart_prefetch,
            sequential_read_threshold: threshold,
            ..Default::default()
        }
    }

    fn create_test_path() -> Path {
        Path::from_str("/test/file.txt").unwrap()
    }

    #[test]
    fn test_initial_state() {
        let conf = create_test_conf(true, 10);
        let detector = ReadDetector::with_conf(&conf, 1000);

        assert!(detector.enabled);
        assert_eq!(detector.read_pattern(), ReadPattern::Sequential);
        assert!(detector.is_sequential());
        assert!(!detector.is_random());
        assert_eq!(detector.seq_count(), 0);
        assert_eq!(detector.read_parallel(), 1);
    }

    #[test]
    fn test_disabled_detector() {
        let conf = create_test_conf(false, 10);
        let mut detector = ReadDetector::with_conf(&conf, 1000);
        let path = create_test_path();

        assert!(!detector.enabled);

        // record_seek should do nothing when disabled
        detector.record_seek(&path);
        assert_eq!(detector.read_pattern(), ReadPattern::Sequential);

        // record_read should return false when disabled
        let changed = detector.record_read(0, 100, &path);
        assert!(!changed);
        assert_eq!(detector.read_pattern(), ReadPattern::Sequential);
    }

    #[test]
    fn test_record_seek_sequential_to_random() {
        let conf = create_test_conf(true, 10);
        let mut detector = ReadDetector::with_conf(&conf, 1000);
        let path = create_test_path();

        // Initially Sequential
        assert_eq!(detector.read_pattern(), ReadPattern::Sequential);

        // Seek should switch to Random
        detector.record_seek(&path);
        assert_eq!(detector.read_pattern(), ReadPattern::Random);
        assert!(detector.is_random());
        assert_eq!(detector.seq_count(), 0);
    }

    #[test]
    fn test_record_seek_random_stays_random() {
        let conf = create_test_conf(true, 10);
        let mut detector = ReadDetector::with_conf(&conf, 1000);
        let path = create_test_path();

        // Switch to Random first
        detector.record_seek(&path);
        assert_eq!(detector.read_pattern(), ReadPattern::Random);

        // Another seek should keep Random
        detector.record_seek(&path);
        assert_eq!(detector.read_pattern(), ReadPattern::Random);
        assert_eq!(detector.seq_count(), 0);
    }

    #[test]
    fn test_record_read_first_read() {
        let conf = create_test_conf(true, 10);
        let mut detector = ReadDetector::with_conf(&conf, 1000);
        let path = create_test_path();

        // First read (last_read_pos == -1)
        let changed = detector.record_read(0, 100, &path);
        assert!(!changed); // Pattern doesn't change until threshold reached
        assert_eq!(detector.seq_count(), 1);
        assert_eq!(detector.read_pattern(), ReadPattern::Sequential);
    }

    #[test]
    fn test_record_read_sequential_reads() {
        let conf = create_test_conf(true, 3);
        let mut detector = ReadDetector::with_conf(&conf, 1000);
        let path = create_test_path();

        // Sequential reads
        assert!(!detector.record_read(0, 100, &path));
        assert_eq!(detector.seq_count(), 1);

        assert!(!detector.record_read(100, 200, &path));
        assert_eq!(detector.seq_count(), 2);

        assert!(!detector.record_read(200, 300, &path));
        assert_eq!(detector.seq_count(), 3);

        // After threshold, should still be Sequential (was already Sequential)
        assert!(!detector.record_read(300, 400, &path));
        assert_eq!(detector.seq_count(), 4);
        assert_eq!(detector.read_pattern(), ReadPattern::Sequential);
    }

    #[test]
    fn test_record_read_random_to_sequential() {
        let conf = create_test_conf(true, 3);
        let mut detector = ReadDetector::with_conf(&conf, 1000);
        let path = create_test_path();

        // Switch to Random first
        detector.record_seek(&path);
        assert_eq!(detector.read_pattern(), ReadPattern::Random);

        // Sequential reads should accumulate count
        assert!(!detector.record_read(0, 100, &path));
        assert_eq!(detector.seq_count(), 1);
        assert_eq!(detector.read_pattern(), ReadPattern::Random);

        assert!(!detector.record_read(100, 200, &path));
        assert_eq!(detector.seq_count(), 2);
        assert_eq!(detector.read_pattern(), ReadPattern::Random);

        // Third sequential read should switch to Sequential
        assert!(detector.record_read(200, 300, &path));
        assert_eq!(detector.seq_count(), 3);
        assert_eq!(detector.read_pattern(), ReadPattern::Sequential);
    }

    #[test]
    fn test_record_read_jump_read_resets_count() {
        let conf = create_test_conf(true, 10);
        let mut detector = ReadDetector::with_conf(&conf, 1000);
        let path = create_test_path();

        // Sequential reads
        detector.record_read(0, 100, &path);
        assert_eq!(detector.seq_count(), 1);

        detector.record_read(100, 200, &path);
        assert_eq!(detector.seq_count(), 2);

        // Jump read (not continuous)
        detector.record_read(500, 600, &path);
        assert_eq!(detector.seq_count(), 0);
        assert_eq!(detector.read_pattern(), ReadPattern::Sequential); // Still Sequential
    }

    #[test]
    fn test_record_read_sequential_after_jump() {
        let conf = create_test_conf(true, 3);
        let mut detector = ReadDetector::with_conf(&conf, 1000);
        let path = create_test_path();

        // Switch to Random
        detector.record_seek(&path);
        assert_eq!(detector.read_pattern(), ReadPattern::Random);

        // Jump read resets count
        detector.record_read(0, 100, &path);
        assert_eq!(detector.seq_count(), 1);

        detector.record_read(500, 600, &path); // Jump
        assert_eq!(detector.seq_count(), 0);

        // Sequential reads after jump
        detector.record_read(600, 700, &path);
        assert_eq!(detector.seq_count(), 1);

        detector.record_read(700, 800, &path);
        assert_eq!(detector.seq_count(), 2);

        detector.record_read(800, 900, &path);
        assert_eq!(detector.seq_count(), 3);
        assert_eq!(detector.read_pattern(), ReadPattern::Sequential);
    }

    #[test]
    fn test_seek_resets_state() {
        let conf = create_test_conf(true, 10);
        let mut detector = ReadDetector::with_conf(&conf, 1000);
        let path = create_test_path();

        // Build up sequential count
        detector.record_read(0, 100, &path);
        detector.record_read(100, 200, &path);
        assert_eq!(detector.seq_count(), 2);

        // Seek resets everything
        detector.record_seek(&path);
        assert_eq!(detector.seq_count(), 0);
        assert_eq!(detector.read_pattern(), ReadPattern::Random);
    }

    #[test]
    fn test_set_last_read_pos() {
        let conf = create_test_conf(true, 10);
        let mut detector = ReadDetector::with_conf(&conf, 1000);

        detector.set_last_read_pos(500);

        // Next read should be considered sequential if it starts at 500
        let path = create_test_path();
        detector.record_read(500, 600, &path);
        assert_eq!(detector.seq_count(), 1);
    }

    #[test]
    fn test_threshold_boundary() {
        let conf = create_test_conf(true, 5);
        let mut detector = ReadDetector::with_conf(&conf, 1000);
        let path = create_test_path();

        // Switch to Random
        detector.record_seek(&path);

        // Read exactly threshold - 1 times
        for i in 0..4 {
            let start = i * 100;
            let end = (i + 1) * 100;
            assert!(!detector.record_read(start, end, &path));
            assert_eq!(detector.read_pattern(), ReadPattern::Random);
        }

        // 5th read should switch to Sequential
        assert!(detector.record_read(400, 500, &path));
        assert_eq!(detector.read_pattern(), ReadPattern::Sequential);
    }

    #[test]
    fn test_large_file_parallel_calculation() {
        let mut conf = create_test_conf(true, 10);
        conf.large_file_size = 1024 * 1024 * 1024; // 1GB
        conf.max_read_parallel = 8;

        // Small file
        let detector = ReadDetector::with_conf(&conf, 100);
        assert_eq!(detector.read_parallel(), 1);

        // Large file
        let detector = ReadDetector::with_conf(&conf, 10 * 1024 * 1024 * 1024); // 10GB
        assert!(detector.read_parallel() > 1);
        assert!(detector.read_parallel() <= 8);
    }

    #[test]
    fn test_read_pattern_does_not_switch_back_from_sequential() {
        let conf = create_test_conf(true, 3);
        let mut detector = ReadDetector::with_conf(&conf, 1000);
        let path = create_test_path();

        // Build up to Sequential
        detector.record_seek(&path);
        detector.record_read(0, 100, &path);
        detector.record_read(100, 200, &path);
        detector.record_read(200, 300, &path);
        assert_eq!(detector.read_pattern(), ReadPattern::Sequential);

        // Jump read should reset count but keep Sequential
        detector.record_read(1000, 1100, &path);
        assert_eq!(detector.seq_count(), 0);
        assert_eq!(detector.read_pattern(), ReadPattern::Sequential); // Still Sequential!
    }

    #[test]
    fn test_multiple_seeks() {
        let conf = create_test_conf(true, 10);
        let mut detector = ReadDetector::with_conf(&conf, 1000);
        let path = create_test_path();

        // Multiple seeks
        detector.record_seek(&path);
        assert_eq!(detector.read_pattern(), ReadPattern::Random);

        detector.record_seek(&path);
        assert_eq!(detector.read_pattern(), ReadPattern::Random);

        detector.record_seek(&path);
        assert_eq!(detector.read_pattern(), ReadPattern::Random);
    }

    #[test]
    fn test_zero_length_read() {
        let conf = create_test_conf(true, 10);
        let mut detector = ReadDetector::with_conf(&conf, 1000);
        let path = create_test_path();

        // Zero length read (start == end)
        detector.record_read(100, 100, &path);
        assert_eq!(detector.seq_count(), 1); // Still counts as sequential
        assert_eq!(detector.read_pattern(), ReadPattern::Sequential);
    }
}
