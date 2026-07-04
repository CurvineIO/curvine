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
//

use super::WorkloadKind;
use orpc::{err_box, CommonResult};
use serde::Serialize;

pub const DEFAULT_DURATION_MS: u64 = 30_000;
pub const DEFAULT_FILES_PER_DIR: usize = 10_000;
pub const DEFAULT_BASIC_BIG_FILE_COUNT: usize = 1;
pub const DEFAULT_BIG_FILE_COUNT: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BenchMode {
    Client,
    Fuse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BenchProfile {
    Basic,
    Deep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchTarget {
    Curvine,
    Fuse,
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchConfig {
    pub base_path: String,
    pub mode: BenchMode,
    pub profile: BenchProfile,
    pub target: BenchTarget,
    pub threads: usize,
    pub block_size: usize,
    pub big_file_size: usize,
    pub big_file_count: usize,
    pub small_file_size: usize,
    pub small_file_count: usize,
    pub keep_data: bool,
    pub duration_ms: Option<u64>,
    pub total_size: Option<u64>,
    pub total_ops: Option<u64>,
    pub working_set_files: Option<usize>,
    pub files_per_dir: usize,
    pub workload: Option<WorkloadKind>,
}

#[derive(Debug, Clone)]
pub struct BenchPrefillConfig {
    pub base_path: String,
    pub mode: BenchMode,
    pub target: BenchTarget,
    pub threads: usize,
    pub block_size: usize,
    pub file_size: usize,
    pub files: Option<usize>,
    pub target_size: Option<u64>,
    pub files_per_dir: usize,
}

impl BenchPrefillConfig {
    pub(crate) fn file_count(&self) -> CommonResult<usize> {
        if self.files_per_dir == 0 {
            return err_box!("--files-per-dir must be greater than 0");
        }
        match (self.files, self.target_size) {
            (Some(files), None) => {
                if files == 0 {
                    return err_box!("--files must be greater than 0");
                }
                Ok(files)
            }
            (None, Some(target_size)) => {
                if target_size == 0 {
                    return err_box!("--target-size must be greater than 0");
                }
                if self.file_size == 0 {
                    return err_box!("--target-size requires --file-size greater than 0");
                }
                let file_size = self.file_size as u64;
                let files = target_size.div_ceil(file_size);
                usize::try_from(files)
                    .map_err(|e| format!("target size requires too many files: {}", e).into())
            }
            (Some(_), Some(_)) => err_box!("Use either --files or --target-size, not both"),
            (None, None) => err_box!("Prefill requires --files or --target-size"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_prefill_config() -> BenchPrefillConfig {
        BenchPrefillConfig {
            base_path: "cv://default/bench/ws".to_string(),
            mode: BenchMode::Client,
            target: BenchTarget::Curvine,
            threads: 1,
            block_size: 1024,
            file_size: 1024,
            files: None,
            target_size: Some(2500),
            files_per_dir: 100,
        }
    }

    fn assert_err_contains(result: CommonResult<usize>, needle: &str) {
        let err = result.expect_err("expected validation error");
        assert!(
            err.to_string().contains(needle),
            "expected error containing {needle:?}, got {err}"
        );
    }

    #[test]
    fn prefill_target_size_rounds_up_to_file_count() {
        assert_eq!(sample_prefill_config().file_count().unwrap(), 3);
    }

    #[test]
    fn prefill_rejects_zero_files() {
        let mut config = sample_prefill_config();
        config.files = Some(0);
        config.target_size = None;
        assert_err_contains(config.file_count(), "--files must be greater than 0");
    }

    #[test]
    fn prefill_rejects_zero_target_size() {
        let mut config = sample_prefill_config();
        config.target_size = Some(0);
        assert_err_contains(config.file_count(), "--target-size must be greater than 0");
    }

    #[test]
    fn prefill_rejects_target_size_with_zero_file_size() {
        let mut config = sample_prefill_config();
        config.file_size = 0;
        assert_err_contains(
            config.file_count(),
            "--target-size requires --file-size greater than 0",
        );
    }

    #[test]
    fn prefill_rejects_files_and_target_size_together() {
        let mut config = sample_prefill_config();
        config.files = Some(10);
        assert_err_contains(
            config.file_count(),
            "Use either --files or --target-size, not both",
        );
    }

    #[test]
    fn prefill_requires_files_or_target_size() {
        let mut config = sample_prefill_config();
        config.target_size = None;
        assert_err_contains(
            config.file_count(),
            "Prefill requires --files or --target-size",
        );
    }
}
