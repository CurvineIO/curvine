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

use curvine_model::FileStatus;

/// ContentSummary represents the summary of a directory or file in the filesystem.
/// It includes the total length (size in bytes), the space consumed by all
/// replicas, number of files, and number of directories.
#[derive(Debug, Clone, Default)]
pub struct ContentSummary {
    /// Total size in bytes
    pub length: i64,
    /// Total size in bytes of all replicas
    pub space_consumed: i64,
    /// Number of files
    pub file_count: u64,
    /// Number of directories (including the directory itself)
    pub directory_count: u64,
}

impl ContentSummary {
    /// Creates a ContentSummary for a single file
    pub fn for_file(status: &FileStatus) -> Self {
        ContentSummary {
            length: status.len,
            space_consumed: status.len * status.replicas as i64,
            file_count: 1,
            directory_count: 0,
        }
    }

    /// Creates a ContentSummary for an empty directory
    pub fn for_empty_dir() -> Self {
        ContentSummary {
            length: 0,
            space_consumed: 0,
            file_count: 0,
            directory_count: 1, // Count the directory itself
        }
    }

    /// Merges another ContentSummary into this one
    pub fn merge(&mut self, other: &ContentSummary) {
        self.length += other.length;
        self.space_consumed += other.space_consumed;
        self.file_count += other.file_count;
        self.directory_count += other.directory_count;
    }
}

#[cfg(test)]
mod tests {
    use super::ContentSummary;
    use curvine_model::FileStatus;

    fn file(len: i64, replicas: i32) -> FileStatus {
        FileStatus {
            len,
            replicas,
            ..Default::default()
        }
    }

    #[test]
    fn file_space_consumed_counts_every_replica() {
        let summary = ContentSummary::for_file(&file(100, 3));
        assert_eq!(summary.length, 100);
        assert_eq!(summary.space_consumed, 300);
        assert_eq!(summary.file_count, 1);
        assert_eq!(summary.directory_count, 0);
    }

    #[test]
    fn dir_space_consumed_sums_files_with_their_own_replicas() {
        let mut nested = ContentSummary::for_empty_dir();
        nested.merge(&ContentSummary::for_file(&file(10, 2)));

        let mut summary = ContentSummary::for_empty_dir();
        summary.merge(&ContentSummary::for_file(&file(100, 1)));
        summary.merge(&ContentSummary::for_file(&file(50, 3)));
        summary.merge(&nested);

        assert_eq!(summary.length, 160);
        assert_eq!(summary.space_consumed, 100 + 150 + 20);
        assert_eq!(summary.file_count, 3);
        assert_eq!(summary.directory_count, 2);
    }
}
