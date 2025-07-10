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

/// ContentSummary represents the summary of a directory or file in the filesystem.
/// It includes the total length (size in bytes), number of files, and number of directories.
#[derive(Debug, Clone, Default)]
pub struct ContentSummary {
    /// Total size in bytes
    pub length: i64,
    /// Number of files
    pub file_count: u64,
    /// Number of directories (including the directory itself)
    pub directory_count: u64,
}

impl ContentSummary {
    /// Creates a ContentSummary for a file with the given length
    pub fn for_file(length: i64) -> Self {
        ContentSummary {
            length,
            file_count: 1,
            directory_count: 0,
        }
    }

    /// Creates a ContentSummary for an empty directory
    pub fn for_empty_dir() -> Self {
        ContentSummary {
            length: 0,
            file_count: 0,
            directory_count: 1, // Count the directory itself
        }
    }

    /// Merges another ContentSummary into this one
    pub fn merge(&mut self, other: &ContentSummary) {
        self.length += other.length;
        self.file_count += other.file_count;
        self.directory_count += other.directory_count;
    }
}
