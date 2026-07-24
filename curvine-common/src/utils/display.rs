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

use std::fmt;
use std::fmt::Display;

/// Configuration options for progress display
pub struct ProgressDisplayOptions {
    /// Progress bar width
    pub width: usize,
    /// Progress bar fills characters
    pub fill_char: char,
    /// Progress bar blank characters
    pub empty_char: char,
}

impl Default for ProgressDisplayOptions {
    fn default() -> Self {
        Self {
            width: 30,
            fill_char: '█',
            empty_char: '░',
        }
    }
}

/// Progress display trait, used to uniformly process progress output format
pub trait ProgressDisplay {
    /// Get the current progress value (0-100)
    fn progress(&self) -> f64;

    /// Get the completed size
    fn completed_size(&self) -> u64;

    /// Get the total size
    fn total_size(&self) -> u64;

    /// Format progress bar
    fn format_progress_bar(&self, opts: &ProgressDisplayOptions) -> String {
        let percentage = self.progress();
        let width = opts.width;
        let filled = ((width as f64 * percentage / 100.0) as usize).min(width);
        let empty = width - filled;

        format!(
            "{}{}",
            opts.fill_char.to_string().repeat(filled),
            opts.empty_char.to_string().repeat(empty)
        )
    }

    /// Format progress information, including progress bar, percentage and size information
    fn format_progress(&self) -> String {
        let opts = ProgressDisplayOptions::default();
        let progress_bar = self.format_progress_bar(&opts);
        let percentage = self.progress();

        format!(
            "│ 📊 Progress: {:.1}%\n│ [{}] {}/{} bytes",
            percentage,
            progress_bar,
            self.completed_size(),
            self.total_size()
        )
    }
}

/// Basic progress display implementation
pub struct BasicProgress {
    completed: u64,
    total: u64,
}

impl BasicProgress {
    pub fn new(completed: u64, total: u64) -> Self {
        Self { completed, total }
    }
}

impl ProgressDisplay for BasicProgress {
    fn progress(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            ((self.completed as f64 / self.total as f64) * 100.0).min(100.0)
        }
    }

    fn completed_size(&self) -> u64 {
        if self.total == 0 {
            self.completed
        } else {
            self.completed.min(self.total)
        }
    }

    fn total_size(&self) -> u64 {
        self.total
    }
}

impl Display for BasicProgress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", self.format_progress())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_progress() {
        let progress = BasicProgress::new(50, 100);
        assert_eq!(progress.progress(), 50.0);

        let display = progress.format_progress();
        assert!(display.contains("50.0%"));
        assert!(display.contains("50/100"));
    }

    #[test]
    fn test_custom_progress_bar() {
        let progress = BasicProgress::new(75, 100);
        let opts = ProgressDisplayOptions {
            width: 10,
            fill_char: '#',
            empty_char: '-',
        };

        let bar = progress.format_progress_bar(&opts);
        assert_eq!(bar, "#######---");
    }

    #[test]
    fn test_basic_progress_caps_over_complete_display() {
        let progress = BasicProgress::new(66, 33);

        assert_eq!(progress.progress(), 100.0);

        let display = progress.format_progress();
        assert!(display.contains("100.0%"));
        assert!(display.contains("33/33"));
        assert!(!display.contains("200.0%"));
        assert!(!display.contains("66/33"));
    }
}
