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

use crate::proto::{GetMountTableResponse, MountResponse, UnMountResponse};
use crate::state::{JobStatus, LoadJobResult, WorkState};
use chrono::DateTime;
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
            (self.completed as f64 / self.total as f64) * 100.0
        }
    }

    fn completed_size(&self) -> u64 {
        self.completed
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

impl Display for LoadJobResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\n✅ Load job submitted successfully")?;
        writeln!(f, "┌─────────────────────────────────────")?;
        writeln!(f, "│ 🔑 Job ID: {}", self.job_id)?;
        writeln!(f, "│ 📁 Target path: {}", self.target_path)?;
        writeln!(f, "└─────────────────────────────────────")?;
        writeln!(
            f,
            "\nTo check job status, run: curvine load-status {}",
            self.job_id
        )?;
        Ok(())
    }
}
impl Display for WorkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{:?}", self)
    }
}

impl Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Get the color identification corresponding to the status
        let state_color = match self.state {
            WorkState::Pending => "⚪",
            WorkState::Loading => "🔵",
            WorkState::Completed => "🟢",
            WorkState::Failed => "🔴",
            WorkState::Canceled => "⚫",
        };

        // Format time
        let format_time = |time: Option<i64>| -> String {
            time.map(|t| {
                let dt = DateTime::from_timestamp(t, 0).unwrap();
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            })
            .unwrap_or_else(|| "N/A".to_string())
        };

        // Show task status
        writeln!(f, "\n📋 Load Job Status")?;
        writeln!(f, "┌──────────────────────────────────────────")?;
        writeln!(f, "│ 🔑 Job ID: {}", self.job_id)?;
        writeln!(f, "│ 📁 Source: {}", self.source_path)?;
        writeln!(f, "│ 📂 Target: {}", self.target_path)?;
        writeln!(f, "│ 🚦 Status: {} {:?}", state_color, self.state)?;

        writeln!(f, "│ 📝 Message: {}", self.progress.message)?;

        // Show progress information
        let loaded = self.progress.loaded_size;
        let total = self.progress.total_size;
        let progress = BasicProgress::new(loaded as u64, total as u64);
        write!(f, "{}", progress.format_progress())?;

        writeln!(f, "│")?;
        writeln!(
            f,
            "│ 🔄 Updated: {}",
            format_time(Some(self.progress.update_time))
        )?;
        writeln!(f, "└──────────────────────────────────────────")?;

        Ok(())
    }
}

impl Display for MountResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "│ ✅️ mount success.")?;
        Ok(())
    }
}

impl Display for UnMountResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "│ ✅️ unmount success.")?;
        Ok(())
    }
}

impl Display for GetMountTableResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.mount_table.is_empty() {
            return writeln!(f, "Mount Table: (empty)");
        }

        // Calculate the maximum width of each column
        let mut id_width = 2; //length of "ID"
        let mut curvine_width = 12; //length of "Curvine Path"
        let mut ufs_width = 8; //length of "UFS Path"
        for mnt in &self.mount_table {
            id_width = id_width.max(mnt.mount_id.to_string().len());
            curvine_width = curvine_width.max(mnt.cv_path.len());
            ufs_width = ufs_width.max(mnt.ufs_path.len());
        }

        // For the sake of beauty, add some filling
        id_width += 2;
        curvine_width += 2;
        ufs_width += 2;

        // Table header
        writeln!(f, "Mount Table:")?;

        // Top border
        write!(f, "+")?;
        write!(f, "{:-^width$}+", "", width = id_width)?;
        write!(f, "{:-^width$}+", "", width = curvine_width)?;
        writeln!(f, "{:-^width$}+", "", width = ufs_width)?;

        // Title line
        write!(f, "|")?;
        write!(f, " {:<width$}|", "ID", width = id_width - 1)?;
        write!(f, " {:<width$}|", "Curvine Path", width = curvine_width - 1)?;
        writeln!(f, " {:<width$}|", "UFS Path", width = ufs_width - 1)?;

        // Dividing line
        write!(f, "+")?;
        write!(f, "{:-^width$}+", "", width = id_width)?;
        write!(f, "{:-^width$}+", "", width = curvine_width)?;
        writeln!(f, "{:-^width$}+", "", width = ufs_width)?;

        // Data line
        for mnt in &self.mount_table {
            write!(f, "|")?;
            write!(f, " {:<width$}|", mnt.mount_id, width = id_width - 1)?;
            write!(f, " {:<width$}|", mnt.cv_path, width = curvine_width - 1)?;
            writeln!(f, " {:<width$}|", mnt.ufs_path, width = ufs_width - 1)?;
        }

        // The lower border
        write!(f, "+")?;
        write!(f, "{:-^width$}+", "", width = id_width)?;
        write!(f, "{:-^width$}+", "", width = curvine_width)?;
        writeln!(f, "{:-^width$}+", "", width = ufs_width)?;

        // Summary of information
        writeln!(f, "Total mount points: {}", self.mount_table.len())?;

        Ok(())
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
}
