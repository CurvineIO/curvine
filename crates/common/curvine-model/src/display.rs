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

use crate::{JobStatus, JobTaskState, LoadJobResult};
use chrono::DateTime;
use std::fmt;
use std::fmt::Display;

struct BasicProgress {
    completed: u64,
    total: u64,
}

impl BasicProgress {
    fn new(completed: u64, total: u64) -> Self {
        Self { completed, total }
    }

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

    fn format_progress(&self) -> String {
        let percentage = self.progress();
        let width = 30;
        let filled = ((width as f64 * percentage / 100.0) as usize).min(width);
        let empty = width - filled;
        let progress_bar = format!("{}{}", "█".repeat(filled), "░".repeat(empty));

        format!(
            "│ 📊 Progress: {:.1}%\n│ [{}] {}/{} bytes",
            percentage,
            progress_bar,
            self.completed_size(),
            self.total
        )
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

impl Display for JobTaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{:?}", self)
    }
}

impl Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state_color = match self.state {
            JobTaskState::Pending => "⚪",
            JobTaskState::Loading => "🔵",
            JobTaskState::Completed => "🟢",
            JobTaskState::Failed => "🔴",
            JobTaskState::Canceled => "⚫",
            JobTaskState::UNKNOWN => "Unknown",
        };

        let format_time = |time: Option<i64>| -> String {
            time.map(|t| {
                let dt = DateTime::from_timestamp_millis(t).unwrap();
                dt.format("%Y-%m-%d %H:%M:%S").to_string()
            })
            .unwrap_or_else(|| "N/A".to_string())
        };

        writeln!(f, "\n📋 Load Job Status")?;
        writeln!(f, "┌──────────────────────────────────────────")?;
        writeln!(f, "│ 🔑 Job ID: {}", self.job_id)?;
        writeln!(f, "│ 📁 Source: {}", self.source_path)?;
        writeln!(f, "│ 📂 Target: {}", self.target_path)?;
        writeln!(f, "│ 🚦 Status: {} {:?}", state_color, self.state)?;

        writeln!(f, "│ 📝 Message: {}", self.progress.message)?;

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
