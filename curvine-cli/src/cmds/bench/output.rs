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

use comfy_table::{presets::ASCII_MARKDOWN, Cell, CellAlignment, Table};
use curvine_client::bench::{
    BenchConfig, BenchOpResult, BenchPrefillReport, BenchProfile, BenchReport, BenchResultGroup,
    BenchTarget, LatencyMode, WorkloadKind,
};
use orpc::common::ByteUnit;

pub(super) fn print_startup_config(conf_source: &str, config: &BenchConfig) {
    let duration = config
        .duration_ms
        .map(format_duration)
        .unwrap_or_else(|| "none".to_string());
    println!("Configuration: {conf_source}");
    println!(
        "Profile: {:?}, Target: {:?}, Mode: {:?}, Path: {}, BlockSize: {}, BigFileSize: {}, BigFileCount: {}, SmallFileSize: {}, SmallFileCount: {}, NumThreads: {}, KeepData: {}, Duration: {}, TotalSize: {}, TotalOps: {}, WorkingSetFiles: {:?}, FilesPerDir: {}",
        config.profile,
        config.target,
        config.mode,
        config.base_path,
        ByteUnit::byte_to_string(config.block_size as u64),
        ByteUnit::byte_to_string(config.big_file_size as u64),
        config.big_file_count,
        ByteUnit::byte_to_string(config.small_file_size as u64),
        config.small_file_count,
        config.threads,
        config.keep_data,
        duration,
        config
            .total_size
            .map(ByteUnit::byte_to_string)
            .unwrap_or_else(|| "none".to_string()),
        config
            .total_ops
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
        config.working_set_files,
        config.files_per_dir
    );
}

pub(super) fn print_resource_hint(config: &BenchConfig) {
    if config.profile == BenchProfile::Deep {
        let total_buffer = config.block_size.saturating_mul(config.threads);
        println!(
            "Estimated IO buffer: up to {} (threads x block size)",
            ByteUnit::byte_to_string(total_buffer as u64)
        );
    }
}

pub(super) fn print_report(report: &BenchReport) {
    println!("Benchmark finished!");
    println!("Temp path: {}", report.temp_path);
    let results = report.results.iter().collect::<Vec<_>>();
    print_group_panels(target_label(report.config.target), &results, report);
}

pub(super) fn print_prefill_report(report: &BenchPrefillReport) {
    println!("Prefill finished!");
    println!("Base path: {}", report.base_path);
    println!(
        "Files: {}, FileSize: {}, Bytes: {}, Operations: {}, Errors: {}, Elapsed: {:.2}s",
        report.files,
        ByteUnit::byte_to_string(report.file_size as u64),
        ByteUnit::byte_to_string(report.bytes),
        report.operations,
        report.errors,
        report.elapsed_ms / 1000.0,
    );
}

fn format_duration(duration_ms: u64) -> String {
    if duration_ms.is_multiple_of(3_600_000) {
        return format!("{}h", duration_ms / 3_600_000);
    }
    if duration_ms.is_multiple_of(60_000) {
        return format!("{}m", duration_ms / 60_000);
    }
    if duration_ms.is_multiple_of(1_000) {
        return format!("{}s", duration_ms / 1_000);
    }
    format!("{}ms", duration_ms)
}

fn target_label(target: BenchTarget) -> &'static str {
    match target {
        BenchTarget::Curvine => "Curvine",
        BenchTarget::Fuse => "Fuse",
    }
}

fn print_group_panels(target: &str, results: &[&BenchOpResult], report: &BenchReport) {
    if results.is_empty() {
        return;
    }

    let mut start = 0;
    while start < results.len() {
        let group = results[start].group;
        let mut end = start + 1;
        while end < results.len() && results[end].group == group {
            end += 1;
        }
        let title = panel_title(target, group, report);
        print_panel(&title, &results[start..end]);
        start = end;
    }
}

pub(super) fn cell_right(text: impl ToString) -> Cell {
    Cell::new(text.to_string()).set_alignment(CellAlignment::Right)
}

pub(super) fn cell_f2(value: f64) -> Cell {
    cell_right(format!("{value:.2}"))
}

pub(super) fn md_table() -> Table {
    let mut table = Table::new();
    table.load_preset(ASCII_MARKDOWN);
    table
}

fn panel_title(target: &str, group: BenchResultGroup, report: &BenchReport) -> String {
    match group {
        BenchResultGroup::MixedWorkload => {
            format!("{target} {} ({})", group.label(), workload_summary(report))
        }
        _ => format!("{target} {}", group.label()),
    }
}

fn workload_summary(report: &BenchReport) -> String {
    match &report.config.workload {
        Some(WorkloadKind::Custom(spec)) => {
            let spec = spec
                .ops
                .iter()
                .map(|item| format!("{}:{}", item.op.workload_name(), item.weight))
                .collect::<Vec<_>>()
                .join(",");
            format!("custom: {spec}")
        }
        Some(kind) => kind.name().to_string(),
        None => "custom".to_string(),
    }
}

fn print_panel(title: &str, results: &[&BenchOpResult]) {
    if results.is_empty() {
        return;
    }
    println!();
    println!("{title}:");

    let mut table = md_table();
    let show_percentiles = results
        .iter()
        .any(|item| item.latency_mode == LatencyMode::Percentiles);
    if show_percentiles {
        table.set_header(vec![
            Cell::new("ITEM"),
            cell_right("VALUE"),
            cell_right("AVG COST"),
            cell_right("P50(ms)"),
            cell_right("P95(ms)"),
            cell_right("P99(ms)"),
            cell_right("MAX(ms)"),
            cell_right("SAMPLES"),
            cell_right("ERRORS"),
        ]);
    } else {
        table.set_header(vec![
            Cell::new("ITEM"),
            cell_right("VALUE"),
            cell_right("AVG COST"),
            cell_right("ERRORS"),
        ]);
    }

    for item in results {
        let mut row = vec![
            Cell::new(&item.item),
            cell_right(format!("{:.2} {}", item.value, item.value_unit)),
            cell_right(format!("{:.2} {}", item.cost, item.cost_unit)),
        ];
        if show_percentiles {
            row.push(cell_f2(item.latency_p50_ms));
            row.push(cell_f2(item.latency_p95_ms));
            row.push(cell_f2(item.latency_p99_ms));
            row.push(cell_f2(item.latency_max_ms));
            row.push(cell_right(item.latency_samples));
        }
        row.push(cell_right(item.errors));
        table.add_row(row);
    }
    println!("{table}");

    if show_percentiles {
        if let Some(min_samples) = results.iter().map(|item| item.latency_samples).min() {
            if min_samples < 20 {
                println!(
                    "Note: percentile samples are low (min {min_samples}); increase --small-file-count, --duration, or -p for more stable P50/P95/P99."
                );
            }
        }
    }

    let mut printed = false;
    for item in results {
        if item.errors == 0 || item.error_samples.is_empty() {
            continue;
        }
        if !printed {
            println!("Error samples:");
            printed = true;
        }
        let samples = item.error_samples.join(" | ");
        println!("  - {}: {}", item.item, samples);
    }
}
