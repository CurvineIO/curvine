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

use clap::{Parser, Subcommand, ValueEnum};
use curvine_client::bench::{
    infer_mode, parse_duration, BenchConfig, BenchMode, BenchPrefillConfig, BenchProfile,
    BenchTarget, CurvineBenchRunner, WorkloadKind, DEFAULT_BASIC_BIG_FILE_COUNT,
    DEFAULT_BIG_FILE_COUNT, DEFAULT_DURATION_MS, DEFAULT_FILES_PER_DIR,
};
use curvine_client::unified::UnifiedFileSystem;
use orpc::common::ByteUnit;
use orpc::{err_box, CommonResult};

mod output;
use output::{print_prefill_report, print_report, print_resource_hint, print_startup_config};

const DEFAULT_THREADS: usize = 1;
const DEFAULT_BIG_FILE_SIZE: &str = "64MB";
const DEFAULT_BLOCK_SIZE: &str = "256KB";
const DEFAULT_SMALL_FILE_SIZE: &str = "128KB";
const DEFAULT_SMALL_FILE_COUNT: usize = 100;

#[derive(Parser, Debug)]
#[command(
    after_help = "Examples:\n  curvine-cli bench --workload metadata --small-file-count 1000\n  curvine-cli bench --workload mixed_metadata --working-set-files 100000\n  curvine-cli bench --workload create:50,open:30,delete:20 --duration 30s\n  curvine-cli bench prefill --path cv://default/bench/ws --files 100000 --file-size 0\n\nWorkload presets:\n  metadata          Run single-op metadata suite\n  throughput        Run single-op throughput suite\n  mixed_metadata    Run preset metadata mix against a reusable working set\n  mixed_throughput  Run preset read/write throughput mix\n\nCustom workload format:\n  op:weight[,op:weight...] where op is create, open, get_status, list_status, rename, or delete"
)]
pub struct BenchCommand {
    #[clap(
        long,
        value_name = "PATH",
        default_value = "cv://default/__curvine_bench__",
        help = "Benchmark base path. In --mode auto: cv:// => client, file:// => fuse, absolute paths => client"
    )]
    pub path: String,

    #[clap(
        long,
        value_enum,
        default_value = "basic",
        help = "Benchmark coverage profile: basic runs single-op suites; deep also runs mixed workloads"
    )]
    pub profile: BenchProfileArg,

    #[clap(
        long,
        value_enum,
        default_value = "auto",
        help = "Access mode. auto only uses path syntax: cv:// => client, file:// => fuse, absolute paths => client; relative paths error"
    )]
    pub mode: BenchModeArg,

    #[clap(short = 'p', long)]
    pub threads: Option<usize>,

    #[clap(
        long,
        value_name = "SIZE",
        help = "I/O block size. Default: 256KB for all profiles"
    )]
    pub block_size: Option<String>,

    #[clap(long, help = "Big-file size for the throughput suite. Default: 64MB")]
    pub big_file_size: Option<String>,

    #[clap(
        long,
        help = "Per-thread big-file count for the throughput suite and mixed workload seeds; defaults are profile-specific"
    )]
    pub big_file_count: Option<usize>,

    #[clap(long, help = "Small-file size for mixed workload writes")]
    pub small_file_size: Option<String>,

    #[clap(
        long,
        help = "Per-thread file count for the metadata suite and mixed workload seeds"
    )]
    pub small_file_count: Option<usize>,

    #[clap(
        long,
        value_name = "WORKLOAD",
        help = "Run a selected workload preset or custom op mix",
        long_help = "Run a selected workload.\n\nPresets:\n  metadata          Run single-op metadata suite\n  throughput        Run single-op throughput suite\n  mixed_metadata    Run preset metadata mix against a reusable working set\n  mixed_throughput  Run preset read/write throughput mix\n\nCustom format:\n  op:weight[,op:weight...]\n\nSupported custom ops: create, open, get_status, list_status, rename, delete.\nExample: --workload create:50,open:30,delete:20"
    )]
    pub workload: Option<String>,

    #[clap(
        long,
        help = "Mixed workload duration limit; single-op metadata/throughput suites ignore it"
    )]
    pub duration: Option<String>,

    #[clap(
        long,
        value_name = "SIZE",
        help = "Mixed workload byte limit; single-op metadata/throughput suites ignore it"
    )]
    pub total_size: Option<String>,

    #[clap(
        long,
        help = "Mixed workload operation limit; single-op metadata/throughput suites ignore it"
    )]
    pub total_ops: Option<u64>,

    #[clap(long)]
    pub working_set_files: Option<usize>,

    #[clap(long, default_value_t = DEFAULT_FILES_PER_DIR)]
    pub files_per_dir: usize,

    #[clap(long, default_value = "false")]
    pub keep_data: bool,

    #[clap(long, default_value = "false")]
    pub json: bool,

    #[command(subcommand)]
    pub command: Option<BenchSubcommand>,
}

#[derive(Subcommand, Debug)]
pub enum BenchSubcommand {
    /// Prefill reusable files for --working-set-files and mixed workloads
    Prefill(BenchPrefillCommand),
}

#[derive(Parser, Debug)]
#[command(
    after_help = "Examples:\n  curvine-cli bench prefill --path cv://default/bench/ws --files 100000 --file-size 0\n  curvine-cli bench prefill --path file:///mnt/curvine/bench/ws --target-size 10GB --file-size 1MB\n\nUse the same base path later with:\n  curvine-cli bench --path cv://default/bench/ws --workload mixed_metadata --working-set-files 100000"
)]
pub struct BenchPrefillCommand {
    #[clap(
        long,
        value_name = "PATH",
        default_value = "cv://default/__curvine_bench__",
        help = "Working-set base path. In --mode auto: cv:// => client, file:// => fuse, absolute paths => client"
    )]
    pub path: String,

    #[clap(
        long,
        value_enum,
        default_value = "auto",
        help = "Access mode. auto only uses path syntax: cv:// => client, file:// => fuse, absolute paths => client; relative paths error"
    )]
    pub mode: BenchModeArg,

    #[clap(short = 'p', long)]
    pub threads: Option<usize>,

    #[clap(long, value_name = "SIZE", default_value = "0")]
    pub file_size: String,

    #[clap(long)]
    pub files: Option<usize>,

    #[clap(long, value_name = "SIZE")]
    pub target_size: Option<String>,

    #[clap(long, value_name = "SIZE", default_value = "4MB")]
    pub block_size: String,

    #[clap(long, default_value_t = DEFAULT_FILES_PER_DIR)]
    pub files_per_dir: usize,

    #[clap(long, default_value = "false")]
    pub json: bool,
}

#[derive(Clone, Debug, ValueEnum)]
pub enum BenchModeArg {
    Auto,
    Client,
    Fuse,
}

impl BenchModeArg {
    fn resolve(&self, path: &str) -> CommonResult<BenchMode> {
        match self {
            Self::Auto => infer_mode(path),
            Self::Client => Ok(BenchMode::Client),
            Self::Fuse => Ok(BenchMode::Fuse),
        }
    }
}

#[derive(Clone, Debug, ValueEnum)]
pub enum BenchProfileArg {
    Basic,
    Deep,
}

impl BenchCommand {
    pub async fn execute(&self, fs: UnifiedFileSystem, conf_source: String) -> CommonResult<()> {
        if let Some(command) = &self.command {
            return match command {
                BenchSubcommand::Prefill(cmd) => cmd.execute(fs, conf_source).await,
            };
        }

        let profile = to_bench_profile(&self.profile);
        let path = self.path.clone();
        let mode = self.mode.resolve(&path)?;

        let duration = self.duration.as_deref().map(parse_duration).transpose()?;
        let workload = self
            .workload
            .as_ref()
            .map(|v| WorkloadKind::parse_or_preset(v))
            .transpose()?;

        let threads = self.threads.unwrap_or(DEFAULT_THREADS);
        if threads == 0 {
            return err_box!("--threads must be greater than 0");
        }
        if self.files_per_dir == 0 {
            return err_box!("--files-per-dir must be greater than 0");
        }
        if self.working_set_files == Some(0) {
            return err_box!("--working-set-files must be greater than 0");
        }
        let config = self.build_config(path, mode, profile, duration, workload, threads)?;

        if !self.json {
            print_startup_config(&conf_source, &config);
            print_resource_hint(&config);
        }

        let runner = CurvineBenchRunner::new(fs, config);
        let report = runner.run().await?;
        if self.json {
            println!("{}", serde_json::to_string_pretty(&report)?);
        } else {
            print_report(&report);
        }
        Ok(())
    }

    fn build_config(
        &self,
        path: String,
        mode: BenchMode,
        profile: BenchProfile,
        duration: Option<std::time::Duration>,
        workload: Option<WorkloadKind>,
        threads: usize,
    ) -> CommonResult<BenchConfig> {
        Ok(BenchConfig {
            base_path: path,
            mode,
            profile,
            target: match mode {
                BenchMode::Client => BenchTarget::Curvine,
                BenchMode::Fuse => BenchTarget::Fuse,
            },
            threads,
            block_size: parse_block_size(self.block_size.as_deref().unwrap_or(DEFAULT_BLOCK_SIZE))?,
            big_file_size: parse_size(
                self.big_file_size
                    .as_deref()
                    .unwrap_or(DEFAULT_BIG_FILE_SIZE),
            )?,
            big_file_count: self.big_file_count.unwrap_or(match profile {
                BenchProfile::Basic => DEFAULT_BASIC_BIG_FILE_COUNT,
                BenchProfile::Deep => DEFAULT_BIG_FILE_COUNT,
            }),
            small_file_size: parse_size(
                self.small_file_size
                    .as_deref()
                    .unwrap_or(DEFAULT_SMALL_FILE_SIZE),
            )?,
            small_file_count: self.small_file_count.unwrap_or(DEFAULT_SMALL_FILE_COUNT),
            keep_data: self.keep_data,
            duration_ms: Some(
                duration
                    .map(|value| value.as_millis() as u64)
                    .unwrap_or(DEFAULT_DURATION_MS),
            ),
            total_size: self.total_size.as_deref().map(parse_size_u64).transpose()?,
            total_ops: self.total_ops,
            working_set_files: self.working_set_files,
            files_per_dir: self.files_per_dir,
            workload,
        })
    }
}

impl BenchPrefillCommand {
    pub async fn execute(&self, fs: UnifiedFileSystem, conf_source: String) -> CommonResult<()> {
        let path = self.path.clone();
        let mode = self.mode.resolve(&path)?;
        let threads = self.threads.unwrap_or(DEFAULT_THREADS);
        if threads == 0 {
            return err_box!("--threads must be greater than 0");
        }
        if self.files_per_dir == 0 {
            return err_box!("--files-per-dir must be greater than 0");
        }
        if self.files == Some(0) {
            return err_box!("--files must be greater than 0");
        }
        let target_size = self
            .target_size
            .as_deref()
            .map(parse_size_u64)
            .transpose()?;
        if target_size == Some(0) {
            return err_box!("--target-size must be greater than 0");
        }

        let config = BenchPrefillConfig {
            base_path: path,
            mode,
            target: match mode {
                BenchMode::Client => BenchTarget::Curvine,
                BenchMode::Fuse => BenchTarget::Fuse,
            },
            threads,
            block_size: parse_block_size(&self.block_size)?,
            file_size: parse_size(&self.file_size)?,
            files: self.files,
            target_size,
            files_per_dir: self.files_per_dir,
        };

        if !self.json {
            println!("Configuration: {conf_source}");
            println!(
                "Prefill Target: {:?}, Mode: {:?}, Path: {}, FileSize: {}, Files: {:?}, TargetSize: {:?}, FilesPerDir: {}, NumThreads: {}",
                config.target,
                config.mode,
                config.base_path,
                ByteUnit::byte_to_string(config.file_size as u64),
                config.files,
                config.target_size.map(ByteUnit::byte_to_string),
                config.files_per_dir,
                config.threads,
            );
        }

        let report = CurvineBenchRunner::prefill(fs, config).await?;
        if self.json {
            println!("{}", serde_json::to_string_pretty(&report)?);
        } else {
            print_prefill_report(&report);
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    use curvine_client::bench::{workset_path, BenchOpResult, BenchResultGroup, LatencyMode};

    #[test]
    fn parses_path_option() {
        let cmd =
            BenchCommand::try_parse_from(["bench", "--path", "cv://default/bench", "-p", "4"])
                .unwrap();

        assert_eq!(cmd.path, "cv://default/bench");
        assert_eq!(cmd.threads, Some(4));
    }

    #[test]
    fn rejects_positional_path() {
        let result = BenchCommand::try_parse_from(["bench", "metadata", "-p", "32"]);

        assert!(result.is_err());
    }

    #[test]
    fn default_values_are_stable() {
        let cmd = BenchCommand::try_parse_from(["bench", "--profile", "deep"]).unwrap();
        let config = cmd
            .build_config(
                cmd.path.clone(),
                BenchMode::Client,
                BenchProfile::Deep,
                None,
                None,
                cmd.threads.unwrap_or(DEFAULT_THREADS),
            )
            .unwrap();

        assert_eq!(config.big_file_size, 64 * 1024 * 1024);
        assert_eq!(config.big_file_count, DEFAULT_BIG_FILE_COUNT);
        assert_eq!(config.block_size, 256 * 1024);
        assert_eq!(config.small_file_size, 128 * 1024);
        assert_eq!(config.small_file_count, 100);
        assert_eq!(config.duration_ms, Some(DEFAULT_DURATION_MS));
        assert_eq!(config.threads, 1);
    }

    #[test]
    fn basic_profile_uses_light_big_file_default() {
        let cmd = BenchCommand::try_parse_from(["bench", "--profile", "basic"]).unwrap();
        let config = cmd
            .build_config(
                cmd.path.clone(),
                BenchMode::Client,
                BenchProfile::Basic,
                None,
                None,
                cmd.threads.unwrap_or(DEFAULT_THREADS),
            )
            .unwrap();

        assert_eq!(config.big_file_count, DEFAULT_BASIC_BIG_FILE_COUNT);
    }

    #[test]
    fn explicit_values_override_defaults() {
        let cmd = BenchCommand::try_parse_from([
            "bench",
            "--big-file-size",
            "2MB",
            "--big-file-count",
            "3",
            "--small-file-count",
            "3",
            "--block-size",
            "256KB",
            "-p",
            "2",
            "--duration",
            "2s",
        ])
        .unwrap();
        let duration = cmd
            .duration
            .as_ref()
            .map(|value| parse_duration(value))
            .transpose()
            .unwrap();
        let config = cmd
            .build_config(
                cmd.path.clone(),
                BenchMode::Client,
                BenchProfile::Basic,
                duration,
                None,
                cmd.threads.unwrap_or(DEFAULT_THREADS),
            )
            .unwrap();

        assert_eq!(config.big_file_size, 2 * 1024 * 1024);
        assert_eq!(config.big_file_count, 3);
        assert_eq!(config.block_size, 256 * 1024);
        assert_eq!(config.small_file_count, 3);
        assert_eq!(config.duration_ms, Some(2_000));
        assert_eq!(config.threads, 2);
    }

    #[test]
    fn rejects_zero_block_size() {
        let cmd = BenchCommand::try_parse_from(["bench", "--block-size", "0"]).unwrap();
        let err = cmd
            .build_config(
                cmd.path.clone(),
                BenchMode::Client,
                BenchProfile::Basic,
                None,
                None,
                cmd.threads.unwrap_or(DEFAULT_THREADS),
            )
            .expect_err("expected zero block size to be rejected");
        assert!(
            err.to_string()
                .contains("--block-size must be greater than 0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn prefill_rejects_zero_block_size() {
        let cmd = BenchCommand::try_parse_from([
            "bench",
            "prefill",
            "--path",
            "cv://default/bench/ws",
            "--files",
            "10",
            "--file-size",
            "1KB",
            "--block-size",
            "0",
        ])
        .unwrap();
        let Some(BenchSubcommand::Prefill(prefill)) = cmd.command else {
            panic!("expected prefill subcommand");
        };
        let err = parse_block_size(&prefill.block_size).expect_err("expected zero block size");
        assert!(
            err.to_string()
                .contains("--block-size must be greater than 0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parses_prefill_subcommand() {
        let cmd = BenchCommand::try_parse_from([
            "bench",
            "prefill",
            "--path",
            "cv://default/bench/ws",
            "--files",
            "1000",
            "--file-size",
            "0",
            "--files-per-dir",
            "100",
            "-p",
            "8",
        ])
        .unwrap();

        let Some(BenchSubcommand::Prefill(prefill)) = cmd.command else {
            panic!("expected prefill subcommand");
        };
        assert_eq!(prefill.path, "cv://default/bench/ws");
        assert_eq!(prefill.files, Some(1000));
        assert_eq!(prefill.files_per_dir, 100);
        assert_eq!(prefill.threads, Some(8));
    }

    #[test]
    fn parses_working_set_and_limits() {
        let cmd = BenchCommand::try_parse_from([
            "bench",
            "--profile",
            "deep",
            "--working-set-files",
            "1000000",
            "--files-per-dir",
            "10000",
            "--duration",
            "30s",
            "--total-ops",
            "200000",
        ])
        .unwrap();
        let duration = cmd
            .duration
            .as_deref()
            .map(parse_duration)
            .transpose()
            .unwrap();
        let config = cmd
            .build_config(
                cmd.path.clone(),
                BenchMode::Client,
                BenchProfile::Deep,
                duration,
                None,
                cmd.threads.unwrap_or(DEFAULT_THREADS),
            )
            .unwrap();

        assert_eq!(config.working_set_files, Some(1_000_000));
        assert_eq!(config.files_per_dir, 10_000);
        assert_eq!(config.duration_ms, Some(30_000));
        assert_eq!(config.total_ops, Some(200_000));
    }

    #[test]
    fn workset_path_uses_directory_shards() {
        assert_eq!(
            workset_path("cv://default/bench/ws", 12_345, 10_000),
            "cv://default/bench/ws/part-000001/item-000000012345"
        );
    }

    #[test]
    fn parses_workload_preset() {
        let parse = |value: &str| WorkloadKind::parse_or_preset(value).unwrap();

        assert!(matches!(parse("metadata"), WorkloadKind::Metadata));
        assert!(matches!(parse("throughput"), WorkloadKind::Throughput));
        assert!(matches!(
            parse("mixed_metadata"),
            WorkloadKind::MixedMetadata
        ));
        assert!(matches!(
            parse("mixed_throughput"),
            WorkloadKind::MixedThroughput
        ));
        assert!(matches!(
            parse("create:50,delete:50"),
            WorkloadKind::Custom(_)
        ));

        let spec = WorkloadKind::MixedMetadata.mixed_spec().unwrap().unwrap();
        spec.validate_metadata_only().unwrap();
    }

    #[test]
    fn help_mentions_workload_presets_and_prefill_usage() {
        let help = <BenchCommand as clap::CommandFactory>::command()
            .render_long_help()
            .to_string();

        assert!(help.contains("Workload presets"));
        assert!(help.contains("mixed_metadata"));
        assert!(help.contains("mixed_throughput"));
        assert!(help.contains("create:50,open:30,delete:20"));
        assert!(help.contains("bench prefill"));
        assert!(help.contains("Mixed workload duration limit"));
        assert!(help.contains("Mixed workload byte limit"));
        assert!(help.contains("Mixed workload operation limit"));
        assert!(help.contains("single-op metadata/throughput suites ignore it"));
    }

    #[test]
    fn percentile_table_includes_max_latency_column() {
        let result = BenchOpResult {
            op: curvine_client::bench::BenchOp::Create,
            group: BenchResultGroup::Metadata,
            latency_mode: LatencyMode::Percentiles,
            item: "Create file".to_string(),
            value: 1.0,
            value_unit: "ops/s".to_string(),
            cost: 1.0,
            cost_unit: "ms/op".to_string(),
            bytes: 0,
            files: 1,
            operations: 1,
            errors: 0,
            error_samples: vec![],
            latency_samples: 1,
            elapsed_ms: 1.0,
            latency_avg_ms: 1.0,
            latency_p50_ms: 1.0,
            latency_p95_ms: 1.0,
            latency_p99_ms: 1.0,
            latency_max_ms: 1.0,
        };
        let mut table = output::md_table();
        table.set_header(vec![
            comfy_table::Cell::new("ITEM"),
            output::cell_right("VALUE"),
            output::cell_right("AVG COST"),
            output::cell_right("P50(ms)"),
            output::cell_right("P95(ms)"),
            output::cell_right("P99(ms)"),
            output::cell_right("MAX(ms)"),
            output::cell_right("SAMPLES"),
            output::cell_right("ERRORS"),
        ]);
        table.add_row(vec![
            comfy_table::Cell::new(&result.item),
            output::cell_right(format!("{:.2} {}", result.value, result.value_unit)),
            output::cell_right(format!("{:.2} {}", result.cost, result.cost_unit)),
            output::cell_f2(result.latency_p50_ms),
            output::cell_f2(result.latency_p95_ms),
            output::cell_f2(result.latency_p99_ms),
            output::cell_f2(result.latency_max_ms),
            output::cell_right(result.latency_samples),
            output::cell_right(result.errors),
        ]);

        assert!(table.to_string().contains("MAX(ms)"));
    }

    #[test]
    fn resolve_auto_mode_by_syntax() {
        assert_eq!(
            BenchModeArg::Auto.resolve("cv://default/bench").unwrap(),
            BenchMode::Client
        );
        assert_eq!(
            BenchModeArg::Auto.resolve("file:///tmp/bench").unwrap(),
            BenchMode::Fuse
        );
        assert_eq!(
            BenchModeArg::Auto.resolve("/bench/path").unwrap(),
            BenchMode::Client
        );
        assert!(BenchModeArg::Auto.resolve("relative/path").is_err());
        assert!(BenchModeArg::Auto.resolve("s3://bucket/bench").is_err());
    }
}

fn to_bench_profile(profile: &BenchProfileArg) -> BenchProfile {
    match profile {
        BenchProfileArg::Basic => BenchProfile::Basic,
        BenchProfileArg::Deep => BenchProfile::Deep,
    }
}

fn parse_size_u64(value: &str) -> CommonResult<u64> {
    Ok(ByteUnit::from_str(value)?.as_byte())
}

fn parse_size(value: &str) -> CommonResult<usize> {
    let size = parse_size_u64(value)?;
    usize::try_from(size).map_err(|e| format!("Size '{}' is too large: {}", value, e).into())
}

fn parse_block_size(value: &str) -> CommonResult<usize> {
    let block_size = parse_size(value)?;
    if block_size == 0 {
        return err_box!("--block-size must be greater than 0");
    }
    Ok(block_size)
}
