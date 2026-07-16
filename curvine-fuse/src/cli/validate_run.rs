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

use crate::cli::FuseRuntimeArgs;
use curvine_common::conf::FuseConf;
use orpc::CommonResult;
use std::fs::read_to_string;

/// Validates configuration by loading and initializing cluster settings without mounting.
///
/// Beyond the hard load/parse checks done by `get_conf`, this surfaces two
/// problems that would otherwise only appear at runtime (or never):
///   - `fuse.state_dir` must be (or be creatable as) a writable directory —
///     hard error, since a bad value fails the SIGUSR1 persist/restore path,
///     often mid graceful-upgrade;
///   - unrecognized `[fuse]` TOML keys — warning, since `FuseConf`
///     intentionally ignores unknown keys for legacy compatibility
///     (`#[serde(default)]`, no `deny_unknown_fields`). A misspelled key is
///     silently dropped so its setting never takes effect; a deprecated/renamed
///     key may still apply via `#[serde(alias)]`. Either way it is worth
///     surfacing so the user can verify intent.
///
/// Exits quietly on success; failures are returned via `CommonResult` for stderr reporting.
pub fn run_validate_config(args: FuseRuntimeArgs) -> CommonResult<()> {
    let conf = args.get_conf()?;

    // (a) state_dir must be a writable directory (create if missing).
    conf.fuse.validate_state_dir()?;

    // (b) Warn on unrecognized [fuse] keys. Re-read the raw TOML because the
    // typed load already discarded them. A read failure here is non-fatal: the
    // typed load above already succeeded, so we simply skip the key audit.
    match read_to_string(args.conf_path()) {
        Ok(raw) => match FuseConf::unrecognized_fuse_keys_from_toml(&raw) {
            Ok(unknown) if !unknown.is_empty() => {
                for key in &unknown {
                    eprintln!(
                        "Warning: unrecognized [fuse] config key '{}' — possible typo or a \
                         deprecated/renamed setting; please verify it is still valid",
                        key
                    );
                }
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("Warning: could not audit [fuse] keys: {}", e);
            }
        },
        Err(e) => {
            eprintln!(
                "Warning: could not re-read config file '{}' for [fuse] key audit: {}",
                args.conf_path(),
                e
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{FuseCli, FuseSubcommand};
    use clap::Parser;
    use std::io::Write;

    // Builds FuseRuntimeArgs pointing at a real config file on disk, the same
    // way the CLI does (`curvine-fuse validate-config --conf <path>`).
    fn args_for(conf_path: &str) -> FuseRuntimeArgs {
        let cli = FuseCli::try_parse_from(["curvine-fuse", "validate-config", "--conf", conf_path])
            .expect("parse validate-config");
        match cli.cmd {
            Some(FuseSubcommand::ValidateConfig(args)) => args,
            _ => panic!("expected validate-config subcommand"),
        }
    }

    // Writes `body` to a uniquely named temp .toml and returns its path.
    fn write_temp_toml(tag: &str, body: &str) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("cv_validate_{}_{}.toml", tag, std::process::id()));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        path
    }

    #[test]
    fn validate_config_ok_for_clean_conf() {
        let state_dir = std::env::temp_dir().join(format!("cv_vc_state_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&state_dir);
        let body = format!(
            r#"
[fuse]
io_threads = 8
state_dir = "{}"
"#,
            state_dir.to_string_lossy()
        );
        let conf = write_temp_toml("ok", &body);

        run_validate_config(args_for(conf.to_str().unwrap())).expect("clean config must validate");
        // state_dir should have been created by the writability check.
        assert!(state_dir.is_dir());

        let _ = std::fs::remove_file(&conf);
        let _ = std::fs::remove_dir_all(&state_dir);
    }

    #[test]
    fn validate_config_errors_when_state_dir_is_a_file() {
        // Point state_dir at an existing regular file → hard error.
        let file = std::env::temp_dir().join(format!("cv_vc_notdir_{}", std::process::id()));
        std::fs::write(&file, b"x").unwrap();
        let body = format!(
            r#"
[fuse]
io_threads = 8
state_dir = "{}"
"#,
            file.to_string_lossy()
        );
        let conf = write_temp_toml("notdir", &body);

        let err = run_validate_config(args_for(conf.to_str().unwrap()))
            .expect_err("non-directory state_dir must fail");
        assert!(
            err.to_string().contains("is not a directory"),
            "unexpected error: {}",
            err
        );

        let _ = std::fs::remove_file(&conf);
        let _ = std::fs::remove_file(&file);
    }

    #[test]
    fn validate_config_ok_but_warns_on_unrecognized_key() {
        // An unrecognized key does NOT fail validation (warning only); the run
        // still returns Ok. The warning itself goes to stderr via eprintln.
        let state_dir = std::env::temp_dir().join(format!("cv_vc_warn_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&state_dir);
        let body = format!(
            r#"
[fuse]
io_threads = 8
state_dir = "{}"
direct_iox = true
"#,
            state_dir.to_string_lossy()
        );
        let conf = write_temp_toml("warn", &body);

        run_validate_config(args_for(conf.to_str().unwrap()))
            .expect("unrecognized key must warn, not fail");

        let _ = std::fs::remove_file(&conf);
        let _ = std::fs::remove_dir_all(&state_dir);
    }
}
