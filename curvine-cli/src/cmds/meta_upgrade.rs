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

//! `curvine-cli meta-upgrade`: offline conversion of master metadata written by
//! a pre-codec build.
//!
//! A temporary tool, so it carries no feature gate: it links `curvine-master`,
//! which means the CLI binary always ships rocksdb and raft.

use crate::upgrade::{run, UpgradeOptions, UpgradeReport};
use clap::Parser;
use curvine_config::{ClusterConf, ConfigLoader, DiscoveredPath};
use curvine_core_error::{err_box, CommonResult};
use curvine_raft::conf::JournalConfExt;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(after_long_help = "WHAT IS CONVERTED:\n\
    \x20 checkpoint/ck-*       the checkpoint database the journal snapshot names\n\
    \x20                       (the newest when there is no journal)\n\
    \x20 journal data/         raft log entries (payload only), plus the file list\n\
    \x20                       of the checkpoint its snapshot restores from\n\
    \x20 meta data/            only with --with-meta-data, implied when there is no\n\
    \x20                       journal to restore it from\n\
    \n\
    SOURCES AND OUTPUT:\n\
    \x20 The sources default to master.meta_dir and journal.journal_dir read from the\n\
    \x20 cluster config, which is discovered automatically (--conf, CURVINE_CONF_FILE,\n\
    \x20 ./curvine-cluster.toml, ./conf/, ./etc/, ~/.curvine/, $CURVINE_HOME/conf/), so a\n\
    \x20 normal run needs no flags: the output defaults to <meta-dir>.upgrade-out.\n\
    \x20 Either source can be overridden with\n\
    \x20 --meta-dir / --journal-dir. They are opened read-only and are never modified.\n\
    \x20 Every converted database is written under --out-dir, mirroring the source\n\
    \x20 layout, so the source keeps working with the pre-upgrade binary and is itself\n\
    \x20 the rollback copy. Re-running rebuilds the output from the sources.\n\
    \n\
    INSTALLING THE RESULT:\n\
    \x20 Back up the live directories, then rename <out-dir>/meta and\n\
    \x20 <out-dir>/journal over them (the configured master.meta_dir and\n\
    \x20 journal.journal_dir, or the --meta-dir/--journal-dir override when the\n\
    \x20 config does not name them). The journal snapshot is rewritten to\n\
    \x20 restore from <master.meta_dir>/checkpoint/<ck> and to name this\n\
    \x20 member, so a moved tree still reuses its checkpoint. Not at --out-dir.\n\
    \n\
    The converted store must be read by the same revision this tool was built from:\n\
    re-encoding protobuf drops fields the compiled schema does not know about.")]
pub struct MetaUpgradeCommand {
    /// Root directory for the converted output; the sources are left untouched.
    /// Defaults to <meta-dir>.upgrade-out, a sibling of the meta directory.
    #[arg(long, value_name = "DIR")]
    pub out_dir: Option<String>,

    /// Override master.meta_dir from the config
    #[arg(long, value_name = "DIR")]
    pub meta_dir: Option<String>,

    /// Override journal.journal_dir from the config
    #[arg(long, value_name = "DIR")]
    pub journal_dir: Option<String>,

    /// Also convert meta/data; without it data/ is replaced by an empty store
    /// that the master overwrites from the converted checkpoint on startup
    #[arg(long)]
    pub with_meta_data: bool,

    /// Skip every write; report the volume the conversion would move
    #[arg(long)]
    pub dry_run: bool,
}

impl MetaUpgradeCommand {
    /// Run the conversion. `conf_path` is the enclosing CLI's global `--conf`.
    pub fn execute(self, conf_path: Option<&str>) -> CommonResult<()> {
        // The sources live in the cluster config. When both are overridden they
        // need no config, but an explicitly configured file still owns the
        // install target and this member's id, so it is loaded even then --
        // without probing the default locations, which an override-only run
        // must not pick up by accident.
        let both_overridden = self.meta_dir.is_some() && self.journal_dir.is_some();
        let conf = load_conf(conf_path, both_overridden)?;

        let meta_dir = match self.meta_dir {
            Some(dir) => dir,
            None => {
                let dir = conf
                    .as_ref()
                    .map(|conf| conf.master.meta_dir.clone())
                    .unwrap_or_default();
                if dir.trim().is_empty() {
                    return err_box!(
                        "no meta directory: pass --meta-dir, or set master.meta_dir in the config \
                         found via --conf, {}, or the default locations",
                        ClusterConf::ENV_CONF_FILE
                    );
                }
                dir
            }
        };

        if conf.is_none() && self.journal_dir.is_none() {
            eprintln!("note: no cluster config found, converting the meta directory only");
        }

        // `journal.enable` describes how the *next* start will run, not what the
        // old build left behind: a cluster that ran with the journal disabled
        // still has meta data to convert, and one that ran with it enabled has a
        // raft log that must be converted too. The directory decides.
        let journal_dir = match self.journal_dir {
            Some(dir) => Some(dir),
            None => conf
                .as_ref()
                .map(|conf| conf.journal.journal_dir.clone())
                .filter(|dir| !dir.trim().is_empty())
                .filter(|dir| has_journal_store(dir)),
        };

        // Without a raft log there is no snapshot the master could rebuild
        // meta/data from, so the working copy has to be converted here.
        let with_data = self.with_meta_data || journal_dir.is_none();
        if journal_dir.is_none() {
            // Not fatal, but the install is incomplete: a master started with
            // format_master=false needs meta/ and journal/ to be either both
            // RocksDB stores or both empty, so a meta-only tree only works
            // where the journal is not installed alongside it.
            eprintln!(
                "warning: no raft log store found; the output holds the meta directory only. \
                 A master also requires journal data/ to be a rocksdb store."
            );
        }

        // Default the output to a sibling of the meta directory, so a normal
        // run needs no flags. Trailing separators are trimmed first: appending
        // to `.../meta/` would otherwise nest the output inside the source,
        // which the overlap check then rejects.
        let out_dir = self.out_dir.unwrap_or_else(|| {
            let dir = format!("{}.upgrade-out", meta_dir.trim_end_matches(['/', '\\']));
            eprintln!("note: --out-dir not given, writing to {}", dir);
            dir
        });

        let mut opts = UpgradeOptions::new(meta_dir, out_dir).with_meta_data(with_data);
        if let Some(dir) = journal_dir {
            opts = opts.with_journal_dir(dir);
        }
        opts = apply_install_targets(opts, conf.as_ref());
        if let Some(conf) = conf.as_ref() {
            match conf.journal.node_id() {
                Ok(node_id) => opts = opts.with_node_id(node_id),
                Err(e) => eprintln!(
                    "warning: cannot resolve this member's node id ({}); it keeps the recorded \
                     one and may copy or download its own checkpoint",
                    e
                ),
            }
        } else {
            eprintln!(
                "note: no cluster config, so the snapshot names {} and keeps the recorded member \
                 id; pass --conf if the tree will be installed elsewhere",
                opts.installed_meta_dir()
            );
        }
        if self.dry_run {
            opts = opts.dry_run();
        }

        let report = run(&opts)?;
        report.print();

        if !report.ok() {
            return err_box!("metadata conversion failed");
        }
        if !opts.dry_run {
            print_install_steps(&opts, &report);
        }
        Ok(())
    }
}

/// Point the converted snapshot and the install steps at the deployment the
/// config describes.
///
/// The checkpoint path and the member id in the snapshot, and the directories
/// the NEXT STEPS moves the output to, belong to the deployment rather than to
/// the data: a source override says where the old build wrote, not where the
/// new build will run. The snapshot and the steps are derived from the same
/// values, so they cannot disagree.
fn apply_install_targets(opts: UpgradeOptions, conf: Option<&ClusterConf>) -> UpgradeOptions {
    let Some(conf) = conf else {
        return opts;
    };

    let mut opts = opts;
    let live_meta = conf.master.meta_dir.clone();
    if !live_meta.trim().is_empty() {
        opts = opts.with_install_meta_dir(live_meta);
    }
    let live_journal = conf.journal.journal_dir.clone();
    if opts.journal_dir.is_some() && !live_journal.trim().is_empty() {
        opts = opts.with_install_journal_dir(live_journal);
    }
    opts
}

/// Print the directory swap the operator has to perform by hand.
///
/// The tool never writes to a live directory: swapping is a destructive step on
/// running state, and keeping the old tree under its own name is what makes a
/// rollback a second rename.
fn print_install_steps(opts: &UpgradeOptions, report: &UpgradeReport) {
    let mut pairs = vec![(
        format!("{}/meta", opts.out_dir),
        opts.installed_meta_dir().to_string(),
    )];
    if let Some(dir) = opts.installed_journal_dir() {
        pairs.push((format!("{}/journal", opts.out_dir), dir.to_string()));
    }

    println!("\nNEXT STEPS (with the whole cluster stopped):");
    for (converted, live) in &pairs {
        println!("  mv {} {}.bak", live, live);
        println!("  mv {} {}", converted, live);
    }
    if let Some(snapshot) = report
        .journal
        .as_ref()
        .and_then(|x| x.snapshot_dir.as_ref())
    {
        println!(
            "  The journal restores meta/data from {}, which this install produces.",
            snapshot
        );
    }
    println!("  Roll back by renaming the .bak directories back.");
}

/// Whether a journal base directory holds a raft log store.
fn has_journal_store(dir: &str) -> bool {
    PathBuf::from(dir).join("data").join("CURRENT").exists()
}

/// Load the cluster config, reporting where it came from. Returns `None` when no
/// config file was configured or found, so the caller can fall back to explicit
/// source directories.
///
/// With `configured_only`, only `--conf` and `CURVINE_CONF_FILE` are consulted:
/// a run whose sources are all given explicitly must not pick up an unrelated
/// file from the working directory.
fn load_conf(explicit: Option<&str>, configured_only: bool) -> CommonResult<Option<ClusterConf>> {
    // `--conf` > CURVINE_CONF_FILE > ./ > ./conf/ > ~/.curvine/ > $CURVINE_HOME/conf/,
    // then the source-tree layout, which keeps the shipped file under `etc/`.
    let found = if configured_only {
        ConfigLoader::discover_configured(explicit)
    } else {
        ConfigLoader::discover(explicit).or_else(etc_config)
    };

    let Some(found) = found else {
        return Ok(None);
    };

    println!("Loading config from {} ({})", found.as_str(), found.source);
    Ok(Some(ClusterConf::from(found.as_str())?))
}

/// Last resort: `etc/curvine-cluster.toml` in the source tree. `ConfigLoader`
/// does not probe it because deployments copy the file to `conf/`.
fn etc_config() -> Option<DiscoveredPath> {
    let name = ConfigLoader::DEFAULT_FILE_NAME;
    let mut candidates = Vec::with_capacity(2);
    if let Some(home) = std::env::var_os("CURVINE_HOME") {
        candidates.push(PathBuf::from(home).join("etc").join(name));
    }
    candidates.push(PathBuf::from("etc").join(name));

    candidates
        .into_iter()
        .find(|path| path.exists())
        .map(|path| DiscoveredPath {
            path,
            source: "etc directory".to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_targets_follow_the_config_not_the_source_overrides() {
        let mut conf = ClusterConf::default();
        conf.master.meta_dir = "/live/meta".to_string();
        conf.journal.journal_dir = "/live/journal".to_string();

        // An override says where the old build wrote, not where the new build
        // will run; the snapshot and the install steps both follow the config.
        let opts = apply_install_targets(
            UpgradeOptions::new("/copy/meta", "/out").with_journal_dir("/copy/journal"),
            Some(&conf),
        );
        assert_eq!(opts.installed_meta_dir(), "/live/meta");
        assert_eq!(opts.installed_journal_dir(), Some("/live/journal"));

        // Without a config the sources are installed on themselves.
        let opts = apply_install_targets(
            UpgradeOptions::new("/copy/meta", "/out").with_journal_dir("/copy/journal"),
            None,
        );
        assert_eq!(opts.installed_meta_dir(), "/copy/meta");
        assert_eq!(opts.installed_journal_dir(), Some("/copy/journal"));
    }
}
