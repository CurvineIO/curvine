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

//! Unified configuration validation.
//!
//! Two complementary layers:
//!
//! 1. [`ConfValidate`] — per-section hard validation + derived-field
//!    initialization. Implementations delegate to the section's existing
//!    `init()` so there is exactly one source of truth for the rules; the
//!    trait gives the pipeline a uniform hook and gives future hot-reload code
//!    a single entry point to re-validate a section before swapping it in.
//!
//! 2. Unknown-key audit ([`audit_unknown_keys`]) — `ClusterConf` is
//!    `#[serde(default)]` without `deny_unknown_fields`, so a misspelled TOML
//!    key is silently dropped at load time.
//!
//!    The audit is EMPIRICAL: each candidate key path is individually probed
//!    by replacing its value with one of the opposite type and attempting a
//!    real deserialization of the mutated document. A consumed key (canonical,
//!    aliased, `Option`, or `skip_serializing`-only) makes the probe fail with
//!    a type error; an ignored key lets it succeed and is reported as unknown.
//!    This derives acceptance from actual serde behavior, so it cannot produce
//!    the false positives a serialized-default schema would (it cannot see
//!    aliases or `skip_serializing`-only fields).
//!
//!    Known gap: keys inside arrays of tables (`[[worker.spdk_disk.targets]]`)
//!    are not audited — element-level probing is deferred. Tables are walked
//!    structurally; only scalar leaves are probed, and an entirely unknown
//!    section is reported per inner leaf rather than at the section root.

use crate::cluster_conf::ClusterConf;
use crate::{ClientConf, FuseConf, JobConf, MasterConf, TransferConf};
use curvine_core_error::{try_err, CommonResult};

/// Uniform validation hook over one configuration section.
pub trait ConfValidate {
    /// Hard-validates the section and initializes derived fields.
    /// Defaults to a no-op for sections without rules.
    fn validate(&mut self) -> CommonResult<()> {
        Ok(())
    }
}

/// Whole-document validation: runs every section in the historical init
/// order so the first failure names the section an operator can act on.
/// This is what the pipeline invokes as its validate stage; individual
/// sections can also be validated on their own (e.g. the transfer profile's
/// reduced set).
impl ConfValidate for ClusterConf {
    fn validate(&mut self) -> CommonResult<()> {
        // Order mirrors the historical init sequence; section errors already
        // carry their own `section.key` context, so they propagate verbatim.
        self.master.validate()?;
        self.client.validate()?;
        self.fuse.validate()?;
        self.job.validate()?;
        self.transfer.validate()?;
        Ok(())
    }
}

impl ConfValidate for ClientConf {
    fn validate(&mut self) -> CommonResult<()> {
        self.init()
    }
}

impl ConfValidate for FuseConf {
    fn validate(&mut self) -> CommonResult<()> {
        self.init()
    }
}

impl ConfValidate for MasterConf {
    fn validate(&mut self) -> CommonResult<()> {
        self.init()
    }
}

impl ConfValidate for JobConf {
    fn validate(&mut self) -> CommonResult<()> {
        self.init()?;
        Ok(())
    }
}

impl ConfValidate for TransferConf {
    fn validate(&mut self) -> CommonResult<()> {
        self.init()?;
        Ok(())
    }
}

/// Returns sorted dotted paths (e.g. `"fuse.state_dir_typo"`) of every scalar
/// key in the raw cluster TOML that no `ClusterConf` field consumes.
///
/// See the module docs for why this is an empirical probe rather than a
/// schema comparison.
pub fn audit_unknown_keys(raw: &str) -> CommonResult<Vec<String>> {
    let doc: toml::Value = try_err!(toml::from_str(raw));
    Ok(audit_unknown_keys_in(&doc))
}

/// [`audit_unknown_keys`] over an already-parsed document.
pub fn audit_unknown_keys_in(doc: &toml::Value) -> Vec<String> {
    let mut candidates = Vec::new();
    collect_leaf_paths(doc, String::new(), &mut candidates);

    let mut unknown = Vec::new();
    for path in candidates {
        let mut mutated = doc.clone();
        if !replace_with_opposite_type(&mut mutated, &path) {
            // Unsupported leaf shape — leave it alone rather than risk a false
            // report (the audit is advisory).
            continue;
        }
        let res: Result<ClusterConf, _> = mutated.try_into();
        match res {
            // The mutated value was rejected -> the key is consumed by serde.
            Err(_) => {}
            // The mutation went unnoticed -> nothing reads this key.
            Ok(_) => unknown.push(path),
        }
    }
    unknown.sort();
    unknown
}

/// Collects dotted paths of every scalar leaf, walking tables structurally.
///
/// Arrays are deliberately not descended into (documented gap): their element
/// schemas would need per-element probing that is not worth the complexity
/// while array fields themselves remain structural nodes here.
fn collect_leaf_paths(value: &toml::Value, prefix: String, out: &mut Vec<String>) {
    match value {
        toml::Value::Table(table) => {
            for (key, child) in table {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                collect_leaf_paths(child, path, out);
            }
        }
        toml::Value::Array(_) => {
            // Documented gap: element keys inside arrays are not audited.
        }
        _ => {
            if !prefix.is_empty() {
                out.push(prefix);
            }
        }
    }
}

/// Replaces the value at a dotted path with one of the opposite type, so a
/// consumed field fails its type check during the probe:
/// string leaves become an integer, everything else becomes a string.
fn replace_with_opposite_type(doc: &mut toml::Value, path: &str) -> bool {
    let segments: Vec<&str> = path.split('.').collect();
    let mut node = doc;
    for seg in segments.iter().take(segments.len() - 1) {
        node = match node.get_mut(*seg) {
            Some(v) => v,
            None => return false,
        };
    }
    let last = *segments.last().expect("non-empty path");
    let leaf = match node.get(last) {
        Some(v) => v,
        None => return false,
    };
    let replacement = match leaf {
        toml::Value::String(_) => toml::Value::Integer(0),
        _ => toml::Value::String("__curvine_audit_probe__".to_string()),
    };
    crate::pipeline::set_dotted(node, last, replacement).is_ok()
}

/// Loads-time convenience: prints one stderr warning per unknown key. Warnings
/// (not errors) keep forward/backward compatibility across versions — newer
/// binaries reading older files and vice versa must not fail to start.
pub fn warn_unknown_keys(raw: &str) {
    match audit_unknown_keys(raw) {
        Ok(keys) if !keys.is_empty() => {
            for key in &keys {
                eprintln!(
                    "[WARN] unrecognized config key '{}' — possible typo or a \
                     deprecated/renamed setting; verify it is still valid",
                    key
                );
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn clean_document_has_no_unknown_keys() {
        let raw = r#"
            net_interface = ""
            [master]
            hostname = "m1"
            [master.compatibility]
            mode = "diagnose"
            [fuse]
            mnt_path = "/mnt/curvine"
        "#;
        assert!(audit_unknown_keys(raw).unwrap().is_empty());
    }

    #[test]
    fn typoed_keys_are_reported_with_dotted_paths() {
        let raw = r#"
            [fuse]
            mnt_pathh = "/mnt/x"
            io_threadz = 4
        "#;
        let unknown = audit_unknown_keys(raw).unwrap();
        assert_eq!(unknown, vec!["fuse.io_threadz", "fuse.mnt_pathh"]);
    }

    // Documented gap: keys inside arrays of tables are not audited.
    #[test]
    fn array_of_tables_internals_are_not_audited() {
        let raw = r#"
            [[worker.spdk_disk.targets]]
            trtypo = 1
        "#;
        assert!(
            audit_unknown_keys(raw).unwrap().is_empty(),
            "array internals are outside the audit scope (documented gap)"
        );
    }

    // An entirely unknown section is reported per inner leaf.
    #[test]
    fn unknown_section_leaves_are_reported() {
        let raw = r#"
            [s3_gateway_typo]
            enabled = true
            region = "us-east-1"
        "#;
        assert_eq!(
            audit_unknown_keys(raw).unwrap(),
            vec!["s3_gateway_typo.enabled", "s3_gateway_typo.region"]
        );
    }

    // A real field carrying #[serde(skip)] never takes effect and must be
    // surfaced. (attr_ttl is the skip-derived Duration; attr_ttl_ms does not
    // exist as a serde field at all.)
    #[test]
    fn serde_skipped_fields_are_reported_as_unknown() {
        let raw = r#"
            [fuse]
            attr_ttl = 5
        "#;
        assert_eq!(audit_unknown_keys(raw).unwrap(), vec!["fuse.attr_ttl"]);
    }

    // Review-requested cases: fields invisible to a serialized-default schema
    // (Option::None / skip_serializing-only) MUST stay clean — they deserialize
    // and take effect.
    #[test]
    fn option_and_skip_serializing_fields_are_not_false_positives() {
        let raw = r#"
            [fuse]
            max_readahead_kb = 1024

            [transfer]
            store_url = "sqlite:///tmp/t.db"
        "#;
        let unknown: Vec<String> = audit_unknown_keys(raw).unwrap();
        assert!(
            unknown.is_empty(),
            "Option/skip_serializing keys must be recognized: {unknown:?}"
        );
    }

    #[test]
    fn warn_unknown_keys_does_not_panic_on_bad_toml() {
        warn_unknown_keys("this is not = = valid toml");
    }

    #[test]
    fn fuse_section_audit_matches_legacy_helper() {
        let raw = r#"
            [fuse]
            mnt_pathh = "/mnt/x"
        "#;
        let legacy = FuseConf::unrecognized_fuse_keys_from_toml(raw).unwrap();
        let generic: Vec<String> = audit_unknown_keys(raw)
            .unwrap()
            .into_iter()
            .filter(|k| k.starts_with("fuse."))
            .map(|k| k.trim_start_matches("fuse.").to_string())
            .collect();
        assert_eq!(legacy, generic);
    }

    /// Guards the shipped sample: every key documented in
    /// `etc/curvine-cluster.toml` must be recognized by `ClusterConf`, otherwise
    /// every cluster startup would warn about the example itself.
    #[test]
    fn workspace_sample_config_audits_clean() {
        let path = format!(
            "{}/../../../etc/curvine-cluster.toml",
            env!("CARGO_MANIFEST_DIR")
        );
        let raw = fs::read_to_string(&path).expect("read sample config");
        let unknown = audit_unknown_keys(&raw).expect("sample config must parse");
        assert!(
            unknown.is_empty(),
            "shipped etc/curvine-cluster.toml has unrecognized keys: {unknown:?}"
        );
    }
}
