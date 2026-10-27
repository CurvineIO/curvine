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

//! Contract for `ClusterConf::from_with_sources`.
//!
//! The source map is what the admin console displays instead of guessing, so it
//! has to describe the value the process is *actually* using. The loader mutates
//! several values after deserialization (port migration, address synthesis);
//! those must not be reported as `file`, because the file value is not what the
//! process runs with.
//!
//! Environment-layer attribution is covered by the `pipeline` unit tests, which
//! can inject a lookup map; this file exercises the real entry points and
//! therefore must not mutate process-wide environment variables (it would race
//! with the other tests in this binary).

use std::io::Write;

use curvine_config::{ClusterConf, ConfigSource};
use tempfile::NamedTempFile;

fn write_conf(text: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("temp file");
    file.write_all(text.as_bytes()).expect("write conf");
    file.flush().expect("flush conf");
    file
}

fn load(text: &str) -> (ClusterConf, Vec<(String, ConfigSource)>) {
    let file = write_conf(text);
    let (conf, sources) = ClusterConf::from_with_sources(file.path().to_str().unwrap())
        .expect("conf must load through from_with_sources");
    (conf, sources.into_iter().collect())
}

fn load_transfer(text: &str) -> (ClusterConf, Vec<(String, ConfigSource)>) {
    let file = write_conf(text);
    let (conf, sources) = ClusterConf::from_transfer_with_sources(file.path().to_str().unwrap())
        .expect("conf must load through from_transfer_with_sources");
    (conf, sources.into_iter().collect())
}

fn source_of<'a>(sources: &'a [(String, ConfigSource)], path: &str) -> Option<&'a ConfigSource> {
    sources
        .iter()
        .find(|(key, _)| key == path)
        .map(|(_, source)| source)
}

#[test]
fn file_supplied_value_maps_to_file() {
    let (conf, sources) = load(
        r#"
        [master]
        hostname = "file-master"
    "#,
    );

    assert_eq!(conf.master.hostname, "file-master");
    assert_eq!(
        source_of(&sources, "master.hostname"),
        Some(&ConfigSource::File),
        "a value read from the file must be attributed to the file layer"
    );
}

/// `normalize_split_web_ports` rewrites 9000 -> 9010 after deserialization.
/// The file said 9000; the process runs 9010. Reporting `file` here would send
/// an operator to edit a line the process is already ignoring.
#[test]
fn migrated_web_port_is_reported_as_derived() {
    let (conf, sources) = load(
        r#"
        [master]
        hostname = "file-master"
        web_port = 9000

        [web]
        port = 9000
    "#,
    );

    assert_eq!(
        conf.master.web_port, 9010,
        "legacy unified port must be migrated"
    );
    assert_eq!(
        source_of(&sources, "master.web_port"),
        Some(&ConfigSource::Derived),
        "the effective value is computed, not read from the file"
    );
}

/// An explicitly split port is not migrated, so its source stays `file`.
#[test]
fn explicit_web_port_keeps_file_source() {
    let (conf, sources) = load(
        r#"
        [master]
        hostname = "file-master"
        web_port = 9010

        [web]
        port = 9000
    "#,
    );

    assert_eq!(conf.master.web_port, 9010);
    assert_eq!(
        source_of(&sources, "master.web_port"),
        Some(&ConfigSource::File)
    );
}

/// `client.master_addrs` is synthesized when the file leaves it empty, so it has
/// no file entry of its own and must be marked derived.
#[test]
fn synthesized_master_addrs_is_reported_as_derived() {
    let (conf, sources) = load(
        r#"
        [master]
        hostname = "file-master"
        rpc_port = 8995

        [journal]
        journal_addrs = []
    "#,
    );

    assert_eq!(conf.client.master_addrs.len(), 1);
    assert_eq!(conf.client.master_addrs[0].hostname, "file-master");
    assert_eq!(
        source_of(&sources, "client.master_addrs"),
        Some(&ConfigSource::Derived)
    );
}

/// A file that already lists `client.master_addrs` keeps the file attribution.
#[test]
fn configured_master_addrs_keeps_file_source() {
    let (conf, sources) = load(
        r#"
        [master]
        hostname = "file-master"

        [journal]
        journal_addrs = []

        [[client.master_addrs]]
        hostname = "m1"
        port = 8995
    "#,
    );

    assert_eq!(conf.client.master_addrs.len(), 1);
    assert_eq!(conf.client.master_addrs[0].hostname, "m1");
    assert_eq!(
        source_of(&sources, "client.master_addrs"),
        Some(&ConfigSource::File)
    );
}

/// The transfer profile skips the port migration entirely, so a legacy port in
/// the file stays as-is and keeps its file attribution.
#[test]
fn transfer_profile_does_not_migrate_web_port() {
    let (conf, sources) = load_transfer(
        r#"
        [master]
        web_port = 9000

        [web]
        port = 9000
    "#,
    );

    assert_eq!(
        conf.master.web_port, 9000,
        "the transfer profile must not migrate the master metrics port"
    );
    assert_eq!(
        source_of(&sources, "master.web_port"),
        Some(&ConfigSource::File),
        "an untouched value must keep its file attribution"
    );
}
