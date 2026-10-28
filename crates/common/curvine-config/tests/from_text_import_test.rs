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

//! Contract for `ClusterConf::from_text_import`.
//!
//! An imported cluster file describes a *different host* than the one the web
//! console runs on, so loading it must not apply this host's `CURVINE_*`
//! hostname overrides or `net_interface` resolution. This lives in its own test
//! binary because it mutates process-wide environment variables, which would
//! race with the other `ClusterConf::from*` tests.

use curvine_config::ClusterConf;
use std::io::Write;
use tempfile::NamedTempFile;

/// A cluster whose hostnames have nothing to do with the local host, plus a
/// `net_interface` that only makes sense on the imported cluster's machines.
const IMPORTED: &str = r#"
    cluster_id = "cluster-b"
    net_interface = ""

    [master]
    hostname = "master-b.example"

    [journal]
    hostname = "journal-b.example"

    [client]
    master_addrs = [{ hostname = "master-b.example", port = 8995 }]
"#;

/// Same shape, but with a `net_interface` set: resolving it locally would either
/// fail (no such interface) or overwrite every hostname with a local IPv4.
const IMPORTED_WITH_INTERFACE: &str = r#"
    cluster_id = "cluster-c"
    net_interface = "curvine_no_such_if0"

    [master]
    hostname = "master-c.example"

    [journal]
    hostname = "journal-c.example"
"#;

struct EnvGuard(Vec<(&'static str, Option<std::ffi::OsString>)>);

impl EnvGuard {
    fn set(pairs: &[(&'static str, &'static str)]) -> Self {
        let saved = pairs
            .iter()
            .map(|(key, value)| {
                let previous = std::env::var_os(key);
                std::env::set_var(key, value);
                (*key, previous)
            })
            .collect();
        Self(saved)
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, value) in self.0.drain(..) {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

fn env_overrides() -> EnvGuard {
    EnvGuard::set(&[
        (ClusterConf::ENV_MASTER_HOSTNAME, "local-host-from-env"),
        (ClusterConf::ENV_WORKER_HOSTNAME, "local-worker-from-env"),
        (ClusterConf::ENV_CLIENT_HOSTNAME, "local-client-from-env"),
        (
            ClusterConf::ENV_TRANSFER_HOSTNAME,
            "local-transfer-from-env",
        ),
    ])
}

#[test]
fn imported_conf_keeps_its_own_hostnames() {
    let _env = env_overrides();

    let conf = ClusterConf::from_text_import(IMPORTED).expect("import must succeed");

    assert_eq!(conf.cluster_id, "cluster-b");
    assert_eq!(conf.master.hostname, "master-b.example");
    assert_eq!(conf.journal.hostname, "journal-b.example");
    // Nothing was rewritten from the local environment.
    assert_eq!(conf.worker.hostname, ClusterConf::DEFAULT_HOSTNAME);
    assert_eq!(conf.client.hostname, ClusterConf::DEFAULT_HOSTNAME);
    assert_eq!(conf.transfer.hostname, ClusterConf::DEFAULT_HOSTNAME);
    // ...and the derived master address follows the imported cluster.
    assert_eq!(conf.client.master_addrs.len(), 1);
    // The file's own master endpoint survives verbatim — nothing derived from
    // this host replaced it.
    assert_eq!(conf.client.master_addrs[0].hostname, "master-b.example");
    assert_eq!(conf.client.master_addrs[0].port, conf.master.rpc_port);
}

/// Negative control for [`imported_conf_keeps_its_own_hostnames`]: the very same
/// text loaded through `from()` *does* pick up the local environment, so the
/// assertion above proves the import path skipped it rather than that the
/// variables were never read.
#[test]
fn file_loader_applies_local_hostname_overrides() {
    let _env = env_overrides();
    let mut file = NamedTempFile::new().expect("temp file");
    file.write_all(IMPORTED.as_bytes()).expect("write conf");
    file.flush().expect("flush conf");

    let conf = ClusterConf::from(file.path().to_str().unwrap()).expect("file must load");

    assert_eq!(conf.master.hostname, "local-host-from-env");
    assert_eq!(conf.journal.hostname, "local-host-from-env");
}

#[test]
fn imported_conf_does_not_resolve_net_interface() {
    let _env = env_overrides();

    // If `apply_hostname_overrides` ran, this import would fail on the unknown
    // interface; both the error and a rewritten hostname are rejected here.
    let conf =
        ClusterConf::from_text_import(IMPORTED_WITH_INTERFACE).expect("import must not resolve");

    assert_eq!(conf.net_interface, "curvine_no_such_if0");
    assert_eq!(conf.master.hostname, "master-c.example");
    assert_eq!(conf.journal.hostname, "journal-c.example");
}

#[test]
fn imported_conf_requires_explicit_cluster_id() {
    let text = r#"
        [master]
        hostname = "master-b.example"
    "#;

    let err = ClusterConf::from_text_import(text)
        .expect_err("an import without cluster_id must be rejected")
        .to_string();

    assert!(err.contains("cluster_id"), "unexpected error: {err}");
}

#[test]
fn imported_conf_rejects_blank_cluster_id() {
    let text = r#"
        cluster_id = "   "

        [master]
        hostname = "master-b.example"
    "#;

    let err = ClusterConf::from_text_import(text)
        .expect_err("a blank cluster_id must be rejected")
        .to_string();

    assert!(err.contains("cluster_id"), "unexpected error: {err}");
}

#[test]
fn imported_conf_normalizes_and_validates() {
    let text = r#"
        cluster_id = " cluster-d "

        [master]
        hostname = " master-d.example "

        [client]
        master_addrs = [{ hostname = " master-d.example ", port = 8995 }]
    "#;

    let conf = ClusterConf::from_text_import(text).expect("import must succeed");

    assert_eq!(conf.cluster_id, "cluster-d");
    assert_eq!(conf.master.hostname, "master-d.example");
    assert_eq!(conf.client.master_addrs[0].hostname, "master-d.example");
}
