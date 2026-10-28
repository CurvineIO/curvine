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

//! Multi-cluster support for the admin console.
//!
//! A `curvine-web` process is configured against exactly one cluster (the
//! *default* cluster, see [`ClusterConf`]). This module adds the optional
//! ability to serve additional, imported clusters from the same process:
//! an imported cluster is a full [`ClusterConf`] TOML file describing another
//! cluster, kept on disk and selected per request through the
//! [`CLUSTER_HEADER`] request header.
//!
//! Requests without the header keep resolving to the default cluster, so the
//! single-cluster behaviour is unchanged.

mod registry;

pub use self::registry::{
    clusters_dir_for, is_valid_cluster_id, ClusterCtx, ClusterRegistry, ClusterSource,
    ClusterSummary, Entry, ProbeResult, RegistryError, ResolveError,
};
pub(crate) use self::registry::{configuration_errors, configuration_warnings};

/// Request header selecting the target cluster, e.g. `X-Curvine-Cluster: nailu`.
///
/// Absent (or empty) means "the default cluster". The value is a cluster id as
/// recorded in the registry, never a file path.
pub const CLUSTER_HEADER: &str = "x-curvine-cluster";

/// Path prefix of the cluster-management API, which is intentionally cluster
/// independent: it must work before the caller knows which clusters exist.
pub const CLUSTER_API_PREFIX: &str = "/api/v1/clusters";

/// Whether `path` is served by the cluster-management API (and therefore not
/// subject to per-cluster resolution).
pub fn is_cluster_api_path(path: &str) -> bool {
    path == CLUSTER_API_PREFIX
        || path.starts_with("/api/v1/clusters/")
        // `/api/v1/clusters` itself has no trailing slash, so a bare prefix
        // match is enough here.
        || path == "/api/v1/clusters/"
}
