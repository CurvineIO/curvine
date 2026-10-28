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

//! Registry of the clusters a console process can serve.
//!
//! Three entry kinds share one table:
//!
//! * the **default** cluster — the process' own [`ClusterConf`], always present,
//!   never removable, and the target of every request that does not name a
//!   cluster;
//! * **imported** clusters — `<dir>/<id>.toml` files, each a full cluster
//!   configuration, loaded with [`ClusterConf::from_text_import`] so that this
//!   host's environment (`CURVINE_*_HOSTNAME`, `net_interface`) cannot rewrite
//!   the imported cluster's hostnames;
//! * **invalid** entries — a file that failed to load. They stay visible with
//!   their error instead of aborting startup, so one broken import cannot make
//!   the console unusable.
//!
//! Client construction is lazy: [`UnifiedFileSystem::with_rt`] builds clients
//! and a connection pool but performs no handshake, so importing a cluster that
//! is currently unreachable succeeds. Reachability is a separate, explicit
//! probe (`ProbeResult`), which is what `status: "unreachable"` reports.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use log::{info, warn};
use serde::Serialize;
use serde_json::Value;

use curvine_config::ClusterConf;
use curvine_error::FsResult;
use curvine_runtime::runtime::Runtime;
use curvine_unified_fs::UnifiedFileSystem;

use crate::observability::ObservabilityService;
use crate::router::admin_handler::{sanitize_error_text, ResyncTaskState};
use crate::util::addr::configured_master_addrs;

/// Longest accepted cluster id (the file name, minus `.toml`).
pub const MAX_CLUSTER_ID_LEN: usize = 63;

/// The accepted cluster id shape, quoted in every error message about ids.
pub(crate) const CLUSTER_ID_HINT: &str = "[a-z0-9_-]{1,63}";

/// Cluster ids double as file names and as header values, so they stay in the
/// portable lowercase `[a-z0-9_-]` set: no separators, no dots, nothing to
/// escape.
///
/// Lowercase only is not cosmetic: `beta` and `BETA` are the same file path on
/// a case-insensitive filesystem (macOS, Windows), so accepting both would let
/// one import silently overwrite the other and make `remove`/`replace` address
/// the wrong entry. The leading character must not be `-` so an id can never be
/// mistaken for a command-line flag.
pub fn is_valid_cluster_id(id: &str) -> bool {
    fn allowed(c: char) -> bool {
        c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-'
    }

    let mut chars = id.chars();
    match chars.next() {
        Some(first) if allowed(first) && first != '-' => {}
        _ => return false,
    }
    id.len() <= MAX_CLUSTER_ID_LEN && chars.all(allowed)
}

/// The one sentence every id rejection uses, wherever it is reported.
pub(crate) fn invalid_cluster_id_message(id: &str) -> String {
    format!("invalid cluster id '{id}': ids must match {CLUSTER_ID_HINT}")
}

/// Structural problems that make an imported configuration unusable.
///
/// Deliberately offline: this answers "could this text ever reach a cluster",
/// not "is the cluster up" (that is the probe's job). It lives next to the
/// registry rather than in the route so `/validate`, `create` and the directory
/// loader all report the same thing — a configuration the dry run rejects must
/// not be storable, and a stored one that stops parsing must degrade to
/// [`Entry::Invalid`] with a reason.
pub(crate) fn configuration_errors(conf: &ClusterConf) -> Vec<String> {
    let mut errors = Vec::new();

    // The id is validated here rather than only at the route, so that `/validate`,
    // `create`/`PUT` and the directory loader answer the same question the same
    // way: a text whose `cluster_id` the registry would refuse is reported as an
    // error by the dry run too.
    if !is_valid_cluster_id(&conf.cluster_id) {
        errors.push(invalid_cluster_id_message(&conf.cluster_id));
    }

    if conf.master.rpc_port == 0 {
        errors.push("master.rpc_port is 0: no master can listen on port 0".to_string());
    }

    let addrs = configured_master_addrs(conf);
    if addrs.is_empty() {
        errors.push(
            "no master address: set [client] master_addrs or a routable [master] hostname"
                .to_string(),
        );
    }

    for addr in &addrs {
        if addr.hostname.trim().is_empty() {
            errors.push("master address has an empty host".to_string());
        }
        if addr.port == 0 {
            errors.push(format!("master address '{}' uses port 0", addr.hostname));
        }
    }

    errors
}

/// Non-fatal problems worth showing before a cluster is saved.
///
/// Text-level on purpose: the fallback that produces `localhost` is a property
/// of the *defaults* that fill in for missing keys, so it cannot be seen in the
/// resulting `ClusterConf` alone.
pub(crate) fn configuration_warnings(conf: &ClusterConf, text: &str) -> Vec<String> {
    let mut warnings = Vec::new();
    let addrs = configured_master_addrs(conf);

    // Presence has to be read from the text: what is *missing* is exactly what
    // the defaulted `ClusterConf` can no longer show. `build_document_with` with
    // an empty lookup adds no environment layer of its own; when even the parse
    // fails there is nothing to say (the import path already reported it).
    let doc = curvine_config::pipeline::build_document_with(text, &[], &|_: &str| None).ok();
    if let Some(doc) = doc {
        let declares = |table: &str, key: &str| -> bool {
            doc.get(table)
                .and_then(|table| table.get(key))
                .and_then(|value| value.as_array())
                .is_some_and(|values| !values.is_empty())
        };

        if !declares("client", "master_addrs") && !declares("journal", "journal_addrs") {
            warnings.push(
                "no [client] master_addrs or [journal] journal_addrs: the master address would \
                 fall back to the built-in default localhost:8995, which likely points at the \
                 wrong cluster"
                    .to_string(),
            );
            return warnings;
        }
    }

    if !addrs.is_empty() && addrs.iter().all(|addr| is_loopback(addr.hostname.as_str())) {
        warnings.push(
            "every master address resolves to loopback: the cluster is only reachable from this \
             host"
                .to_string(),
        );
    }

    warnings
}

/// Reports the soft problems of a configuration bound for disk. Warnings only:
/// a single-node cluster that legitimately says `localhost` must still be
/// importable, and the caller has already been told about these in `/validate`.
fn log_configuration_warnings(id: &str, conf: &ClusterConf, text: &str) {
    for warning in configuration_warnings(conf, text) {
        warn!("cluster '{id}': {warning}");
    }
}

fn is_loopback(host: &str) -> bool {
    let host = host.trim().trim_end_matches('.');
    host.eq_ignore_ascii_case("localhost")
        || host.to_ascii_lowercase().ends_with(".localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// Directory an import is read from / written to.
///
/// `web.clusters_dir` wins when set. Otherwise the directory is derived from the
/// config file location (`<conf dir>/clusters`) so a read-only install is
/// self-contained; without a known config path, `./clusters` is used.
pub fn clusters_dir_for(conf: &ClusterConf, conf_path: Option<&Path>) -> PathBuf {
    let configured = conf.web.clusters_dir.trim();
    if !configured.is_empty() {
        return PathBuf::from(configured);
    }
    match conf_path.and_then(|path| path.parent()) {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join("clusters"),
        _ => PathBuf::from("clusters"),
    }
}

/// Where a registered cluster came from — and therefore whether it can be
/// deleted through the API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterSource {
    /// The process' own configuration; not a file this registry owns.
    Default,
    /// An imported configuration file.
    File(PathBuf),
}

impl ClusterSource {
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Default)
    }

    /// Stable wire value used by the API (`default` | `file`).
    pub fn kind(&self) -> &'static str {
        if self.is_default() {
            "default"
        } else {
            "file"
        }
    }

    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::Default => None,
            Self::File(path) => Some(path),
        }
    }
}

/// Everything that belongs to one cluster rather than to the console process.
///
/// The per-cluster job/state maps live here on purpose: `resync_tasks`,
/// `load_jobs` and `load_job_cache` are keyed by job/task id, so sharing one map
/// across clusters would let a task id from cluster A resolve to cluster B's
/// state (and cancel the wrong job).
pub struct ClusterCtx {
    pub id: String,
    pub display_name: String,
    pub source: ClusterSource,
    pub conf: ClusterConf,
    pub unified_fs: UnifiedFileSystem,
    pub observability: Arc<ObservabilityService>,
    pub resync_tasks: Arc<Mutex<HashMap<String, ResyncTaskState>>>,
    pub load_jobs: Arc<Mutex<VecDeque<String>>>,
    pub load_job_cache: Arc<Mutex<HashMap<String, Value>>>,
    /// Result of the most recent explicit probe (see the `.../probe` endpoint).
    pub last_probe: Arc<Mutex<Option<ProbeResult>>>,
}

impl ClusterCtx {
    pub fn new(
        id: String,
        display_name: String,
        source: ClusterSource,
        conf: ClusterConf,
        rt: Arc<Runtime>,
    ) -> FsResult<Self> {
        // Lazy by design: no connection is established here, so an import of a
        // cluster that happens to be down still registers. Note that client
        // metrics are a process-wide singleton, so all clusters share one client
        // metric set; per-cluster observability lives in `observability` below.
        let unified_fs = UnifiedFileSystem::with_rt(conf.clone(), rt.clone())?;
        let observability = Arc::new(ObservabilityService::new(conf.clone(), unified_fs.clone()));
        observability.start(rt);

        Ok(Self {
            id,
            display_name,
            source,
            conf,
            unified_fs,
            observability,
            resync_tasks: Arc::new(Mutex::new(HashMap::new())),
            load_jobs: Arc::new(Mutex::new(VecDeque::new())),
            load_job_cache: Arc::new(Mutex::new(HashMap::new())),
            last_probe: Arc::new(Mutex::new(None)),
        })
    }

    /// `display_name` with the cluster id as fallback.
    pub fn label(&self) -> &str {
        if self.display_name.trim().is_empty() {
            &self.id
        } else {
            &self.display_name
        }
    }

    /// Configured master endpoints, `host:port`. Never the full configuration.
    pub fn master_addrs(&self) -> Vec<String> {
        configured_master_addrs(&self.conf)
            .iter()
            .map(|addr| addr.to_string())
            .collect()
    }
}

/// Outcome of a reachability probe against one cluster.
#[derive(Debug, Clone, Serialize)]
pub struct ProbeResult {
    pub reachable: bool,
    pub active_master: Option<String>,
    /// Reserved: the master does not report its version in `FilesystemInfo`, and
    /// adding an RPC just for a label is not worth it.
    pub version: Option<String>,
    pub latency_ms: u64,
    pub message: Option<String>,
}

/// One registry row.
pub enum Entry {
    Ready(Arc<ClusterCtx>),
    Invalid { error: String, path: PathBuf },
}

/// Wire projection of one cluster.
///
/// Deliberately a whitelist of fields: the underlying [`ClusterConf`] carries
/// credentials (S3 keys, DB passwords), so it is never serialized here.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterSummary {
    pub id: String,
    pub display_name: String,
    /// `default` or `file`.
    pub source: &'static str,
    pub master_addrs: Vec<String>,
    /// `ready`, `invalid` (config did not load) or `unreachable` (last probe
    /// failed).
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Whether the stored/loaded configuration may be replaced (PUT).
    pub editable: bool,
    /// Whether the cluster may be deleted (DELETE).
    pub removable: bool,
}

/// Why a registry mutation was refused.
///
/// Carries its own stable API code so the HTTP layer only has to map it to a
/// status code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    /// The configuration did not load (`invalid_config`).
    InvalidConfig(String),
    /// The cluster id is syntactically unusable (`invalid_config`).
    InvalidId(String),
    /// A cluster with that id already exists, including the default id
    /// (`cluster_exists`).
    Exists(String),
    /// `web.max_clusters` reached (`cluster_limit`).
    Limit(usize),
    /// The entry may not be removed or replaced, e.g. the default cluster
    /// (`not_removable`).
    NotRemovable(String),
    /// No such cluster (`unknown_cluster`).
    Unknown(String),
    /// Persisting/removing the file failed (`internal_error`).
    Io(String),
}

impl RegistryError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidConfig(_) | Self::InvalidId(_) => "invalid_config",
            Self::Exists(_) => "cluster_exists",
            Self::Limit(_) => "cluster_limit",
            Self::NotRemovable(_) => "not_removable",
            Self::Unknown(_) => "unknown_cluster",
            Self::Io(_) => "internal_error",
        }
    }
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfig(msg) => write!(f, "{msg}"),
            Self::InvalidId(id) => write!(f, "{}", invalid_cluster_id_message(id)),
            Self::Exists(id) => write!(f, "cluster '{id}' already exists"),
            Self::Limit(max) => write!(f, "cluster limit of {max} reached"),
            Self::NotRemovable(id) => write!(
                f,
                "cluster '{id}' is the built-in default cluster: it cannot be removed or replaced \
                 (edit its own configuration file instead)"
            ),
            Self::Unknown(id) => write!(f, "unknown cluster '{id}'"),
            Self::Io(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for RegistryError {}

/// Why the `X-Curvine-Cluster` header could not be resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    /// No usable entry: either no such id, or the entry failed to load.
    NotFound(String),
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(id) => write!(f, "unknown cluster '{id}'"),
        }
    }
}

impl std::error::Error for ResolveError {}

/// The set of clusters this process serves.
pub struct ClusterRegistry {
    default_id: String,
    dir: PathBuf,
    max: usize,
    entries: RwLock<BTreeMap<String, Entry>>,
    rt: Arc<Runtime>,
}

impl std::fmt::Debug for ClusterRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterRegistry")
            .field("default_id", &self.default_id)
            .field("dir", &self.dir)
            .field("max", &self.max)
            .finish()
    }
}

impl ClusterRegistry {
    /// Builds the registry: registers `conf` as the default cluster and loads
    /// every `*.toml` in `dir`.
    ///
    /// A missing directory is not an error and is not created — a read-only
    /// installation must not gain a side effect just for starting up.
    pub fn new(
        conf: ClusterConf,
        rt: Arc<Runtime>,
        dir: PathBuf,
        max: usize,
    ) -> Result<Self, RegistryError> {
        let max = max.max(1);
        let default_id = conf.cluster_id.clone();
        let display_name = conf.web.display_name.trim().to_string();
        let ctx = ClusterCtx::new(
            default_id.clone(),
            display_name,
            ClusterSource::Default,
            conf,
            rt.clone(),
        )
        .map_err(|err| RegistryError::InvalidConfig(format!("default cluster: {err}")))?;

        let registry = Self {
            default_id,
            dir,
            max,
            entries: RwLock::new(BTreeMap::new()),
            rt,
        };
        registry
            .write_entries()
            .insert(registry.default_id.clone(), Entry::Ready(Arc::new(ctx)));
        registry.load_dir();
        Ok(registry)
    }

    pub fn default_id(&self) -> &str {
        &self.default_id
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn max(&self) -> usize {
        self.max
    }

    /// The cluster every header-less request is served by.
    pub fn default_ctx(&self) -> Arc<ClusterCtx> {
        match self.read_entries().get(&self.default_id) {
            Some(Entry::Ready(ctx)) => Arc::clone(ctx),
            // An invariant, not a runtime condition: the default entry is
            // inserted at construction and every mutation preserves it.
            _ => unreachable!("the default cluster entry must always be registered"),
        }
    }

    /// Resolves the `X-Curvine-Cluster` header value.
    ///
    /// `None` (header absent) selects the default cluster. An unknown or invalid
    /// (not loadable) id is a `NotFound`: nothing else can be served for it.
    pub fn resolve(&self, header: Option<&str>) -> Result<Arc<ClusterCtx>, ResolveError> {
        let id = header.map(str::trim).filter(|value| !value.is_empty());
        let Some(id) = id else {
            return Ok(self.default_ctx());
        };
        match self.read_entries().get(id) {
            Some(Entry::Ready(ctx)) => Ok(Arc::clone(ctx)),
            _ => Err(ResolveError::NotFound(id.to_string())),
        }
    }

    pub fn contains(&self, id: &str) -> bool {
        self.read_entries().contains_key(id)
    }

    /// Whitelisted projections of every registered cluster: the default first,
    /// then imported clusters by id.
    pub fn summary(&self) -> Vec<ClusterSummary> {
        let entries = self.read_entries();
        let mut out: Vec<ClusterSummary> = entries
            .iter()
            .map(|(id, entry)| summarize(&self.default_id, id, entry))
            .collect();
        out.sort_by(|a, b| {
            (a.source != "default", a.id.as_str()).cmp(&(b.source != "default", b.id.as_str()))
        });
        out
    }

    /// Registers a new imported cluster and persists its configuration.
    pub fn register_from_text(&self, text: &str) -> Result<Arc<ClusterCtx>, RegistryError> {
        let conf = ClusterConf::from_text_import(text)
            .map_err(|err| RegistryError::InvalidConfig(err.to_string()))?;
        let id = conf.cluster_id.clone();
        log_configuration_warnings(&id, &conf, text);
        if !is_valid_cluster_id(&id) {
            return Err(RegistryError::InvalidId(id));
        }
        if id == self.default_id {
            return Err(RegistryError::Exists(id));
        }
        {
            let entries = self.read_entries();
            if entries.contains_key(&id) {
                return Err(RegistryError::Exists(id));
            }
            if entries.len() >= self.max {
                return Err(RegistryError::Limit(self.max));
            }
        }

        let path = self.file_path(&id);
        let ctx = Arc::new(self.build_ctx(id.clone(), &conf, path)?);

        {
            let mut entries = self.write_entries();
            // Re-check under the write lock: a concurrent register of the same
            // id must not leave this context's collector running forever.
            if entries.contains_key(&id) {
                ctx.observability.stop();
                return Err(RegistryError::Exists(id));
            }
            if entries.len() >= self.max {
                ctx.observability.stop();
                return Err(RegistryError::Limit(self.max));
            }
            if let Err(err) = write_cluster_file(&self.dir, &id, text) {
                ctx.observability.stop();
                return Err(err);
            }
            entries.insert(id.clone(), Entry::Ready(Arc::clone(&ctx)));
        }

        info!("registered cluster '{}' from {}", id, self.dir.display());
        Ok(ctx)
    }

    /// Replaces an imported cluster's configuration (hot swap).
    ///
    /// The new context is built and validated *before* the old one is dropped,
    /// so a bad replacement leaves the running cluster untouched.
    pub fn replace_from_text(
        &self,
        id: &str,
        text: &str,
    ) -> Result<Arc<ClusterCtx>, RegistryError> {
        if id == self.default_id {
            return Err(RegistryError::NotRemovable(id.to_string()));
        }
        if !self.contains(id) {
            return Err(RegistryError::Unknown(id.to_string()));
        }
        let conf = ClusterConf::from_text_import(text)
            .map_err(|err| RegistryError::InvalidConfig(err.to_string()))?;
        log_configuration_warnings(id, &conf, text);
        if conf.cluster_id != id {
            return Err(RegistryError::InvalidConfig(format!(
                "cluster_id '{}' does not match the target cluster '{id}'",
                conf.cluster_id
            )));
        }

        let path = self.file_path(id);
        let ctx = Arc::new(self.build_ctx(id.to_string(), &conf, path)?);

        {
            let mut entries = self.write_entries();
            if !entries.contains_key(id) {
                ctx.observability.stop();
                return Err(RegistryError::Unknown(id.to_string()));
            }
            if let Err(err) = write_cluster_file(&self.dir, id, text) {
                ctx.observability.stop();
                return Err(err);
            }
            let previous = entries.insert(id.to_string(), Entry::Ready(Arc::clone(&ctx)));
            if let Some(Entry::Ready(previous)) = previous {
                previous.observability.stop();
            }
        }

        info!("replaced configuration of cluster '{}'", id);
        Ok(ctx)
    }

    /// Deletes an imported cluster (and its file).
    pub fn remove(&self, id: &str) -> Result<(), RegistryError> {
        if id == self.default_id {
            return Err(RegistryError::NotRemovable(id.to_string()));
        }

        let mut entries = self.write_entries();
        let Some(entry) = entries.get(id) else {
            return Err(RegistryError::Unknown(id.to_string()));
        };
        let path = entry
            .source_path()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.file_path(id));

        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(RegistryError::Io(format!(
                    "failed to delete {}: {err}",
                    path.display()
                )))
            }
        }
        if let Some(entry) = entries.remove(id) {
            entry.stop();
        }
        info!("removed cluster '{}'", id);
        Ok(())
    }

    /// Re-scans the cluster directory and returns the refreshed projections.
    ///
    /// Imported clusters are rebuilt from their files (which also picks up
    /// added, deleted and previously broken files); the default cluster is
    /// untouched. Rebuilding replaces the per-cluster clients, so it is an
    /// explicit admin action rather than something a request does implicitly.
    pub fn reload(&self) -> Vec<ClusterSummary> {
        self.load_dir();
        self.summary()
    }

    /// Reads the raw configuration text of an imported cluster from disk.
    ///
    /// Used by `GET /api/v1/clusters/:id/config` so the console can pre-fill its
    /// edit form: `PUT` needs the whole document, and the summary projection
    /// deliberately carries no configuration. The file is read as-is — comments,
    /// ordering and formatting are what the operator wrote, and rewriting it
    /// through the parser would silently drop them.
    ///
    /// Errors are shaped for a path parameter, where the only question is "is
    /// there such a cluster": the built-in default has no file (`NotRemovable`,
    /// reported as 409 by the route), and an unknown, malformed or vanished id
    /// is `Unknown` (404). A file that is not valid UTF-8 degrades the same way
    /// — from the console's point of view such a cluster has no readable text.
    ///
    /// Trust model: the returned document may carry `[client]` credentials. This
    /// is the same exposure as the existing `/api/config`, which already returns
    /// the raw `ClusterConf` of the selected cluster to any authenticated console
    /// session — the console reads this host's configuration files, and the
    /// `/api/` prefix is behind `require_auth`. It is not an accidental leak, so
    /// it is intentionally not narrowed further here.
    pub fn config_text(&self, id: &str) -> Result<String, RegistryError> {
        if id == self.default_id {
            return Err(RegistryError::NotRemovable(id.to_string()));
        }
        if !is_valid_cluster_id(id) {
            return Err(RegistryError::Unknown(id.to_string()));
        }

        let path = {
            let entries = self.read_entries();
            match entries.get(id) {
                None => return Err(RegistryError::Unknown(id.to_string())),
                Some(Entry::Ready(ctx)) => match ctx.source.path() {
                    Some(path) => path.to_path_buf(),
                    None => return Err(RegistryError::NotRemovable(id.to_string())),
                },
                // A file that stopped parsing is exactly what an operator wants to
                // open and repair, so its text stays readable.
                Some(Entry::Invalid { path, .. }) => path.clone(),
            }
        };

        fs::read_to_string(&path).map_err(|err| {
            warn!(
                "cannot read configuration of cluster '{}' from {}: {err}",
                id,
                path.display()
            );
            RegistryError::Unknown(id.to_string())
        })
    }

    fn build_ctx(
        &self,
        id: String,
        conf: &ClusterConf,
        path: PathBuf,
    ) -> Result<ClusterCtx, RegistryError> {
        // Same gate as the `/validate` dry run: a text the dry run rejects must
        // not be storable, and a stored one that stops passing degrades to
        // `Entry::Invalid` instead of exposing an unreachable cluster as ready.
        let errors = configuration_errors(conf);
        if !errors.is_empty() {
            return Err(RegistryError::InvalidConfig(errors.join("; ")));
        }

        let display_name = conf.web.display_name.trim().to_string();
        ClusterCtx::new(
            id,
            display_name,
            ClusterSource::File(path),
            conf.clone(),
            self.rt.clone(),
        )
        .map_err(|err| RegistryError::InvalidConfig(err.to_string()))
    }

    fn file_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.toml"))
    }

    fn read_entries(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<String, Entry>> {
        self.entries.read().unwrap_or_else(|err| err.into_inner())
    }

    fn write_entries(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<String, Entry>> {
        self.entries.write().unwrap_or_else(|err| err.into_inner())
    }

    /// Directory listing of `<dir>/*.toml`, sorted so loading is deterministic.
    fn scan_files(&self) -> Vec<(String, PathBuf)> {
        let read_dir = match fs::read_dir(&self.dir) {
            Ok(read_dir) => read_dir,
            Err(err) => {
                // A missing directory is the normal read-only-install case.
                if err.kind() != std::io::ErrorKind::NotFound {
                    warn!(
                        "cluster directory {} could not be read: {err}",
                        self.dir.display()
                    );
                }
                return Vec::new();
            }
        };

        let mut files: Vec<(String, PathBuf)> = read_dir
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let path = entry.path();
                if !path.is_file() || path.extension().and_then(|ext| ext.to_str()) != Some("toml")
                {
                    return None;
                }
                let stem = path.file_stem()?.to_str()?.to_string();
                // Skip dot files: the atomic writer's temporary files.
                if stem.starts_with('.') {
                    return None;
                }
                Some((stem, path))
            })
            .collect();
        files.sort();
        files
    }

    /// Rebuilds every imported entry from `<dir>`.
    fn load_dir(&self) {
        let files = self.scan_files();
        let capacity = self.max.saturating_sub(1);
        let mut next: BTreeMap<String, Entry> = BTreeMap::new();
        let mut loaded = 0usize;

        for (stem, path) in files {
            if loaded >= capacity {
                next.insert(
                    stem,
                    Entry::Invalid {
                        error: format!(
                            "cluster limit of {} reached; this cluster was not loaded",
                            self.max
                        ),
                        path,
                    },
                );
                continue;
            }

            let text = match fs::read_to_string(&path) {
                Ok(text) => text,
                Err(err) => {
                    next.insert(
                        stem,
                        Entry::Invalid {
                            error: format!("failed to read {}: {err}", path.display()),
                            path,
                        },
                    );
                    continue;
                }
            };

            match self.load_file(&stem, &path, &text) {
                Ok(ctx) => {
                    loaded += 1;
                    next.insert(stem, Entry::Ready(Arc::new(ctx)));
                }
                Err((key, error)) => {
                    next.insert(key, Entry::Invalid { error, path });
                }
            }
        }

        let mut entries = self.write_entries();
        let previous = std::mem::take(&mut *entries);
        for (id, entry) in previous {
            if id == self.default_id {
                entries.insert(id, entry);
            } else {
                // Anything the rescan did not reproduce must not keep
                // collecting metrics.
                entry.stop();
            }
        }
        entries.extend(next);
    }

    fn load_file(
        &self,
        stem: &str,
        path: &Path,
        text: &str,
    ) -> Result<ClusterCtx, (String, String)> {
        let conf = ClusterConf::from_text_import(text)
            .map_err(|err| (stem.to_string(), err.to_string()))?;
        let id = conf.cluster_id.clone();
        log_configuration_warnings(&id, &conf, text);
        if !is_valid_cluster_id(&id) {
            return Err((stem.to_string(), RegistryError::InvalidId(id).to_string()));
        }
        if id != stem {
            // Keeping id == file name is what makes remove/replace able to find
            // the file again, so a mismatch is reported instead of guessed at.
            return Err((
                stem.to_string(),
                format!("cluster_id '{id}' does not match the file name '{stem}.toml'"),
            ));
        }
        if id == self.default_id {
            return Err((
                id.clone(),
                format!("cluster_id '{id}' is the id of the default cluster"),
            ));
        }
        self.build_ctx(id, &conf, path.to_path_buf())
            .map_err(|err| (stem.to_string(), err.to_string()))
    }
}

impl Entry {
    fn source_path(&self) -> Option<&Path> {
        match self {
            Self::Ready(ctx) => ctx.source.path(),
            Self::Invalid { path, .. } => Some(path),
        }
    }

    /// Stops background work attached to this entry.
    fn stop(&self) {
        if let Self::Ready(ctx) = self {
            ctx.observability.stop();
        }
    }
}

fn summarize(default_id: &str, id: &str, entry: &Entry) -> ClusterSummary {
    match entry {
        Entry::Ready(ctx) => {
            let reachable = ctx
                .last_probe
                .lock()
                .ok()
                .and_then(|probe| probe.as_ref().map(|probe| probe.reachable));
            ClusterSummary {
                id: id.to_string(),
                display_name: ctx.label().to_string(),
                source: ctx.source.kind(),
                master_addrs: ctx.master_addrs(),
                status: match reachable {
                    Some(false) => "unreachable",
                    _ => "ready",
                },
                error: None,
                editable: !ctx.source.is_default(),
                removable: !ctx.source.is_default() && id != default_id,
            }
        }
        Entry::Invalid { error, .. } => ClusterSummary {
            id: id.to_string(),
            display_name: id.to_string(),
            source: "file",
            master_addrs: Vec::new(),
            status: "invalid",
            // A hand-edited file that stopped parsing reports the parser's reason,
            // which carries `(path/to/file.rs:line)` and a logger tag until it is
            // sanitised: this string goes straight to the cluster page.
            error: Some(sanitize_error_text(error)),
            // An unloadable file can still be fixed in place or deleted.
            editable: id != default_id,
            removable: id != default_id,
        },
    }
}

/// Writes `{dir}/{id}.toml` atomically: temp file → fsync → rename.
///
/// `0o600` because a cluster configuration may carry S3 credentials; a world
/// readable file would leak them past the console's auth.
fn write_cluster_file(dir: &Path, id: &str, text: &str) -> Result<(), RegistryError> {
    fs::create_dir_all(dir).map_err(|err| {
        RegistryError::Io(format!(
            "failed to create cluster directory {}: {err}",
            dir.display()
        ))
    })?;

    let path = dir.join(format!("{id}.toml"));
    let tmp = dir.join(format!(".{id}.toml.tmp"));

    let result = (|| -> std::io::Result<()> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&tmp)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, &path)?;
        // Persist the rename itself; best effort (some filesystems refuse to
        // open directories for fsync).
        let _ = fs::File::open(dir).and_then(|dir| dir.sync_all());
        Ok(())
    })();

    if let Err(err) = result {
        let _ = fs::remove_file(&tmp);
        return Err(RegistryError::Io(format!(
            "failed to write {}: {err}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::sync::OnceLock;

    /// `tempfile` is not a dev-dependency of this crate, so tests own the
    /// directory lifecycle themselves.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "curvine-cluster-registry-{tag}-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, AtomicOrdering::SeqCst)
            ));
            fs::create_dir_all(&path).expect("create temp dir");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// One runtime for every test: each one owns threads, and the registry only
    /// needs a handle to spawn on.
    fn test_rt() -> Arc<Runtime> {
        static RT: OnceLock<Arc<Runtime>> = OnceLock::new();
        Arc::clone(RT.get_or_init(|| Arc::new(Runtime::single())))
    }

    fn default_conf() -> ClusterConf {
        let mut conf = ClusterConf {
            cluster_id: "default-cluster".to_string(),
            ..Default::default()
        };
        conf.web.display_name = "Default".to_string();
        // Keep the tests from spawning collectors that scrape nothing.
        conf.web.observability.enabled = false;
        conf
    }

    fn cluster_toml(id: &str) -> String {
        format!(
            r#"
    cluster_id = "{id}"
    net_interface = ""

    [web]
    display_name = "{id} display"

    [master]
    hostname = "{id}-master.example"

    [journal]
    hostname = "{id}-journal.example"

    # Without this, the derived client address falls back to the built-in
    # journal default (localhost), exactly as for any other curvine config.
    [client]
    master_addrs = [{{ hostname = "{id}-master.example", port = 8995 }}]

    [web.observability]
    enabled = false
"#
        )
    }

    fn registry(dir: &TempDir, max: usize) -> ClusterRegistry {
        ClusterRegistry::new(default_conf(), test_rt(), dir.path().to_path_buf(), max)
            .expect("registry")
    }

    fn summary_for<'a>(summaries: &'a [ClusterSummary], id: &str) -> &'a ClusterSummary {
        summaries
            .iter()
            .find(|entry| entry.id == id)
            .unwrap_or_else(|| panic!("cluster '{id}' missing from {:?}", summaries.len()))
    }

    #[test]
    fn absent_header_selects_the_default_cluster() {
        let dir = TempDir::new("default");
        let registry = registry(&dir, 4);

        let ctx = registry.resolve(None).expect("default");
        assert_eq!(ctx.id, "default-cluster");
        assert!(ctx.source.is_default());

        // A blank header is the same as no header: proxies commonly forward it.
        let blank = registry.resolve(Some("   ")).expect("default");
        assert_eq!(blank.id, "default-cluster");
    }

    #[test]
    fn header_selects_the_imported_cluster() {
        let dir = TempDir::new("select");
        let registry = registry(&dir, 4);
        registry
            .register_from_text(&cluster_toml("cluster-b"))
            .expect("register");

        let ctx = registry.resolve(Some("cluster-b")).expect("cluster-b");
        assert_eq!(ctx.id, "cluster-b");
        assert_eq!(ctx.display_name, "cluster-b display");
        assert_eq!(ctx.master_addrs(), vec!["cluster-b-master.example:8995"]);
        assert_eq!(registry.resolve(None).unwrap().id, "default-cluster");
    }

    #[test]
    fn unknown_or_broken_cluster_is_not_found() {
        let dir = TempDir::new("unknown");
        fs::write(
            dir.path().join("broken.toml"),
            "cluster_id = \"broken\"\n[nope\n",
        )
        .expect("write broken file");
        let registry = registry(&dir, 4);

        // Listed, but not servable.
        let summaries = registry.summary();
        let broken = summary_for(&summaries, "broken");
        assert_eq!(broken.status, "invalid");
        assert!(broken.error.is_some());

        assert!(matches!(
            registry.resolve(Some("broken")),
            Err(ResolveError::NotFound(id)) if id == "broken"
        ));
        assert!(matches!(
            registry.resolve(Some("does-not-exist")),
            Err(ResolveError::NotFound(_))
        ));
    }

    #[test]
    fn a_broken_file_does_not_block_the_healthy_ones() {
        let dir = TempDir::new("degrade");
        fs::write(dir.path().join("broken.toml"), "this is not toml").expect("write");
        fs::write(dir.path().join("cluster-b.toml"), cluster_toml("cluster-b")).expect("write");
        let registry = registry(&dir, 8);

        assert!(registry.resolve(Some("cluster-b")).is_ok());
        assert!(registry.resolve(None).is_ok());
        let summaries = registry.summary();
        assert_eq!(summaries.len(), 3);
        assert_eq!(summary_for(&summaries, "cluster-b").status, "ready");
        assert_eq!(summary_for(&summaries, "broken").status, "invalid");
    }

    #[test]
    fn summary_never_leaks_configuration_text() {
        let dir = TempDir::new("projection");
        let registry = registry(&dir, 4);
        let conf_with_secret = format!(
            "{}\n[s3]\naccess_key = \"AKIA-SECRET\"\nsecret_key = \"TOP-SECRET\"\n",
            cluster_toml("cluster-secret")
        );
        registry
            .register_from_text(&conf_with_secret)
            .expect("register");

        let summaries = registry.summary();
        let summary = summary_for(&summaries, "cluster-secret");
        assert_eq!(summary.source, "file");
        assert!(summary.removable);
        assert!(summary.editable);

        let rendered = serde_json::to_string(&summaries).expect("serialize summaries");
        assert!(!rendered.contains("TOP-SECRET"), "{rendered}");
        assert!(!rendered.contains("AKIA-SECRET"), "{rendered}");
    }

    #[test]
    fn duplicate_id_is_rejected() {
        let dir = TempDir::new("duplicate");
        let registry = registry(&dir, 4);
        registry
            .register_from_text(&cluster_toml("cluster-b"))
            .expect("register");

        assert!(matches!(
            registry.register_from_text(&cluster_toml("cluster-b")),
            Err(RegistryError::Exists(id)) if id == "cluster-b"
        ));
        // The default cluster cannot be re-imported either.
        assert!(matches!(
            registry.register_from_text(&cluster_toml("default-cluster")),
            Err(RegistryError::Exists(_))
        ));
        assert_eq!(registry.summary().len(), 2);
    }

    #[test]
    fn cluster_limit_is_enforced_counting_the_default() {
        let dir = TempDir::new("limit");
        let registry = registry(&dir, 2);
        registry
            .register_from_text(&cluster_toml("cluster-b"))
            .expect("first import fits");

        assert!(matches!(
            registry.register_from_text(&cluster_toml("cluster-c")),
            Err(RegistryError::Limit(2))
        ));
        assert!(!dir.path().join("cluster-c.toml").exists());
    }

    #[test]
    fn registered_file_is_private() {
        let dir = TempDir::new("mode");
        let registry = registry(&dir, 4);
        registry
            .register_from_text(&cluster_toml("cluster-b"))
            .expect("register");

        let path = dir.path().join("cluster-b.toml");
        assert!(path.exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).expect("metadata").permissions().mode();
            // Cluster files may embed S3 credentials: never group/world readable.
            assert_eq!(mode & 0o777, 0o600, "mode {:o}", mode & 0o777);
        }
    }

    #[test]
    fn remove_deletes_the_file_but_refuses_the_default() {
        let dir = TempDir::new("remove");
        let registry = registry(&dir, 4);
        registry
            .register_from_text(&cluster_toml("cluster-b"))
            .expect("register");

        assert!(matches!(
            registry.remove("default-cluster"),
            Err(RegistryError::NotRemovable(_))
        ));
        assert!(registry.remove("cluster-b").is_ok());
        assert!(!dir.path().join("cluster-b.toml").exists());
        assert!(matches!(
            registry.remove("cluster-b"),
            Err(RegistryError::Unknown(_))
        ));
        assert_eq!(registry.summary().len(), 1);
    }

    #[test]
    fn replace_swaps_the_configuration_and_keeps_the_id() {
        let dir = TempDir::new("replace");
        let registry = registry(&dir, 4);
        registry
            .register_from_text(&cluster_toml("cluster-b"))
            .expect("register");

        let updated =
            cluster_toml("cluster-b").replace("cluster-b-master.example", "new-master.example");
        let ctx = registry
            .replace_from_text("cluster-b", &updated)
            .expect("replace");
        assert_eq!(ctx.master_addrs(), vec!["new-master.example:8995"]);
        assert_eq!(
            registry.resolve(Some("cluster-b")).unwrap().master_addrs(),
            vec!["new-master.example:8995"]
        );
        assert!(fs::read_to_string(dir.path().join("cluster-b.toml"))
            .expect("read file")
            .contains("new-master.example"));

        // A replacement whose id disagrees with the target is refused, and the
        // running cluster keeps its previous configuration.
        assert!(matches!(
            registry.replace_from_text("cluster-b", &cluster_toml("cluster-c")),
            Err(RegistryError::InvalidConfig(_))
        ));
        assert_eq!(
            registry.resolve(Some("cluster-b")).unwrap().master_addrs(),
            vec!["new-master.example:8995"]
        );
    }

    #[test]
    fn reload_picks_up_added_removed_and_repaired_files() {
        let dir = TempDir::new("reload");
        fs::write(dir.path().join("cluster-b.toml"), cluster_toml("cluster-b")).expect("write");
        fs::write(dir.path().join("cluster-c.toml"), "not toml at all").expect("write");
        let registry = registry(&dir, 8);

        assert_eq!(
            summary_for(&registry.summary(), "cluster-c").status,
            "invalid"
        );

        // Repair the broken file, add a new one and delete an existing one.
        fs::write(dir.path().join("cluster-c.toml"), cluster_toml("cluster-c")).expect("write");
        fs::write(dir.path().join("cluster-d.toml"), cluster_toml("cluster-d")).expect("write");
        fs::remove_file(dir.path().join("cluster-b.toml")).expect("remove");

        let summaries = registry.reload();
        assert_eq!(summaries.len(), 3);
        assert_eq!(summary_for(&summaries, "cluster-c").status, "ready");
        assert_eq!(summary_for(&summaries, "cluster-d").status, "ready");
        assert!(summaries.iter().all(|entry| entry.id != "cluster-b"));
        // The default entry survives a reload.
        assert_eq!(summaries[0].id, "default-cluster");
        assert_eq!(summaries[0].source, "default");
        assert!(!summaries[0].removable);
    }

    #[test]
    fn missing_directory_neither_fails_nor_is_created() {
        let dir = TempDir::new("missing");
        let nested = dir.path().join("no-such-dir");
        let registry =
            ClusterRegistry::new(default_conf(), test_rt(), nested.clone(), 4).expect("registry");

        assert_eq!(registry.summary().len(), 1);
        assert!(!nested.exists(), "startup must not create the directory");
    }

    #[test]
    fn file_name_must_match_the_declared_cluster_id() {
        let dir = TempDir::new("mismatch");
        fs::write(
            dir.path().join("other-name.toml"),
            cluster_toml("cluster-b"),
        )
        .expect("write");
        let registry = registry(&dir, 4);

        let summaries = registry.summary();
        let entry = summary_for(&summaries, "other-name");
        assert_eq!(entry.status, "invalid");
        assert!(entry
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("does not match"));
    }

    // -- structural checks (shared with the `/validate` dry run) -------------

    fn conf_from(text: &str) -> ClusterConf {
        ClusterConf::from_text_import(text).expect("importable configuration")
    }

    #[test]
    fn a_configuration_without_any_master_address_is_rejected() {
        let mut conf = default_conf();
        conf.client.master_addrs.clear();
        conf.journal.journal_addrs.clear();

        let errors = configuration_errors(&conf);
        assert!(
            errors
                .iter()
                .any(|error| error.contains("no master address")),
            "{errors:?}"
        );
    }

    #[test]
    fn a_zero_rpc_port_is_rejected() {
        let conf = conf_from(
            r#"
    cluster_id = "cluster-b"

    [master]
    rpc_port = 0

    [client]
    master_addrs = [{ hostname = "cluster-b.example", port = 8995 }]
"#,
        );

        let errors = configuration_errors(&conf);
        assert!(
            errors
                .iter()
                .any(|error| error.contains("master.rpc_port is 0")),
            "{errors:?}"
        );
    }

    #[test]
    fn an_address_with_a_zero_or_empty_part_is_rejected() {
        use curvine_net::net::InetAddr;

        let mut conf = default_conf();
        conf.client.master_addrs = vec![InetAddr::new("", 0)];

        let errors = configuration_errors(&conf);
        assert!(
            errors.iter().any(|error| error.contains("empty host")),
            "{errors:?}"
        );
        assert!(
            errors.iter().any(|error| error.contains("uses port 0")),
            "{errors:?}"
        );
    }

    #[test]
    fn a_usable_configuration_has_no_errors_and_no_warnings() {
        let text = cluster_toml("cluster-b");
        let conf = conf_from(&text);

        assert!(configuration_errors(&conf).is_empty());
        assert!(
            configuration_warnings(&conf, &text).is_empty(),
            "{:?}",
            configuration_warnings(&conf, &text)
        );
    }

    #[test]
    fn missing_master_addresses_warn_about_the_localhost_fallback() {
        // The regression from the E2E run: this parses fine and derives
        // localhost:8995, which is almost never the intended cluster.
        let text = r#"
    cluster_id = "cluster-b"

    [master]
    hostname = "cluster-b.example"
"#;
        let conf = conf_from(text);

        assert!(configuration_errors(&conf).is_empty());
        let warnings = configuration_warnings(&conf, text);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("localhost:8995"), "{warnings:?}");
    }

    #[test]
    fn loopback_only_master_addresses_are_warned_about() {
        let text = r#"
    cluster_id = "cluster-b"

    [client]
    master_addrs = [{ hostname = "127.0.0.1", port = 8995 }]
"#;
        let conf = conf_from(text);

        let warnings = configuration_warnings(&conf, text);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("loopback"), "{warnings:?}");
    }

    #[test]
    fn a_structural_problem_is_not_storable() {
        let dir = TempDir::new("structural");
        let registry = registry(&dir, 4);

        // Same gate as `/validate`: what the dry run rejects, `create` refuses.
        let result = registry.register_from_text(
            r#"
    cluster_id = "cluster-b"

    [master]
    rpc_port = 0

    [client]
    master_addrs = [{ hostname = "cluster-b.example", port = 8995 }]
"#,
        );
        let err = match result {
            Ok(_) => panic!("port 0 is not a usable master port"),
            Err(err) => err,
        };
        assert!(matches!(err, RegistryError::InvalidConfig(_)), "{err:?}");
        assert!(
            !dir.path().join("cluster-b.toml").exists(),
            "a rejected import must not be written"
        );
    }

    #[test]
    fn cluster_id_shape_is_lowercase_and_bounded() {
        for valid in [
            "b",
            "beta",
            "beta_2",
            "beta-2",
            &"a".repeat(MAX_CLUSTER_ID_LEN),
        ] {
            assert!(is_valid_cluster_id(valid), "'{valid}' should be accepted");
        }

        // Uppercase is rejected because the id is also a file name: on a
        // case-insensitive filesystem `beta` and `BETA` are the same file.
        for invalid in [
            "",
            "BETA",
            "Beta",
            "-beta",
            "beta.toml",
            "beta/beta",
            "beta beta",
            "béta",
            &"a".repeat(MAX_CLUSTER_ID_LEN + 1),
        ] {
            assert!(
                !is_valid_cluster_id(invalid),
                "'{invalid}' should be rejected"
            );
        }
    }

    #[test]
    fn uppercase_id_is_refused_and_nothing_is_written() {
        let dir = TempDir::new("uppercase");
        let registry = registry(&dir, 4);

        let err = match registry.register_from_text(&cluster_toml("BETA")) {
            Ok(_) => panic!("an uppercase id must be refused"),
            Err(err) => err,
        };
        match &err {
            RegistryError::InvalidId(id) => assert_eq!(id, "BETA"),
            other => panic!("expected InvalidId, got {other:?}"),
        }
        // The message names the rule, because it is rendered in the UI verbatim.
        assert!(
            err.to_string().contains("ids must match [a-z0-9_-]{1,63}"),
            "{err}"
        );
        assert!(
            !dir.path().join("BETA.toml").exists(),
            "a refused id must not create a file"
        );
        assert!(registry.summary().iter().all(|entry| entry.id != "BETA"));
    }

    #[test]
    fn configuration_errors_reports_an_unusable_id() {
        // `/validate` reaches the id rule through this function, so the dry run
        // cannot answer `ok: true` for a text `create` would refuse.
        let conf = ClusterConf {
            cluster_id: "BETA".to_string(),
            ..default_conf()
        };
        let errors = configuration_errors(&conf);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(
            errors[0].contains("invalid cluster id 'BETA'"),
            "{errors:?}"
        );
    }

    #[test]
    fn config_text_returns_the_file_verbatim() {
        let dir = TempDir::new("config-text");
        let registry = registry(&dir, 4);
        // Comments must survive: the console round-trips this text through PUT.
        let text = format!("# hand written\n{}", cluster_toml("cluster-b"));
        registry.register_from_text(&text).expect("register");

        assert_eq!(registry.config_text("cluster-b").expect("text"), text);
    }

    #[test]
    fn config_text_refuses_the_default_cluster() {
        let dir = TempDir::new("config-text-default");
        let registry = registry(&dir, 4);

        // 409, not 404: the cluster exists but is not editable (same answer as PUT).
        let err = registry
            .config_text("default-cluster")
            .expect_err("default");
        assert!(matches!(err, RegistryError::NotRemovable(_)), "{err:?}");
    }

    #[test]
    fn config_text_is_not_found_for_unknown_or_malformed_ids() {
        let dir = TempDir::new("config-text-unknown");
        let registry = registry(&dir, 4);

        for id in ["cluster-b", "BETA", "../etc/passwd", ""] {
            let err = registry
                .config_text(id)
                .expect_err(&format!("'{id}' must not resolve"));
            assert!(
                matches!(err, RegistryError::Unknown(_)),
                "'{id}' -> {err:?}"
            );
        }
    }

    #[test]
    fn config_text_stays_readable_for_a_file_that_stopped_parsing() {
        let dir = TempDir::new("config-text-broken");
        // A hand-edited file is exactly what an operator wants to open and repair,
        // so a broken entry keeps its text available.
        fs::write(
            dir.path().join("broken.toml"),
            "cluster_id = \"broken\"\n= nope\n",
        )
        .expect("write");

        let registry = registry(&dir, 4);
        let text = registry.config_text("broken").expect("text");
        assert!(text.contains("= nope"), "{text}");
    }
}
