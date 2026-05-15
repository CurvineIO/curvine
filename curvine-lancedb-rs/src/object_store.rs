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
// WITHOUT WARRANTIES OR ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeSet, HashMap};
use std::env;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::result::Result as StdResult;
use std::sync::Arc;

use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use curvine_client::file::CurvineFileSystem;
use curvine_common::conf::ClusterConf;
use curvine_common::error::FsError;
use curvine_common::fs::{Path as CurvinePath, Reader, Writer};
use curvine_common::state::FileStatus;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use lance_core::error::Result;
use lance_core::Error as LanceError;
use lance_io::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreProvider, StorageOptions,
    DEFAULT_CLOUD_IO_PARALLELISM,
};
use lancedb_upstream::ObjectStoreRegistry;
use lancedb_upstream::Session;
use md5::{Digest, Md5};
use object_store::path::Path;
use object_store::{
    Attributes, Error as OsError, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore as ObjectStoreTrait, PutMode, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, Result as OsResult, UploadPart,
};
use tokio::sync::Mutex;
use url::Url;
use uuid::Uuid;

pub const CURVINE_SCHEME: &str = "curvine";

pub const CURVINE_CONF_FILE_KEY: &str = "curvine.conf.path";

const COPY_CHUNK_BYTES: usize = 1024 * 1024;
const MULTIPART_STAGING_ROOT: &str = "/.curvine/lancedb/multipart";
const INTERNAL_RESERVED_ROOT: &str = ".curvine";

#[derive(Clone)]
struct CurvineContext {
    fs: CurvineFileSystem,
    workspace_root: CurvinePath,
}

impl Debug for CurvineContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("CurvineContext")
            .field("workspace_root", &self.workspace_root.full_path())
            .finish()
    }
}

#[derive(Clone)]
pub struct CurvineObjectStore {
    context: Arc<CurvineContext>,
}

#[derive(Debug)]
struct CurvineMultipartUpload {
    store: CurvineObjectStore,
    upload_id: String,
    dest: Path,
    next_part: usize,
    completed_parts: Arc<Mutex<Vec<CompletedPart>>>,
}

#[derive(Debug, Clone)]
struct CompletedPart {
    part_idx: usize,
    etag: String,
    path: CurvinePath,
}

impl Debug for CurvineObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("CurvineObjectStore")
            .field("workspace_root", &self.context.workspace_root.full_path())
            .finish()
    }
}

impl Display for CurvineObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "CurvineObjectStore({})",
            self.context.workspace_root.full_path()
        )
    }
}

/// [`ObjectStoreProvider`] for `curvine://` URIs.
///
/// The opening URI is turned into one absolute Curvine workspace path (authority, when present,
/// is the first path segment: `curvine://tenant/a` → `/tenant/a`). [`ObjectStoreProvider::extract_path`]
/// applies the same validation and returns an empty [`Path`] because Lance object
/// keys are relative to that workspace root. See `docs/phase4-curvine-object-store-and-lancedb.md` in this crate.
#[derive(Debug, Clone, Default)]
pub struct CurvineObjectStoreProvider;

impl CurvineObjectStoreProvider {
    pub fn new() -> Self {
        Self
    }

    fn create_context(
        &self,
        base_path: &Url,
        params: &ObjectStoreParams,
    ) -> Result<Arc<CurvineContext>> {
        let conf_path =
            resolve_curvine_conf_path(params).ok_or_else(missing_curvine_config_error)?;

        let conf = ClusterConf::from(&conf_path).map_err(|e| {
            LanceError::invalid_input(format!(
                "Failed to load Curvine configuration from '{}': {e}",
                conf_path
            ))
        })?;

        let rt = Arc::new(conf.client_rpc_conf().create_runtime());
        let fs = CurvineFileSystem::with_rt(conf, rt).map_err(|e| {
            LanceError::invalid_input(format!(
                "Failed to initialize Curvine filesystem (config '{}'): {e}",
                conf_path
            ))
        })?;

        let workspace_root = curvine_workspace_root_from_uri(base_path).map_err(|e| {
            LanceError::invalid_input(format!(
                "Invalid curvine:// workspace URI '{}': {e}",
                base_path
            ))
        })?;

        Ok(Arc::new(CurvineContext { fs, workspace_root }))
    }
}

fn resolve_curvine_conf_path(params: &ObjectStoreParams) -> Option<String> {
    params
        .storage_options()
        .and_then(|opts| opts.get(CURVINE_CONF_FILE_KEY))
        .cloned()
        .or_else(|| env::var(ClusterConf::ENV_CONF_FILE).ok())
}

fn missing_curvine_config_error() -> LanceError {
    LanceError::invalid_input(format!(
        "Missing Curvine cluster configuration: set storage option `{CURVINE_CONF_FILE_KEY}` \
         (highest priority) or environment variable `{}` to the Curvine client configuration file path.",
        ClusterConf::ENV_CONF_FILE
    ))
}

fn curvine_reader_stream(
    mut reader: impl Reader + Send + 'static,
    location: Path,
    range: std::ops::Range<u64>,
) -> impl futures::Stream<Item = OsResult<Bytes>> {
    stream! {
        let mut remaining = range.end.saturating_sub(range.start) as usize;
        while remaining > 0 {
            let chunk = match reader.async_read(Some(remaining)).await {
                Ok(chunk) => chunk,
                Err(e) => {
                    yield Err(fs_error_to_object_store(&location, e));
                    return;
                }
            };

            if chunk.is_empty() {
                break;
            }

            remaining = remaining.saturating_sub(chunk.len());
            yield Ok(chunk.to_bytes());
        }

        if let Err(e) = reader.complete().await {
            yield Err(fs_error_to_object_store(&location, e));
        }
    }
}

#[async_trait]
impl ObjectStoreProvider for CurvineObjectStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let context = self.create_context(&base_path, params)?;
        let storage_options = StorageOptions(params.storage_options().cloned().unwrap_or_default());
        let download_retry_count = storage_options.download_retry_count();

        let prefix = ObjectStoreProvider::calculate_object_store_prefix(
            self,
            &base_path,
            params.storage_options(),
        )?;

        let mut store = ObjectStore::new(
            Arc::new(CurvineObjectStore { context }),
            base_path,
            params.block_size,
            None,
            params.use_constant_size_upload_parts,
            params.list_is_lexically_ordered.unwrap_or(false),
            DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count,
            params.storage_options(),
        );
        store.store_prefix = prefix;
        Ok(store)
    }

    /// The opening `curvine://...` URI identifies the workspace root, not an object key.
    ///
    /// We therefore validate the URI with the same absolute-path merger used for
    /// `workspace_root`, and then return an empty relative [`Path`]. All later
    /// `head/get/put/list/...` calls operate on keys that are relative to that
    /// workspace root.
    fn extract_path(&self, url: &Url) -> Result<Path> {
        curvine_workspace_root_from_uri(url).map_err(|e| {
            LanceError::invalid_input(format!("Invalid curvine:// URI `{}`: {e}", url))
        })?;
        Ok(Path::default())
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        let host = url.host_str().unwrap_or("");
        let path = url.path().trim_end_matches('/');
        Ok(format!("curvine${host}{path}"))
    }
}

#[async_trait]
impl ObjectStoreTrait for CurvineObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        if !opts.attributes.is_empty() {
            return Err(OsError::NotImplemented);
        }

        let path = self.object_path(location)?;
        let overwrite = match opts.mode {
            PutMode::Overwrite => true,
            PutMode::Create => false,
            PutMode::Update(_) => return Err(OsError::NotImplemented),
        };
        let mut writer = self
            .context
            .fs
            .create(&path, overwrite)
            .await
            .map_err(|e| fs_error_to_object_store(location, e))?;

        for chunk in payload.iter() {
            writer
                .write(chunk)
                .await
                .map_err(|e| fs_error_to_object_store(location, e))?;
        }

        writer
            .complete()
            .await
            .map_err(|e| fs_error_to_object_store(location, e))?;

        let meta = self.head(location).await?;

        Ok(PutResult {
            e_tag: meta.e_tag,
            version: meta.version,
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        if !opts.attributes.is_empty() {
            return Err(OsError::NotImplemented);
        }

        let upload_id = Uuid::new_v4().to_string();
        let upload_dir = self.multipart_dir(&upload_id)?;
        self.context
            .fs
            .mkdir(&upload_dir, true)
            .await
            .map_err(|e| fs_error_to_object_store(location, e))?;

        Ok(Box::new(CurvineMultipartUpload {
            store: self.clone(),
            upload_id,
            dest: location.clone(),
            next_part: 0,
            completed_parts: Arc::new(Mutex::new(Vec::new())),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        if options.version.is_some() {
            return Err(OsError::NotImplemented);
        }

        if options.head {
            let meta = self.head(location).await?;
            options.check_preconditions(&meta)?;
            let stream = stream::once(async move { Ok::<Bytes, OsError>(Bytes::new()) }).boxed();
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                meta,
                range: 0..0,
                attributes: Attributes::default(),
            });
        }

        let cv_path = self.object_path(location)?;
        let meta = self.head(location).await?;
        options.check_preconditions(&meta)?;

        let mut reader = self
            .context
            .fs
            .open(&cv_path)
            .await
            .map_err(|e| fs_error_to_object_store(location, e))?;

        let range = match options.range {
            Some(range) => range
                .as_range(meta.size)
                .map_err(|source| OsError::Generic {
                    store: CURVINE_SCHEME,
                    source: Box::new(source),
                })?,
            None => 0..meta.size,
        };

        if range.start > 0 {
            reader
                .seek(range.start as i64)
                .await
                .map_err(|e| fs_error_to_object_store(location, e))?;
        }

        let stream = curvine_reader_stream(reader, location.clone(), range.clone()).boxed();

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range,
            attributes: Attributes::default(),
        })
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        let cv_path = self.object_path(location)?;
        let status = self
            .context
            .fs
            .get_status(&cv_path)
            .await
            .map_err(|e| fs_error_to_object_store(location, e))?;

        if status.is_dir {
            return Err(OsError::NotFound {
                path: location.to_string(),
                source: "directory prefixes are not object heads".into(),
            });
        }

        Ok(file_status_to_object_meta(location.clone(), status))
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        let cv_path = self.object_path(location)?;
        self.context
            .fs
            .delete(&cv_path, false)
            .await
            .map_err(|e| fs_error_to_object_store(location, e))?;
        let _ = self.prune_empty_parents(&cv_path, location).await;
        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        Box::pin(stream! {
            let metas = match store.collect_under_prefix(prefix.as_ref()).await {
                Ok(m) => m,
                Err(err) => {
                    yield Err(err);
                    return;
                }
            };
            for meta in metas {
                yield Ok(meta);
            }
        })
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        let root_path = match prefix {
            Some(prefix) => self.object_path(prefix)?,
            None => self.context.workspace_root.clone(),
        };

        let statuses = self
            .list_curvine_dir_or_empty(&root_path, prefix.unwrap_or(&Path::default()))
            .await?;

        let base_prefix = prefix.cloned().unwrap_or_default();
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();

        for status in statuses {
            let entry_location = relative_object_path(&self.context.workspace_root, &status.path)
                .map_err(|msg| OsError::Generic {
                store: CURVINE_SCHEME,
                source: msg.into(),
            })?;
            if self.is_internal_reserved_location(&entry_location) {
                continue;
            }

            let (first, nested) = {
                let mut parts = match entry_location.prefix_match(&base_prefix) {
                    Some(parts) => parts,
                    None => continue,
                };

                let first = match parts.next() {
                    Some(p) => p,
                    None => continue,
                };

                let nested = parts.next().is_some();
                (first, nested)
            };

            if nested {
                continue;
            }

            if status.is_dir {
                let prefix = base_prefix.child(first);
                if self.is_internal_reserved_location(&prefix) {
                    continue;
                }
                common_prefixes.insert(prefix);
            } else {
                objects.push(file_status_to_object_meta(entry_location, status));
            }
        }

        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    /// Read object `from` fully and write to `to`. The destination is opened with replace semantics
    /// (`create(..., overwrite = true)`): an existing object at `to` is replaced. The source object
    /// is left unchanged (copy, not move).
    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        let from_cv = self.object_path(from)?;
        let to_cv = self.object_path(to)?;
        let meta = self.head(from).await?;
        let size = meta.size;

        let mut reader = self
            .context
            .fs
            .open(&from_cv)
            .await
            .map_err(|e| fs_error_to_object_store(from, e))?;

        let mut writer = self
            .context
            .fs
            .create(&to_cv, true)
            .await
            .map_err(|e| fs_error_to_object_store(to, e))?;

        self.stream_copy_contents(from, to, size, &mut reader, &mut writer)
            .await?;
        reader
            .complete()
            .await
            .map_err(|e| fs_error_to_object_store(from, e))?;
        writer
            .complete()
            .await
            .map_err(|e| fs_error_to_object_store(to, e))?;
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        let from_cv = self.object_path(from)?;
        let to_cv = self.object_path(to)?;
        let meta = self.head(from).await?;
        let size = meta.size;

        let mut reader = self
            .context
            .fs
            .open(&from_cv)
            .await
            .map_err(|e| fs_error_to_object_store(from, e))?;

        let mut writer = self
            .context
            .fs
            .create(&to_cv, false)
            .await
            .map_err(|e| fs_error_to_object_store(to, e))?;

        let copy_result = self
            .stream_copy_contents(from, to, size, &mut reader, &mut writer)
            .await;

        let finalize_result = async {
            copy_result?;
            reader
                .complete()
                .await
                .map_err(|e| fs_error_to_object_store(from, e))?;
            writer
                .complete()
                .await
                .map_err(|e| fs_error_to_object_store(to, e))?;
            Ok(())
        }
        .await;

        if finalize_result.is_err() {
            let _ = self.context.fs.delete(&to_cv, false).await;
        }

        finalize_result
    }
}

impl CurvineObjectStore {
    fn object_path(&self, location: &Path) -> OsResult<CurvinePath> {
        let rel = location.as_ref().trim_start_matches('/');
        if self.is_root_workspace() && is_internal_reserved_relative_path(rel) {
            return Err(OsError::NotSupported {
                source: format!(
                    "`{INTERNAL_RESERVED_ROOT}` is a reserved Curvine namespace for root workspaces"
                )
                .into(),
            });
        }

        let base = self
            .context
            .workspace_root
            .full_path()
            .trim_end_matches('/');

        let full = if rel.is_empty() {
            base.to_string()
        } else {
            format!("{base}/{rel}")
        };

        CurvinePath::from_str(full).map_err(|e| OsError::Generic {
            store: CURVINE_SCHEME,
            source: e.to_string().into(),
        })
    }

    async fn list_curvine_dir_or_empty(
        &self,
        dir: &CurvinePath,
        err_location: &Path,
    ) -> OsResult<Vec<FileStatus>> {
        match self.context.fs.list_status(dir).await {
            Ok(entries) => Ok(entries),
            Err(e)
                if matches!(
                    &e,
                    FsError::FileNotFound(_) | FsError::Expired(_) | FsError::JobNotFound(_)
                ) =>
            {
                Ok(Vec::new())
            }
            Err(e) => Err(fs_error_to_object_store(err_location, e)),
        }
    }

    async fn collect_under_prefix(&self, prefix: Option<&Path>) -> OsResult<Vec<ObjectMeta>> {
        let root_path = match prefix {
            Some(prefix) => self.object_path(prefix)?,
            None => self.context.workspace_root.clone(),
        };

        let mut out = Vec::new();
        self.collect_files_recursive(&root_path, &mut out).await?;
        Ok(out)
    }

    async fn collect_files_recursive(
        &self,
        dir: &CurvinePath,
        out: &mut Vec<ObjectMeta>,
    ) -> OsResult<()> {
        let statuses = self
            .list_curvine_dir_or_empty(dir, &Path::default())
            .await?;

        for status in statuses {
            if status.is_dir {
                let child = CurvinePath::from_str(&status.path).map_err(|e| OsError::Generic {
                    store: CURVINE_SCHEME,
                    source: e.to_string().into(),
                })?;
                if self.is_multipart_internal_dir(&child) {
                    continue;
                }
                Box::pin(self.collect_files_recursive(&child, out)).await?;
            } else {
                let path = relative_object_path(&self.context.workspace_root, &status.path)
                    .map_err(|msg| OsError::Generic {
                        store: CURVINE_SCHEME,
                        source: msg.into(),
                    })?;
                if self.is_internal_reserved_location(&path) {
                    continue;
                }
                out.push(file_status_to_object_meta(path, status));
            }
        }

        Ok(())
    }

    fn multipart_dir(&self, upload_id: &str) -> OsResult<CurvinePath> {
        let workspace_id = workspace_staging_id(&self.context.workspace_root);
        CurvinePath::from_str(format!(
            "{}/{}/{}",
            MULTIPART_STAGING_ROOT, workspace_id, upload_id
        ))
        .map_err(|e| OsError::Generic {
            store: CURVINE_SCHEME,
            source: e.to_string().into(),
        })
    }

    fn multipart_part_path(&self, upload_id: &str, part_idx: usize) -> OsResult<CurvinePath> {
        let workspace_id = workspace_staging_id(&self.context.workspace_root);
        CurvinePath::from_str(format!(
            "{}/{}/{}/part-{:08}",
            MULTIPART_STAGING_ROOT, workspace_id, upload_id, part_idx
        ))
        .map_err(|e| OsError::Generic {
            store: CURVINE_SCHEME,
            source: e.to_string().into(),
        })
    }

    fn multipart_final_path(&self, upload_id: &str) -> OsResult<CurvinePath> {
        let workspace_id = workspace_staging_id(&self.context.workspace_root);
        CurvinePath::from_str(format!(
            "{}/{}/{}/final",
            MULTIPART_STAGING_ROOT, workspace_id, upload_id
        ))
        .map_err(|e| OsError::Generic {
            store: CURVINE_SCHEME,
            source: e.to_string().into(),
        })
    }

    async fn cleanup_multipart(&self, upload_id: &str) -> OsResult<()> {
        let dir = self.multipart_dir(upload_id)?;
        match self.context.fs.delete(&dir, true).await {
            Ok(_) => Ok(()),
            Err(FsError::FileNotFound(_))
            | Err(FsError::Expired(_))
            | Err(FsError::JobNotFound(_)) => Ok(()),
            Err(e) => Err(fs_error_to_object_store(&Path::default(), e)),
        }
    }

    async fn prepare_multipart_destination(
        &self,
        dest: &CurvinePath,
        location: &Path,
    ) -> OsResult<()> {
        match self.context.fs.get_status(dest).await {
            Ok(status) if status.is_dir => {
                return Err(OsError::AlreadyExists {
                    path: location.to_string(),
                    source: "multipart destination is a directory prefix".into(),
                });
            }
            Ok(_) => {}
            Err(FsError::FileNotFound(_))
            | Err(FsError::Expired(_))
            | Err(FsError::JobNotFound(_)) => {}
            Err(e) => return Err(fs_error_to_object_store(location, e)),
        }

        if let Some(parent) = dest.parent().map_err(|e| OsError::Generic {
            store: CURVINE_SCHEME,
            source: e.to_string().into(),
        })? {
            if !parent.is_root() {
                self.context
                    .fs
                    .mkdir(&parent, true)
                    .await
                    .map_err(|e| fs_error_to_object_store(location, e))?;
            }
        }

        Ok(())
    }

    async fn prune_empty_parents(&self, path: &CurvinePath, location: &Path) -> OsResult<()> {
        let workspace_root = self
            .context
            .workspace_root
            .full_path()
            .trim_end_matches('/');
        let mut current = path.parent().map_err(|e| OsError::Generic {
            store: CURVINE_SCHEME,
            source: e.to_string().into(),
        })?;

        while let Some(dir) = current {
            let full_path = dir.full_path().trim_end_matches('/').to_string();
            if full_path.is_empty() || full_path == "/" || full_path == workspace_root {
                break;
            }
            if !full_path.starts_with(&format!("{workspace_root}/")) {
                break;
            }

            match self.context.fs.delete(&dir, false).await {
                Ok(()) => {
                    current = dir.parent().map_err(|e| OsError::Generic {
                        store: CURVINE_SCHEME,
                        source: e.to_string().into(),
                    })?;
                }
                Err(FsError::DirNotEmpty(_))
                | Err(FsError::FileNotFound(_))
                | Err(FsError::Expired(_))
                | Err(FsError::JobNotFound(_)) => break,
                Err(e) => return Err(fs_error_to_object_store(location, e)),
            }
        }

        Ok(())
    }

    async fn stream_copy_contents(
        &self,
        from: &Path,
        to: &Path,
        size: u64,
        reader: &mut impl Reader,
        writer: &mut impl Writer,
    ) -> OsResult<()> {
        let mut remaining = size as usize;
        let mut buf = vec![0u8; COPY_CHUNK_BYTES];
        while remaining > 0 {
            let take = remaining.min(COPY_CHUNK_BYTES);
            let n = reader
                .read_full(&mut buf[..take])
                .await
                .map_err(|e| fs_error_to_object_store(from, e))?;
            if n == 0 {
                break;
            }
            writer
                .write(&buf[..n])
                .await
                .map_err(|e| fs_error_to_object_store(to, e))?;
            remaining = remaining.saturating_sub(n);
        }

        Ok(())
    }

    fn is_multipart_internal_dir(&self, dir: &CurvinePath) -> bool {
        if !self.is_root_workspace() {
            return false;
        }

        let dir = dir.full_path().trim_end_matches('/');
        dir == format!("/{INTERNAL_RESERVED_ROOT}")
            || dir.starts_with(&format!("/{INTERNAL_RESERVED_ROOT}/"))
    }

    fn is_internal_reserved_location(&self, location: &Path) -> bool {
        self.is_root_workspace()
            && is_internal_reserved_relative_path(location.as_ref().trim_start_matches('/'))
    }

    fn is_root_workspace(&self) -> bool {
        self.context.workspace_root.full_path() == "/"
    }
}

#[async_trait]
impl MultipartUpload for CurvineMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let store = self.store.clone();
        let upload_id = self.upload_id.clone();
        let part_idx = self.next_part;
        self.next_part += 1;
        let completed_parts = Arc::clone(&self.completed_parts);

        Box::pin(async move {
            let path = store.multipart_part_path(&upload_id, part_idx)?;
            let mut writer = store
                .context
                .fs
                .create(&path, true)
                .await
                .map_err(|e| fs_error_to_object_store(&Path::default(), e))?;

            let mut hasher = Md5::new();
            for chunk in data.iter() {
                hasher.update(chunk);
                writer
                    .write(chunk)
                    .await
                    .map_err(|e| fs_error_to_object_store(&Path::default(), e))?;
            }
            writer
                .complete()
                .await
                .map_err(|e| fs_error_to_object_store(&Path::default(), e))?;

            let etag = format!("\"{:x}\"", hasher.finalize());
            let mut guard = completed_parts.lock().await;
            guard.push(CompletedPart {
                part_idx,
                etag,
                path,
            });
            Ok(())
        })
    }

    async fn complete(&mut self) -> OsResult<PutResult> {
        let mut parts = self.completed_parts.lock().await.clone();
        parts.sort_by_key(|p| p.part_idx);

        let dest = self.store.object_path(&self.dest)?;
        self.store
            .prepare_multipart_destination(&dest, &self.dest)
            .await?;
        let staging_final = self.store.multipart_final_path(&self.upload_id)?;
        let mut writer = self
            .store
            .context
            .fs
            .create(&staging_final, true)
            .await
            .map_err(|e| fs_error_to_object_store(&self.dest, e))?;

        let mut etag_inputs = String::new();
        let write_result: OsResult<()> = async {
            for part in parts {
                let part_meta = self
                    .store
                    .context
                    .fs
                    .get_status(&part.path)
                    .await
                    .map_err(|e| fs_error_to_object_store(&self.dest, e))?;
                let mut reader = self
                    .store
                    .context
                    .fs
                    .open(&part.path)
                    .await
                    .map_err(|e| fs_error_to_object_store(&self.dest, e))?;
                self.store
                    .stream_copy_contents(
                        &Path::default(),
                        &self.dest,
                        part_meta.len as u64,
                        &mut reader,
                        &mut writer,
                    )
                    .await?;
                reader
                    .complete()
                    .await
                    .map_err(|e| fs_error_to_object_store(&self.dest, e))?;
                etag_inputs.push_str(&part.etag);
            }
            writer
                .complete()
                .await
                .map_err(|e| fs_error_to_object_store(&self.dest, e))?;
            self.store
                .context
                .fs
                .rename(&staging_final, &dest)
                .await
                .map_err(|e| fs_error_to_object_store(&self.dest, e))
                .and_then(|renamed| {
                    if renamed {
                        Ok(())
                    } else {
                        Err(OsError::Generic {
                            store: CURVINE_SCHEME,
                            source: "multipart final rename reported no-op".into(),
                        })
                    }
                })?;
            Ok(())
        }
        .await;

        match write_result {
            Ok(()) => {
                let _ = self.store.cleanup_multipart(&self.upload_id).await;
                Ok(PutResult {
                    e_tag: Some(etag_inputs),
                    version: None,
                })
            }
            Err(err) => Err(err),
        }
    }

    async fn abort(&mut self) -> OsResult<()> {
        self.store.cleanup_multipart(&self.upload_id).await
    }
}

pub fn curvine_registry() -> Arc<ObjectStoreRegistry> {
    let registry = Arc::new(ObjectStoreRegistry::default());
    registry.insert(CURVINE_SCHEME, Arc::new(CurvineObjectStoreProvider::new()));
    registry
}

pub fn curvine_session() -> Arc<Session> {
    Arc::new(Session::new(0, 0, curvine_registry()))
}

/// Absolute Curvine filesystem path for `url`: uses `url.path()`, and when `authority` is non-empty
/// inserts it as the first segment (`curvine://tenant/foo` → `/tenant/foo`; `curvine:///foo` → `/foo`).
fn curvine_absolute_path_str_from_uri(url: &Url) -> StdResult<String, String> {
    let authority = url.host_str().unwrap_or_default();
    let raw_path = url.path();
    let full = if authority.is_empty() {
        raw_path.to_string()
    } else if raw_path == "/" {
        format!("/{authority}")
    } else {
        format!("/{authority}{raw_path}")
    };
    Ok(full)
}

fn curvine_workspace_root_from_uri(url: &Url) -> StdResult<CurvinePath, String> {
    let full = curvine_absolute_path_str_from_uri(url)?;
    CurvinePath::from_str(&full).map_err(|e| e.to_string())
}

/// Curvine [`FileStatus`] → [`ObjectMeta`].
///
/// - **size / last_modified**: from `len` and `mtime` (ms since epoch on wire).
/// - **e_tag**: weak synthetic tag `W/"cv:{inode}:{mtime_ms}"` for stable referential
///   identity; **not** a content digest. Do not use for byte-accurate conditional semantics
///   until a content hash is wired through the filesystem.
/// - **version**: always `None` (no object-version id exposed yet).
fn file_status_to_object_meta(location: Path, status: FileStatus) -> ObjectMeta {
    let secs = status.mtime.div_euclid(1000);
    let millis = status.mtime.rem_euclid(1000) as u32;
    let weak_etag = Some(format!("W/\"cv:{}:{}\"", status.id, status.mtime));
    ObjectMeta {
        location,
        last_modified: DateTime::<Utc>::from_timestamp(secs, millis * 1_000_000)
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
        size: status.len as u64,
        e_tag: weak_etag,
        version: None,
    }
}

fn relative_object_path(root: &CurvinePath, full_path: &str) -> StdResult<Path, String> {
    let root = root.full_path().trim_end_matches('/');
    let relative = full_path
        .strip_prefix(root)
        .unwrap_or(full_path)
        .trim_start_matches('/');
    Path::parse(relative).map_err(|e| e.to_string())
}

fn is_internal_reserved_relative_path(rel: &str) -> bool {
    rel == INTERNAL_RESERVED_ROOT || rel.starts_with(&format!("{INTERNAL_RESERVED_ROOT}/"))
}

fn workspace_staging_id(workspace_root: &CurvinePath) -> String {
    workspace_root
        .full_path()
        .trim_start_matches('/')
        .replace('/', "__")
}

fn fs_error_to_object_store(location: &Path, error: FsError) -> OsError {
    match error {
        e @ FsError::FileNotFound(_) | e @ FsError::Expired(_) | e @ FsError::JobNotFound(_) => {
            OsError::NotFound {
                path: location.to_string(),
                source: Box::new(e),
            }
        }
        e @ FsError::FileAlreadyExists(_) => OsError::AlreadyExists {
            path: location.to_string(),
            source: Box::new(e),
        },
        e @ FsError::Unsupported(_) | e @ FsError::UnsupportedUfsRead(_) => OsError::NotSupported {
            source: Box::new(e),
        },
        e => OsError::Generic {
            store: CURVINE_SCHEME,
            source: Box::new(e),
        },
    }
}
