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

use crate::OpendalConf;
#[cfg(feature = "opendal-oss")]
use crate::OssHdfsConf;
use crate::{err_ufs, FOLDER_SUFFIX};
use bytes::BytesMut;
use curvine_common::error::FsError;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::{FileStatus, FileType, SetAttrOpts};
use curvine_common::FsResult;
use futures::StreamExt;
use log::warn;
use opendal::services::*;
use opendal::{
    layers::{LoggingLayer, RetryLayer, TimeoutLayer},
    Metadata, Operator,
};
use orpc::sys::DataSlice;
use orpc::{err_box, err_ext, try_option_mut};
use std::collections::HashMap;
use std::time::Duration;

pub const HDFS_SCHEMA: &str = "hdfs";

/// OpenDAL Reader implementation
pub struct OpendalReader {
    operator: Operator,
    path: Path,
    object_path: String,
    length: i64,
    pos: i64,
    chunk: DataSlice,
    chunk_size: usize,
    byte_stream: Option<opendal::FuturesBytesStream>,
    status: FileStatus,
}

impl OpendalReader {
    async fn create_stream(&mut self, start: i64) -> FsResult<()> {
        if start >= self.length {
            self.byte_stream = None;
            return Ok(());
        }

        let reader = self
            .operator
            .reader_with(&self.object_path)
            .chunk(self.chunk_size)
            .await
            .map_err(|e| FsError::common(format!("Failed to create reader: {}", e)))?;

        self.byte_stream = Some(
            reader
                .into_bytes_stream(start as u64..self.length as u64)
                .await
                .map_err(|e| FsError::common(format!("Failed to create stream: {}", e)))?,
        );

        Ok(())
    }

    async fn try_recover_shrunk_object(&mut self) -> FsResult<bool> {
        let latest_len = match self.operator.stat(&self.object_path).await {
            Ok(metadata) => metadata.content_length() as i64,
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => 0,
            Err(e) => {
                return Err(FsError::common(format!(
                    "Failed to restat file after read error: {}",
                    e
                )));
            }
        };

        if latest_len < self.length {
            warn!(
                "opendal reader detected shrink during read, path={}, len {} -> {}, pos={}",
                self.path, self.length, latest_len, self.pos
            );
            self.length = latest_len;
            self.status.len = latest_len;
            if self.pos > self.length {
                self.pos = self.length;
            }
            self.byte_stream = None;
            self.chunk = DataSlice::Empty;
            return Ok(true);
        }

        Ok(false)
    }
}

impl Reader for OpendalReader {
    fn status(&self) -> &FileStatus {
        &self.status
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn len(&self) -> i64 {
        self.length
    }

    fn chunk_mut(&mut self) -> &mut DataSlice {
        &mut self.chunk
    }

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    async fn read_chunk0(&mut self) -> FsResult<DataSlice> {
        if !self.has_remaining() {
            return Ok(DataSlice::Empty);
        }

        let mut recoveries = 0;
        loop {
            if self.byte_stream.is_none() {
                self.create_stream(self.pos).await?;
            }

            if let Some(stream) = &mut self.byte_stream {
                match stream.next().await {
                    Some(Ok(chunk)) => return Ok(DataSlice::Bytes(chunk)),

                    Some(Err(e)) => {
                        if recoveries < 8 && self.try_recover_shrunk_object().await? {
                            recoveries += 1;
                            if !self.has_remaining() {
                                return Ok(DataSlice::Empty);
                            }
                            continue;
                        }

                        return Err(FsError::common(format!("Failed to read chunk: {}", e)));
                    }

                    None => return Ok(DataSlice::Empty),
                }
            } else {
                return Ok(DataSlice::Empty);
            }
        }
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return Err(FsError::common("Invalid seek position"));
        }

        // For read path, seeking past EOF should behave as EOF instead of error.
        let target = pos.min(self.length);

        // If seeking backward or forward significantly, reset the stream
        if target < self.pos || target > self.pos + (self.chunk_size as i64 * 2) {
            self.byte_stream = None;
            self.chunk = DataSlice::Empty;
            self.create_stream(target).await?;
        } else {
            // Skip forward in the current stream
            while self.pos < target {
                let skip_bytes = (target - self.pos).min(self.chunk_size as i64) as usize;
                if self.chunk.is_empty() {
                    self.chunk = self.read_chunk0().await?;
                }
                if self.chunk.is_empty() {
                    break;
                }
                let actual_skip = skip_bytes.min(self.chunk.len());
                self.chunk.advance(actual_skip);
                self.pos += actual_skip as i64;
            }
        }

        self.pos = target;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.byte_stream = None;
        self.chunk = DataSlice::Empty;
        Ok(())
    }
}

/// OpenDAL Writer implementation
pub struct OpendalWriter {
    operator: Operator,
    path: Path,
    object_path: String,
    status: FileStatus,
    pos: i64,
    append_mode: bool,
    chunk: BytesMut,
    chunk_size: usize,
    writer: Option<opendal::Writer>,
}

impl Writer for OpendalWriter {
    fn status(&self) -> &FileStatus {
        &self.status
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    fn chunk_mut(&mut self) -> &mut BytesMut {
        &mut self.chunk
    }

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    async fn write_chunk(&mut self, chunk: DataSlice) -> FsResult<i64> {
        if self.writer.is_none() {
            self.writer = Some(
                self.operator
                    .writer(&self.object_path)
                    .await
                    .map_err(|e| FsError::common(format!("Failed to create writer: {}", e)))?,
            );
        }

        let data = bytes::Bytes::copy_from_slice(chunk.as_slice());
        let len = data.len() as i64;

        let writer = try_option_mut!(self.writer);
        writer
            .write(data)
            .await
            .map_err(|e| FsError::common(format!("Failed to write: {}", e)))?;

        Ok(len)
    }

    async fn flush(&mut self) -> FsResult<()> {
        self.flush_chunk().await?;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.flush().await?;

        if let Some(mut writer) = self.writer.take() {
            writer
                .close()
                .await
                .map_err(|e| FsError::common(format!("Failed to close writer: {}", e)))?;
        }

        Ok(())
    }

    async fn cancel(&mut self) -> FsResult<()> {
        self.writer = None;
        Ok(())
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if self.append_mode {
            // For O_APPEND writes from FUSE, kernel-provided offsets can be stale across fds.
            // Keep appending at current writer position instead of rejecting as random write.
            return Ok(());
        }

        if self.pos != pos {
            err_box!("not support random write")
        } else {
            Ok(())
        }
    }
}

/// OpenDAL file system implementation
#[derive(Clone)]
pub struct OpendalFileSystem {
    operator: Operator,
    scheme: String,
    bucket_or_container: String,
}

impl OpendalFileSystem {
    fn add_stability_layers(
        base_op: Operator,
        conf: &HashMap<String, String>,
    ) -> FsResult<Operator> {
        let opendal_conf = OpendalConf::from_map(conf)
            .map_err(|e| FsError::common(format!("Failed to parse OpenDAL config: {}", e)))?;

        let total_timeout_ms = opendal_conf.total_timeout_ms();

        let op = base_op
            .layer(LoggingLayer::default())
            .layer(TimeoutLayer::new().with_io_timeout(Duration::from_millis(total_timeout_ms)))
            .layer(
                RetryLayer::new()
                    .with_min_delay(Duration::from_millis(opendal_conf.retry_interval_ms))
                    .with_max_delay(Duration::from_millis(opendal_conf.retry_max_delay_ms))
                    .with_max_times(opendal_conf.retry_times as usize)
                    .with_factor(2.0)
                    .with_jitter(),
            );

        Ok(op)
    }

    pub fn new(path: &Path, conf: HashMap<String, String>) -> FsResult<Self> {
        let scheme = path
            .scheme()
            .ok_or_else(|| FsError::invalid_path(path.full_path(), "Missing scheme"))?;

        let bucket_or_container = path
            .authority()
            .ok_or_else(|| {
                FsError::invalid_path(path.full_path(), "URI missing bucket/container name")
            })?
            .to_string();

        let operator = match scheme {
            // OSS native implementation (higher priority than HDFS-based OSS)
            #[cfg(feature = "opendal-oss")]
            "oss" => {
                let mut builder = Oss::default();
                builder = builder.bucket(&bucket_or_container);

                if let Some(endpoint) = conf.get(OssHdfsConf::USER_ENDPOINT) {
                    builder = builder.endpoint(endpoint);
                }
                if let Some(access_key) = conf.get(OssHdfsConf::USER_ACCESS_KEY_ID) {
                    builder = builder.access_key_id(access_key);
                }
                if let Some(secret_key) = conf.get(OssHdfsConf::USER_ACCESS_KEY_SECRET) {
                    builder = builder.access_key_secret(secret_key);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create OSS operator: {}", e)))?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-hdfs")]
            "hdfs" => {
                use crate::jni::{register_jvm, JVM};

                register_jvm();

                let _ = JVM.get_or_init().map_err(|e| {
                    FsError::common(format!("Failed to initialize JVM for HDFS: {}", e))
                })?;

                let mut builder = Hdfs::default();

                let namenode = if let Some(namenode_config) = conf.get("hdfs.namenode") {
                    namenode_config.clone()
                } else {
                    format!("hdfs://{}", bucket_or_container)
                };

                builder = builder.name_node(&namenode);

                let root_path = conf.get("hdfs.root").map(|s| s.as_str()).unwrap_or("/");
                builder = builder.root(root_path);

                let hdfs_user = conf
                    .get("hdfs.user")
                    .cloned()
                    .or_else(|| std::env::var("HADOOP_USER_NAME").ok())
                    .or_else(|| std::env::var("USER").ok());

                if let Some(user) = hdfs_user {
                    builder = builder.user(&user);
                }

                if let Some(ccache) = conf.get("hdfs.kerberos.ccache") {
                    builder = builder.kerberos_ticket_cache_path(ccache);
                } else if let Ok(ccache) = std::env::var("KRB5CCNAME") {
                    builder = builder.kerberos_ticket_cache_path(&ccache);
                }

                if let Some(krb5_conf) = conf.get("hdfs.kerberos.krb5_conf") {
                    std::env::set_var("KRB5_CONFIG", krb5_conf);
                }

                if conf
                    .get("hdfs.atomic_write_dir")
                    .map(|s| s == "true")
                    .unwrap_or(false)
                {
                    let atomic_dir = format!("{}/atomic_write_dir", root_path);
                    builder = builder.atomic_write_dir(&atomic_dir);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create HDFS operator: {}", e)))?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-webhdfs")]
            "webhdfs" => {
                let mut builder = Webhdfs::default();

                let endpoint = if let Some(endpoint_config) = conf.get("webhdfs.endpoint") {
                    endpoint_config.clone()
                } else {
                    format!("http://{}", bucket_or_container)
                };

                builder = builder.endpoint(&endpoint);

                let root_path = conf.get("webhdfs.root").map(|s| s.as_str()).unwrap_or("/");
                builder = builder.root(root_path);

                let atomic_dir = format!("{}/atomic_write_dir", root_path);
                builder = builder.atomic_write_dir(&atomic_dir);

                let base_op = Operator::new(builder)
                    .map_err(|e| {
                        FsError::common(format!("Failed to create WebHDFS operator: {}", e))
                    })?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-s3")]
            "s3" | "s3a" => {
                let mut builder = S3::default();
                builder = builder.bucket(&bucket_or_container);

                if let Some(endpoint) = conf.get("s3.endpoint_url") {
                    builder = builder.endpoint(endpoint);
                }
                if let Some(region) = conf.get("s3.region_name") {
                    builder = builder.region(region);
                }
                if let Some(access_key) = conf.get("s3.credentials.access") {
                    builder = builder.access_key_id(access_key);
                }
                if let Some(secret_key) = conf.get("s3.credentials.secret") {
                    builder = builder.secret_access_key(secret_key);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create S3 operator: {}", e)))?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-gcs")]
            "gcs" | "gs" => {
                let mut builder = Gcs::default();
                builder = builder.bucket(&bucket_or_container);

                if let Some(service_account) = conf.get("gcs.service_account") {
                    builder = builder.credential(service_account);
                }
                if let Some(endpoint) = conf.get("gcs.endpoint_url") {
                    builder = builder.endpoint(endpoint);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create GCS operator: {}", e)))?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-azblob")]
            "azblob" => {
                let mut builder = Azblob::default();
                builder = builder.container(&bucket_or_container);

                if let Some(account_name) = conf.get("azure.account_name") {
                    builder = builder.account_name(account_name);
                }
                if let Some(account_key) = conf.get("azure.account_key") {
                    builder = builder.account_key(account_key);
                }
                if let Some(endpoint) = conf.get("azure.endpoint_url") {
                    builder = builder.endpoint(endpoint);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| {
                        FsError::common(format!("Failed to create Azure operator: {}", e))
                    })?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-cos")]
            "cos" => {
                let mut builder = Cos::default();
                builder = builder.bucket(&bucket_or_container);

                if let Some(endpoint) = conf.get("cos.endpoint_url") {
                    builder = builder.endpoint(endpoint);
                }
                if let Some(access_key) = conf.get("cos.credentials.access") {
                    builder = builder.secret_id(access_key);
                }
                if let Some(secret_key) = conf.get("cos.credentials.secret") {
                    builder = builder.secret_key(secret_key);
                }

                let base_op = Operator::new(builder)
                    .map_err(|e| FsError::common(format!("Failed to create COS operator: {}", e)))?
                    .finish();

                Self::add_stability_layers(base_op, &conf)?
            }

            #[cfg(feature = "opendal-hdfs-native")]
            "hdfs" => Self::create_hdfs_native_operator(&bucket_or_container, &conf)?,

            _ => {
                return Err(FsError::unsupported(format!(
                    "Unsupported scheme: {}",
                    scheme
                )));
            }
        };

        Ok(Self {
            operator,
            scheme: scheme.to_string(),
            bucket_or_container,
        })
    }

    /// Create HDFS Native operator (Rust native implementation, no JVM required)
    ///
    /// Note: HdfsNative uses system-level Kerberos configuration via environment variables.
    /// Supported configurations:
    /// - hdfs.namenode: NameNode address (required)
    /// - hdfs.root: Root path (default: "/")
    /// - hdfs.kerberos.krb5_conf: Path to krb5.conf file
    /// - hdfs.kerberos.ccache: Path to Kerberos ticket cache
    /// - hdfs.kerberos.keytab: Path to keytab file
    #[cfg(feature = "opendal-hdfs-native")]
    fn create_hdfs_native_operator(
        bucket_or_container: &str,
        conf: &HashMap<String, String>,
    ) -> FsResult<Operator> {
        let namenode = if let Some(namenode_config) = conf.get("hdfs.namenode") {
            namenode_config.clone()
        } else {
            format!("hdfs://{}", bucket_or_container)
        };

        let root_path = conf.get("hdfs.root").map(|s| s.as_str()).unwrap_or("/");

        // Set HADOOP_USER_NAME environment variable for HDFS authentication
        // HdfsNative reads this environment variable to determine the user
        let hdfs_user = conf
            .get("hdfs.user")
            .cloned()
            .or_else(|| std::env::var("HADOOP_USER_NAME").ok())
            .or_else(|| std::env::var("USER").ok());

        if let Some(user) = hdfs_user {
            std::env::set_var("HADOOP_USER_NAME", &user);
            log::debug!("Set HADOOP_USER_NAME to: {}", user);
        }

        // Configure Kerberos environment if needed
        // HdfsNative relies on system-level Kerberos configuration:
        // 1. Set KRB5_CONFIG environment variable (krb5.conf path)
        // 2. Set KRB5CCNAME environment variable (ticket cache path) or use kinit
        // 3. Optionally set KRB5_KTNAME (keytab file path)

        if let Some(krb5_conf) = conf.get("hdfs.kerberos.krb5_conf") {
            std::env::set_var("KRB5_CONFIG", krb5_conf);
        }

        if let Some(ccache) = conf.get("hdfs.kerberos.ccache") {
            std::env::set_var("KRB5CCNAME", ccache);
        } else if let Ok(ccache) = std::env::var("KRB5CCNAME") {
            // Use existing KRB5CCNAME from environment
            log::debug!("Using Kerberos ticket cache from KRB5CCNAME: {}", ccache);
        }

        if let Some(keytab) = conf.get("hdfs.kerberos.keytab") {
            std::env::set_var("KRB5_KTNAME", keytab);
        }

        let mut builder = HdfsNative::default();
        builder = builder.name_node(&namenode);
        builder = builder.root(root_path);

        let base_op = Operator::new(builder)
            .map_err(|e| FsError::common(format!("Failed to create HDFS Native operator: {}", e)))?
            .finish();

        Self::add_stability_layers(base_op, conf)
    }

    fn get_object_path(&self, path: &Path) -> FsResult<String> {
        match path.path().strip_prefix('/') {
            Some(v) => Ok(v.to_string()),
            None => err_box!("path {} invalid", path),
        }
    }

    fn get_dir_path(&self, path: &Path) -> FsResult<String> {
        let object_path = self.get_object_path(path)?;
        let dir_path = if object_path.is_empty() {
            "/".to_string()
        } else if object_path.ends_with(FOLDER_SUFFIX) {
            object_path.to_string()
        } else {
            format!("{}{}", object_path, FOLDER_SUFFIX)
        };
        Ok(dir_path)
    }

    pub fn write_status(path: &Path) -> FileStatus {
        FileStatus {
            path: path.full_path().to_owned(),
            name: path.name().to_owned(),
            is_dir: false,
            is_complete: false,
            replicas: 1,
            block_size: 4 * 1024 * 1024,
            file_type: FileType::File,
            mode: 0o777,
            ..Default::default()
        }
    }

    pub fn read_status(path: &Path, metadata: &Metadata) -> FileStatus {
        let mtime = metadata
            .last_modified()
            .map(|t| t.into_inner().as_millisecond())
            .unwrap_or(0);
        let len = metadata.content_length() as i64;

        FileStatus {
            path: path.full_path().to_owned(),
            name: path.name().to_owned(),
            is_dir: metadata.is_dir(),
            file_type: if metadata.is_dir() {
                FileType::Dir
            } else {
                FileType::File
            },
            mtime,
            len,
            is_complete: true,
            replicas: 1,
            block_size: 4 * 1024 * 1024,
            mode: 0o777,
            ..Default::default()
        }
    }

    async fn get_object_status(&self, object_path: &str) -> FsResult<Option<Metadata>> {
        match self.operator.stat(object_path).await {
            Ok(m) => Ok(Some(m)),
            Err(e) => {
                if e.kind() == opendal::ErrorKind::NotFound {
                    Ok(None)
                } else {
                    err_box!(format!("failed to stat: {}", e))
                }
            }
        }
    }

    pub async fn get_file_status(&self, path: &Path) -> FsResult<Option<FileStatus>> {
        let path_str = path.full_path();

        let likely_dir = if path_str.ends_with('/') {
            true
        } else {
            let name = path.name();
            let has_extension = name.contains('.') && !name.starts_with('.');
            !has_extension
        };

        let (first_path, second_path) = if likely_dir {
            (self.get_dir_path(path)?, self.get_object_path(path)?)
        } else {
            (self.get_object_path(path)?, self.get_dir_path(path)?)
        };

        let mut metadata = self.get_object_status(&first_path).await?;

        if metadata.is_none() {
            metadata = self.get_object_status(&second_path).await?;
        }

        Ok(metadata.map(|m| Self::read_status(path, &m)))
    }

    async fn delete_object_with_verification(
        &self,
        object_path: &str,
        target: &str,
    ) -> FsResult<()> {
        match self.operator.delete(object_path).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(()),
            Err(delete_err) => {
                // Some S3-compatible stores can return an error even when delete has been applied.
                // Verify existence once and treat "already gone" as success.
                match self.operator.stat(object_path).await {
                    Ok(_) => Err(FsError::common(format!(
                        "Failed to delete {} {}: {}",
                        target, object_path, delete_err
                    ))),
                    Err(stat_err) if stat_err.kind() == opendal::ErrorKind::NotFound => {
                        warn!(
                            "delete {} {} returned error but object is already gone: {}",
                            target, object_path, delete_err
                        );
                        Ok(())
                    }
                    Err(stat_err) => Err(FsError::common(format!(
                        "Failed to delete {} {}: {}; stat after delete also failed: {}",
                        target, object_path, delete_err, stat_err
                    ))),
                }
            }
        }
    }
}

impl FileSystem<OpendalWriter, OpendalReader> for OpendalFileSystem {
    // Creates a directory; the directory must end with "/".
    // OpenDal always creates directories recursively.
    async fn mkdir(&self, path: &Path, _create_parent: bool) -> FsResult<bool> {
        let object_path = self.get_dir_path(path)?;

        self.operator
            .create_dir(&object_path)
            .await
            .map_err(|e| FsError::common(format!("Failed to create directory: {}", e)))?;

        Ok(true)
    }

    /// OpenDal only supports overwrite, so the overwrite parameter is ignored here.
    async fn create(&self, path: &Path, overwrite: bool) -> FsResult<OpendalWriter> {
        let object_path = self.get_object_path(path)?;

        let exist = self.get_object_status(&object_path).await?.is_some();
        if !exist || overwrite {
            // If no data is written to OpenDal, no file will be created.
            // This does not conform to POSIX semantics, so an empty file is created.
            self.operator
                .write(&object_path, opendal::Buffer::new())
                .await
                .map_err(|e| {
                    FsError::common(format!(
                        "Failed to create empty file {}: {}",
                        path.full_path(),
                        e
                    ))
                })?;
        }

        let status = Self::write_status(path);
        Ok(OpendalWriter {
            operator: self.operator.clone(),
            path: path.clone(),
            object_path,
            status,
            pos: 0,
            append_mode: false,
            chunk: BytesMut::with_capacity(8 * 1024 * 1024),
            chunk_size: 8 * 1024 * 1024,
            writer: None,
        })
    }

    async fn append(&self, path: &Path) -> FsResult<OpendalWriter> {
        // OpenDAL doesn't support append for most backends
        // For now, return an error
        let object_path = self.get_object_path(path)?;
        let status = self.get_file_status(path).await?;

        match status {
            None => err_ext!(FsError::file_not_found(path.full_path())),
            Some(s) => {
                if s.len < 8 * 1024 * 1024 {
                    let chunk = self.operator.read(&object_path).await.map_err(|e| {
                        FsError::common(format!(
                            "Failed to read existing file {} for append: {}",
                            path.full_path(),
                            e
                        ))
                    })?;
                    return Ok(OpendalWriter {
                        operator: self.operator.clone(),
                        path: path.clone(),
                        object_path,
                        pos: s.len,
                        status: s,
                        append_mode: true,
                        chunk: BytesMut::from(chunk.to_vec().as_slice()),
                        chunk_size: 8 * 1024 * 1024,
                        writer: None,
                    });
                }
                err_ext!(FsError::unsupported(format!(
                    "Append operation is not supported for file {}",
                    path.full_path()
                )))
            }
        }
    }

    async fn exists(&self, path: &Path) -> FsResult<bool> {
        match self.get_file_status(path).await? {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    async fn open(&self, path: &Path) -> FsResult<OpendalReader> {
        let object_path = self.get_object_path(path)?;

        let metadata = self
            .operator
            .stat(&object_path)
            .await
            .map_err(|e| FsError::common(format!("Failed to stat file: {}", e)))?;
        let status = Self::read_status(path, &metadata);

        Ok(OpendalReader {
            operator: self.operator.clone(),
            path: path.clone(),
            object_path,
            length: status.len,
            pos: 0,
            chunk: DataSlice::Empty,
            chunk_size: 8 * 1024 * 1024,
            byte_stream: None,
            status,
        })
    }

    async fn rename(&self, src: &Path, dst: &Path) -> FsResult<bool> {
        let src_path = self.get_object_path(src)?;
        let dst_path = self.get_object_path(dst)?;

        // Try direct rename first
        match self.operator.rename(&src_path, &dst_path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == opendal::ErrorKind::Unsupported => {
                self.operator
                    .copy(&src_path, &dst_path)
                    .await
                    .map_err(|e| {
                        FsError::common(format!("failed to copy source file for rename: {}", e))
                    })?;

                // Delete source file
                self.operator.delete(&src_path).await.map_err(|e| {
                    FsError::common(format!("failed to delete source file after rename: {}", e))
                })?;

                Ok(true)
            }

            Err(e) => Err(FsError::common(format!("failed to rename: {}", e))),
        }
    }

    async fn delete(&self, path: &Path, recursive: bool) -> FsResult<()> {
        let object_path = self.get_object_path(path)?;

        if recursive {
            let dir_path = self.get_dir_path(path)?;
            // Check if it's a directory
            match self.operator.stat(&object_path).await {
                Ok(metadata) if metadata.is_dir() => {
                    self.operator.remove_all(&object_path).await.map_err(|e| {
                        FsError::common(format!("Failed to delete recursive: {}", e))
                    })?
                }
                _ => {
                    self.delete_object_with_verification(&object_path, "file")
                        .await?
                }
            }

            if dir_path != object_path {
                // Prefix-only directories may have no marker at object_path.
                // Ensure recursive delete still clears descendants under `<path>/`.
                self.operator.remove_all(&dir_path).await.map_err(|e| {
                    FsError::common(format!(
                        "Failed to delete recursive directory prefix {}: {}",
                        dir_path, e
                    ))
                })?;

                self.delete_object_with_verification(&dir_path, "directory marker")
                    .await?;
            }
        } else {
            // Try to delete as file first.
            self.delete_object_with_verification(&object_path, "file")
                .await?;

            // Also try to delete as directory marker (with suffix)
            // S3 delete is idempotent, so it's safe to try deleting the marker even if it doesn't exist
            // or if we just deleted a file.
            let dir_path = self.get_dir_path(path)?;
            if dir_path != object_path {
                self.delete_object_with_verification(&dir_path, "directory marker")
                    .await?;
            }
        }

        Ok(())
    }

    async fn get_status(&self, path: &Path) -> FsResult<FileStatus> {
        match self.get_file_status(path).await? {
            Some(v) => Ok(v),
            None => err_ext!(FsError::file_not_found(path.full_path())),
        }
    }

    async fn list_status(&self, path: &Path) -> FsResult<Vec<FileStatus>> {
        let dir_path = self.get_dir_path(path)?;

        let list_result = self
            .operator
            .list(&dir_path)
            .await
            .map_err(|e| FsError::common(format!("Failed to list directory: {}", e)))?;

        let mut statuses = Vec::new();
        for entry in list_result {
            let raw_path = format!(
                "{}://{}/{}",
                self.scheme,
                self.bucket_or_container,
                entry.path().trim_end_matches('/')
            );
            let entry_path = Path::from_str(&raw_path)?;

            if entry_path.path() == path.path() {
                continue;
            }

            let metadata = entry.metadata();
            let status = Self::read_status(&entry_path, metadata);
            statuses.push(status);
        }

        Ok(statuses)
    }

    async fn set_attr(&self, _path: &Path, _opts: SetAttrOpts) -> FsResult<()> {
        err_ufs!("SetAttr operation is not supported by OpenDAL file system")
    }
}

#[cfg(all(test, feature = "opendal-s3"))]
mod tests {
    use super::*;
    use opendal::services::S3;

    fn new_test_writer(pos: i64, append_mode: bool) -> OpendalWriter {
        let mut builder = S3::default();
        builder = builder.bucket("test-bucket");
        builder = builder.region("us-east-1");
        builder = builder.endpoint("http://127.0.0.1:1");

        let operator = Operator::new(builder).unwrap().finish();

        OpendalWriter {
            operator,
            path: Path::from_str("s3://test-bucket/test.txt").unwrap(),
            object_path: "test.txt".to_string(),
            status: FileStatus::default(),
            pos,
            append_mode,
            chunk: BytesMut::with_capacity(8 * 1024 * 1024),
            chunk_size: 8 * 1024 * 1024,
            writer: None,
        }
    }

    #[tokio::test]
    async fn append_like_seek_should_tolerate_stale_offset() {
        let mut writer = new_test_writer(128, true);

        let res = writer.seek(0).await;
        assert!(
            res.is_ok(),
            "append-like write should not fail on stale offset, got: {:?}",
            res
        );
    }

    #[tokio::test]
    async fn non_append_seek_should_reject_random_offset() {
        let mut writer = new_test_writer(128, false);

        let res = writer.seek(0).await;
        assert!(res.is_err(), "non-append random write should fail");
    }
}
