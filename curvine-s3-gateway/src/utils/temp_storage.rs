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

use bytes::BytesMut;
use curvine_client::unified::UnifiedFileSystem;
use curvine_fs_api::{FileSystem, Path, Reader, Writer};
use std::future::Future;

const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;
const MIN_BUFFER_SIZE: usize = 64 * 1024;

/// Verification a staged object body must satisfy before it is handed to the
/// object handler.
#[derive(Clone, Copy, Debug, Default)]
pub struct StageExpect<'a> {
    /// Expected `x-amz-content-sha256` in lowercase hex, unless the client sent
    /// `UNSIGNED-PAYLOAD`.
    pub sha256_hex: Option<&'a str>,
    /// Expected `content-length`.
    pub content_length: Option<usize>,
}

impl StageExpect<'_> {
    /// Returns a message describing the mismatch, or `Ok(())` when the staged
    /// bytes satisfy the request's declared hash and length.
    pub fn check(self, staged: usize, sha256_hex: &str) -> Result<(), String> {
        if let Some(expected) = self.content_length {
            if staged != expected {
                return Err(format!(
                    "staged body length {} does not match content-length {}",
                    staged, expected
                ));
            }
        }
        if let Some(expected) = self.sha256_hex {
            if !expected.eq_ignore_ascii_case(sha256_hex) {
                return Err("staged body sha256 does not match x-amz-content-sha256".to_string());
            }
        }
        Ok(())
    }
}

/// Modes for staged upload data.
///
/// `File::create` and `create_dir_all` use `0666 & ~umask` / `0777 & ~umask`,
/// which is typically `0644` / `0755`: on the default `/tmp/curvine-temp`
/// staging directory any local user could read in-flight object bodies. The
/// credentials store in this crate solves the same problem with the same
/// modes.
const PRIVATE_FILE_MODE: u32 = 0o600;
const PRIVATE_DIR_MODE: u32 = 0o700;

/// Create a staging directory readable only by its owner.
async fn create_private_dir(dir: &str) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|e| format!("Failed to create temp dir {}: {}", dir, e))?;

    // Tighten an existing directory too, so a directory left by an earlier
    // version does not keep its looser mode.
    tokio::fs::set_permissions(dir, std::fs::Permissions::from_mode(PRIVATE_DIR_MODE))
        .await
        .map_err(|e| format!("Failed to restrict temp dir {}: {}", dir, e))
}

/// Open a staging file readable only by its owner.
///
/// `exclusive` keeps the original per-caller semantics: a part is rewritten in
/// place, while a single-object staging body must not already exist.
async fn open_private_writer(path: &str, exclusive: bool) -> std::io::Result<tokio::fs::File> {
    use std::os::unix::fs::PermissionsExt;

    let mut opts = tokio::fs::OpenOptions::new();
    opts.write(true).mode(PRIVATE_FILE_MODE);
    if exclusive {
        opts.create_new(true);
    } else {
        opts.create(true).truncate(true);
    }
    let file = opts.open(path).await?;
    // An existing file keeps its mode across open, so tighten it explicitly.
    // Best effort: a freshly created file already has PRIVATE_FILE_MODE from
    // `mode()`, so a failure here only leaves a pre-existing file as it was and
    // must not fail the upload.
    let _ = file
        .set_permissions(std::fs::Permissions::from_mode(PRIVATE_FILE_MODE))
        .await;
    Ok(file)
}

/// Compare the MD5 recomputed while reading a part with the ETag the client
/// listed in `CompleteMultipartUpload`.
///
/// The ETag is client supplied and is only a string, so a mismatch means the
/// staged bytes are not the ones the client believes it uploaded. Refusing it
/// here is what stops `Complete` from publishing a shorter object and
/// returning success. An empty or absent ETag cannot match a digest, so it is
/// rejected rather than treated as "nothing to check".
fn verify_part_etag(
    part_number: u32,
    expected_etag: &str,
    digest: &md5::Digest,
) -> Result<(), String> {
    let actual = format!("{:x}", digest);
    // Clients may quote the ETag, may omit the quotes, or may send it weak
    // (`W/"..."`); all three denote the same MD5.
    let expected = expected_etag
        .trim()
        .trim_start_matches("W/")
        .trim_matches('"')
        .to_ascii_lowercase();
    if expected != actual {
        return Err(format!(
            "part {} etag {} does not match the staged part ({})",
            part_number, expected_etag, actual
        ));
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct TempStorageConfig {
    pub temp_dir: String,
    pub memory_buffer_threshold: usize,
    pub max_memory_buffer: usize,
}

impl TempStorageConfig {
    pub fn new(temp_dir: String, memory_buffer_threshold: usize, max_memory_buffer: usize) -> Self {
        Self {
            temp_dir,
            memory_buffer_threshold,
            max_memory_buffer,
        }
    }

    #[inline]
    pub fn buffer_size(&self) -> usize {
        if self.max_memory_buffer > 0 {
            self.max_memory_buffer.max(MIN_BUFFER_SIZE)
        } else {
            DEFAULT_BUFFER_SIZE
        }
    }
}

#[derive(Clone)]
pub struct LocalTempStorage {
    config: TempStorageConfig,
}

impl LocalTempStorage {
    pub fn new(config: TempStorageConfig) -> Self {
        Self { config }
    }

    #[inline]
    fn part_path(&self, upload_id: &str, part_number: u32) -> String {
        format!("{}/{}/{}", self.config.temp_dir, upload_id, part_number)
    }

    #[inline]
    fn dir_path(&self, upload_id: &str) -> String {
        format!("{}/{}", self.config.temp_dir, upload_id)
    }

    pub fn config(&self) -> &TempStorageConfig {
        &self.config
    }

    /// Create directory for upload session, readable only by its owner.
    pub async fn create_dir(&self, upload_id: &str) -> Result<(), String> {
        let dir = self.dir_path(upload_id);
        create_private_dir(&dir).await
    }

    /// Remove entire upload session directory (cleanup after complete/abort)
    pub async fn cleanup(&self, upload_id: &str) -> Result<(), String> {
        let dir = self.dir_path(upload_id);
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(format!("Failed to cleanup temp dir {}: {}", dir, e)),
        }
    }

    /// Stream a part body into the upload session, verifying it while writing.
    ///
    /// Returns the part ETag (its MD5). The body is consumed chunk by chunk: a
    /// part may be up to the S3 5 GiB limit, so buffering it first would be an
    /// out-of-memory abort rather than a failed request. A part that fails
    /// `expect` is removed, so the session only ever holds verified parts.
    pub async fn write_part_stream<R: crate::utils::io::PollRead + Send>(
        &self,
        upload_id: &str,
        part_number: u32,
        body: &mut R,
        expect: StageExpect<'_>,
    ) -> Result<String, String> {
        use sha2::Digest;
        use tokio::io::AsyncWriteExt;

        let path = self.part_path(upload_id, part_number);
        let dir = self.dir_path(upload_id);
        create_private_dir(&dir).await?;

        let mut file = open_private_writer(&path, false)
            .await
            .map_err(|e| format!("Failed to create part file {}: {}", path, e))?;

        let mut hasher = md5::Context::new();
        let mut sha256 = sha2::Sha256::new();
        let buffer_size = self.config.buffer_size();
        let threshold = self.config.memory_buffer_threshold;
        let mut memory_buf = BytesMut::with_capacity(threshold.min(buffer_size));
        let mut staged: usize = 0;

        loop {
            // A read error (dropped connection, timed-out client) must not leave
            // a partial part at its final path: `CompleteMultipartUpload`
            // streams whatever a part file holds, so it would publish a
            // truncated object and report success.
            let chunk = match body.poll_read().await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(e) => {
                    drop(file);
                    let _ = tokio::fs::remove_file(&path).await;
                    return Err(format!("Failed to read from body: {}", e));
                }
            };
            staged += chunk.len();
            sha256.update(&chunk);
            hasher.consume(&chunk);

            if memory_buf.len() + chunk.len() <= threshold {
                memory_buf.extend_from_slice(&chunk);
            } else {
                if !memory_buf.is_empty() {
                    file.write_all(&memory_buf)
                        .await
                        .map_err(|e| format!("Failed to write to part file: {}", e))?;
                    memory_buf.clear();
                }
                file.write_all(&chunk)
                    .await
                    .map_err(|e| format!("Failed to write to part file: {}", e))?;
            }
        }

        if !memory_buf.is_empty() {
            file.write_all(&memory_buf)
                .await
                .map_err(|e| format!("Failed to write to part file: {}", e))?;
        }

        file.flush()
            .await
            .map_err(|e| format!("Failed to flush part file: {}", e))?;
        drop(file);

        if let Err(mismatch) = expect.check(staged, &hex::encode(sha256.finalize())) {
            let _ = tokio::fs::remove_file(&path).await;
            return Err(mismatch);
        }

        let digest = hasher.compute();
        Ok(format!("\"{:x}\"", digest))
    }

    /// Stages a single-object body on local disk and returns the reader plus
    /// the temp path to remove afterwards.
    pub async fn stage_object<R: crate::utils::io::PollRead + Send>(
        &self,
        staging_id: &str,
        body: &mut R,
        expect: StageExpect<'_>,
    ) -> Result<crate::utils::io::PollReaderEnum, String> {
        use crate::utils::io::PollReaderEnum;
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};

        let dir = format!("{}/{}", self.config.temp_dir, staging_id);
        let path = format!("{}/body", dir);
        create_private_dir(&dir).await?;

        let mut file = open_private_writer(&path, true)
            .await
            .map_err(|e| format!("Failed to create staging file {}: {}", path, e))?;

        let mut hasher = sha2::Sha256::new();
        let mut staged: usize = 0;
        while let Some(chunk) = body.poll_read().await? {
            use sha2::Digest;
            hasher.update(&chunk);
            staged += chunk.len();
            file.write_all(&chunk)
                .await
                .map_err(|e| format!("Failed to stage body: {}", e))?;
        }
        file.flush()
            .await
            .map_err(|e| format!("Failed to flush staged body: {}", e))?;

        use sha2::Digest;
        expect.check(staged, &hex::encode(hasher.finalize()))?;

        file.seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|e| format!("Failed to rewind staged body: {}", e))?;

        Ok(PollReaderEnum::File(file))
    }

    /// Removes a staged single-object body.
    pub async fn cleanup_object(&self, staging_id: &str) -> Result<(), String> {
        let dir = format!("{}/{}", self.config.temp_dir, staging_id);
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(format!("Failed to remove staging dir {}: {}", dir, e)),
        }
    }

    /// Whether a part of this upload is staged.
    ///
    /// `CompleteMultipartUpload` checks this before touching the destination, so
    /// a stale upload id cannot destroy an existing object.
    pub async fn part_exists(&self, upload_id: &str, part_number: u32) -> bool {
        tokio::fs::try_exists(self.part_path(upload_id, part_number))
            .await
            .unwrap_or(false)
    }

    /// Copy a staged part into the destination, verifying it on the way out.
    ///
    /// The MD5 is recomputed while streaming and compared with the ETag the
    /// client listed in `CompleteMultipartUpload`. Staging already refuses a
    /// part whose write failed, so this is the second line of defence: a part
    /// file that was truncated or altered after staging is caught here instead
    /// of being published as a shorter object with a success response.
    pub async fn read_part_stream<W: Writer>(
        &self,
        upload_id: &str,
        part_number: u32,
        expected_etag: &str,
        writer: &mut W,
    ) -> Result<(), String> {
        use tokio::io::AsyncReadExt;

        let path = self.part_path(upload_id, part_number);
        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| format!("Failed to open part file {}: {}", path, e))?;

        let buffer_size = self.config.buffer_size();
        let mut buf = BytesMut::with_capacity(buffer_size);
        buf.resize(buffer_size, 0);
        let mut hasher = md5::Context::new();

        loop {
            let n = file
                .read(&mut buf)
                .await
                .map_err(|e| format!("Failed to read from part file: {}", e))?;
            if n == 0 {
                break;
            }

            hasher.consume(&buf[..n]);
            writer
                .write(&buf[..n])
                .await
                .map_err(|e| format!("Failed to write to destination: {}", e))?;
        }

        verify_part_etag(part_number, expected_etag, &hasher.compute())?;
        Ok(())
    }
}

// ============================================================================
// Curvine Temp Storage Implementation
// ============================================================================

/// Curvine distributed filesystem based temporary storage (distributed mode)
#[derive(Clone)]
pub struct CurvineTempStorage {
    fs: UnifiedFileSystem,
    config: TempStorageConfig,
}

impl CurvineTempStorage {
    pub fn new(fs: UnifiedFileSystem, config: TempStorageConfig) -> Self {
        Self { fs, config }
    }

    #[inline]
    fn part_path(&self, upload_id: &str, part_number: u32) -> Result<Path, String> {
        Path::from_str(format!(
            "{}/{}/{}",
            self.config.temp_dir, upload_id, part_number
        ))
        .map_err(|e| format!("Invalid part path: {}", e))
    }

    #[inline]
    fn dir_path(&self, upload_id: &str) -> Result<Path, String> {
        Path::from_str(format!("{}/{}", self.config.temp_dir, upload_id))
            .map_err(|e| format!("Invalid dir path: {}", e))
    }

    pub fn config(&self) -> &TempStorageConfig {
        &self.config
    }

    /// Create directory for upload session
    pub async fn create_dir(&self, upload_id: &str) -> Result<(), String> {
        let path = self.dir_path(upload_id)?;
        self.fs
            .mkdir(&path, true)
            .await
            .map_err(|e| format!("Failed to create temp dir in Curvine: {}", e))?;
        Ok(())
    }

    /// Remove entire upload session directory (cleanup after complete/abort).
    ///
    /// Multipart staging is internal state, never user data, so it is purged
    /// permanently: a soft delete would move every part into `/.Trash` and
    /// retain a full copy of each upload for `trash_retention` (7d default).
    pub async fn cleanup(&self, upload_id: &str) -> Result<(), String> {
        let path = self.dir_path(upload_id)?;
        match self.fs.delete_with_opts(&path, true, true).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("not found")
                    || msg.contains("No such file")
                    || msg.contains("not exists")
                {
                    Ok(())
                } else {
                    Err(format!("Failed to cleanup temp dir in Curvine: {}", e))
                }
            }
        }
    }

    /// Stream a part body into Curvine staging, verifying it while writing.
    ///
    /// Same contract as the local backend: no full-part buffering, and a part
    /// that fails `expect` is deleted instead of being left for `Complete`.
    pub async fn write_part_stream<R: crate::utils::io::PollRead + Send>(
        &self,
        upload_id: &str,
        part_number: u32,
        body: &mut R,
        expect: StageExpect<'_>,
    ) -> Result<String, String> {
        use sha2::Digest;

        let dir_path = self.dir_path(upload_id)?;
        let _ = self.fs.mkdir(&dir_path, true).await;

        let path = self.part_path(upload_id, part_number)?;
        let mut writer = self
            .fs
            .create(&path, true)
            .await
            .map_err(|e| format!("Failed to create part file in Curvine: {}", e))?;

        let mut hasher = md5::Context::new();
        let mut sha256 = sha2::Sha256::new();
        let buffer_size = self.config.buffer_size();
        let threshold = self.config.memory_buffer_threshold;
        let mut memory_buf = BytesMut::with_capacity(threshold.min(buffer_size));
        let mut staged: usize = 0;

        loop {
            // Same contract as the local backend: a read error removes the
            // staged part, so `CompleteMultipartUpload` cannot assemble a
            // truncated object from it.
            let chunk = match body.poll_read().await {
                Ok(Some(chunk)) => chunk,
                Ok(None) => break,
                Err(e) => {
                    let _ = self.fs.delete_with_opts(&path, true, true).await;
                    return Err(format!("Failed to read from body: {}", e));
                }
            };
            staged += chunk.len();
            sha256.update(&chunk);
            hasher.consume(&chunk);

            if memory_buf.len() + chunk.len() <= threshold {
                memory_buf.extend_from_slice(&chunk);
            } else {
                if !memory_buf.is_empty() {
                    writer
                        .write(&memory_buf)
                        .await
                        .map_err(|e| format!("Failed to write part data to Curvine: {}", e))?;
                    memory_buf.clear();
                }
                writer
                    .write(&chunk)
                    .await
                    .map_err(|e| format!("Failed to write part data to Curvine: {}", e))?;
            }
        }

        if !memory_buf.is_empty() {
            writer
                .write(&memory_buf)
                .await
                .map_err(|e| format!("Failed to write part data to Curvine: {}", e))?;
        }

        writer
            .complete()
            .await
            .map_err(|e| format!("Failed to complete part write in Curvine: {}", e))?;

        if let Err(mismatch) = expect.check(staged, &hex::encode(sha256.finalize())) {
            let _ = self.fs.delete_with_opts(&path, true, true).await;
            return Err(mismatch);
        }

        let digest = hasher.compute();
        Ok(format!("\"{:x}\"", digest))
    }

    /// Stages a single-object body in Curvine and returns a reader over it.
    pub async fn stage_object<R: crate::utils::io::PollRead + Send>(
        &self,
        staging_id: &str,
        body: &mut R,
        expect: StageExpect<'_>,
    ) -> Result<crate::utils::io::PollReaderEnum, String> {
        use crate::utils::io::PollReaderEnum;
        use sha2::Digest;

        let path = Path::from_str(format!("{}/{}/body", self.config.temp_dir, staging_id))
            .map_err(|e| format!("Invalid staging path: {}", e))?;
        let mut writer = self
            .fs
            .create(&path, true)
            .await
            .map_err(|e| format!("Failed to create staging file in Curvine: {}", e))?;

        let mut hasher = sha2::Sha256::new();
        let mut staged: usize = 0;
        while let Some(chunk) = body.poll_read().await? {
            hasher.update(&chunk);
            staged += chunk.len();
            writer
                .write(&chunk)
                .await
                .map_err(|e| format!("Failed to stage body in Curvine: {}", e))?;
        }
        writer
            .complete()
            .await
            .map_err(|e| format!("Failed to complete staged body in Curvine: {}", e))?;

        expect.check(staged, &hex::encode(hasher.finalize()))?;

        let reader = self
            .fs
            .open(&path)
            .await
            .map_err(|e| format!("Failed to reopen staged body in Curvine: {}", e))?;
        Ok(PollReaderEnum::Curvine(Box::new(reader)))
    }

    /// Removes a staged single-object body. Staging is internal state, so it
    /// is purged permanently instead of being parked in `/.Trash`.
    pub async fn cleanup_object(&self, staging_id: &str) -> Result<(), String> {
        let path = Path::from_str(format!("{}/{}", self.config.temp_dir, staging_id))
            .map_err(|e| format!("Invalid staging path: {}", e))?;
        match self.fs.delete_with_opts(&path, true, true).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("not found") || msg.contains("not exists") {
                    Ok(())
                } else {
                    Err(format!("Failed to remove staging dir: {}", e))
                }
            }
        }
    }

    /// Whether a part of this upload is staged.
    pub async fn part_exists(&self, upload_id: &str, part_number: u32) -> bool {
        match self.part_path(upload_id, part_number) {
            Ok(path) => self.fs.get_status(&path).await.is_ok(),
            Err(_) => false,
        }
    }

    /// Copy a staged part into the destination, verifying it on the way out.
    ///
    /// Same contract as the local backend: the MD5 is recomputed while
    /// streaming and compared with the ETag from `CompleteMultipartUpload`.
    pub async fn read_part_stream<W: Writer>(
        &self,
        upload_id: &str,
        part_number: u32,
        expected_etag: &str,
        writer: &mut W,
    ) -> Result<(), String> {
        let path = self.part_path(upload_id, part_number)?;
        let mut reader = self
            .fs
            .open(&path)
            .await
            .map_err(|e| format!("Failed to open part file in Curvine: {}", e))?;

        let buffer_size = self.config.buffer_size();
        let mut buf = BytesMut::with_capacity(buffer_size);
        buf.resize(buffer_size, 0);
        let mut hasher = md5::Context::new();

        loop {
            let n = reader
                .read(&mut buf)
                .await
                .map_err(|e| format!("Failed to read from part file: {}", e))?;
            if n == 0 {
                break;
            }

            hasher.consume(&buf[..n]);
            writer
                .write(&buf[..n])
                .await
                .map_err(|e| format!("Failed to write to destination: {}", e))?;
        }

        verify_part_etag(part_number, expected_etag, &hasher.compute())?;

        reader
            .complete()
            .await
            .map_err(|e| format!("Failed to complete reader in Curvine: {}", e))?;

        Ok(())
    }
}

// ============================================================================
// Temp Storage Enum (Static Dispatch)
// ============================================================================

/// Temp storage implementation enum for static dispatch
///
/// This enum provides zero-cost abstraction over different storage backends.
/// The compiler will generate optimized code for each variant without
/// dynamic dispatch overhead.
#[derive(Clone)]
pub enum TempStorageEnum {
    /// Local filesystem storage (standalone mode)
    Local(LocalTempStorage),
    /// Curvine distributed filesystem storage (distributed mode)
    Curvine(CurvineTempStorage),
}

impl TempStorageEnum {
    // ========================================================================
    // Factory Method
    // ========================================================================

    /// Factory method: create storage based on configuration
    ///
    /// # Arguments
    /// * `enable_distributed` - If true, use Curvine DFS; if false, use local FS
    ///   Must match `worker.enable_s3_gateway` configuration.
    /// * `local_temp_dir` - Local filesystem temp directory path
    /// * `curvine_temp_prefix` - Curvine DFS temp directory prefix (e.g., "/.s3temp")
    /// * `memory_buffer_threshold` - Memory buffer threshold for small parts
    /// * `max_memory_buffer` - Maximum memory buffer size
    /// * `fs` - UnifiedFileSystem instance (required when enable_distributed=true)
    ///
    /// # Panics
    /// Panics if `enable_distributed=true` but `fs` is `None`.
    pub fn create(
        enable_distributed: bool,
        local_temp_dir: String,
        curvine_temp_prefix: String,
        memory_buffer_threshold: usize,
        max_memory_buffer: usize,
        fs: Option<UnifiedFileSystem>,
    ) -> Self {
        if enable_distributed {
            let config = TempStorageConfig::new(
                curvine_temp_prefix,
                memory_buffer_threshold,
                max_memory_buffer,
            );
            TempStorageEnum::Curvine(CurvineTempStorage::new(
                fs.expect(
                    "UnifiedFileSystem required for distributed mode (enable_s3_gateway=true)",
                ),
                config,
            ))
        } else {
            let config =
                TempStorageConfig::new(local_temp_dir, memory_buffer_threshold, max_memory_buffer);
            TempStorageEnum::Local(LocalTempStorage::new(config))
        }
    }

    // ========================================================================
    // Delegated Methods (Static Dispatch)
    // ========================================================================

    /// Get storage configuration
    pub fn config(&self) -> &TempStorageConfig {
        match self {
            TempStorageEnum::Local(s) => s.config(),
            TempStorageEnum::Curvine(s) => s.config(),
        }
    }

    /// Create directory for upload session
    pub fn create_dir(&self, upload_id: &str) -> impl Future<Output = Result<(), String>> + Send {
        let this = self.clone();
        let upload_id = upload_id.to_string();
        async move {
            match this {
                TempStorageEnum::Local(s) => s.create_dir(&upload_id).await,
                TempStorageEnum::Curvine(s) => s.create_dir(&upload_id).await,
            }
        }
    }

    /// Remove entire upload session directory (cleanup after complete/abort)
    pub fn cleanup(&self, upload_id: &str) -> impl Future<Output = Result<(), String>> + Send {
        let this = self.clone();
        let upload_id = upload_id.to_string();
        async move {
            match this {
                TempStorageEnum::Local(s) => s.cleanup(&upload_id).await,
                TempStorageEnum::Curvine(s) => s.cleanup(&upload_id).await,
            }
        }
    }

    /// Check if using distributed storage
    pub fn is_distributed(&self) -> bool {
        matches!(self, TempStorageEnum::Curvine(_))
    }

    /// Get storage type name for logging
    pub fn storage_type(&self) -> &'static str {
        match self {
            TempStorageEnum::Local(_) => "local",
            TempStorageEnum::Curvine(_) => "curvine",
        }
    }

    /// Stages a single-object body in the configured temp storage.
    pub async fn stage_object<R: crate::utils::io::PollRead + Send>(
        &self,
        staging_id: &str,
        body: &mut R,
        expect: StageExpect<'_>,
    ) -> Result<crate::utils::io::PollReaderEnum, String> {
        match self {
            TempStorageEnum::Local(s) => s.stage_object(staging_id, body, expect).await,
            TempStorageEnum::Curvine(s) => s.stage_object(staging_id, body, expect).await,
        }
    }

    /// Removes a staged single-object body.
    pub async fn cleanup_object(&self, staging_id: &str) -> Result<(), String> {
        match self {
            TempStorageEnum::Local(s) => s.cleanup_object(staging_id).await,
            TempStorageEnum::Curvine(s) => s.cleanup_object(staging_id).await,
        }
    }

    /// Stream write part data, returns ETag (MD5 hash)
    pub async fn write_part_stream<R: crate::utils::io::PollRead + Send>(
        &self,
        upload_id: &str,
        part_number: u32,
        body: &mut R,
        expect: StageExpect<'_>,
    ) -> Result<String, String> {
        match self {
            TempStorageEnum::Local(s) => {
                s.write_part_stream(upload_id, part_number, body, expect)
                    .await
            }
            TempStorageEnum::Curvine(s) => {
                s.write_part_stream(upload_id, part_number, body, expect)
                    .await
            }
        }
    }

    /// Whether a part of this upload is staged, in either backend.
    pub async fn part_exists(&self, upload_id: &str, part_number: u32) -> bool {
        match self {
            TempStorageEnum::Local(s) => s.part_exists(upload_id, part_number).await,
            TempStorageEnum::Curvine(s) => s.part_exists(upload_id, part_number).await,
        }
    }

    /// Copy a staged part into the destination, verifying its ETag.
    pub async fn read_part_stream<W: Writer>(
        &self,
        upload_id: &str,
        part_number: u32,
        expected_etag: &str,
        writer: &mut W,
    ) -> Result<(), String> {
        match self {
            TempStorageEnum::Local(s) => {
                s.read_part_stream(upload_id, part_number, expected_etag, writer)
                    .await
            }
            TempStorageEnum::Curvine(s) => {
                s.read_part_stream(upload_id, part_number, expected_etag, writer)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_temp_storage_config() {
        let config = TempStorageConfig::new("/tmp/test".to_string(), 1024 * 1024, 16 * 1024 * 1024);
        assert_eq!(config.temp_dir, "/tmp/test");
        assert_eq!(config.memory_buffer_threshold, 1024 * 1024);
        assert_eq!(config.max_memory_buffer, 16 * 1024 * 1024);
    }

    #[test]
    fn test_local_temp_storage_paths() {
        let config = TempStorageConfig::new("/tmp/curvine".to_string(), 1024, 4096);
        let storage = LocalTempStorage::new(config);

        assert_eq!(
            storage.part_path("upload-123", 1),
            "/tmp/curvine/upload-123/1"
        );
        assert_eq!(storage.dir_path("upload-123"), "/tmp/curvine/upload-123");
    }

    #[test]
    fn test_factory_creates_local_storage() {
        let storage = TempStorageEnum::create(
            false, // enable_distributed = false (enable_s3_gateway=false)
            "/tmp/local".to_string(),
            "/.s3temp".to_string(),
            1024,
            4096,
            None,
        );

        assert!(!storage.is_distributed());
        assert_eq!(storage.storage_type(), "local");
        assert_eq!(storage.config().temp_dir, "/tmp/local");
        assert_eq!(storage.config().memory_buffer_threshold, 1024);
        assert_eq!(storage.config().max_memory_buffer, 4096);
    }

    #[test]
    fn test_factory_creates_curvine_storage() {
        let result = std::panic::catch_unwind(|| {
            TempStorageEnum::create(
                true, // enable_distributed = true (enable_s3_gateway=true)
                "/tmp/local".to_string(),
                "/.s3temp".to_string(),
                1024,
                4096,
                None, // fs is None, should panic
            );
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_config_buffer_size() {
        let config1 = TempStorageConfig::new("/tmp".to_string(), 1024, 0);
        assert_eq!(config1.buffer_size(), DEFAULT_BUFFER_SIZE);

        let config2 = TempStorageConfig::new("/tmp".to_string(), 1024, 16 * 1024 * 1024);
        assert_eq!(config2.buffer_size(), 16 * 1024 * 1024);

        let config3 = TempStorageConfig::new("/tmp".to_string(), 1024, 32 * 1024);
        assert_eq!(config3.buffer_size(), MIN_BUFFER_SIZE);

        let config4 = TempStorageConfig::new("/tmp".to_string(), 1024, 1024);
        assert_eq!(config4.buffer_size(), MIN_BUFFER_SIZE);
    }

    #[test]
    fn test_storage_type_discrimination() {
        let local_storage = TempStorageEnum::create(
            false,
            "/tmp/local".to_string(),
            "/.s3temp".to_string(),
            1024,
            4096,
            None,
        );

        assert_eq!(local_storage.storage_type(), "local");
        assert!(!local_storage.is_distributed());

        match local_storage {
            TempStorageEnum::Local(_) => {}
            TempStorageEnum::Curvine(_) => panic!("Should be Local storage"),
        }
    }

    #[tokio::test]
    async fn test_local_storage_create_dir() {
        use std::env;

        let temp_path = env::temp_dir()
            .join(format!("curvine-test-{}", uuid::Uuid::new_v4()))
            .to_str()
            .unwrap()
            .to_string();

        let config = TempStorageConfig::new(temp_path.clone(), 1024, 4096);
        let storage = LocalTempStorage::new(config);

        let upload_id = "test-upload-123";
        let result = storage.create_dir(upload_id).await;
        assert!(result.is_ok());

        let expected_dir = std::path::PathBuf::from(&temp_path).join(upload_id);
        assert!(expected_dir.exists());
        assert!(expected_dir.is_dir());
        let _ = std::fs::remove_dir_all(&temp_path);
    }

    #[tokio::test]
    async fn test_local_storage_cleanup() {
        use std::env;

        let temp_path = env::temp_dir()
            .join(format!("curvine-test-{}", uuid::Uuid::new_v4()))
            .to_str()
            .unwrap()
            .to_string();

        let config = TempStorageConfig::new(temp_path.clone(), 1024, 4096);
        let storage = LocalTempStorage::new(config);

        let upload_id = "test-upload-456";
        storage.create_dir(upload_id).await.unwrap();

        let expected_dir = std::path::PathBuf::from(&temp_path).join(upload_id);
        assert!(expected_dir.exists());

        let result = storage.cleanup(upload_id).await;
        assert!(result.is_ok());
        assert!(!expected_dir.exists());
        let _ = std::fs::remove_dir_all(&temp_path);
    }

    /// A reader that yields one chunk and then fails, standing in for a client
    /// that drops the connection mid-part.
    struct FailingBody {
        sent: bool,
    }

    #[async_trait::async_trait]
    impl crate::utils::io::PollRead for FailingBody {
        async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String> {
            if self.sent {
                return Err("connection reset".to_string());
            }
            self.sent = true;
            Ok(Some(vec![9u8; 1024]))
        }
    }

    /// A body that fails mid-read must not leave a partial part behind:
    /// `CompleteMultipartUpload` streams whatever a part file holds, so a
    /// truncated part would be published as a successful object.
    #[tokio::test]
    async fn test_local_write_part_stream_removes_a_partial_part() {
        use std::env;

        let temp_path = env::temp_dir()
            .join(format!("curvine-test-{}", uuid::Uuid::new_v4()))
            .to_str()
            .unwrap()
            .to_string();
        let config = TempStorageConfig::new(temp_path.clone(), 1024, 4096);
        let storage = LocalTempStorage::new(config);
        storage.create_dir("upload-partial").await.unwrap();

        let mut body = FailingBody { sent: false };
        let err = storage
            .write_part_stream("upload-partial", 1, &mut body, StageExpect::default())
            .await
            .expect_err("a broken body must fail the part");
        assert!(err.contains("connection reset"), "{err}");

        let part = storage.part_path("upload-partial", 1);
        assert!(
            !std::path::Path::new(&part).exists(),
            "a partially read part must not survive: {part}"
        );

        storage.cleanup("upload-partial").await.unwrap();
        let _ = std::fs::remove_dir_all(&temp_path);
    }

    /// The ETag listed in `CompleteMultipartUpload` is the only statement about
    /// what the client uploaded, so it is compared with the MD5 recomputed
    /// while reading. Quotes, `W/` prefixes and case all denote the same MD5.
    #[test]
    fn part_etag_matches_the_staged_digest() {
        let data = b"part body";
        let mut hasher = md5::Context::new();
        hasher.consume(data);
        let digest = hasher.compute();
        let hex = format!("{:x}", digest);

        for accepted in [
            hex.clone(),
            format!("\"{hex}\""),
            format!("W/\"{hex}\""),
            hex.to_uppercase(),
            format!(" \"{hex}\" "),
        ] {
            assert!(
                verify_part_etag(1, &accepted, &digest).is_ok(),
                "expected {accepted} to be accepted"
            );
        }

        // An empty ETag is not "skip the check": it cannot match a digest.
        assert!(verify_part_etag(1, "", &digest).is_err());
        assert!(verify_part_etag(1, "   ", &digest).is_err());
        assert!(verify_part_etag(1, "deadbeef", &digest).is_err());

        // A part truncated after staging hashes differently, which is exactly
        // what a mid-write failure used to leave behind.
        let mut short = md5::Context::new();
        short.consume(&data[..4]);
        let err = verify_part_etag(2, &hex, &short.compute())
            .expect_err("a part that does not match its etag must be rejected");
        assert!(err.contains("part 2"), "{err}");
        assert!(err.contains("etag"), "{err}");
    }

    /// With the default `/tmp/curvine-temp` staging dir, default modes let any
    /// local user read in-flight object bodies.
    #[tokio::test]
    async fn test_local_staging_data_is_owner_only() {
        use std::env;
        use std::io::Cursor;
        use std::os::unix::fs::PermissionsExt;

        let temp_path = env::temp_dir()
            .join(format!("curvine-test-{}", uuid::Uuid::new_v4()))
            .to_str()
            .unwrap()
            .to_string();
        let storage = LocalTempStorage::new(TempStorageConfig::new(temp_path.clone(), 1024, 4096));
        storage.create_dir("upload-perm").await.unwrap();

        let part_dir = storage.dir_path("upload-perm");
        let part_dir_mode = std::fs::metadata(&part_dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(part_dir_mode, 0o700, "part dir mode {part_dir_mode:o}");

        let mut body = tokio::io::BufReader::new(Cursor::new(vec![1u8; 2048]));
        storage
            .write_part_stream("upload-perm", 1, &mut body, StageExpect::default())
            .await
            .unwrap();
        let part = storage.part_path("upload-perm", 1);
        let part_mode = std::fs::metadata(&part).unwrap().permissions().mode() & 0o777;
        assert_eq!(part_mode, 0o600, "part file mode {part_mode:o}");

        // The single-object staging body is the same kind of data.
        let mut src = tokio::io::BufReader::new(Cursor::new(vec![2u8; 64]));
        storage
            .stage_object("stage-perm", &mut src, StageExpect::default())
            .await
            .unwrap();
        let staged = format!("{}/stage-perm/body", temp_path);
        let staged_mode = std::fs::metadata(&staged).unwrap().permissions().mode() & 0o777;
        assert_eq!(staged_mode, 0o600, "staged body mode {staged_mode:o}");
        let staging_dir_mode = std::fs::metadata(format!("{}/stage-perm", temp_path))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            staging_dir_mode, 0o700,
            "staging dir mode {staging_dir_mode:o}"
        );

        storage.cleanup("upload-perm").await.unwrap();
        let _ = std::fs::remove_dir_all(&temp_path);
    }

    #[tokio::test]
    async fn test_local_storage_write_read_stream() {
        use std::env;
        use std::io::Cursor;

        let temp_path = env::temp_dir()
            .join(format!("curvine-test-{}", uuid::Uuid::new_v4()))
            .to_str()
            .unwrap()
            .to_string();

        let config = TempStorageConfig::new(temp_path.clone(), 1024, 4096);
        let storage = LocalTempStorage::new(config);

        let upload_id = "test-upload-stream";
        storage.create_dir(upload_id).await.unwrap();

        let small_data = b"Hello, World!";
        let mut body = tokio::io::BufReader::new(Cursor::new(small_data.to_vec()));

        let etag = storage
            .write_part_stream(upload_id, 1, &mut body, StageExpect::default())
            .await;
        assert!(etag.is_ok());
        let etag_value = etag.unwrap();
        assert!(etag_value.starts_with('"'));
        assert!(etag_value.ends_with('"'));

        let part_path = storage.part_path(upload_id, 1);
        let file_data = tokio::fs::read(&part_path).await.unwrap();
        assert_eq!(file_data, small_data);

        let large_data: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();
        let mut body2 = tokio::io::BufReader::new(Cursor::new(large_data.clone()));

        let etag2 = storage
            .write_part_stream(upload_id, 2, &mut body2, StageExpect::default())
            .await;
        assert!(etag2.is_ok());

        let part_path2 = storage.part_path(upload_id, 2);
        let file_data2 = tokio::fs::read(&part_path2).await.unwrap();
        assert_eq!(file_data2, large_data);

        storage.cleanup(upload_id).await.unwrap();
        let _ = std::fs::remove_dir_all(&temp_path);
    }
}

#[cfg(test)]
mod stage_expect_tests {
    use super::StageExpect;

    #[test]
    fn accepts_matching_length_and_hash() {
        let expect = StageExpect {
            sha256_hex: Some("abcdef"),
            content_length: Some(10),
        };
        assert!(expect.check(10, "abcdef").is_ok());
        // Hex comparison is case-insensitive.
        assert!(expect.check(10, "ABCDEF").is_ok());
    }

    #[test]
    fn rejects_length_mismatch() {
        let expect = StageExpect {
            sha256_hex: None,
            content_length: Some(10),
        };
        let err = expect.check(9, "x").unwrap_err();
        assert!(err.contains("does not match content-length"), "{err}");
    }

    #[test]
    fn rejects_hash_mismatch() {
        let expect = StageExpect {
            sha256_hex: Some("abcdef"),
            content_length: None,
        };
        let err = expect.check(10, "000000").unwrap_err();
        assert!(err.contains("sha256"), "{err}");
    }

    #[test]
    fn unsigned_payload_skips_hash_verification() {
        let expect = StageExpect {
            sha256_hex: None,
            content_length: None,
        };
        assert!(expect.check(0, "").is_ok());
        assert!(expect.check(12345, "anything").is_ok());
    }
}
