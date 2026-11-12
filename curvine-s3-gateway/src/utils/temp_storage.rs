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

/// What an uploaded part recorded about itself, published together with its
/// bytes.
///
/// The local backend stores it as a trailer at the end of the part file; the
/// Curvine backend stores it in an inode xattr so preflight needs one metadata
/// call per part. Both are written before the publishing rename, so bytes and
/// record always come from the same upload generation.
///
/// Bytes and record are one file published by a single atomic rename, so a
/// concurrent re-upload of the same part can never publish one generation's
/// bytes with another generation's record (a separate sidecar could, across
/// gateway instances sharing the staging area).
///
/// `size` is taken from the staged bytes themselves rather than remembered from
/// the request, so the value cannot drift from what is on disk; `etag` is the
/// MD5 computed while writing. `CompleteMultipartUpload` validates the request
/// against this record, and a part whose record is missing is treated as never
/// having been uploaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartMeta {
    pub etag: String,
    pub size: usize,
}

/// Curvine inode xattr holding a staged part's record. One `get_status` on the
/// part then yields both the record and the length, so preflight costs one
/// round trip per part instead of opening and reading a second file.
const XATTR_PART_META: &str = "s3.part";

/// Trailer layout: magic(8) + version(1) + etag ascii(32) + size u64 LE(8).
const PART_TRAILER_MAGIC: &[u8; 8] = b"CVS3PART";
const PART_TRAILER_VERSION: u8 = 1;
pub const PART_TRAILER_SIZE: usize = 8 + 1 + 32 + 8;

impl PartMeta {
    /// Serialise into the fixed-width trailer appended to a part file.
    fn encode_trailer(&self) -> [u8; PART_TRAILER_SIZE] {
        let mut out = [0u8; PART_TRAILER_SIZE];
        out[..8].copy_from_slice(PART_TRAILER_MAGIC);
        out[8] = PART_TRAILER_VERSION;
        out[9..41].copy_from_slice(self.etag.as_bytes());
        out[41..49].copy_from_slice(&(self.size as u64).to_le_bytes());
        out
    }

    /// `"<etag>:<size>"`, the form stored in a Curvine inode xattr so one
    /// `get_status` returns a part's whole record.
    fn encode_xattr(&self) -> String {
        format!("{}:{}", self.etag, self.size)
    }

    fn decode_xattr(text: &str) -> Option<Self> {
        let (etag, size) = text.trim().split_once(':')?;
        Some(Self {
            etag: etag.to_string(),
            size: size.parse().ok()?,
        })
    }

    fn decode_trailer(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != PART_TRAILER_SIZE
            || &bytes[..8] != PART_TRAILER_MAGIC
            || bytes[8] != PART_TRAILER_VERSION
        {
            return None;
        }
        let etag = std::str::from_utf8(&bytes[9..41]).ok()?;
        let size = u64::from_le_bytes(bytes[41..49].try_into().ok()?);
        Some(Self {
            etag: etag.to_string(),
            size: usize::try_from(size).ok()?,
        })
    }
}

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

/// Removes a staging temporary file unless its upload published it.
///
/// A part is written to `.tmp-<uuid>` and published by one rename. Without this,
/// every failed attempt would leave its own temporary file in the session
/// directory until Complete or Abort, and forever once the client abandons the
/// upload.
struct TmpFile(Option<String>);

impl TmpFile {
    fn new(path: String) -> Self {
        Self(Some(path))
    }

    /// The temporary path was published (or is otherwise gone): stop cleaning.
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for TmpFile {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = std::fs::remove_file(path);
        }
    }
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
            "InvalidPart: part {} etag {} does not match the staged part ({})",
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

        // Stage to a unique temporary name and publish with one rename, so the
        // final part file is always one coherent generation: bytes and record
        // can never come from different concurrent uploads of the same part.
        let tmp_path = format!("{}.tmp-{}", path, uuid::Uuid::new_v4());
        let mut tmp = TmpFile::new(tmp_path.clone());
        let mut file = open_private_writer(&tmp_path, false)
            .await
            .map_err(|e| format!("Failed to create part file {}: {}", tmp_path, e))?;

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
                    // `tmp` removes the partial part on the way out.
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

        expect.check(staged, &hex::encode(sha256.finalize()))?;

        // The record rides in the part file itself: one rename publishes bytes
        // and record together, so `Complete` can never see a torn generation.
        let digest = hasher.compute();
        let etag = format!("{:x}", digest);
        let trailer = PartMeta {
            etag: etag.clone(),
            size: staged,
        }
        .encode_trailer();
        file.write_all(&trailer)
            .await
            .map_err(|e| format!("Failed to append part record: {}", e))?;
        file.flush()
            .await
            .map_err(|e| format!("Failed to flush part file: {}", e))?;
        drop(file);

        tokio::fs::rename(&tmp_path, &path)
            .await
            .map_err(|e| format!("Failed to publish part {}: {}", path, e))?;
        tmp.disarm();

        Ok(format!("\"{etag}\""))
    }

    /// The record in this part file's trailer.
    pub async fn part_meta(&self, upload_id: &str, part_number: u32) -> Option<PartMeta> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let path = self.part_path(upload_id, part_number);
        let mut file = tokio::fs::File::open(&path).await.ok()?;
        let len = file.metadata().await.ok()?.len();
        if len < PART_TRAILER_SIZE as u64 {
            return None;
        }
        file.seek(std::io::SeekFrom::Start(len - PART_TRAILER_SIZE as u64))
            .await
            .ok()?;
        let mut trailer = [0u8; PART_TRAILER_SIZE];
        file.read_exact(&mut trailer).await.ok()?;
        PartMeta::decode_trailer(&trailer)
    }

    /// Whether an upload session exists.
    pub async fn upload_exists(&self, upload_id: &str) -> bool {
        tokio::fs::try_exists(self.dir_path(upload_id))
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

        // The trailer is the part's own record, not payload: copy exactly the
        // bytes that were staged.
        let payload_len = file
            .metadata()
            .await
            .map_err(|e| e.to_string())?
            .len()
            .saturating_sub(PART_TRAILER_SIZE as u64);
        let buffer_size = self.config.buffer_size();
        let mut buf = BytesMut::with_capacity(buffer_size);
        buf.resize(buffer_size, 0);
        let mut hasher = md5::Context::new();
        let mut remaining = payload_len;

        while remaining > 0 {
            let want = std::cmp::min(buf.len() as u64, remaining) as usize;
            let n = file
                .read(&mut buf[..want])
                .await
                .map_err(|e| format!("Failed to read from part file: {}", e))?;
            if n == 0 {
                return Err(format!(
                    "InvalidPart: part {} of upload {} was truncated after staging",
                    part_number, upload_id
                ));
            }
            remaining -= n as u64;

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
        // Stage to a unique temporary name and publish with one rename, so the
        // final part file is always one coherent generation: bytes and record
        // can never come from different concurrent uploads of the same part,
        // even across gateway instances sharing this staging area.
        let tmp_path = Path::from_str(format!("{}.tmp-{}", path, uuid::Uuid::new_v4()))
            .map_err(|e| format!("Invalid part path: {}", e))?;
        let mut writer = self
            .fs
            .create(&tmp_path, true)
            .await
            .map_err(|e| format!("Failed to create part file in Curvine: {}", e))?;

        // One attempt up to the record: any failure drops the temporary part
        // instead of leaving it in the session directory.
        let staged = async {
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
                    Err(e) => return Err(format!("Failed to read from body: {}", e)),
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

            expect.check(staged, &hex::encode(sha256.finalize()))?;
            writer
                .complete()
                .await
                .map_err(|e| format!("Failed to complete part write in Curvine: {}", e))?;

            // The record rides on the part inode and the rename below is the
            // only publish point, so bytes and record always come from the same
            // upload generation. It is also what keeps preflight to one
            // `get_status` per part instead of an extra open and read.
            let digest = hasher.compute();
            let etag = format!("{:x}", digest);
            let record = PartMeta {
                etag: etag.clone(),
                size: staged,
            }
            .encode_xattr();
            self.fs
                .set_attr(
                    &tmp_path,
                    curvine_model::SetAttrOptsBuilder::new()
                        .add_x_attr(XATTR_PART_META, record.into_bytes())
                        .build(),
                )
                .await
                .map_err(|e| format!("Failed to record part metadata in Curvine: {}", e))?;
            Ok(etag)
        }
        .await;

        let etag = match staged {
            Ok(etag) => etag,
            Err(e) => {
                let _ = self.fs.delete_with_opts(&tmp_path, true, true).await;
                return Err(e);
            }
        };

        if let Err(e) = self.fs.rename(&tmp_path, &path).await {
            let _ = self.fs.delete_with_opts(&tmp_path, true, true).await;
            return Err(format!("Failed to publish part in Curvine: {}", e));
        }

        Ok(format!("\"{etag}\""))
    }

    /// The record on this part's inode.
    ///
    /// One metadata call per part, which is why the record is an xattr here
    /// rather than a trailer: preflight runs once per listed part, up to the S3
    /// 10,000-part maximum, on the Complete critical path.
    pub async fn part_meta(&self, upload_id: &str, part_number: u32) -> Option<PartMeta> {
        let path = self.part_path(upload_id, part_number).ok()?;
        let status = self.fs.get_status(&path).await.ok()?;
        let raw = status.x_attr.get(XATTR_PART_META)?;
        PartMeta::decode_xattr(&String::from_utf8_lossy(raw))
    }

    /// Whether an upload session exists.
    pub async fn upload_exists(&self, upload_id: &str) -> bool {
        match self.dir_path(upload_id) {
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

        // The record lives in an xattr here, so the whole file is payload.
        let mut remaining = reader.len() as u64;
        let buffer_size = self.config.buffer_size();
        let mut buf = BytesMut::with_capacity(buffer_size);
        buf.resize(buffer_size, 0);
        let mut hasher = md5::Context::new();

        while remaining > 0 {
            let want = std::cmp::min(buf.len() as u64, remaining) as usize;
            let read = match reader.read(&mut buf[..want]).await {
                Ok(n) => n,
                Err(e) => {
                    // Complete on this path too: EOF alone never sends the
                    // completion RPC, so dropping the reader would leave a
                    // worker-side read context open.
                    let _ = reader.complete().await;
                    return Err(format!("Failed to read from part file: {}", e));
                }
            };
            if read == 0 {
                let _ = reader.complete().await;
                return Err(format!(
                    "InvalidPart: part {} of upload {} was truncated after staging",
                    part_number, upload_id
                ));
            }
            remaining -= read as u64;

            hasher.consume(&buf[..read]);
            if let Err(e) = writer.write(&buf[..read]).await {
                let _ = reader.complete().await;
                return Err(format!("Failed to write to destination: {}", e));
            }
        }

        let verified = verify_part_etag(part_number, expected_etag, &hasher.compute());
        // Complete on both paths: EOF does not send the completion RPC, so
        // dropping the reader on the verify-error path would leak a read
        // context.
        let completed = reader.complete().await;
        verified?;
        completed.map_err(|e| format!("Failed to complete reader in Curvine: {}", e))?;
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

    /// Whether an upload session exists, in either backend.
    pub async fn upload_exists(&self, upload_id: &str) -> bool {
        match self {
            TempStorageEnum::Local(s) => s.upload_exists(upload_id).await,
            TempStorageEnum::Curvine(s) => s.upload_exists(upload_id).await,
        }
    }

    /// The record published with an uploaded part, in either backend.
    pub async fn part_meta(&self, upload_id: &str, part_number: u32) -> Option<PartMeta> {
        match self {
            TempStorageEnum::Local(s) => s.part_meta(upload_id, part_number).await,
            TempStorageEnum::Curvine(s) => s.part_meta(upload_id, part_number).await,
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

    /// The record is published with the bytes by one rename, so a part that is
    /// rewritten leaves exactly one coherent generation behind: the record must
    /// always describe the bytes on disk, never an earlier attempt's.
    #[tokio::test]
    async fn a_part_record_always_describes_its_own_bytes() {
        use std::env;
        use std::io::Cursor;

        let temp_path = env::temp_dir()
            .join(format!("curvine-test-{}", uuid::Uuid::new_v4()))
            .to_str()
            .unwrap()
            .to_string();
        let storage = LocalTempStorage::new(TempStorageConfig::new(temp_path.clone(), 1024, 4096));
        storage.create_dir("upload-atomic").await.unwrap();

        let first = vec![7u8; 4096];
        let mut body = tokio::io::BufReader::new(Cursor::new(first.clone()));
        let etag = storage
            .write_part_stream("upload-atomic", 1, &mut body, StageExpect::default())
            .await
            .unwrap();

        let second = vec![9u8; 2048];
        let mut body = tokio::io::BufReader::new(Cursor::new(second.clone()));
        let second_etag = storage
            .write_part_stream("upload-atomic", 1, &mut body, StageExpect::default())
            .await
            .unwrap();

        let meta = storage.part_meta("upload-atomic", 1).await.unwrap();
        assert_eq!(meta.size, second.len(), "the record must track the rewrite");
        assert_eq!(meta.etag, second_etag.trim_matches('"'));
        assert_ne!(meta.etag, etag.trim_matches('"'));

        storage.cleanup("upload-atomic").await.unwrap();
        let _ = std::fs::remove_dir_all(&temp_path);
    }

    /// Complete must assemble the staged payload only. The trailer is internal
    /// bookkeeping; the file must therefore be the payload followed by exactly
    /// one fixed-width record, so `read_part_stream` can stop at the boundary.
    #[tokio::test]
    async fn a_part_file_is_its_payload_followed_by_one_record() {
        use std::env;
        use std::io::Cursor;

        let temp_path = env::temp_dir()
            .join(format!("curvine-test-{}", uuid::Uuid::new_v4()))
            .to_str()
            .unwrap()
            .to_string();
        let storage = LocalTempStorage::new(TempStorageConfig::new(temp_path.clone(), 1024, 4096));
        storage.create_dir("upload-layout").await.unwrap();

        let payload = b"payload-bytes".to_vec();
        let mut body = tokio::io::BufReader::new(Cursor::new(payload.clone()));
        let etag = storage
            .write_part_stream("upload-layout", 1, &mut body, StageExpect::default())
            .await
            .unwrap();

        let bytes = tokio::fs::read(storage.part_path("upload-layout", 1))
            .await
            .unwrap();
        assert_eq!(bytes.len(), payload.len() + PART_TRAILER_SIZE);
        assert_eq!(&bytes[..payload.len()], &payload[..]);

        let meta = PartMeta::decode_trailer(&bytes[payload.len()..]).expect("trailer must decode");
        assert_eq!(meta.size, payload.len());
        assert_eq!(meta.etag, etag.trim_matches('"'));

        // A record that does not describe its own payload length is not usable
        // as a preflight source of truth.
        assert!(PartMeta::decode_trailer(&bytes[..PART_TRAILER_SIZE]).is_none());

        storage.cleanup("upload-layout").await.unwrap();
        let _ = std::fs::remove_dir_all(&temp_path);
    }

    /// A part with no record was never staged (or lost its bytes): preflight
    /// must see that, so Complete rejects before creating the assembly object.
    #[tokio::test]
    async fn a_missing_or_unrecorded_part_has_no_metadata() {
        use std::env;
        use std::io::Cursor;

        let temp_path = env::temp_dir()
            .join(format!("curvine-test-{}", uuid::Uuid::new_v4()))
            .to_str()
            .unwrap()
            .to_string();
        let storage = LocalTempStorage::new(TempStorageConfig::new(temp_path.clone(), 1024, 4096));
        storage.create_dir("upload-meta").await.unwrap();

        assert!(!storage.upload_exists("upload-absent").await);
        assert!(storage.upload_exists("upload-meta").await);

        // Never uploaded.
        assert!(storage.part_meta("upload-meta", 5).await.is_none());

        // Uploaded, then the body disappeared: the record went with it.
        let mut body = tokio::io::BufReader::new(Cursor::new(vec![1u8; 1024]));
        storage
            .write_part_stream("upload-meta", 1, &mut body, StageExpect::default())
            .await
            .unwrap();
        assert!(storage.part_meta("upload-meta", 1).await.is_some());
        tokio::fs::remove_file(storage.part_path("upload-meta", 1))
            .await
            .unwrap();
        assert!(storage.part_meta("upload-meta", 1).await.is_none());

        storage.cleanup("upload-meta").await.unwrap();
        let _ = std::fs::remove_dir_all(&temp_path);
    }

    /// A part is staged under `.tmp-<uuid>` and published by one rename. A
    /// failed attempt must take its temporary file with it: otherwise every
    /// retry adds one more file to the session directory until Complete, and
    /// they stay forever if the client abandons the upload.
    #[tokio::test]
    async fn a_failed_part_write_leaves_no_temporary_file() {
        use std::env;
        use std::io::Cursor;

        let temp_path = env::temp_dir()
            .join(format!("curvine-test-{}", uuid::Uuid::new_v4()))
            .to_str()
            .unwrap()
            .to_string();
        let storage = LocalTempStorage::new(TempStorageConfig::new(temp_path.clone(), 1024, 4096));
        storage.create_dir("upload-tmp").await.unwrap();

        // A length mismatch fails after the bytes were written to the temporary
        // part, which is exactly the path that used to leak it.
        let mut body = tokio::io::BufReader::new(Cursor::new(vec![4u8; 512]));
        let err = storage
            .write_part_stream(
                "upload-tmp",
                1,
                &mut body,
                StageExpect {
                    sha256_hex: None,
                    content_length: Some(4096),
                },
            )
            .await
            .expect_err("a short body must be rejected");
        assert!(err.contains("length"), "{err}");

        let mut entries = tokio::fs::read_dir(storage.dir_path("upload-tmp"))
            .await
            .unwrap();
        let mut left = Vec::new();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            left.push(entry.file_name().to_string_lossy().to_string());
        }
        assert!(left.is_empty(), "a failed attempt left {left:?}");

        storage.cleanup("upload-tmp").await.unwrap();
        let _ = std::fs::remove_dir_all(&temp_path);
    }

    /// The Curvine backend stores its record in an inode xattr, not as a file
    /// trailer, so this is the codec preflight depends on in the mode where
    /// `enable_s3_gateway = true`. Only a well-formed record may decode: a
    /// garbage value must mean "part not staged", never a bogus length.
    #[test]
    fn xattr_record_round_trips_and_rejects_garbage() {
        let meta = PartMeta {
            etag: "0123456789abcdef0123456789abcdef".to_string(),
            size: 5 << 20,
        };
        assert_eq!(PartMeta::decode_xattr(&meta.encode_xattr()), Some(meta));

        for garbage in [
            "",
            "no-separator",
            "etag:not-a-number",
            "etag:99999999999999999999999999",
        ] {
            assert_eq!(PartMeta::decode_xattr(garbage), None, "{garbage:?}");
        }
    }

    /// A rejected Complete must not destroy staged parts: the client fixes the
    /// list and retries. Validation only reads, so the parts and their records
    /// must still be there afterwards.
    #[tokio::test]
    async fn a_rejected_part_list_leaves_the_staged_parts_in_place() {
        use std::env;
        use std::io::Cursor;

        let temp_path = env::temp_dir()
            .join(format!("curvine-test-{}", uuid::Uuid::new_v4()))
            .to_str()
            .unwrap()
            .to_string();
        let storage = LocalTempStorage::new(TempStorageConfig::new(temp_path.clone(), 1024, 4096));
        storage.create_dir("upload-retry").await.unwrap();

        let mut body = tokio::io::BufReader::new(Cursor::new(vec![3u8; 2048]));
        let etag = storage
            .write_part_stream("upload-retry", 1, &mut body, StageExpect::default())
            .await
            .unwrap();

        // A Complete naming a part that was never uploaded is rejected...
        assert!(storage.part_meta("upload-retry", 2).await.is_none());
        // ...and the staged part with its record survives for the retry.
        assert!(storage.upload_exists("upload-retry").await);
        assert!(tokio::fs::try_exists(storage.part_path("upload-retry", 1))
            .await
            .unwrap());
        let meta = storage.part_meta("upload-retry", 1).await.unwrap();
        assert_eq!(meta.etag, etag.trim_matches('"'));

        storage.cleanup("upload-retry").await.unwrap();
        let _ = std::fs::remove_dir_all(&temp_path);
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
        // Bytes and record are one file: payload followed by the trailer.
        assert_eq!(file_data.len(), small_data.len() + PART_TRAILER_SIZE);
        assert_eq!(&file_data[..small_data.len()], small_data);
        let meta = storage.part_meta(upload_id, 1).await.unwrap();
        assert_eq!(meta.size, small_data.len());
        assert_eq!(meta.etag, etag_value.trim_matches('"'));

        let large_data: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();
        let mut body2 = tokio::io::BufReader::new(Cursor::new(large_data.clone()));

        let etag2 = storage
            .write_part_stream(upload_id, 2, &mut body2, StageExpect::default())
            .await;
        assert!(etag2.is_ok());

        let part_path2 = storage.part_path(upload_id, 2);
        let file_data2 = tokio::fs::read(&part_path2).await.unwrap();
        assert_eq!(file_data2.len(), large_data.len() + PART_TRAILER_SIZE);
        assert_eq!(&file_data2[..large_data.len()], &large_data[..]);

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
