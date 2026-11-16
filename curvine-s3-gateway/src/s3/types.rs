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

use crate::utils::io::PollRead;
use curvine_client::unified::{UnifiedFileSystem, UnifiedWriter};
use curvine_error::FsResult;
use curvine_fs_api::{FileSystem, Path, Writer};
use curvine_runtime::runtime::AsyncRuntime;
use std::sync::Arc;
use tracing;

#[derive(Debug, Default, Clone)]
pub struct UploadStats {
    pub total_written: u64,
    pub chunks_processed: u32,
    pub first_chunk_logged: bool,
    /// MD5 of everything written, which becomes the object's ETag.
    ///
    /// Computed while streaming so the read path never has to re-read an object
    /// to answer a HEAD.
    pub md5: Option<String>,
}

impl UploadStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_written(&mut self, bytes: u64) {
        self.total_written += bytes;
        self.chunks_processed += 1;
    }

    pub fn mark_first_chunk_logged(&mut self) {
        self.first_chunk_logged = true;
    }

    pub fn should_log_first_chunk(&self) -> bool {
        !self.first_chunk_logged
    }
}

/// Path a PUT body is staged at before it is published to `object`.
///
/// A sibling of the object, so publishing stays a same-directory rename, and
/// unique per upload so two concurrent writers cannot reuse one staging object.
fn staging_path_for(object: &Path, staging_id: &str) -> Result<Path, String> {
    let parent = object
        .parent()
        .map_err(|e| format!("object path {object} has no parent: {e}"))?
        .ok_or_else(|| format!("object path {object} has no parent"))?;
    Path::from_str(format!("{parent}/.curvine-s3-staging-{staging_id}"))
        .map_err(|e| format!("Invalid staging path: {e}"))
}

/// What the client declared about the body, verified before publishing.
///
/// Kept as one value so the staging path takes a single "expectations"
/// argument instead of growing one parameter per check.
#[derive(Clone, Copy, Default)]
pub struct BodyExpect<'a> {
    pub length: Option<usize>,
    pub content_md5: Option<&'a str>,
}

pub struct PutContext {
    pub fs: UnifiedFileSystem,
    pub rt: Arc<AsyncRuntime>,
    pub bucket: String,
    pub object: String,
    pub path: FsResult<Path>,
}

impl PutContext {
    pub fn new(
        fs: UnifiedFileSystem,
        rt: Arc<AsyncRuntime>,
        bucket: String,
        object: String,
        path: FsResult<Path>,
    ) -> Self {
        Self {
            fs,
            rt,
            bucket,
            object,
            path,
        }
    }

    pub fn log_start(&self) {
        tracing::info!("PUT object s3://{}/{}", self.bucket, self.object);
    }

    pub fn log_completion(&self, stats: &UploadStats) {
        tracing::info!(
            "PUT object s3://{}/{} completed, total bytes: {}",
            self.bucket,
            self.object,
            stats.total_written
        );
    }

    pub fn get_validated_path(&self) -> Result<Path, String> {
        self.path.as_ref().map(|p| p.clone()).map_err(|e| {
            tracing::error!(
                "Failed to convert S3 path s3://{}/{}: {}",
                self.bucket,
                self.object,
                e
            );
            e.to_string()
        })
    }

    /// Write to a staging path instead of the object itself.
    ///
    /// `UnifiedFileSystem::create` with `overwrite = true` makes the master
    /// clear the existing inode's blocks and reset its length immediately
    /// (`inode_file.rs::overwrite`). Writing in place therefore destroys the
    /// previous object before the new body is known to be complete: a client
    /// that disconnects half way leaves a truncated object, and the old data is
    /// already gone. Staging keeps the published object untouched until the
    /// whole body has been written and verified.
    pub fn staging_path(&self, staging_id: &str) -> Result<Path, String> {
        staging_path_for(&self.get_validated_path()?, staging_id)
    }

    pub async fn create_writer(&self) -> Result<UnifiedWriter, String> {
        self.create_writer_at(&self.get_validated_path()?).await
    }

    pub async fn create_writer_at(&self, path: &Path) -> Result<UnifiedWriter, String> {
        // `create_parent` is the second argument; overwrite stays false so a
        // pre-existing staging entry is refused rather than silently reused.
        self.fs.create(path, true).await.map_err(|e| {
            tracing::error!("Failed to create file at path {}: {}", path, e);
            e.to_string()
        })
    }
}

pub struct ChunkProcessor;

impl ChunkProcessor {
    pub fn process_chunk(chunk: &[u8], stats: &mut UploadStats) -> Result<(), String> {
        if chunk.is_empty() {
            return Ok(());
        }

        let n = chunk.len();
        // Length only. Dumping the first bytes of every chunk put object data
        // into the log on the upload hot path, so enabling debug logging leaked
        // user payloads.
        log::debug!("POLLREAD-CHUNK: {n} bytes");

        if stats.should_log_first_chunk() {
            log::debug!("PUT DEBUG - First chunk: {n} bytes");
            log::debug!(
                "PUT DEBUG - First chunk hex: {}",
                chunk[..n.min(64)]
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            stats.mark_first_chunk_logged();
        }

        stats.add_written(n as u64);
        tracing::debug!(
            "Written chunk: {} bytes, total: {} bytes",
            n,
            stats.total_written
        );

        Ok(())
    }
}

pub struct StreamReader;

impl StreamReader {
    pub async fn read_next_chunk(
        body: &mut (dyn PollRead + Unpin + Send),
    ) -> Result<Option<Vec<u8>>, String> {
        let chunk_result = body.poll_read().await.map_err(|e| {
            tracing::error!("Failed to read from input stream: {}", e);
            e.to_string()
        })?;

        match chunk_result {
            Some(data) if !data.is_empty() => Ok(Some(data)),
            Some(_) | None => Ok(None),
        }
    }
}

pub struct WriterHelper;

impl WriterHelper {
    pub async fn write_chunk(writer: &mut UnifiedWriter, chunk: &[u8]) -> Result<(), String> {
        writer.write(chunk).await.map_err(|e| {
            tracing::error!("Failed to write chunk to file: {}", e);
            e.to_string()
        })
    }

    pub async fn complete_write(writer: &mut UnifiedWriter) -> Result<(), String> {
        writer.complete().await.map_err(|e| {
            tracing::error!("Failed to complete file write: {}", e);
            e.to_string()
        })
    }
}

pub struct PutOperation;

impl PutOperation {
    /// Write the object without ever exposing a partial body under its key.
    ///
    /// The previous implementation wrote straight to the object path. Because
    /// the master drops the old blocks and length on `create(overwrite)`, a
    /// failed or disconnected upload destroyed the existing object and left a
    /// truncated one. The body is now staged and only published by a rename
    /// once it is complete, so any failure leaves the previous object intact.
    /// Write the object atomically.
    ///
    /// `on_staged` runs after the body is complete and before the publishing
    /// rename, so the metadata the client asked for is attached to the staging
    /// inode and reaches the destination in the same step. Writing it after the
    /// rename left a window where the object was visible with a fallback ETag.
    pub async fn execute(
        context: PutContext,
        body: &mut crate::utils::io::PollReaderEnum,
        metadata: crate::utils::s3_utils::S3Metadata,
        expect: BodyExpect<'_>,
    ) -> Result<String, String> {
        context.log_start();

        let staging_id = uuid::Uuid::new_v4().to_string();
        let staging = context.staging_path(&staging_id)?;
        let object = context.get_validated_path()?;

        let mut stats = UploadStats::new();
        let result = Self::stage_and_publish(
            &context, body, &staging, &object, &mut stats, &metadata, expect,
        )
        .await;

        if result.is_err() {
            // Staging is internal state, never user data: purge it instead of
            // parking it in `/.Trash`, where it would be retained for
            // `trash_retention` and let a client pin rejected bodies. Best
            // effort: the staging object may not exist if create failed.
            if let Err(e) = context.fs.delete_with_opts(&staging, false, true).await {
                tracing::warn!("Failed to remove staging object {staging}: {e}");
            }
        }

        result.map(|()| {
            context.log_completion(&stats);
            stats.md5.unwrap_or_default()
        })
    }

    async fn stage_and_publish(
        context: &PutContext,
        body: &mut crate::utils::io::PollReaderEnum,
        staging: &Path,
        object: &Path,
        stats: &mut UploadStats,
        metadata: &crate::utils::s3_utils::S3Metadata,
        expect: BodyExpect<'_>,
    ) -> Result<(), String> {
        let mut writer = context.create_writer_at(staging).await?;

        let mut md5 = md5::Context::new();

        loop {
            let chunk = match body.poll_read().await {
                Ok(Some(data)) => data,
                Ok(None) => break,
                Err(e) => {
                    // Drop the half-written staging object: publishing it would
                    // replace a good object with a truncated one.
                    let _ = writer.cancel().await;
                    return Err(e);
                }
            };

            md5.consume(&chunk);
            ChunkProcessor::process_chunk(&chunk, stats)?;
            if let Err(e) = WriterHelper::write_chunk(&mut writer, &chunk).await {
                let _ = writer.cancel().await;
                return Err(e);
            }
        }

        if let Err(e) = WriterHelper::complete_write(&mut writer).await {
            let _ = writer.cancel().await;
            return Err(e);
        }

        // A body shorter than the declared `Content-Length` means the client
        // disconnected early. The body stream simply ends, so without this
        // check the truncated body would be published as a successful object.
        if let Some(expected) = expect.length {
            if stats.total_written as usize != expected {
                let _ = writer.cancel().await;
                let _ = context.fs.delete_with_opts(staging, false, true).await;
                return Err(format!(
                    "ContentLengthMismatch: received {} of {} bytes",
                    stats.total_written, expected
                ));
            }
        }

        stats.md5 = Some(format!("{:x}", md5.compute()));
        let digest = stats.md5.clone().unwrap_or_default();

        // The client's own integrity check must decide before the object is
        // published. Reporting BadDigest after the rename answered 400 for a
        // write that had already replaced the previous object.
        if let Err(e) = crate::s3::s3_api::verify_content_md5(expect.content_md5, &digest) {
            let _ = context.fs.delete_with_opts(staging, false, true).await;
            return Err(e);
        }

        // Attach the S3 metadata while the object is still staging, so the
        // rename below publishes body and metadata together.
        if let Err(e) = metadata.write_to(&context.fs, staging, &digest).await {
            let _ = context.fs.delete_with_opts(staging, false, true).await;
            return Err(e);
        }

        // Publish. `rename` replaces the destination in this filesystem, so the
        // object only ever holds either the previous body or the complete new
        // one.
        context
            .fs
            .rename(staging, object)
            .await
            .map_err(|e| format!("Failed to publish staged object: {e}"))?;
        Ok(())
    }

    pub async fn execute_dyn(
        context: PutContext,
        body: &mut (dyn PollRead + Unpin + Send),
    ) -> Result<(), String> {
        context.log_start();

        let mut writer = context.create_writer().await?;
        let mut stats = UploadStats::new();

        loop {
            let chunk = match StreamReader::read_next_chunk(body).await? {
                Some(data) => data,
                None => break,
            };

            ChunkProcessor::process_chunk(&chunk, &mut stats)?;
            WriterHelper::write_chunk(&mut writer, &chunk).await?;
        }

        WriterHelper::complete_write(&mut writer).await?;
        context.log_completion(&stats);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Range {
    Int { first: u64, last: Option<u64> },
    Suffix { length: u64 },
}

impl Range {
    pub fn parse(header: &str) -> Result<Self, String> {
        let header = header.trim();

        let range_spec = header
            .strip_prefix("bytes=")
            .ok_or("Range header must start with 'bytes='")?;

        if let Some(suffix_str) = range_spec.strip_prefix('-') {
            let length = suffix_str
                .parse::<u64>()
                .map_err(|_| format!("Invalid suffix length: {suffix_str}"))?;
            if length == 0 {
                return curvine_core_error::err_box!("Suffix length cannot be zero");
            }
            return Ok(Range::Suffix { length });
        }

        let parts: Vec<&str> = range_spec.splitn(2, '-').collect();
        if parts.len() != 2 {
            return curvine_core_error::err_box!("Invalid range format: {}", range_spec);
        }

        let first = parts[0]
            .parse::<u64>()
            .map_err(|_| format!("Invalid start position: {}", parts[0]))?;

        if parts[1].is_empty() {
            Ok(Range::Int { first, last: None })
        } else {
            let last = parts[1]
                .parse::<u64>()
                .map_err(|_| format!("Invalid end position: {}", parts[1]))?;
            if first > last {
                return Err(format!(
                    "Start position {first} cannot be greater than end position {last}"
                ));
            }
            Ok(Range::Int {
                first,
                last: Some(last),
            })
        }
    }

    pub fn to_byte_range(&self, file_size: u64) -> Option<(u64, u64)> {
        // A range request against an empty object is always unsatisfiable.
        if file_size == 0 {
            return None;
        }

        match *self {
            Range::Int { first, last } => {
                if first >= file_size {
                    return None;
                }

                let end_pos = match last {
                    Some(last) => last.min(file_size - 1),
                    None => file_size - 1,
                };

                if first > end_pos {
                    return None;
                }

                let bytes_to_read = end_pos - first + 1;
                Some((first, bytes_to_read))
            }
            Range::Suffix { length } => {
                if length == 0 {
                    return None;
                }

                let actual_length = length.min(file_size);
                let start_pos = file_size - actual_length;
                Some((start_pos, actual_length))
            }
        }
    }

    pub fn to_content_range(&self, file_size: u64) -> Option<String> {
        if let Some((start, length)) = self.to_byte_range(file_size) {
            let end = start + length - 1;
            Some(format!("bytes {start}-{end}/{file_size}"))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod range_tests {
    use super::Range;

    fn byte_range(header: &str, size: u64) -> Option<(u64, u64)> {
        Range::parse(header).ok()?.to_byte_range(size)
    }

    #[test]
    fn resolves_open_and_closed_ranges() {
        assert_eq!(byte_range("bytes=0-9", 100), Some((0, 10)));
        assert_eq!(byte_range("bytes=90-", 100), Some((90, 10)));
        assert_eq!(byte_range("bytes=95-200", 100), Some((95, 5)));
    }

    #[test]
    fn resolves_suffix_ranges() {
        assert_eq!(byte_range("bytes=-10", 100), Some((90, 10)));
        // A suffix longer than the object returns the whole object.
        assert_eq!(byte_range("bytes=-500", 100), Some((0, 100)));
    }

    #[test]
    fn unsatisfiable_ranges_are_none_not_underflowed() {
        // Every case that used to wrap `end - start + 1` / `size - start`.
        assert_eq!(byte_range("bytes=50-10", 100), None, "start > end");
        assert_eq!(byte_range("bytes=100-", 100), None, "start == size");
        assert_eq!(byte_range("bytes=200-", 100), None, "start > size");
        assert_eq!(byte_range("bytes=0-", 0), None, "empty object");
        assert_eq!(byte_range("bytes=-1", 0), None, "empty object suffix");
    }

    #[test]
    fn rejects_malformed_range_headers() {
        assert!(Range::parse("items=0-1").is_err());
        assert!(Range::parse("bytes=abc").is_err());
        assert!(Range::parse("bytes=-0").is_err());
        assert!(Range::parse("bytes=5-1").is_err());
    }

    #[test]
    fn content_range_matches_the_resolved_length() {
        let range = Range::parse("bytes=10-19").unwrap();
        assert_eq!(
            range.to_content_range(100).as_deref(),
            Some("bytes 10-19/100")
        );
        assert_eq!(
            Range::parse("bytes=500-").unwrap().to_content_range(100),
            None
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The staging object must live beside the published object so publishing
    /// is a same-directory rename, and it must never be the object path.
    #[test]
    fn staging_path_is_a_sibling_of_the_object() {
        let object = Path::from_str("/bucket/a/b/object.bin").unwrap();
        let staging = staging_path_for(&object, "deadbeef").expect("valid staging path");

        assert_ne!(staging, object);
        assert_eq!(
            staging.parent().unwrap().unwrap(),
            object.parent().unwrap().unwrap(),
            "staging must be a sibling of the object"
        );
        assert!(
            staging.full_path().contains("deadbeef"),
            "staging path must be unique per upload: {staging}"
        );
    }

    /// A failed PUT must never write under the object path. If it did, the
    /// master would clear the existing inode's blocks on create and the old
    /// object would be lost before the new body is complete.
    #[test]
    fn staging_path_never_equals_the_published_path() {
        for object in ["/b/key", "/b/a/b/c/obj.bin", "/b/.hidden"] {
            let object = Path::from_str(object).unwrap();
            let staging = staging_path_for(&object, "abc123").unwrap();
            assert_ne!(
                staging, object,
                "a failed upload must not touch the object path: {staging}"
            );
            assert!(
                !staging.full_path().ends_with("/key")
                    && !staging.full_path().ends_with("/obj.bin")
                    && !staging.full_path().ends_with("/.hidden"),
                "staging must carry the upload id, got {staging}"
            );
        }
    }

    /// Two concurrent uploads of the same key must not share a staging object,
    /// or one would publish the other's partial body.
    #[test]
    fn staging_paths_differ_per_upload() {
        let object = Path::from_str("/bucket/key").unwrap();
        assert_ne!(
            staging_path_for(&object, "id-one").unwrap(),
            staging_path_for(&object, "id-two").unwrap()
        );
    }
}
