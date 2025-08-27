use chrono;
use curvine_client::unified::UnifiedFileSystem;
use curvine_common::fs::{FileSystem, Path, Reader, Writer};
use curvine_common::state::FileType;
use curvine_common::FsResult;
use orpc::runtime::AsyncRuntime;
use orpc::runtime::RpcRuntime;
use tokio::io::AsyncWriteExt;
use tracing;
use uuid;

#[derive(Clone)]
pub struct S3Handlers {
    pub fs: UnifiedFileSystem,
    pub region: String,
    pub rt: std::sync::Arc<AsyncRuntime>,
}

impl S3Handlers {
    /// Create a new S3Handlers instance
    ///
    /// # Arguments
    /// * `fs` - The unified filesystem instance
    /// * `region` - The S3 region to report
    /// * `rt` - Shared runtime to schedule internal blocking tasks if needed
    pub fn new(fs: UnifiedFileSystem, region: String, rt: std::sync::Arc<AsyncRuntime>) -> Self {
        tracing::debug!("Creating new S3Handlers with region: {}", region);
        Self { fs, region, rt }
    }

    /// Convert S3 bucket and object key to Curvine filesystem path
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `key` - S3 object key
    ///
    /// # Returns
    /// * `FsResult<Path>` - Curvine filesystem path or error
    fn cv_object_path(&self, bucket: &str, key: &str) -> FsResult<Path> {
        tracing::debug!("Converting S3 path: s3://{}/{}", bucket, key);

        if bucket.is_empty() || key.is_empty() {
            tracing::warn!("Invalid S3 path: bucket or key is empty");
            return Err(curvine_common::error::FsError::invalid_path(
                "",
                "bucket or key is empty",
            ));
        }
        if bucket.contains('/') {
            tracing::warn!(
                "Invalid bucket name '{}': contains invalid characters",
                bucket
            );
            return Err(curvine_common::error::FsError::invalid_path(
                bucket,
                "contains invalid characters",
            ));
        }

        let path = format!("/{}/{}", bucket, key);
        tracing::debug!("Mapped S3 path to Curvine path: {}", path);
        Ok(Path::from_str(&path)?)
    }

    /// Convert S3 bucket name to Curvine filesystem path
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    ///
    /// # Returns
    /// * `FsResult<Path>` - Curvine filesystem path or error
    fn cv_bucket_path(&self, bucket: &str) -> FsResult<Path> {
        tracing::debug!("Converting S3 bucket: s3://{}", bucket);

        if bucket.is_empty() {
            tracing::warn!("Invalid bucket name: bucket name is empty");
            return Err(curvine_common::error::FsError::invalid_path(
                "",
                "bucket name is empty",
            ));
        }
        if bucket.contains('/') {
            tracing::warn!(
                "Invalid bucket name '{}': contains invalid characters",
                bucket
            );
            return Err(curvine_common::error::FsError::invalid_path(
                bucket,
                "contains invalid characters",
            ));
        }

        let path = format!("/{}", bucket);
        tracing::debug!("Mapped S3 bucket to Curvine path: {}", path);
        Ok(Path::from_str(&path)?)
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::HeadHandler for S3Handlers {
    /// Look up object metadata for HEAD request
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `object` - S3 object key
    ///
    /// # Returns
    /// * Future that resolves to object metadata or None if not found
    fn lookup<'a>(
        &self,
        bucket: &str,
        object: &str,
    ) -> std::pin::Pin<
        Box<
            dyn 'a
                + Send
                + Sync
                + std::future::Future<
                    Output = Result<
                        Option<crate::s3::s3_api::HeadObjectResult>,
                        crate::s3::error::Error,
                    >,
                >,
        >,
    > {
        tracing::debug!("HEAD request for s3://{}/{}", bucket, object);

        let path = match self.cv_object_path(bucket, object) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    "Failed to convert S3 path s3://{}/{}: {}",
                    bucket,
                    object,
                    e
                );
                return Box::pin(async { Ok(None) });
            }
        };

        let fs = self.fs.clone();
        let rt = self.rt.clone();
        Box::pin(async move {
            let res = tokio::task::block_in_place(|| rt.block_on(fs.get_status(&path)));
            match res {
                Ok(st) if st.file_type == FileType::File => {
                    tracing::debug!("Found file at path: {}, size: {}", path, st.len);
                    let mut head: crate::s3::s3_api::HeadObjectResult = Default::default();
                    head.content_length = Some(st.len as usize);
                    head.last_modified = Some(
                        chrono::Utc::now()
                            .format("%a, %d %b %Y %H:%M:%S GMT")
                            .to_string(),
                    );
                    Ok::<Option<crate::s3::s3_api::HeadObjectResult>, crate::s3::error::Error>(
                        Some(head),
                    )
                }
                Ok(st) => {
                    tracing::debug!("Path exists but is not a file: {:?}", st.file_type);
                    Ok(None)
                }
                Err(e) => {
                    tracing::warn!("Failed to get status for path {}: {}", path, e);
                    Ok(None)
                }
            }
        })
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::GetObjectHandler for S3Handlers {
    /// Handle GET object request with optional range support
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `object` - S3 object key
    /// * `_opt` - Get object options including range parameters
    /// * `out` - Output stream for writing object data
    ///
    /// # Returns
    /// * Future that resolves to success or error
    fn handle<'a>(
        &'a self,
        bucket: &str,
        object: &str,
        _opt: crate::s3::s3_api::GetObjectOption,
        out: tokio::sync::Mutex<
            std::pin::Pin<Box<dyn 'a + Send + crate::utils::io::PollWrite + Unpin>>,
        >,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<(), String>>>> {
        let fs = self.fs.clone();
        let rt = self.rt.clone();
        let path = self.cv_object_path(bucket, object);
        let bucket = bucket.to_string();
        let object = object.to_string();

        Box::pin(async move {
            // Log range request if present
            if let Some(start) = _opt.range_start {
                if let Some(end) = _opt.range_end {
                    tracing::info!(
                        "GET object s3://{}/{} with range: bytes={}-{}",
                        bucket,
                        object,
                        start,
                        end
                    );
                } else {
                    tracing::info!(
                        "GET object s3://{}/{} with range: bytes={}-",
                        bucket,
                        object,
                        start
                    );
                }
            } else {
                tracing::info!("GET object s3://{}/{}", bucket, object);
            }

            // Convert S3 path to Curvine path
            let path = path.map_err(|e| {
                tracing::error!(
                    "Failed to convert S3 path s3://{}/{}: {}",
                    bucket,
                    object,
                    e
                );
                e.to_string()
            })?;

            // Open file for reading
            let mut reader =
                tokio::task::block_in_place(|| rt.block_on(fs.open(&path))).map_err(|e| {
                    tracing::error!("Failed to open file at path {}: {}", path, e);
                    e.to_string()
                })?;

            // Apply range if specified
            if let Some(start) = _opt.range_start {
                tracing::debug!("Seeking to position {} for range request", start);
                tokio::task::block_in_place(|| rt.block_on(reader.seek(start as i64))).map_err(
                    |e| {
                        tracing::error!("Failed to seek to position {}: {}", start, e);
                        e.to_string()
                    },
                )?;
            }

            // Calculate how many bytes to read for range requests
            let bytes_to_read = if let (Some(start), Some(end)) = (_opt.range_start, _opt.range_end)
            {
                Some(end - start + 1)
            } else {
                None
            };

            // Read and write data
            let remaining_bytes = bytes_to_read;

            // Always use direct read to fix zero-padding issues
            log::debug!(
                "GetObject: range_bytes={:?}, reader.remaining()={}",
                remaining_bytes,
                reader.remaining()
            );

            let target_read = if let Some(range_bytes) = remaining_bytes {
                range_bytes
            } else {
                reader.remaining().max(0) as u64
            };

            log::debug!("GetObject: will read {} bytes directly", target_read);

            // BWS-RS STYLE: Simple, single-threaded read and write, no complex async pipelines
            let to_read = target_read as usize;
            let mut data = Vec::new();

            // Read all data at once using read_full
            if to_read > 0 {
                let mut buffer = vec![0u8; to_read];
                let n = tokio::task::block_in_place(|| rt.block_on(reader.read_full(&mut buffer)))
                    .map_err(|e| e.to_string())?;
                data = buffer[..n].to_vec();
            } else {
                // Read all available data
                let remaining = reader.remaining().max(0) as usize;
                if remaining > 0 {
                    let mut buffer = vec![0u8; remaining];
                    let n =
                        tokio::task::block_in_place(|| rt.block_on(reader.read_full(&mut buffer)))
                            .map_err(|e| e.to_string())?;
                    data = buffer[..n].to_vec();
                }
            }

            log::debug!("GetObject: read completed {} bytes", data.len());

            // Single write operation - no concurrent access
            let mut guard = out.lock().await;
            guard.poll_write(&data).await.map_err(|e| {
                tracing::error!("Failed to write data to output: {}", e);
                e.to_string()
            })?;

            let total_read = data.len() as u64;

            // Clean up reader to stop background tasks
            if let Err(e) = tokio::task::block_in_place(|| rt.block_on(reader.complete())) {
                tracing::warn!("Failed to complete reader cleanup: {}", e);
                // Don't fail the request for cleanup errors
            }

            tracing::info!(
                "GET object s3://{}/{} completed, total bytes: {}",
                bucket,
                object,
                total_read
            );
            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::PutObjectHandler for S3Handlers {
    /// Handle PUT object request for uploading data
    ///
    /// # Arguments
    /// * `_opt` - Put object options
    /// * `bucket` - S3 bucket name
    /// * `object` - S3 object key
    /// * `body` - Input stream for reading object data
    ///
    /// # Returns
    /// * Future that resolves to success or error
    fn handle<'a>(
        &'a self,
        _opt: &crate::s3::s3_api::PutObjectOption,
        bucket: &'a str,
        object: &'a str,
        body: &'a mut (dyn crate::utils::io::PollRead + Unpin + Send),
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<(), String>>>> {
        let fs = self.fs.clone();
        let rt = self.rt.clone();
        let path = self.cv_object_path(bucket, object);
        let bucket = bucket.to_string();
        let object = object.to_string();

        Box::pin(async move {
            tracing::info!("PUT object s3://{}/{}", bucket, object);

            // Use our PollRead interface instead of AsyncRead
            let path = path.map_err(|e| {
                tracing::error!(
                    "Failed to convert S3 path s3://{}/{}: {}",
                    bucket,
                    object,
                    e
                );
                e.to_string()
            })?;

            let mut writer = tokio::task::block_in_place(|| rt.block_on(fs.create(&path, true)))
                .map_err(|e| {
                    tracing::error!("Failed to create file at path {}: {}", path, e);
                    e.to_string()
                })?;

            let mut total_written = 0u64;
            let mut first_chunk = true;

            loop {
                // Use PollRead interface to get data as Vec<u8>
                let chunk_result = body.poll_read().await.map_err(|e| {
                    tracing::error!("Failed to read from input stream: {}", e);
                    e
                })?;

                let chunk = match chunk_result {
                    Some(data) => data,
                    None => break, // End of stream
                };

                if chunk.is_empty() {
                    break;
                }

                let n = chunk.len();
                log::debug!("POLLREAD-CHUNK: {} bytes", n);
                if n > 0 {
                    log::debug!(
                        "POLLREAD-HEX: {:?}",
                        chunk
                            .iter()
                            .take(30)
                            .map(|b| format!("{:02x}", b))
                            .collect::<Vec<_>>()
                            .join(" ")
                    );
                }

                // DEBUG: Log first chunk details for debugging
                if first_chunk {
                    log::debug!("PUT DEBUG - First chunk: {} bytes", n);
                    log::debug!(
                        "PUT DEBUG - First chunk hex: {:?}",
                        chunk[..n.min(50)]
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<Vec<_>>()
                            .join(" ")
                    );
                    first_chunk = false;
                }

                // Write the chunk data directly
                tokio::task::block_in_place(|| rt.block_on(writer.write(&chunk))).map_err(|e| {
                    tracing::error!("Failed to write chunk to file: {}", e);
                    e.to_string()
                })?;

                total_written += n as u64;
                tracing::debug!("Written chunk: {} bytes, total: {} bytes", n, total_written);
            }

            tokio::task::block_in_place(|| rt.block_on(writer.complete())).map_err(|e| {
                tracing::error!("Failed to complete file write: {}", e);
                e.to_string()
            })?;

            tracing::info!(
                "PUT object s3://{}/{} completed, total bytes: {}",
                bucket,
                object,
                total_written
            );
            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::DeleteObjectHandler for S3Handlers {
    fn handle<'a>(
        &'a self,
        _opt: &'a crate::s3::s3_api::DeleteObjectOption,
        object: &'a str,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<(), String>>>> {
        let fs = self.fs.clone();
        let object = object.to_string();
        let path = Path::from_str(format!("/{}", object));
        Box::pin(async move {
            let path = path.map_err(|e| e.to_string())?;
            match fs.delete(&path, false).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    let msg = e.to_string();
                    // treat not-found as success to be S3 compatible
                    if msg.contains("No such file")
                        || msg.contains("not exists")
                        || msg.contains("not found")
                    {
                        Ok(())
                    } else {
                        Err(msg)
                    }
                }
            }
        })
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::CreateBucketHandler for S3Handlers {
    fn handle<'a>(
        &'a self,
        _opt: &'a crate::s3::s3_api::CreateBucketOption,
        bucket: &'a str,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<(), String>>>> {
        let fs = self.fs.clone();
        let bucket = bucket.to_string();
        let path = self.cv_bucket_path(&bucket);
        Box::pin(async move {
            let path = path.map_err(|e| e.to_string())?;
            fs.mkdir(&path, true).await.map_err(|e| e.to_string())?;
            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::DeleteBucketHandler for S3Handlers {
    fn handle<'a>(
        &'a self,
        _opt: &'a crate::s3::s3_api::DeleteBucketOption,
        bucket: &'a str,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<(), String>>>> {
        let fs = self.fs.clone();
        let bucket = bucket.to_string();
        let path = self.cv_bucket_path(&bucket);
        Box::pin(async move {
            let path = path.map_err(|e| e.to_string())?;
            fs.delete(&path, false).await.map_err(|e| e.to_string())
        })
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::ListBucketHandler for S3Handlers {
    fn handle<'a>(
        &'a self,
        _opt: &'a crate::s3::s3_api::ListBucketsOption,
    ) -> std::pin::Pin<
        Box<
            dyn 'a
                + Send
                + std::future::Future<Output = Result<Vec<crate::s3::s3_api::Bucket>, String>>,
        >,
    > {
        let fs = self.fs.clone();
        let region = self.region.clone();
        Box::pin(async move {
            let mut buckets = vec![];
            let root = Path::from_str("/").map_err(|e| e.to_string())?;
            let list = fs.list_status(&root).await.map_err(|e| e.to_string())?;
            let now = chrono::Utc::now().to_rfc3339();
            for st in list {
                if st.is_dir {
                    buckets.push(crate::s3::s3_api::Bucket {
                        name: st.name,
                        creation_date: now.clone(),
                        bucket_region: region.clone(),
                    });
                }
            }
            Ok(buckets)
        })
    }
}

impl crate::s3::s3_api::GetBucketLocationHandler for S3Handlers {
    fn handle<'a>(
        &'a self,
        _loc: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<dyn 'a + Send + std::future::Future<Output = Result<Option<&'static str>, ()>>>,
    > {
        Box::pin(async move { Ok(Some("us-east-1")) })
    }
}

#[async_trait::async_trait]
impl crate::s3::s3_api::MultiUploadObjectHandler for S3Handlers {
    fn handle_create_session<'a>(
        &'a self,
        _bucket: &'a str,
        _key: &'a str,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<String, ()>>>> {
        Box::pin(async move {
            let upload_id = uuid::Uuid::new_v4().to_string();
            Ok(upload_id)
        })
    }

    fn handle_upload_part<'a>(
        &'a self,
        _bucket: &'a str,
        _key: &'a str,
        upload_id: &'a str,
        part_number: u32,
        body: &'a mut (dyn tokio::io::AsyncRead + Unpin + Send),
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<String, ()>>>> {
        Box::pin(async move {
            use bytes::BytesMut;
            use tokio::io::AsyncReadExt;

            let dir = format!("/tmp/curvine-multipart/{}", upload_id);
            let _ = tokio::fs::create_dir_all(&dir).await;
            let path = format!("{}/{}", dir, part_number);
            let mut file = match tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .await
            {
                Ok(f) => f,
                Err(_) => return Err(()),
            };

            let mut hasher = md5::Context::new();
            let mut total_data = BytesMut::new();

            // Read data chunk by chunk and accumulate in BytesMut (no pre-filled zeros)
            let mut temp_buf = vec![0u8; 1024 * 1024];
            loop {
                let n = match body.read(&mut temp_buf).await {
                    Ok(n) => n,
                    Err(_) => return Err(()),
                };
                if n == 0 {
                    break;
                }
                let actual_data = &temp_buf[..n];
                hasher.consume(actual_data);
                total_data.extend_from_slice(actual_data);
            }

            // Write all accumulated data at once
            if let Err(_) = file.write_all(&total_data).await {
                return Err(());
            }

            let digest = hasher.compute();
            Ok(format!("\"{:x}\"", digest))
        })
    }

    fn handle_complete<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        upload_id: &'a str,
        data: &'a [(&'a str, u32)],
        _opts: crate::s3::s3_api::MultiUploadObjectCompleteOption,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<String, ()>>>> {
        let fs = self.fs.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let path_res = self.cv_object_path(&bucket, &key);
        Box::pin(async move {
            use tokio::io::AsyncReadExt;
            let final_path = match path_res {
                Ok(p) => p,
                Err(_) => return Err(()),
            };
            let mut writer = match fs.create(&final_path, true).await {
                Ok(w) => w,
                Err(_) => return Err(()),
            };
            let dir = format!("/tmp/curvine-multipart/{}", upload_id);
            let mut part_list = data.to_vec();
            part_list.sort_by_key(|(_, n)| *n);
            for (_, num) in part_list {
                let path = format!("{}/{}", dir, num);
                let mut file = match tokio::fs::OpenOptions::new().read(true).open(&path).await {
                    Ok(f) => f,
                    Err(_) => return Err(()),
                };
                let mut buf = [0u8; 1024 * 1024];
                loop {
                    let n = match file.read(&mut buf).await {
                        Ok(n) => n,
                        Err(_) => return Err(()),
                    };
                    if n == 0 {
                        break;
                    }
                    if let Err(_) = writer.write(&buf[..n]).await {
                        return Err(());
                    };
                }
            }
            if let Err(_) = writer.complete().await {
                return Err(());
            }
            let _ = tokio::fs::remove_dir_all(&dir).await;
            Ok("etag-not-computed".to_string())
        })
    }

    fn handle_abort<'a>(
        &'a self,
        _bucket: &'a str,
        _key: &'a str,
        upload_id: &'a str,
    ) -> std::pin::Pin<Box<dyn 'a + Send + std::future::Future<Output = Result<(), ()>>>> {
        Box::pin(async move {
            let dir = format!("/tmp/curvine-multipart/{}", upload_id);
            let _ = tokio::fs::remove_dir_all(&dir).await;
            Ok(())
        })
    }
}

impl crate::s3::s3_api::ListObjectHandler for S3Handlers {
    fn handle<'a>(
        &'a self,
        opt: &'a crate::s3::s3_api::ListObjectOption,
        bucket: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn 'a
                + Send
                + std::future::Future<
                    Output = Result<Vec<crate::s3::s3_api::ListObjectContent>, String>,
                >,
        >,
    > {
        let fs = self.fs.clone();
        let _region = self.region.clone();
        let bucket = bucket.to_string();
        let prefix = opt.prefix.clone();
        Box::pin(async move {
            let bkt_path = match self.cv_bucket_path(&bucket) {
                Ok(p) => p,
                Err(e) => return Err(e.to_string()),
            };
            let root = if let Some(pref) = &prefix {
                Path::from_str(format!("{}/{}", bkt_path.full_path(), pref))
                    .map_err(|e| e.to_string())?
            } else {
                bkt_path
            };
            let list = fs.list_status(&root).await.map_err(|e| e.to_string())?;
            let mut contents = Vec::new();
            for st in list {
                if st.is_dir {
                    // skip directories for now; S3 v2 can emit CommonPrefixes if delimiter set
                    continue;
                }
                contents.push(crate::s3::s3_api::ListObjectContent {
                    key: if let Some(pref) = &prefix {
                        format!("{}/{}", pref.trim_matches('/'), st.name)
                    } else {
                        st.name
                    },
                    last_modified: Some(
                        chrono::Utc::now()
                            .format("%a, %d %b %Y %H:%M:%S GMT")
                            .to_string(),
                    ),
                    etag: None,
                    size: st.len as u64,
                    storage_class: None,
                    owner: None,
                });
            }
            Ok(contents)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::fs::Path;
    use std::sync::Arc;

    // Test path mapping functions directly
    #[test]
    fn test_cv_object_path() {
        let handlers = S3Handlers::new(
            curvine_client::unified::UnifiedFileSystem::with_rt(
                curvine_common::conf::ClusterConf::default(),
                Arc::new(orpc::runtime::AsyncRuntime::new("test", 1, 1)),
            )
            .unwrap(),
            "us-east-1".to_string(),
            Arc::new(orpc::runtime::AsyncRuntime::new("test", 1, 1)),
        );

        // Test bucket and object path mapping
        let path = handlers.cv_object_path("mybucket", "folder/file.txt");
        assert!(path.is_ok());
        assert_eq!(path.unwrap().to_string(), "/mybucket/folder/file.txt");

        let path = handlers.cv_object_path("bucket", "object");
        assert!(path.is_ok());
        assert_eq!(path.unwrap().to_string(), "/bucket/object");
    }

    #[test]
    fn test_cv_bucket_path() {
        let handlers = S3Handlers::new(
            curvine_client::unified::UnifiedFileSystem::with_rt(
                curvine_common::conf::ClusterConf::default(),
                Arc::new(orpc::runtime::AsyncRuntime::new("test", 1, 1)),
            )
            .unwrap(),
            "us-east-1".to_string(),
            Arc::new(orpc::runtime::AsyncRuntime::new("test", 1, 1)),
        );

        // Test bucket path mapping
        let path = handlers.cv_bucket_path("mybucket");
        assert!(path.is_ok());
        assert_eq!(path.unwrap().to_string(), "/mybucket");
    }

    #[test]
    fn test_path_validation() {
        let handlers = S3Handlers::new(
            curvine_client::unified::UnifiedFileSystem::with_rt(
                curvine_common::conf::ClusterConf::default(),
                Arc::new(orpc::runtime::AsyncRuntime::new("test", 1, 1)),
            )
            .unwrap(),
            "us-east-1".to_string(),
            Arc::new(orpc::runtime::AsyncRuntime::new("test", 1, 1)),
        );

        // Test invalid bucket names
        let path = handlers.cv_bucket_path("");
        assert!(path.is_err());

        let path = handlers.cv_bucket_path("invalid/bucket");
        assert!(path.is_err());

        // Test invalid object names
        let path = handlers.cv_object_path("bucket", "");
        assert!(path.is_err());
    }
}
