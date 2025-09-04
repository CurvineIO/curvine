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

use crate::{err_ufs, UfsUtils};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::ByteStream;
use aws_smithy_types::error::display::DisplayErrorContext;
use curvine_common::fs::{Path, Reader};
use curvine_common::FsResult;
use log::info;
use orpc::common::ByteUnit;
use orpc::sys::DataSlice;
use orpc::{err_box, try_option_mut};

/// S3 asynchronous block reader
///
/// Responsible for reading data blocks from S3 storage, supporting streaming reading and random access
/// Use AWS SDK to interact with S3 services to achieve efficient data transmission
#[derive(Debug)]
pub struct S3Reader {
    client: Client,
    chunk: DataSlice,
    path: Path,
    bucket: String,
    key: String,
    len: i64,
    pos: i64,
    byte_stream: Option<ByteStream>,
}

impl S3Reader {
    /// Create a new S3 reader
    ///
    /// # Parameters
    /// -`uri`: S3 file URI, format s3://bucket/key
    /// -`config`: Connection configuration, including authentication information, etc.
    ///
    /// # return
    /// Return FsResult<Self>, containing the initialized reader or error
    pub async fn new(client: Client, path: &Path) -> FsResult<Self> {
        let (bucket, key) = UfsUtils::get_bucket_key(path)?;

        // Get object metadata
        let head_result = client.head_object().bucket(bucket).key(key).send().await;

        let head_result = match head_result {
            Err(e) => {
                return err_ufs!(
                    "S3 head_object failed for bucket={}, key={}, error_code={}, details: {:?}",
                    bucket,
                    key,
                    e.code().unwrap_or("Unknown"),
                    DisplayErrorContext(&e)
                )
            }
            Ok(v) => v,
        };

        let len = match head_result.content_length {
            Some(v) => v,
            None => return err_ufs!("S3 error: Missing content length"),
        };

        info!(
            "Created S3Reader {}, len = {}",
            path.full_path(),
            ByteUnit::byte_to_string(len as u64)
        );

        Ok(Self {
            client,
            chunk: DataSlice::Empty,
            path: path.clone(),
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            len,
            pos: 0,
            byte_stream: None,
        })
    }

    /// Initialize the data flow
    async fn get_stream(&mut self) -> FsResult<&mut ByteStream> {
        if self.byte_stream.is_none() {
            let range = format!("bytes={}-", self.pos);
            let resp = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(&self.key)
                .range(range)
                .send()
                .await;
            let resp = match resp {
                Ok(v) => v,
                Err(e) => {
                    return err_ufs!(
                        "S3 get_object failed for bucket={}, key={}, range=bytes={}- , error_code={}, details: {:?}",
                        &self.bucket,
                        &self.key,
                        self.pos,
                        e.code().unwrap_or("Unknown"),
                        DisplayErrorContext(&e)
                    );
                }
            };

            let _ = self.byte_stream.insert(resp.body);
        }

        Ok(try_option_mut!(self.byte_stream))
    }
}

impl Reader for S3Reader {
    fn path(&self) -> &Path {
        &self.path
    }

    fn len(&self) -> i64 {
        self.len
    }

    fn chunk_mut(&mut self) -> &mut DataSlice {
        &mut self.chunk
    }

    fn chunk_size(&self) -> usize {
        128 * 1024
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    async fn read_chunk0(&mut self) -> FsResult<DataSlice> {
        // S3 report an error when reading a 0-byte object
        if self.is_empty() {
            return Ok(DataSlice::Empty);
        }

        let stream = self.get_stream().await?;
        if let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(chunk) => Ok(DataSlice::Bytes(chunk)),
                Err(e) => {
                    err_ufs!(
                        "S3 chunk read failed for bucket={}, key={}, pos={}, details: {:?}",
                        &self.bucket,
                        &self.key,
                        self.pos,
                        DisplayErrorContext(&e)
                    )
                }
            }
        } else {
            Ok(DataSlice::Empty)
        }
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return err_box!("Cannot seek to negative offset");
        } else if self.pos == pos {
            return Ok(());
        } else if pos > self.len {
            return err_box!("Seek position {} can not exceed file len {}", pos, self.len);
        }

        self.pos = pos;
        let _ = self.byte_stream.take();
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        let _ = self.byte_stream.take();
        Ok(())
    }
}

impl Drop for S3Reader {
    fn drop(&mut self) {
        info!("Close S3Reader {}", self.path.full_path());
    }
}
