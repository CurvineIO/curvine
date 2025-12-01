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

use std::io::Write;

use bytes::BytesMut;
use sha1::Digest;

use crate::utils::consts::*;

pub type GenericResult<T> = Result<T, String>;
pub mod consts;
pub mod s3_utils;
pub mod temp_file_manager;
#[derive(Debug)]
pub struct BaseKv<K: PartialOrd, V> {
    pub key: K,
    pub val: V,
}

pub mod io {
    use std::ops::{Deref, DerefMut};
    use tokio::io::AsyncReadExt;

    pub enum PollReaderEnum {
        Body(crate::http::axum::BodyReader),
        File(tokio::fs::File),
        InMemory(InMemoryPollReader),
        BufCursor(tokio::io::BufReader<std::io::Cursor<Vec<u8>>>),
    }

    impl PollReaderEnum {
        #[inline]
        pub async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String> {
            match self {
                PollReaderEnum::Body(r) => r.poll_read_impl().await,
                PollReaderEnum::File(f) => {
                    let mut buf = vec![0u8; 64 * 1024];
                    match f.read(&mut buf).await {
                        Ok(0) => Ok(None),
                        Ok(n) => {
                            buf.truncate(n);
                            Ok(Some(buf))
                        }
                        Err(e) => Err(e.to_string()),
                    }
                }
                PollReaderEnum::InMemory(r) => r.poll_read_impl().await,
                PollReaderEnum::BufCursor(r) => {
                    let mut buf = vec![0u8; 64 * 1024];
                    match r.read(&mut buf).await {
                        Ok(0) => Ok(None),
                        Ok(n) => {
                            buf.truncate(n);
                            Ok(Some(buf))
                        }
                        Err(e) => Err(e.to_string()),
                    }
                }
            }
        }
    }

    pub struct InMemoryPollReader {
        pub data: Vec<u8>,
        pub offset: usize,
    }

    impl InMemoryPollReader {
        pub fn new(data: Vec<u8>) -> Self {
            Self { data, offset: 0 }
        }

        #[inline]
        pub async fn poll_read_impl(&mut self) -> Result<Option<Vec<u8>>, String> {
            if self.offset >= self.data.len() {
                return Ok(None);
            }
            let chunk_size = std::cmp::min(64 * 1024, self.data.len() - self.offset);
            let chunk = self.data[self.offset..self.offset + chunk_size].to_vec();
            self.offset += chunk_size;
            Ok(Some(chunk))
        }
    }

    pub enum PollWriterEnum<'a> {
        VecBuf(&'a mut Vec<u8>),
    }

    impl<'a> PollWriterEnum<'a> {
        #[inline]
        pub async fn poll_write(&mut self, buff: &[u8]) -> Result<usize, std::io::Error> {
            match self {
                PollWriterEnum::VecBuf(v) => {
                    v.extend_from_slice(buff);
                    Ok(buff.len())
                }
            }
        }

        #[inline]
        pub async fn poll_write_vec(&mut self, vec: Vec<u8>) -> Result<usize, std::io::Error> {
            self.poll_write(&vec).await
        }
    }

    pub enum AsyncReadEnum {
        File(tokio::fs::File),
        BufCursor(tokio::io::BufReader<std::io::Cursor<Vec<u8>>>),
    }

    impl AsyncReadEnum {
        #[inline]
        pub async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            use tokio::io::AsyncReadExt;
            match self {
                AsyncReadEnum::File(f) => f.read(buf).await,
                AsyncReadEnum::BufCursor(r) => r.read(buf).await,
            }
        }

        #[inline]
        pub async fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
            use tokio::io::AsyncReadExt;
            match self {
                AsyncReadEnum::File(f) => f.read_to_end(buf).await,
                AsyncReadEnum::BufCursor(r) => r.read_to_end(buf).await,
            }
        }
    }

    /// Legacy trait for async reading - uses async_trait for object safety
    ///
    /// **Prefer `PollReaderEnum` for new code** - provides zero-allocation dispatch.
    /// This trait is kept for backward compatibility with `dyn PollRead` usage.
    ///
    /// The reason we still use async_trait here is to maintain compatibility with
    /// `dyn PollRead` usage in some places, which requires object safety.
    /// async_trait provides this object safety by generating a vtable at compile time.
    #[async_trait::async_trait]
    pub trait PollRead: Send {
        async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String>;
    }

    /// Legacy trait for async writing - uses async_trait for object safety
    ///
    /// **Prefer `PollWriterEnum` for new code** - provides zero-allocation dispatch.
    /// This trait is kept for backward compatibility with `dyn PollWrite` usage.
    ///
    /// The reason we still use async_trait here is to maintain compatibility with
    /// `dyn PollWrite` usage in some places, which requires object safety.
    /// async_trait provides this object safety by generating a vtable at compile time.
    #[async_trait::async_trait]
    pub trait PollWrite: Send {
        async fn poll_write(&mut self, buff: &[u8]) -> Result<usize, std::io::Error>;
        async fn poll_write_vec(&mut self, vec: Vec<u8>) -> Result<usize, std::io::Error> {
            self.poll_write(&vec).await
        }
    }

    pub struct BuffIo<const N: usize> {
        buff: Vec<u8>,
    }

    impl<const N: usize> Deref for BuffIo<N> {
        type Target = Vec<u8>;

        fn deref(&self) -> &Self::Target {
            &self.buff
        }
    }

    impl<const N: usize> DerefMut for BuffIo<N> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.buff
        }
    }
}
pub enum ChunkParseError {
    HashNoMatch,
    IllegalContent,
    Io(String),
}

#[derive(Clone, Copy)]
enum ParseProcessState {
    Head,
    Content,
    End,
}
struct ChunkHead {
    content_size: usize,
    #[allow(dead_code)]
    ext: String,
    signature: String,
}

///chunk parse, will auto verify sha256 value for every chunk
pub async fn chunk_parse<R: io::PollRead + Send, W: tokio::io::AsyncWrite + Send + Unpin>(
    mut src: R,
    dst: &mut W,
    circle_hasher: &mut crate::auth::sig_v4::HmacSha256CircleHasher,
) -> Result<usize, ChunkParseError> {
    let mut total_buff = BytesMut::with_capacity(10 << 20);
    let mut head = None;
    let mut state = ParseProcessState::Head;
    let mut total_size = 0;
    while let Some(content) = src.poll_read().await.map_err(ChunkParseError::Io)? {
        total_buff.extend_from_slice(&content);
        state = parse_buff(
            &mut total_buff,
            dst,
            state,
            &mut head,
            &mut total_size,
            circle_hasher,
        )
        .await?;
        if let ParseProcessState::End = state {
            return Ok(total_size);
        }
    }
    Ok(total_size)
}

async fn parse_buff<W: tokio::io::AsyncWrite + Send + Unpin>(
    content: &mut bytes::BytesMut,
    dst: &mut W,
    mut state: ParseProcessState,
    head: &mut Option<ChunkHead>,
    total_size: &mut usize,
    circle_hasher: &mut crate::auth::sig_v4::HmacSha256CircleHasher,
) -> Result<ParseProcessState, ChunkParseError> {
    use tokio::io::AsyncWriteExt;
    while !content.is_empty() {
        match state {
            ParseProcessState::Head => {
                if let Some(pos) = content.windows(2).position(|x| x == b"\r\n") {
                    *head = Some(parse_chunk_line(&content[0..pos]).map_err(|_| {
                        log::warn!("parse content line error\n{}", unsafe {
                            std::str::from_utf8_unchecked(&content[0..pos])
                        });
                        ChunkParseError::IllegalContent
                    })?);
                    if let Some(hdr) = head {
                        if hdr.content_size == 0 {
                            let next = circle_hasher.next(EMPTY_PAYLOAD_HASH).unwrap();
                            if hdr.signature.as_str() == next.as_str() {
                                log::info!("all signature verify pass");
                                state = ParseProcessState::End;
                                return Ok(state);
                            } else {
                                log::info!("hash no match expect {} got {}", next, hdr.signature);
                                return Err(ChunkParseError::HashNoMatch);
                            }
                        }
                    } else {
                        log::info!("chunk not complete,wait");
                    }
                    let _ = content.split_to(pos + 2);
                    state = ParseProcessState::Content;
                } else {
                    return Ok(state);
                }
            }

            ParseProcessState::Content => {
                if let Some(hdr) = head {
                    let content_len = content.len();
                    if content_len >= hdr.content_size + 2 {
                        if &content[hdr.content_size..hdr.content_size + 2] != b"\r\n" {
                            log::warn!("content end is not chunk split symbol [{}]", unsafe {
                                std::str::from_utf8_unchecked(
                                    &content[hdr.content_size..hdr.content_size + 2],
                                )
                            });
                            return Err(ChunkParseError::IllegalContent);
                        }
                        let mut hsh = sha2::Sha256::new();
                        let _ = hsh.write_all(&content[0..hdr.content_size]);
                        let hsh = hsh.finalize();
                        let curr_hash = circle_hasher.next(hex::encode(hsh).as_str()).unwrap();
                        if curr_hash != hdr.signature {
                            log::warn!("chunk hash not match, return error");
                            return Err(ChunkParseError::HashNoMatch);
                        }
                        // log::info!(
                        //     "chunk signature verify pass {curr_hash} content length {}\n{}",
                        //     content.len(),
                        //     unsafe { std::str::from_utf8_unchecked(content) }
                        // );
                        dst.write_all(&content[0..hdr.content_size])
                            .await
                            .map_err(|err| {
                                ChunkParseError::Io(format!("write content error {err}"))
                            })?;
                        *total_size += hdr.content_size;
                        let _ = content.split_to(hdr.content_size + 2);
                        // log::info!("{}", unsafe { std::str::from_utf8_unchecked(content) });
                        *head = None;
                        state = ParseProcessState::Head;
                    } else {
                        return Ok(state);
                    }
                }
            }
            ParseProcessState::End => return Ok(state),
        }
    }
    Ok(state)
}

fn parse_chunk_line(src: &[u8]) -> Result<ChunkHead, ()> {
    let ret = src
        .windows(1)
        .position(|r| r == b";")
        .and_then(|p1: usize| {
            let raw = &src[..p1];
            usize::from_str_radix(unsafe { std::str::from_utf8_unchecked(raw) }.trim(), 16)
                .ok()
                .and_then(|size| {
                    let raw = &src[p1 + 1..];
                    let raw = raw.splitn(2, |x| *x == b'=').collect::<Vec<&[u8]>>();
                    if raw.len() != 2 {
                        None
                    } else {
                        let ext = raw[0];
                        let signature = raw[1];
                        Some(ChunkHead {
                            content_size: size,
                            ext: unsafe { std::str::from_utf8_unchecked(ext) }.to_string(),
                            signature: unsafe { std::str::from_utf8_unchecked(signature) }
                                .to_string(),
                        })
                    }
                })
        });
    ret.ok_or(())
}

#[allow(dead_code)]
fn parse_chunk_content<'a>(_src: &'a [u8], _ext: &str, _signature: &str) -> Result<&'a [u8], ()> {
    todo!()
}
