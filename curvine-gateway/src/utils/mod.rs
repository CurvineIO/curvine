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

pub type GenericResult<T> = Result<T, String>;
pub mod utils;
#[derive(Debug)]
pub struct BaseKv<K: PartialOrd, V> {
    pub key: K,
    pub val: V,
}

pub mod io {
    use std::ops::{Deref, DerefMut};

    pub trait PollRead {
        fn poll_read<'a>(
            &'a mut self,
        ) -> std::pin::Pin<
            Box<dyn 'a + Send + std::future::Future<Output = Result<Option<Vec<u8>>, String>>>,
        >;
    }
    pub trait PollWrite {
        fn poll_write<'a>(
            &'a mut self,
            buff: &'a [u8],
        ) -> std::pin::Pin<
            Box<dyn 'a + Send + std::future::Future<Output = Result<usize, std::io::Error>>>,
        >;
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
                            let next=circle_hasher.next(
                                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                                ).unwrap();
                            if hdr.signature.as_str() == next.as_str() {
                                log::info!("all signature verify pass");
                                state = ParseProcessState::End;
                                return Ok(state);
                            } else {
                                log::info!("hash no match expect {next} got {}", hdr.signature);
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
