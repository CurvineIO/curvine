// Copyright 2025 The Curvine Authors
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

use std::sync::{Arc, OnceLock};

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use bytes::{Buf, Bytes};
use crc::{Algorithm, Crc};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use super::io::PollRead;
use super::signed_spool::{SignedSpool, SpoolFile};
use crate::auth::sig_v4::HmacSha256CircleHasher;

pub(crate) const SIGNED_CHUNKS: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
pub(crate) const SIGNED_TRAILER: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
pub(crate) const UNSIGNED_TRAILER: &str = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
const MAX_CHUNK_HEADER: usize = 128;
const MAX_TRAILER_LINE: usize = 256;
// A buffering threshold, not a limit on the wire record size.
const MEMORY_RECORD_THRESHOLD: usize = 16 << 20;
const IO_BLOCK: usize = 256 << 10;
const MALFORMED: &str = "InvalidRequest: malformed aws-chunked body";
const TRUNCATED: &str = "ContentLengthMismatch: incomplete aws-chunked body";
const BAD_SIGNATURE: &str = "SignatureDoesNotMatch: invalid aws-chunked signature";

/// Only the HTTP header-authentication path installs this extension, after
/// verifying the seed signature. SigV2 and presigned requests cannot use it.
#[derive(Clone)]
pub(crate) enum ChunkAuth {
    Signed(HmacSha256CircleHasher),
    Unsigned,
}

const CRC32: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
const CRC32C: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISCSI);
// crc-catalog 2.4 predates the NVME entry; parameters from the CRC catalogue.
const CRC64NVME: Crc<u64> = Crc::<u64>::new(&Algorithm {
    width: 64,
    poly: 0xad93d23594c93659,
    init: u64::MAX,
    refin: true,
    refout: true,
    xorout: u64::MAX,
    check: 0xae8b14860a799888,
    residue: 0xf310303b2b6f6e42,
});

enum Checksum {
    Crc32(crc::Digest<'static, u32>),
    Crc32c(crc::Digest<'static, u32>),
    Crc64(crc::Digest<'static, u64>),
    Sha1(sha1::Sha1),
    Sha256(Sha256),
}

pub(crate) struct ChecksumTrailer {
    name: &'static str,
    digest: Checksum,
    verified: Arc<OnceLock<String>>,
}

impl ChecksumTrailer {
    pub(crate) fn new(name: &str) -> Result<Self, &'static str> {
        let (name, digest) = match name.trim().to_ascii_lowercase().as_str() {
            "x-amz-checksum-crc32" => ("x-amz-checksum-crc32", Checksum::Crc32(CRC32.digest())),
            "x-amz-checksum-crc32c" => ("x-amz-checksum-crc32c", Checksum::Crc32c(CRC32C.digest())),
            "x-amz-checksum-crc64nvme" => (
                "x-amz-checksum-crc64nvme",
                Checksum::Crc64(CRC64NVME.digest()),
            ),
            "x-amz-checksum-sha1" => ("x-amz-checksum-sha1", Checksum::Sha1(sha1::Sha1::new())),
            "x-amz-checksum-sha256" => ("x-amz-checksum-sha256", Checksum::Sha256(Sha256::new())),
            _ => return Err("InvalidRequest: x-amz-trailer must name one supported checksum"),
        };
        Ok(Self {
            name,
            digest,
            verified: Arc::new(OnceLock::new()),
        })
    }

    pub(crate) fn response(&self) -> (&'static str, Arc<OnceLock<String>>) {
        (self.name, Arc::clone(&self.verified))
    }

    fn update(&mut self, data: &[u8]) {
        match &mut self.digest {
            Checksum::Crc32(d) | Checksum::Crc32c(d) => d.update(data),
            Checksum::Crc64(d) => d.update(data),
            Checksum::Sha1(d) => d.update(data),
            Checksum::Sha256(d) => d.update(data),
        }
    }

    fn verify(self, value: &str) -> Result<(), &'static str> {
        let actual = match self.digest {
            Checksum::Crc32(d) | Checksum::Crc32c(d) => STANDARD.encode(d.finalize().to_be_bytes()),
            Checksum::Crc64(d) => STANDARD.encode(d.finalize().to_be_bytes()),
            Checksum::Sha1(d) => STANDARD.encode(d.finalize()),
            Checksum::Sha256(d) => STANDARD.encode(d.finalize()),
        };
        if actual != value {
            return Err("BadDigest: checksum trailer does not match the decoded body");
        }
        let _ = self.verified.set(actual);
        Ok(())
    }
}

struct VerifiedFile {
    file: SpoolFile,
    size: usize,
    offset: usize,
}

pub(crate) struct SignedChunkReader<R> {
    inner: R,
    pending: Bytes,
    auth: ChunkAuth,
    trailer: Option<ChecksumTrailer>,
    spool: Arc<SignedSpool>,
    remaining: usize,
    unsigned_remaining: usize,
    replay: Option<VerifiedFile>,
    finished: bool,
    failed: Option<&'static str>,
}

impl<R: PollRead> SignedChunkReader<R> {
    pub(crate) fn new(
        inner: R,
        auth: ChunkAuth,
        decoded_length: usize,
        trailer: Option<ChecksumTrailer>,
        spool: Arc<SignedSpool>,
    ) -> Self {
        Self {
            inner,
            pending: Bytes::new(),
            auth,
            trailer,
            spool,
            remaining: decoded_length,
            unsigned_remaining: 0,
            replay: None,
            finished: false,
            failed: None,
        }
    }

    async fn fill(&mut self) -> Result<bool, &'static str> {
        while self.pending.is_empty() {
            match self.inner.poll_read().await.map_err(|_| TRUNCATED)? {
                Some(frame) => self.pending = Bytes::from(frame),
                None => return Ok(false),
            }
        }
        Ok(true)
    }

    async fn byte(&mut self) -> Result<u8, &'static str> {
        if !self.fill().await? {
            return Err(TRUNCATED);
        }
        Ok(self.pending.get_u8())
    }

    async fn line<'a>(&mut self, buf: &'a mut [u8]) -> Result<&'a str, &'static str> {
        for i in 0..buf.len() {
            let b = self.byte().await?;
            if b == b'\r' {
                if self.byte().await? != b'\n' {
                    return Err(MALFORMED);
                }
                return std::str::from_utf8(&buf[..i]).map_err(|_| MALFORMED);
            }
            if !b.is_ascii() || (b.is_ascii_control() && b != b'\t') {
                return Err(MALFORMED);
            }
            buf[i] = b;
        }
        Err(MALFORMED)
    }

    async fn header(&mut self) -> Result<(usize, Option<[u8; 64]>), &'static str> {
        let mut buf = [0; MAX_CHUNK_HEADER];
        let line = self.line(&mut buf).await?;
        let (size, signature) = if matches!(self.auth, ChunkAuth::Signed(_)) {
            let (size, sig) = line.split_once(";chunk-signature=").ok_or(MALFORMED)?;
            if !sig.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(MALFORMED);
            }
            (
                size,
                Some(sig.as_bytes().try_into().map_err(|_| MALFORMED)?),
            )
        } else {
            (line, None)
        };
        if size.is_empty() || !size.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(MALFORMED);
        }
        let size = usize::from_str_radix(size, 16).map_err(|_| MALFORMED)?;
        Ok((size, signature))
    }

    async fn read_trailer(&mut self, trailer: ChecksumTrailer) -> Result<(), &'static str> {
        let mut buf = [0; MAX_TRAILER_LINE];
        let line = self.line(&mut buf).await?;
        let (name, value) = line.split_once(':').ok_or(MALFORMED)?;
        if !name.eq_ignore_ascii_case(trailer.name) {
            return Err(MALFORMED);
        }
        let value = value.trim_matches([' ', '\t']);
        if matches!(self.auth, ChunkAuth::Signed(_)) {
            let mut signature_buf = [0; MAX_TRAILER_LINE];
            let signature_line = self.line(&mut signature_buf).await?;
            let (name, signature) = signature_line.split_once(':').ok_or(MALFORMED)?;
            if !name.eq_ignore_ascii_case("x-amz-trailer-signature") {
                return Err(MALFORMED);
            }
            let ChunkAuth::Signed(hasher) = &self.auth else {
                unreachable!()
            };
            let canonical = format!("{}:{value}\n", trailer.name);
            let expected = hasher
                .trailer_signature(&hex::encode(Sha256::digest(canonical)))
                .map_err(|_| BAD_SIGNATURE)?;
            if !bool::from(
                expected
                    .as_bytes()
                    .ct_eq(signature.trim_matches([' ', '\t']).as_bytes()),
            ) {
                return Err(BAD_SIGNATURE);
            }
        }
        if !self.line(&mut [0; MAX_TRAILER_LINE]).await?.is_empty() || self.fill().await? {
            return Err(MALFORMED);
        }
        trailer.verify(value)
    }

    async fn data_slice(&mut self, remaining: usize) -> Result<Bytes, &'static str> {
        if !self.fill().await? {
            return Err(TRUNCATED);
        }
        let n = self.pending.len().min(remaining).min(IO_BLOCK);
        Ok(self.pending.split_to(n))
    }

    async fn expect_crlf(&mut self) -> Result<(), &'static str> {
        if self.byte().await? != b'\r' || self.byte().await? != b'\n' {
            return Err(MALFORMED);
        }
        Ok(())
    }

    fn verify_signature(
        &mut self,
        signature: &[u8; 64],
        digest: &[u8],
    ) -> Result<(), &'static str> {
        let ChunkAuth::Signed(hasher) = &mut self.auth else {
            return Err(MALFORMED);
        };
        let expected = hasher
            .next(&hex::encode(digest))
            .map_err(|_| BAD_SIGNATURE)?;
        if bool::from(expected.as_bytes().ct_eq(signature)) {
            Ok(())
        } else {
            Err(BAD_SIGNATURE)
        }
    }

    fn update_checksum(&mut self, data: &[u8]) {
        if let Some(trailer) = &mut self.trailer {
            trailer.update(data);
        }
    }

    async fn read_unsigned(&mut self) -> Result<Option<Vec<u8>>, &'static str> {
        let bytes = self.data_slice(self.unsigned_remaining).await?;
        self.unsigned_remaining -= bytes.len();
        if self.unsigned_remaining == 0 {
            self.expect_crlf().await?;
        }
        // Until the checksum trailer is verified these bytes belong only in
        // guarded upload staging. The reader must not report successful EOF.
        self.update_checksum(&bytes);
        Ok(Some(bytes.to_vec()))
    }

    async fn read_verified_file(
        &mut self,
        mut replay: VerifiedFile,
    ) -> Result<Option<Vec<u8>>, &'static str> {
        let len = (replay.size - replay.offset).min(IO_BLOCK);
        let data = replay.file.read(replay.offset as u64, len).await?;
        replay.offset += data.len();
        self.update_checksum(&data);
        if replay.offset != replay.size {
            self.replay = Some(replay);
        }
        Ok(Some(data))
    }

    async fn read_buffered(
        &mut self,
        size: usize,
        signature: Option<&[u8; 64]>,
    ) -> Result<Option<Vec<u8>>, &'static str> {
        let mut data = Vec::new();
        while data.len() < size {
            data.extend_from_slice(&self.data_slice(size - data.len()).await?);
        }
        self.expect_crlf().await?;
        if let Some(signature) = signature {
            self.verify_signature(signature, &Sha256::digest(&data))?;
        }
        self.update_checksum(&data);
        Ok(Some(data))
    }

    async fn read_signed_file(
        &mut self,
        size: usize,
        signature: &[u8; 64],
    ) -> Result<Option<Vec<u8>>, &'static str> {
        let file = self.spool.open(size as u64).await?;
        let mut hash = Sha256::new();
        let mut offset = 0;
        while offset < size {
            let len = (size - offset).min(IO_BLOCK);
            let mut data = Vec::with_capacity(len);
            while data.len() < len {
                data.extend_from_slice(&self.data_slice(len - data.len()).await?);
            }
            hash.update(&data);
            file.write(offset as u64, data).await?;
            offset += len;
        }
        self.expect_crlf().await?;
        self.verify_signature(signature, &hash.finalize())?;
        self.read_verified_file(VerifiedFile {
            file,
            size,
            offset: 0,
        })
        .await
    }

    async fn read_chunk(&mut self) -> Result<Option<Vec<u8>>, &'static str> {
        if let Some(replay) = self.replay.take() {
            return self.read_verified_file(replay).await;
        }
        if self.unsigned_remaining != 0 {
            return self.read_unsigned().await;
        }
        let (size, signature) = self.header().await?;
        if size > self.remaining {
            return Err("ContentLengthMismatch: aws-chunked body exceeds the decoded length");
        }
        self.remaining -= size;
        if size != 0 {
            if size <= MEMORY_RECORD_THRESHOLD {
                return self.read_buffered(size, signature.as_ref()).await;
            }
            if let Some(signature) = signature {
                return self.read_signed_file(size, &signature).await;
            }
            self.unsigned_remaining = size;
            return self.read_unsigned().await;
        }

        // Trailers start immediately after the zero-size record line. Without
        // trailers, consume the blank line before accepting the end of body.
        if self.trailer.is_none() {
            self.expect_crlf().await?;
        }
        if let Some(signature) = signature {
            self.verify_signature(&signature, &Sha256::digest([]))?;
        }
        if self.remaining != 0 {
            return Err(TRUNCATED);
        }
        if let Some(trailer) = self.trailer.take() {
            self.read_trailer(trailer).await?;
        } else if self.fill().await? {
            return Err(MALFORMED);
        }
        self.finished = true;
        Ok(None)
    }
}

#[async_trait::async_trait]
impl<R: PollRead> PollRead for SignedChunkReader<R> {
    async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String> {
        if let Some(err) = self.failed {
            return Err(err.into());
        }
        if self.finished {
            return Ok(None);
        }
        // If this future is cancelled after consuming framing, the reader
        // cannot resume from its old position. Never return a successful EOF.
        self.failed = Some(TRUNCATED);
        match self.read_chunk().await {
            Ok(chunk) => {
                self.failed = None;
                Ok(chunk)
            }
            Err(err) => {
                self.failed = Some(err);
                Err(err.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    struct Frames(VecDeque<Vec<u8>>);

    #[async_trait::async_trait]
    impl PollRead for Frames {
        async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String> {
            Ok(self.0.pop_front())
        }
    }

    fn signer() -> HmacSha256CircleHasher {
        HmacSha256CircleHasher::new(
            [7; 32],
            "a".repeat(64),
            "20260901T010203Z".into(),
            "us-east-1".into(),
        )
    }

    fn wire(chunks: &[&[u8]]) -> Vec<u8> {
        let mut signer = signer();
        let mut stream = Vec::new();
        for chunk in chunks.iter().copied().chain([&[][..]]) {
            let sig = signer.next(&hex::encode(Sha256::digest(chunk))).unwrap();
            stream.extend(format!("{:x};chunk-signature={sig}\r\n", chunk.len()).as_bytes());
            stream.extend_from_slice(chunk);
            stream.extend_from_slice(b"\r\n");
        }
        stream
    }

    fn reader(wire: &[u8], frame_size: usize, length: usize) -> SignedChunkReader<Frames> {
        reader_with_spool(wire, frame_size, length, SignedSpool::new("", 0))
    }

    fn reader_with_spool(
        wire: &[u8],
        frame_size: usize,
        length: usize,
        spool: Arc<SignedSpool>,
    ) -> SignedChunkReader<Frames> {
        let frames = wire.chunks(frame_size).map(<[u8]>::to_vec).collect();
        SignedChunkReader::new(
            Frames(frames),
            ChunkAuth::Signed(signer()),
            length,
            None,
            spool,
        )
    }

    #[tokio::test]
    async fn unsigned_trailers_verify_all_supported_algorithms() {
        // Standard check values for "123456789", in network byte order.
        for (algorithm, encoded) in [
            ("crc32", "y/Q5Jg=="),
            ("crc32c", "4waSgw=="),
            ("crc64nvme", "rosUhgp5mIg="),
            ("sha1", "98O8HYCOBHMq32eZZczDTKeuNEE="),
            ("sha256", "FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU="),
        ] {
            let name = format!("x-amz-checksum-{algorithm}");
            let trailer = ChecksumTrailer::new(&name).unwrap();
            let (_, result) = trailer.response();
            let wire = format!("3\r\n123\r\n6\r\n456789\r\n0\r\n{name}:{encoded}\r\n\r\n");
            let frames = wire.as_bytes().chunks(1).map(<[u8]>::to_vec).collect();
            let mut reader = SignedChunkReader::new(
                Frames(frames),
                ChunkAuth::Unsigned,
                9,
                Some(trailer),
                SignedSpool::new("", 0),
            );
            assert_eq!(reader.poll_read().await.unwrap().unwrap(), b"123");
            assert_eq!(reader.poll_read().await.unwrap().unwrap(), b"456789");
            assert!(result.get().is_none());
            assert_eq!(reader.poll_read().await, Ok(None), "{algorithm}");
            assert_eq!(result.get().map(String::as_str), Some(encoded));
        }
    }

    #[tokio::test]
    async fn signed_stream_survives_arbitrary_http_frame_boundaries() {
        let wire = wire(&[b"abc", b"def\r\nghi"]);
        for frame in [1, 7, 85, wire.len()] {
            let mut r = reader(&wire, frame, 11);
            assert_eq!(r.poll_read().await.unwrap().unwrap(), b"abc");
            assert_eq!(r.poll_read().await.unwrap().unwrap(), b"def\r\nghi");
            assert_eq!(r.poll_read().await.unwrap(), None);
            assert_eq!(r.poll_read().await.unwrap(), None);
        }
        let mut empty = reader(&self::wire(&[]), 1, 0);
        assert_eq!(empty.poll_read().await.unwrap(), None);
    }

    #[tokio::test]
    async fn integrity_errors_never_release_an_invalid_chunk_or_eof() {
        let mut bad_data = wire(&[b"abc"]);
        let data = bad_data.windows(5).position(|s| s == b"\nabc\r").unwrap() + 1;
        bad_data[data] = b'X';
        let mut r = reader(&bad_data, 3, 3);
        assert_eq!(r.poll_read().await.unwrap_err(), BAD_SIGNATURE);
        assert_eq!(r.poll_read().await.unwrap_err(), BAD_SIGNATURE);

        let mut bad_terminal = wire(&[b"abc"]);
        let i = bad_terminal.len() - 5;
        bad_terminal[i] = if bad_terminal[i] == b'0' { b'1' } else { b'0' };
        let mut r = reader(&bad_terminal, 21, 3);
        assert_eq!(r.poll_read().await.unwrap().unwrap(), b"abc");
        assert_eq!(r.poll_read().await.unwrap_err(), BAD_SIGNATURE);
    }

    #[tokio::test]
    async fn rejects_incomplete_length_malformed_and_trailing_data() {
        let valid = wire(&[b"abc"]);
        let mut trailing = valid.clone();
        trailing.push(b'X');
        for (data, length, error) in [
            (&valid[..valid.len() - 1], 3, TRUNCATED),
            (&valid[..], 4, TRUNCATED),
            (&trailing[..], 3, MALFORMED),
        ] {
            let mut r = reader(data, 3, length);
            assert_eq!(r.poll_read().await.unwrap().unwrap(), b"abc");
            assert_eq!(r.poll_read().await.unwrap_err(), error);
            assert_eq!(r.poll_read().await.unwrap_err(), error);
        }
        assert!(reader(&valid, 3, 2)
            .poll_read()
            .await
            .unwrap_err()
            .starts_with("ContentLengthMismatch:"));
        assert_eq!(
            reader(&[b'x'; MAX_CHUNK_HEADER], 3, 0)
                .poll_read()
                .await
                .unwrap_err(),
            MALFORMED
        );
    }

    #[tokio::test]
    async fn large_signed_record_is_replayed_in_bounded_slices_only_after_verification() {
        let data = vec![0x7c; 32 << 20];
        let mut wire = wire(&[&data, b"tail"]);
        let dir = tempfile::tempdir().unwrap();
        let spool = SignedSpool::new(dir.path().join("spool"), data.len() as u64);
        let mut r = reader_with_spool(&wire, IO_BLOCK + 1, data.len() + 4, spool.clone());
        for slice in data.chunks(IO_BLOCK) {
            assert_eq!(r.poll_read().await.unwrap().as_deref(), Some(slice));
        }
        assert!(r.replay.is_none());
        assert_eq!(r.poll_read().await.unwrap().unwrap(), b"tail");
        assert_eq!(r.poll_read().await, Ok(None));

        let payload = wire.iter().position(|b| *b == b'\n').unwrap() + 1;
        wire[payload + data.len() - 1] ^= 1;
        let mut r = reader_with_spool(&wire, IO_BLOCK, data.len() + 4, spool);
        assert_eq!(r.poll_read().await.unwrap_err(), BAD_SIGNATURE);
        assert!(r.replay.is_none());
        assert_eq!(r.poll_read().await.unwrap_err(), BAD_SIGNATURE);
    }

    #[tokio::test]
    async fn cancel_after_first_replay_slice_releases_spool_and_budget() {
        use crate::utils::signed_spool::SPOOL_BUSY;
        let dir = tempfile::tempdir().unwrap();
        let size = MEMORY_RECORD_THRESHOLD + 1;
        let spool_dir = dir.path().join("spool");
        let spool = SignedSpool::new(&spool_dir, size as u64);
        let (replayed, ready) = tokio::sync::oneshot::channel();
        let owner = spool.clone();
        let request = tokio::spawn(async move {
            let wire = wire(&[&vec![0x5e; size]]);
            let mut r = reader_with_spool(&wire, IO_BLOCK, size, owner);
            assert_eq!(r.poll_read().await.unwrap().unwrap(), vec![0x5e; IO_BLOCK]);
            assert_eq!(r.replay.as_ref().unwrap().offset, IO_BLOCK);
            replayed.send(()).unwrap();
            // Model the downstream staging writer blocking between reads.
            std::future::pending::<()>().await;
            drop(r);
        });
        ready.await.unwrap();
        assert_eq!(spool.open(1).await.err(), Some(SPOOL_BUSY));
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert!(spool.open(size as u64).await.is_ok());
        assert!(spool_dir.read_dir().unwrap().next().is_none());
    }
}
