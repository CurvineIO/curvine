use std::collections::VecDeque;

use curvine_s3_gateway::utils::io::PollRead;
use curvine_s3_gateway::utils::verifying_reader::VerifyingReader;
use sha2::{Digest, Sha256};

#[derive(Default)]
struct Chunks {
    chunks: VecDeque<Vec<u8>>,
}

impl Chunks {
    fn new(chunks: impl IntoIterator<Item = Vec<u8>>) -> Self {
        Self {
            chunks: chunks.into_iter().collect(),
        }
    }
}

#[async_trait::async_trait]
impl PollRead for Chunks {
    async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String> {
        Ok(self.chunks.pop_front())
    }
}

async fn collect(reader: &mut VerifyingReader<Chunks>) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    while let Some(chunk) = reader.poll_read().await? {
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::default();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

#[tokio::test]
async fn releases_matching_chunks_and_stops_at_eof() {
    let data = b"hello world".to_vec();
    let reader = Chunks::new([b"hel".to_vec(), b"lo world".to_vec()]);
    let mut reader = VerifyingReader::new(reader, Some(sha256_hex(&data)), Some(data.len() as u64));

    assert_eq!(collect(&mut reader).await.unwrap(), data);
    assert_eq!(reader.bytes_seen(), data.len() as u64);
}

#[tokio::test]
async fn hash_mismatch_is_reported_at_eof() {
    let reader = Chunks::new([b"hello".to_vec()]);
    let mut reader = VerifyingReader::new(reader, Some("0".repeat(64)), Some(5));

    let error = collect(&mut reader).await.unwrap_err();
    assert!(error.starts_with("HashMismatch:"), "{error}");
}

#[tokio::test]
async fn unsigned_body_rejects_a_short_body() {
    let reader = Chunks::new([b"hello".to_vec()]);
    let mut reader = VerifyingReader::new(reader, None, Some(10));

    assert_eq!(reader.poll_read().await.unwrap(), Some(b"hello".to_vec()));
    assert_eq!(
        reader.poll_read().await.unwrap_err(),
        "ContentLengthMismatch: received 5 of 10 bytes"
    );
    assert!(
        reader.poll_read().await.is_err(),
        "a short body must not later read as EOF"
    );
}

#[tokio::test]
async fn long_body_is_rejected_before_the_extra_bytes_are_released() {
    let reader = Chunks::new([b"hello".to_vec(), b"!".to_vec()]);
    let mut reader = VerifyingReader::new(reader, None, Some(5));

    assert_eq!(reader.poll_read().await.unwrap(), Some(b"hello".to_vec()));
    assert!(reader
        .poll_read()
        .await
        .unwrap_err()
        .starts_with("ContentLengthMismatch:"));
}

#[tokio::test]
async fn empty_body_with_empty_hash_succeeds() {
    let reader = Chunks::new([]);
    let mut reader = VerifyingReader::new(reader, Some(sha256_hex(b"")), Some(0));

    assert_eq!(reader.poll_read().await.unwrap(), None);
}

/// A reader that fails the way hyper's length decoder does on a truncated
/// request: the stream ends in a transport error, not a clean EOF.
struct TruncatedAfterOneChunk {
    chunk: Option<Vec<u8>>,
}

#[async_trait::async_trait]
impl PollRead for TruncatedAfterOneChunk {
    async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String> {
        match self.chunk.take() {
            Some(chunk) => Ok(Some(chunk)),
            None => Err("end of file before message length reached".to_string()),
        }
    }
}

#[tokio::test]
async fn transport_error_before_the_declared_length_is_a_length_mismatch() {
    let mut reader = VerifyingReader::new(
        TruncatedAfterOneChunk {
            chunk: Some(b"hello".to_vec()),
        },
        None,
        Some(10),
    );

    assert_eq!(reader.poll_read().await.unwrap(), Some(b"hello".to_vec()));
    let error = reader.poll_read().await.unwrap_err();
    assert!(error.starts_with("ContentLengthMismatch:"), "{error}");
    assert!(error.contains("received 5 of 10 bytes"), "{error}");
}

#[tokio::test]
async fn transport_error_after_the_declared_length_passes_through() {
    let mut reader = VerifyingReader::new(
        TruncatedAfterOneChunk {
            chunk: Some(b"hello".to_vec()),
        },
        None,
        Some(5),
    );

    assert_eq!(reader.poll_read().await.unwrap(), Some(b"hello".to_vec()));
    let error = reader.poll_read().await.unwrap_err();
    assert!(!error.starts_with("ContentLengthMismatch:"), "{error}");
    assert!(error.contains("end of file"), "{error}");
}

/// A rejection must be sticky: a consumer that retries after an error must not
/// observe a successful end of body, which is what a rejected object being
/// published would look like.
#[tokio::test]
async fn a_verification_failure_is_returned_by_every_later_read() {
    let reader = Chunks::new([b"hello".to_vec(), b"!".to_vec()]);
    let mut reader = VerifyingReader::new(reader, Some(sha256_hex(b"hello")), Some(5));

    assert_eq!(reader.poll_read().await.unwrap(), Some(b"hello".to_vec()));
    let first = reader.poll_read().await.unwrap_err();
    assert!(first.starts_with("ContentLengthMismatch:"), "{first}");

    let second = reader.poll_read().await.unwrap_err();
    assert_eq!(second, first, "the first failure must repeat exactly");
    assert!(reader.poll_read().await.is_err(), "still failing");
}

/// The hash check runs at EOF, so its rejection must be sticky for the same
/// reason.
#[tokio::test]
async fn a_hash_mismatch_is_returned_by_every_later_read() {
    let reader = Chunks::new([b"hello".to_vec()]);
    let mut reader = VerifyingReader::new(reader, Some("0".repeat(64)), Some(5));

    // The digest is only decidable at EOF, so the chunk is released first.
    assert_eq!(reader.poll_read().await.unwrap(), Some(b"hello".to_vec()));
    let first = reader.poll_read().await.unwrap_err();
    assert!(first.starts_with("HashMismatch:"), "{first}");
    assert_eq!(reader.poll_read().await.unwrap_err(), first);
}
