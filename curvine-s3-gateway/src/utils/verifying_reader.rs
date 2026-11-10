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

use sha2::{Digest, Sha256};

use crate::utils::io::PollRead;

/// A request body reader that validates the body while it streams.
///
/// The wrapper updates an incremental SHA-256 and counts bytes as the consumer
/// reads. It releases each chunk unchanged, so validation costs no buffered
/// copy. When the underlying stream reaches its end, the wrapper checks the
/// declared length and accumulated hash before reporting completion; a mismatch
/// turns the end of the stream into an error instead of a successful read.
pub struct VerifyingReader<R> {
    inner: R,
    expected_sha256: Option<String>,
    expected_length: Option<u64>,
    bytes_seen: u64,
    hasher: Option<Sha256>,
    finished: bool,
}

impl<R> VerifyingReader<R> {
    pub fn new(inner: R, expected_sha256: Option<String>, expected_length: Option<u64>) -> Self {
        let expected_sha256 = expected_sha256.map(|hash| hash.trim().to_ascii_lowercase());
        let hasher = expected_sha256.is_some().then(Sha256::default);
        Self {
            inner,
            expected_sha256,
            expected_length,
            bytes_seen: 0,
            hasher,
            finished: false,
        }
    }

    /// Number of bytes released to the consumer so far.
    pub fn bytes_seen(&self) -> u64 {
        self.bytes_seen
    }
}

#[async_trait::async_trait]
impl<R> PollRead for VerifyingReader<R>
where
    R: PollRead + Send,
{
    async fn poll_read(&mut self) -> Result<Option<Vec<u8>>, String> {
        if self.finished {
            return Ok(None);
        }

        match self.inner.poll_read().await {
            // A truncated HTTP body does not end in a clean EOF: hyper's length
            // decoder reports the premature end as a transport error. When the
            // declared length was not reached, that is a client-side short body
            // and must be answered 400, not become an opaque 500. A transport
            // error after the full length arrived is unrelated and passes
            // through unchanged.
            Err(error) => {
                self.finished = true;
                match self.expected_length {
                    Some(expected_length) if self.bytes_seen < expected_length => Err(format!(
                        "ContentLengthMismatch: received {} of {} bytes (transport: {error})",
                        self.bytes_seen, expected_length
                    )),
                    _ => Err(error),
                }
            }
            Ok(None) => {
                if let Some(expected_length) = self.expected_length {
                    if self.bytes_seen != expected_length {
                        self.finished = true;
                        return Err(format!(
                            "ContentLengthMismatch: received {} of {} bytes",
                            self.bytes_seen, expected_length
                        ));
                    }
                }

                if let Some(expected_hash) = self.expected_sha256.as_deref() {
                    let actual_hash = self
                        .hasher
                        .take()
                        .map(|hasher| hex::encode(hasher.finalize()))
                        .unwrap_or_default();
                    if actual_hash != expected_hash {
                        self.finished = true;
                        return Err(format!(
                            "HashMismatch: received {actual_hash}, expected {expected_hash}"
                        ));
                    }
                }

                self.finished = true;
                Ok(None)
            }
            Ok(Some(chunk)) => {
                let Some(new_bytes_seen) = self.bytes_seen.checked_add(chunk.len() as u64) else {
                    self.finished = true;
                    return Err(format!(
                        "ContentLengthMismatch: body exceeded the declared length {}",
                        self.expected_length
                            .map(|length| length.to_string())
                            .unwrap_or_else(|| "<unbounded>".to_string())
                    ));
                };

                if let Some(expected_length) = self.expected_length {
                    if new_bytes_seen > expected_length {
                        // Terminal: without this a retried read could reach a
                        // no-longer-exceeding EOF and report success for a body
                        // already rejected as too long.
                        self.finished = true;
                        return Err(format!(
                            "ContentLengthMismatch: received more than declared length {}",
                            expected_length
                        ));
                    }
                }

                self.bytes_seen = new_bytes_seen;
                if let Some(hasher) = self.hasher.as_mut() {
                    hasher.update(&chunk);
                }
                Ok(Some(chunk))
            }
        }
    }
}
