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

use std::time::{Duration, Instant};

use futures::StreamExt;
use reqwest::redirect::Policy;
use reqwest::Response;

use super::model::{MetricSample, MetricTarget};
use super::prom_parse::parse_prometheus_text;

pub const MAX_METRICS_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone)]
pub struct MetricsScraper {
    client: reqwest::Client,
}

#[derive(Debug)]
pub struct ScrapeResult {
    pub samples: Vec<MetricSample>,
    pub duration_ms: u64,
    pub error: Option<String>,
    pub parse_error_count: usize,
}

impl MetricsScraper {
    pub fn new(timeout: Duration) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(Policy::none())
            .build()
            .expect("failed to build observability HTTP client");
        Self { client }
    }

    pub async fn scrape(&self, target: &MetricTarget) -> ScrapeResult {
        let started = Instant::now();
        let response = match self.client.get(&target.url).send().await {
            Ok(response) => response,
            Err(err) => return self.error(started, err.to_string()),
        };
        if !response.status().is_success() {
            return self.error(started, format!("HTTP {}", response.status()));
        }
        match read_body_with_limit(response, started).await {
            Ok(body) => Self::parse_payload(&body, started),
            Err(result) => result,
        }
    }

    pub fn parse_payload(payload: &[u8], started: Instant) -> ScrapeResult {
        if payload.is_empty() {
            return ScrapeResult {
                samples: Vec::new(),
                duration_ms: started.elapsed().as_millis() as u64,
                error: Some("empty metrics payload".to_string()),
                parse_error_count: 0,
            };
        }
        if payload.len() > MAX_METRICS_PAYLOAD_BYTES {
            return payload_too_large(payload.len(), started);
        }
        let text = String::from_utf8_lossy(payload);
        if text.trim().is_empty() {
            return ScrapeResult {
                samples: Vec::new(),
                duration_ms: started.elapsed().as_millis() as u64,
                error: Some("empty metrics payload".to_string()),
                parse_error_count: 0,
            };
        }
        let parsed = parse_prometheus_text(&text);
        if parsed.samples.is_empty() {
            let message = if parsed.error_count > 0 {
                format!("no valid samples after {} parse errors", parsed.error_count)
            } else {
                "no valid samples in metrics payload".to_string()
            };
            return ScrapeResult {
                samples: Vec::new(),
                duration_ms: started.elapsed().as_millis() as u64,
                error: Some(message),
                parse_error_count: parsed.error_count,
            };
        }
        ScrapeResult {
            samples: parsed.samples,
            duration_ms: started.elapsed().as_millis() as u64,
            error: None,
            parse_error_count: parsed.error_count,
        }
    }

    fn error(&self, started: Instant, error: impl Into<String>) -> ScrapeResult {
        ScrapeResult {
            samples: Vec::new(),
            duration_ms: started.elapsed().as_millis() as u64,
            error: Some(error.into()),
            parse_error_count: 0,
        }
    }
}

async fn read_body_with_limit(
    response: Response,
    started: Instant,
) -> Result<Vec<u8>, ScrapeResult> {
    if let Some(len) = response.content_length() {
        if len > MAX_METRICS_PAYLOAD_BYTES as u64 {
            return Err(payload_too_large(len as usize, started));
        }
    }

    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(err) => {
                return Err(ScrapeResult {
                    samples: Vec::new(),
                    duration_ms: started.elapsed().as_millis() as u64,
                    error: Some(err.to_string()),
                    parse_error_count: 0,
                });
            }
        };
        if body.len() + chunk.len() > MAX_METRICS_PAYLOAD_BYTES {
            return Err(payload_too_large(body.len() + chunk.len(), started));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn payload_too_large(size: usize, started: Instant) -> ScrapeResult {
    ScrapeResult {
        samples: Vec::new(),
        duration_ms: started.elapsed().as_millis() as u64,
        error: Some(format!(
            "metrics payload too large: {size} bytes (max {MAX_METRICS_PAYLOAD_BYTES})"
        )),
        parse_error_count: 0,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    #[test]
    fn rejects_empty_payload() {
        let result = MetricsScraper::parse_payload(b"", Instant::now());
        assert!(result.error.is_some());
        assert!(result.samples.is_empty());
    }

    #[test]
    fn rejects_oversized_payload() {
        let payload = vec![b'x'; MAX_METRICS_PAYLOAD_BYTES + 1];
        let result = MetricsScraper::parse_payload(&payload, Instant::now());
        assert!(result.error.as_deref().unwrap_or("").contains("too large"));
    }

    #[test]
    fn parses_valid_payload() {
        let result = MetricsScraper::parse_payload(b"capacity 42\n", Instant::now());
        assert!(result.error.is_none());
        assert_eq!(result.samples.len(), 1);
        assert_eq!(result.samples[0].name, "capacity");
        assert_eq!(result.samples[0].value, 42.0);
    }

    #[test]
    fn records_partial_parse_errors() {
        let result = MetricsScraper::parse_payload(b"bad line\ncapacity 1\n", Instant::now());
        assert!(result.error.is_none());
        assert_eq!(result.parse_error_count, 1);
        assert_eq!(result.samples.len(), 1);
    }

    #[test]
    fn rejects_all_malformed_payload() {
        let result = MetricsScraper::parse_payload(b"bad line\nalso bad\n", Instant::now());
        assert!(result.error.is_some());
        assert!(result.samples.is_empty());
        assert_eq!(result.parse_error_count, 2);
        assert!(result
            .error
            .as_deref()
            .unwrap_or("")
            .contains("no valid samples"));
    }

    #[test]
    fn rejects_comments_only_payload() {
        let payload = b"# HELP capacity Total capacity\n# TYPE capacity gauge\n";
        let result = MetricsScraper::parse_payload(payload, Instant::now());
        assert!(result.error.is_some());
        assert!(result.samples.is_empty());
        assert_eq!(result.parse_error_count, 0);
    }

    #[test]
    fn content_length_helper_formats_oversized_error() {
        let started = Instant::now();
        let err = payload_too_large(MAX_METRICS_PAYLOAD_BYTES + 1, started);
        assert!(err.error.as_deref().unwrap_or("").contains("too large"));
    }

    #[tokio::test]
    async fn rejects_oversized_content_length_before_body_read() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    MAX_METRICS_PAYLOAD_BYTES + 1
                );
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });

        let scraper = MetricsScraper::new(Duration::from_secs(5));
        let endpoint = addr.to_string();
        let target =
            MetricTarget::master(endpoint.clone(), format!("http://{endpoint}/metrics"), None);
        let result = scraper.scrape(&target).await;
        assert!(result.error.as_deref().unwrap_or("").contains("too large"));
        assert!(result.samples.is_empty());
    }
}
