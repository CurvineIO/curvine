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

//! Wire-level checks that the credential plan actually reaches the signed request.
//!
//! The `S3OperatorSettings::from_conf` unit tests stop at the resolved struct: deleting
//! `builder.session_token(...)` would leave them green while STS access broke. These tests drive
//! `OpendalFileSystem` against a loopback stand-in for S3 and assert on the raw request bytes, so
//! the SigV4 fields (including `x-amz-security-token`) are covered without cloud credentials.

#[cfg(feature = "opendal-s3")]
mod sts_wire_tests {
    use curvine_core_error::CommonResult;
    use curvine_fs_api::{FileSystem, Path};
    use curvine_runtime::runtime::{AsyncRuntime, RpcRuntime};
    use curvine_ufs_opendal::OpendalFileSystem;
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    const LIST_EMPTY: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
        <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
        <Name>bucket</Name><Prefix>key/</Prefix><KeyCount>0</KeyCount>\
        <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>";

    /// Minimal S3 stand-in. Records every raw request so the test can inspect the signature, and
    /// answers list requests with an empty ListObjectsV2 page and everything else with a 200 object
    /// response (OpenDAL probes the object first and falls back to a listing for directories).
    struct MockS3 {
        endpoint: String,
        requests: Arc<Mutex<Vec<String>>>,
        stop: Arc<AtomicBool>,
    }

    impl MockS3 {
        fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
            let addr = listener.local_addr().expect("local addr");
            listener
                .set_nonblocking(true)
                .expect("non-blocking listener");

            let requests = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));

            let thread_requests = requests.clone();
            let thread_stop = stop.clone();
            std::thread::spawn(move || {
                while !thread_stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let _ = Self::serve(stream, &thread_requests);
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            });

            Self {
                endpoint: format!("http://{}", addr),
                requests,
                stop,
            }
        }

        fn serve(stream: TcpStream, requests: &Arc<Mutex<Vec<String>>>) -> std::io::Result<()> {
            let mut stream = stream;
            stream.set_read_timeout(Some(Duration::from_secs(5)))?;

            let mut raw = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let n = stream.read(&mut chunk)?;
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&chunk[..n]);
                if raw.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let request = String::from_utf8_lossy(&raw).into_owned();
            let is_listing = is_listing_request(&request);
            requests.lock().expect("request log").push(request);

            let (content_type, body) = if is_listing {
                ("application/xml", LIST_EMPTY)
            } else {
                ("application/octet-stream", "abcd")
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\n\
                 Last-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\n\
                 ETag: \"d41d8cd98f00b204e9800998ecf8427e\"\r\nConnection: close\r\n\r\n{}",
                content_type,
                body.len(),
                body
            );

            stream.write_all(response.as_bytes())?;
            stream.flush()
        }

        /// Every request that reached the mock, joined for substring assertions.
        fn wait_for_requests(&self) -> String {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let joined = self
                    .requests
                    .lock()
                    .expect("request log")
                    .iter()
                    .map(|r| r.to_ascii_lowercase())
                    .collect::<Vec<_>>()
                    .join("\n---\n");
                if !joined.is_empty() {
                    return joined;
                }
                assert!(Instant::now() < deadline, "no request reached the mock S3");
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    impl Drop for MockS3 {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
        }
    }

    /// List requests are `GET`s whose target carries a list parameter or names a "directory".
    fn is_listing_request(request: &str) -> bool {
        let Some(line) = request.lines().next() else {
            return false;
        };
        let mut parts = line.split(' ');
        let method = parts.next().unwrap_or_default();
        let target = parts.next().unwrap_or_default();

        method == "GET"
            && (target.contains("list-type=")
                || target.contains("prefix=")
                || target.ends_with('/'))
    }

    fn conf(endpoint: &str, extra: &[(&str, &str)]) -> HashMap<String, String> {
        let mut conf = HashMap::from([
            ("s3.endpoint_url".to_string(), endpoint.to_string()),
            ("s3.region_name".to_string(), "us-east-1".to_string()),
        ]);
        for (key, value) in extra {
            conf.insert((*key).to_string(), (*value).to_string());
        }
        conf
    }

    /// Stat one object against the mock; returns the raw requests the mock observed.
    fn stat_and_capture(conf: HashMap<String, String>, mock: &MockS3) -> String {
        let path = Path::new("s3://bucket/key").expect("valid path");
        let fs = OpendalFileSystem::new(&path, conf).expect("operator must build");

        let rt = AsyncRuntime::single();
        let status = rt.block_on(async { fs.get_status(&path).await });
        assert!(
            status.is_ok(),
            "stat against the mock must succeed: {status:?}"
        );

        mock.wait_for_requests()
    }

    #[test]
    fn sts_mount_signs_requests_with_the_session_token() -> CommonResult<()> {
        let mock = MockS3::start();
        let requests = stat_and_capture(
            conf(
                &mock.endpoint,
                &[
                    ("s3.credentials.type", "sts"),
                    ("s3.credentials.access", "STS_AK"),
                    ("s3.credentials.secret", "STS_SK"),
                    ("s3.credentials.token", "sts-token-value"),
                ],
            ),
            &mock,
        );

        assert!(
            requests.contains("authorization: aws4-hmac-sha256"),
            "the request must be SigV4 signed:\n{requests}"
        );
        assert!(
            requests.contains("credential=sts_ak/"),
            "the request must be signed with the mount access key:\n{requests}"
        );
        assert!(
            requests.contains("x-amz-security-token: sts-token-value"),
            "the session token must be sent as x-amz-security-token:\n{requests}"
        );
        Ok(())
    }

    #[test]
    fn static_mount_does_not_send_a_session_token() -> CommonResult<()> {
        let mock = MockS3::start();
        let requests = stat_and_capture(
            conf(
                &mock.endpoint,
                &[
                    ("s3.credentials.access", "STATIC_AK"),
                    ("s3.credentials.secret", "STATIC_SK"),
                ],
            ),
            &mock,
        );

        assert!(
            requests.contains("credential=static_ak/"),
            "the request must be signed with the mount access key:\n{requests}"
        );
        assert!(
            !requests.contains("x-amz-security-token"),
            "a static mount must not send a session token:\n{requests}"
        );
        Ok(())
    }

    /// #1972 test plan: an `auto` mount on a host without source credentials must fail its first
    /// data access with a credential error rather than a panic.
    #[test]
    fn auto_mount_without_source_credentials_fails_with_a_credential_error() -> CommonResult<()> {
        let mock = MockS3::start();
        let path = Path::new("s3://bucket/key").expect("valid path");
        let fs = OpendalFileSystem::new(
            &path,
            conf(
                &mock.endpoint,
                &[
                    ("s3.credentials.type", "auto"),
                    // Isolate every ambient source so the result never depends on the host identity.
                    ("s3.credentials.disable_config_load", "true"),
                    ("s3.credentials.disable_ec2_metadata", "true"),
                ],
            ),
        )
        .expect("operator must build");

        let rt = AsyncRuntime::single();
        let status = rt.block_on(async { fs.get_status(&path).await });
        let err = status.expect_err("first data access without any credential must fail");
        assert!(
            err.to_string().to_lowercase().contains("credential"),
            "the failure must point at the missing credential: {err}"
        );
        Ok(())
    }
}
