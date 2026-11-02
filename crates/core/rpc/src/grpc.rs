// Copyright 2026 OPPO.
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

//! Common gRPC transport settings and logical request identity.

use serde::{Deserialize, Serialize};
use std::time::Duration;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig};
use tonic::{Request, Status};

/// Keep this ID unchanged when retrying a logical mutation.
pub const REQUEST_ID_METADATA: &str = "x-curvine-request-id";
/// Includes framing overhead above the largest supported 16 MiB data chunk.
pub const MAX_MESSAGE_BYTES: usize = 20 * 1024 * 1024;

pub use tonic_health::server::health_reporter;
pub use tonic_reflection as reflection;

/// File-based TLS identity and reflection settings for one component.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct GrpcConfig {
    pub tls_enabled: bool,
    pub ca_cert_path: String,
    /// Server-side client trust CA; defaults to `ca_cert_path`.
    pub client_ca_cert_path: String,
    pub server_cert_path: String,
    pub server_key_path: String,
    pub client_cert_path: String,
    pub client_key_path: String,
    pub domain_name: String,
    pub require_client_auth: bool,
    pub enable_reflection: bool,
}

impl GrpcConfig {
    /// Construct an HTTP URI with the correct scheme and IPv6 authority.
    pub fn uri(&self, host: &str, port: u16) -> String {
        let host = if host.parse::<std::net::Ipv6Addr>().is_ok() {
            format!("[{host}]")
        } else {
            host.to_string()
        };
        let scheme = if self.tls_enabled { "https" } else { "http" };
        format!("{scheme}://{host}:{port}")
    }

    pub fn client_tls(&self, host: &str) -> std::io::Result<Option<ClientTls>> {
        if !self.tls_enabled {
            self.check_disabled()?;
            return Ok(None);
        }
        let ca = read_required(&self.ca_cert_path, "gRPC CA certificate")?;
        let identity = match (
            self.client_cert_path.is_empty(),
            self.client_key_path.is_empty(),
        ) {
            (true, true) => None,
            (false, false) => Some((
                read_required(&self.client_cert_path, "gRPC client certificate")?,
                read_required(&self.client_key_path, "gRPC client key")?,
            )),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "gRPC client certificate and key must be configured together",
                ))
            }
        };
        let domain = if self.domain_name.is_empty() {
            host
        } else {
            &self.domain_name
        };
        if domain.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "gRPC TLS server name is empty",
            ));
        }
        Ok(Some(ClientTls {
            ca_certificate_pem: ca,
            domain_name: domain.to_string(),
            identity_pem: identity,
        }))
    }

    pub fn server_tls(&self) -> std::io::Result<Option<ServerTls>> {
        if !self.tls_enabled {
            self.check_disabled()?;
            return Ok(None);
        }
        if !self.require_client_auth && !self.client_ca_cert_path.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "gRPC client_ca_cert_path requires require_client_auth=true",
            ));
        }
        let certificate_pem = read_required(&self.server_cert_path, "gRPC server certificate")?;
        let private_key_pem = read_required(&self.server_key_path, "gRPC server key")?;
        let client_ca_pem = if self.require_client_auth {
            let path = if self.client_ca_cert_path.is_empty() {
                &self.ca_cert_path
            } else {
                &self.client_ca_cert_path
            };
            Some(read_required(path, "gRPC client CA certificate")?)
        } else {
            None
        };
        Ok(Some(ServerTls {
            certificate_pem,
            private_key_pem,
            client_ca_pem,
        }))
    }

    fn check_disabled(&self) -> std::io::Result<()> {
        if self.require_client_auth
            || !self.ca_cert_path.is_empty()
            || !self.client_ca_cert_path.is_empty()
            || !self.server_cert_path.is_empty()
            || !self.server_key_path.is_empty()
            || !self.client_cert_path.is_empty()
            || !self.client_key_path.is_empty()
            || !self.domain_name.is_empty()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "gRPC TLS options require tls_enabled=true",
            ));
        }
        Ok(())
    }
}

fn read_required(path: &str, label: &str) -> std::io::Result<Vec<u8>> {
    if path.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{label} path is required"),
        ));
    }
    std::fs::read(path).map_err(|e| {
        std::io::Error::new(e.kind(), format!("failed to read {label} at {path}: {e}"))
    })
}

pub fn set_request_id<T>(request: &mut Request<T>, id: i64) {
    // An ASCII decimal representation of an i64 is always valid metadata.
    request
        .metadata_mut()
        .insert(REQUEST_ID_METADATA, id.to_string().parse().unwrap());
}

/// Missing or invalid IDs must fail closed: silently generating a new one on
/// the server would break deduplication after retries.
pub fn request_id<T>(request: &Request<T>) -> Result<i64, Status> {
    request
        .metadata()
        .get(REQUEST_ID_METADATA)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| Status::invalid_argument("missing or invalid x-curvine-request-id"))
}

#[derive(Clone)]
pub struct ClientTls {
    pub ca_certificate_pem: Vec<u8>,
    pub domain_name: String,
    /// Optional client identity for mutual TLS: certificate chain and key.
    pub identity_pem: Option<(Vec<u8>, Vec<u8>)>,
}

impl std::fmt::Debug for ClientTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientTls")
            .field("domain_name", &self.domain_name)
            .field("client_identity_configured", &self.identity_pem.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct ServerTls {
    pub certificate_pem: Vec<u8>,
    pub private_key_pem: Vec<u8>,
    /// Require and verify client certificates when set.
    pub client_ca_pem: Option<Vec<u8>>,
}

impl std::fmt::Debug for ServerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTls")
            .field("client_auth_configured", &self.client_ca_pem.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub connect_timeout: Duration,
    /// Disable for long-lived streams; enforce per-operation limits instead.
    pub request_timeout: Option<Duration>,
    pub tcp_keepalive: Option<Duration>,
    pub max_message_bytes: usize,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Some(Duration::from_secs(30)),
            tcp_keepalive: Some(Duration::from_secs(60)),
            max_message_bytes: MAX_MESSAGE_BYTES,
        }
    }
}

impl TransportConfig {
    /// Service clients and servers must also apply `max_message_bytes` to
    /// their generated encode/decode limit setters.
    pub fn endpoint(
        &self,
        address: String,
        tls: Option<&ClientTls>,
    ) -> Result<Endpoint, tonic::transport::Error> {
        let mut endpoint = Endpoint::from_shared(address)?
            .connect_timeout(self.connect_timeout)
            .tcp_keepalive(self.tcp_keepalive);
        if let Some(timeout) = self.request_timeout {
            endpoint = endpoint.timeout(timeout);
        }
        if let Some(tls) = tls {
            let mut config = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(&tls.ca_certificate_pem))
                .domain_name(&tls.domain_name);
            if let Some((cert, key)) = &tls.identity_pem {
                config = config.identity(Identity::from_pem(cert, key));
            }
            endpoint = endpoint.tls_config(config)?;
        }
        Ok(endpoint)
    }

    pub fn server(&self, tls: Option<&ServerTls>) -> Result<Server, tonic::transport::Error> {
        let mut server = Server::builder().tcp_keepalive(self.tcp_keepalive);
        if let Some(tls) = tls {
            let mut config = ServerTlsConfig::new().identity(Identity::from_pem(
                &tls.certificate_pem,
                &tls.private_key_pem,
            ));
            if let Some(ca) = &tls.client_ca_pem {
                config = config.client_ca_root(Certificate::from_pem(ca));
            }
            server = server.tls_config(config)?;
        }
        Ok(server)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_id_round_trip_and_missing_rejected() {
        let mut request = Request::new(());
        assert_eq!(
            request_id(&request).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        set_request_id(&mut request, 42);
        assert_eq!(request_id(&request).unwrap(), 42);
    }
}
