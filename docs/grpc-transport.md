# gRPC TLS and reflection

The master, worker, transfer service, and Raft journal use gRPC. TLS and
reflection are disabled by default. Configure each listener under
`[master.grpc]`, `[worker.grpc]`, `[transfer.grpc]`, or `[journal.grpc]`.
Configure outgoing master, worker, and transfer connections under
`[client.grpc]`. Raft peer connections use `[journal.grpc]` for both their
server and client settings.

For example, a worker requiring mutual TLS can use:

```toml
[worker.grpc]
tls_enabled = true
server_cert_path = "/etc/curvine/tls/worker.crt"
server_key_path = "/etc/curvine/tls/worker.key"
ca_cert_path = "/etc/curvine/tls/client-ca.crt"
require_client_auth = true
enable_reflection = false

[client.grpc]
tls_enabled = true
ca_cert_path = "/etc/curvine/tls/server-ca.crt"
client_cert_path = "/etc/curvine/tls/client.crt"
client_key_path = "/etc/curvine/tls/client.key"
# Optional shared override when connecting by IP instead of a DNS SAN.
domain_name = "curvine.example.internal"
```

When `require_client_auth` is false, clients need a trusted server CA but do
not need a client certificate. When it is true, the listener's CA verifies
client certificates, and each connecting component needs a client identity.
For a component that both listens and connects, `client_ca_cert_path` can
specify a separate CA for verifying inbound client certificates; otherwise it
defaults to `ca_cert_path`.
Certificate and key paths must be supplied together. TLS options cannot be
set while `tls_enabled` is false. The configured DNS name (or the destination
host when no override is set) must match the server certificate.
`client.grpc` is shared by the component's outbound master, worker, and
transfer connections, so an override must match every server it contacts.

`enable_reflection` advertises the listener's service names. Descriptor files
can also contain related message and service definitions, so turn reflection on
only where clients need access. Reflection uses the same listener and TLS
policy as the other gRPC services. Files are read when the server or client
endpoint is constructed; restart the component to load rotated certificates.
