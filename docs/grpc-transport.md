# gRPC transport

## Block response frames

Block clients advertise a 256 KiB HTTP/2 receive-frame limit. Peers can send
smaller frames. This limit does not change the read chunk size, message-size
limit or number of outstanding read requests; the default read window remains
one. The setting reduces framing overhead independently of read pipelining.

The transport uses tonic 0.14.9 and prost 0.14.4. Raft keeps its existing
prost 0.11 runtime and storage types, with explicit conversions at the gRPC
boundary. The [matched benchmark reports](../scripts/perf/rpc-tc/CPU-INVESTIGATION.md)
record the upgrade and frame-size changes separately; the ORPC performance
gate remains open.

## Remote read pipelining

The client can overlap requests for sequential chunks on a remote gRPC block
read. Set `read_pipeline_window` in `[client]`:

```toml
[client]
read_pipeline_window = 1  # default; use 0 to opt in to experimental automatic sizing
```

The default `1` requests one chunk at a time. Values from `1` through `32`
select a fixed request window. A value of `0` enables an experimental
automatic policy that starts at one request and can grow to at most four. It
limits speculative data to 4 MiB, while always allowing at least one chunk
even when the configured read chunk size exceeds that budget. The automatic
policy remains opt-in.

After 32 successful sequential responses above 128 MiB/s, auto mode trials
windows of two and then four. Each trial needs 32 new responses and must
improve throughput by at least 10% while keeping request P95 latency within
110% of the previous stable window. A rejected trial, transport failure or
read protocol failure returns to one and disables growth for that pooled
client connection. A nonsequential seek drains pending replies and resets
the window to one. Accepted settings survive completed streams, EOF rewinds
and reuse of that client for the same worker; incomplete trials do not.

## TLS and reflection

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

Server TLS preserves an application's already installed rustls crypto
provider. If none is installed, it selects ring before constructing the
server, including when dependencies enable multiple providers. Reflection
continues to use the v1alpha service.
