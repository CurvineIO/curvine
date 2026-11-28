# Control and data transports

Curvine uses typed gRPC for control traffic and ORPC for bulk data. Transfer
job submission, status and cancellation run over gRPC. The workers executing
those jobs use the ORPC block path for Curvine file data.

| Endpoint | Default | Transport |
| --- | ---: | --- |
| Master metadata, worker reports and embedded transfer coordination | 8995 | gRPC |
| Raft consensus, proposals and snapshot reports | 8996 | gRPC |
| Worker block I/O, including batch and replication data | 8997 | ORPC |
| Raft snapshot downloads | 8999 | ORPC |
| Worker commands and replication coordination | 9005 | gRPC |
| Standalone transfer coordination | 9010 | gRPC |

The worker advertises its ORPC endpoint through the existing `rpc_port` and
its gRPC endpoint through `control_rpc_port`. For Raft, `journal.rpc_port`
serves gRPC and `journal.snapshot_rpc_port` serves ORPC. Peers on different
hosts can use the defaults. If multiple peers share a host, assign distinct
ports and set `snapshot_port` on each entry in `journal.journal_addrs`.

Deploy the master, workers, coordinator and clients in a coordinated upgrade.
Open the control ports to their clients and the ORPC ports to the components
that transfer data. The stock worker control port is 9005 because the MDS
service already uses 8998.

Reads and writes use the upstream ORPC buffering settings. Remove
`read_pipeline_window` and `write_pipeline_window` from configurations
created for the earlier full gRPC prototype. Block stream lifecycle requests
use the same ORPC connection as their data.

## TLS and reflection

With the default embedded transfer service, clients connect to the master
and use its `[master.grpc]` listener. To use a standalone transfer service,
set `transfer.endpoints` and configure that listener under `[transfer.grpc]`.
Worker and Raft listeners use `[worker.grpc]` and `[journal.grpc]`.
Outbound master, worker and transfer control clients use `[client.grpc]`. Raft uses `[journal.grpc]`
in both directions. These settings apply only to gRPC. Keep the ORPC data
and snapshot listeners on trusted networks.

```toml
[worker.grpc]
tls_enabled = true
server_cert_path = "/etc/curvine/tls/worker.crt"
server_key_path = "/etc/curvine/tls/worker.key"
ca_cert_path = "/etc/curvine/tls/client-ca.crt"
require_client_auth = true

[client.grpc]
tls_enabled = true
ca_cert_path = "/etc/curvine/tls/server-ca.crt"
client_cert_path = "/etc/curvine/tls/client.crt"
client_key_path = "/etc/curvine/tls/client.key"
```

Supply certificate and key paths together. The destination DNS name, or the
optional shared `domain_name` override, must match the certificate. Set
`client_ca_cert_path` on a component to use a separate CA for verifying
inbound client certificates. Restart components to load rotated credentials.

Health checks and optional reflection use the gRPC listeners and their TLS
policy. Raft uses tonic 0.14.9 and prost 0.14.4 on the wire, with explicit
conversions to the existing prost 0.11 runtime and persistence types.
