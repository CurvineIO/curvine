# Runtime discovery with etcd

Runtime discovery is optional and disabled by default. Build each participating
binary with `--features discovery-etcd`, for example:

```sh
cargo build --release -p curvine-server --features discovery-etcd
cargo build --release -p curvine-cli --features discovery-etcd
```

Use the same cluster ID, prefix, and etcd endpoints on masters, workers, and
clients. Configure reachable RPC hostnames rather than wildcard bind addresses.

```toml
cluster_id = "example-cluster"

[discovery]
enabled = true
provider = "etcd"
endpoints = ["http://127.0.0.1:2379"]
prefix = "/curvine"
connect_timeout_ms = 3000
request_timeout_ms = 3000
watch_reconnect_min_ms = 1000
watch_reconnect_max_ms = 30000
watch_reconnect_jitter_ratio = 0.2
allow_stale_cache = true
```

Keep the existing journal membership configuration: discovery does not configure
Raft voters or elect a leader. Master registrations use the stable Raft node ID
and the master RPC address. Worker registrations include their process session
identity and heartbeat RPC address. Worker registrations provide visibility;
heartbeats, block reports, and the master placement state still determine data
routing. A `Serving` registration denotes lifecycle readiness, not leadership or
permission to serve follower reads.

Clients load an initial master snapshot before sending metadata requests, then
maintain local candidates through etcd watches. RPCs select from this local cache;
they do not read etcd for every operation. Live endpoint additions, changes, and
removals reconcile the connector membership. Enabled discovery uses the dynamic
snapshot exclusively, without silently combining static seeds. TOML edits need a
process restart; watching etcd does not add a configuration file watcher.

Initial provider connection and service registration have bounded startup
budgets. A client with no usable initial snapshot returns a discovery error.
After watch failure, `allow_stale_cache = true` permits the last dynamic snapshot
while reconnecting; set it to `false` to refuse requests until the resolver has a
healthy view. Cached endpoints may still be unavailable, so ordinary RPC failures
and leader retries remain possible. Disabling discovery restores configured
master addresses.

Master and worker registrations use keepalive leases. A terminal lease failure
triggers registration recovery with bounded backoff. Graceful shutdown marks the
endpoint draining and revokes its lease; abrupt process loss relies on etcd lease
expiry (the current default lease TTL is 10 seconds). Removing an endpoint from
etcd does not remove its Raft vote or replace worker lifecycle protocols.

To inspect registrations with the example configuration:

```sh
etcdctl --endpoints=http://127.0.0.1:2379 get \
  /curvine/example-cluster/services/ --prefix
etcdctl --endpoints=http://127.0.0.1:2379 watch \
  /curvine/example-cluster/services/ --prefix
```

Run the integration tests against a disposable etcd instance:

```sh
export CURVINE_ETCD_ADDR=http://127.0.0.1:2379
cargo test -p curvine-service-discovery --features etcd \
  --test etcd_provider_test -- --ignored --test-threads=1
cargo test -p curvine-server --features discovery-etcd \
  --test discovery_runtime_test -- --ignored --test-threads=1
```

The process test starts an actual master and worker, verifies the published RPC
addresses, connects a client with an unusable static seed, and checks prompt
registration revocation after graceful shutdown. Provider tests cover watch
changes, compaction recovery, cache policy, and registration ownership/lifecycle.
