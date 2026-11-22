# Safe Worker drain and Kubernetes scale-in runbook

This runbook describes how to remove a Curvine Worker without deleting replicas that
have not been copied to a serving Worker. It applies to the decommission intent,
background drain, progress API, and pause/resume controls introduced by T2, T7, and
T8.

> **Safety rule:** terminate a Worker only after the serving Master reports
> `state: "Drained"` for its hostname. A timeout, an unavailable Worker, a
> `Blocked` status, or an empty/unknown estimate is **not** permission to terminate.

## 1. Supported rollout and prerequisites

Use the same compatible Curvine release for all participating Masters and Workers.
Upgrade all Masters before enabling the drain controls, then upgrade Workers. The
existing Worker replication RPC remains compatible with older Workers, but the
safe-drain workflow depends on the T1 read fallback, T2 durable intent, T3 replica
role handling, T4 capacity admission, T5 fenced release, T6 inventory scan, T7
monitor, and T8 API/CLI behavior being deployed together.

The controls are default-off:

```toml
[master]
decommission_drain_enabled = false
# The following values are examples; tune them for the cluster.
decommission_scan_interval_ms = 10000
decommission_blocks_per_scan = 1000
decommission_max_inflight = 64
decommission_max_inflight_per_worker = 4

[journal]
decommission_intent_enabled = false
```

`master.decommission_drain_enabled` controls background copy and guarded release.
`journal.decommission_intent_enabled` controls committed add/remove/pause/resume
changes in a Raft cluster. Enabling one does not implicitly enable the other.
Keep both disabled during a binary rollout until the prerequisites are installed.

For a replicated Master deployment:

1. Confirm quorum health and record the serving Leader's current `cv node --list`
   and `/get-dcm` output. Stop making decommission changes during the rollout.
2. Upgrade every Master, including peers that may rejoin after a restart, with
   `format_master = false`. Keep `journal.decommission_intent_enabled = false`.
3. Upgrade the Workers. Confirm that every Worker can heartbeat and that the
   serving Master sees the expected hostname and storage inventory.
4. Set `journal.decommission_intent_enabled = true` on every Master and restart
   them in a quorum-preserving order. Do not enable the drain until the cluster
   has a serving Leader and all peers have the setting.
5. Re-add any hostnames recorded before the rollout. Verify them on the serving
   Leader, then enable `master.decommission_drain_enabled` on every Master.
6. Restart or reload according to the deployment's normal configuration process;
   these settings are startup configuration. Verify with `/metrics` and the
   status command before starting a scale-in.

On a standalone Master (`journal.enable = false`), decommission changes are
persisted directly to RocksDB before a successful response. The journal flag is
not required in that mode, but the drain flag and the same status checks still
apply.

### Upgrade and rollback boundary

Before the first new decommission journal entry is committed, the existing journal
entry formats are unchanged. Once a new decommission entry (including pause or
resume) has been written, an older Master must not be started against that journal
or its checkpoints: an older binary cannot replay the new variant and does not
honor the persisted intent.

Setting `journal.decommission_intent_enabled = false` prevents new writes; it does
not remove entries already committed and does not make downgrade safe. If rollback
is necessary after activation, restore a consistent pre-activation backup and
account for all metadata and intent changes made after that backup. Never use
`format_master = true` as a rollback procedure on an existing cluster.

## 2. Start a drain

Use the serving Master. The CLI resolves the active Master and sends the mutation
to its web endpoint. Host arguments are hostnames or IP addresses **without RPC
ports**; the durable intent is keyed by hostname, so use the exact hostname shown
by the Worker registration.

```sh
# Mark the Worker host; this does not by itself mean that termination is safe.
cv node --add-decommission -- worker-a:8997

# Inspect all marked hosts, or one exact hostname.
cv node --decommission-status
cv node --decommission-status=worker-a
```

The legacy add/remove commands remain available. In a Raft deployment, a successful
mutation means the Leader committed the intent. If the request times out or the
connection is lost, its result is unknown: query the current Leader before retrying.
A follower's list or status is not authoritative.

After the host is marked, new foreground placement avoids it. With
`master.decommission_drain_enabled = true`, the Leader's monitor scans the host's
registered Workers in bounded pages, copies complete blocks to eligible Live
Workers, commits the replacement with a session/epoch fence, and only then sends
guarded deletion commands for old replicas. A copy that succeeds without its
fenced metadata commit does not authorize deletion.

## 3. Read status and decide whether termination is safe

The status endpoint is also available directly on the Master admin listener:

```sh
curl --fail 'http://MASTER_WEB_HOST:MASTER_WEB_PORT/api/decommission'
curl --fail --get 'http://MASTER_WEB_HOST:MASTER_WEB_PORT/api/decommission' \
  --data-urlencode 'host=worker-a'
```

The response is an array with one row per marked hostname. Relevant fields include
`state`, `paused`, `remaining_blocks`, `remaining_bytes`, `inflight`, `blockers`,
and `last_scan_ms`.

Only `Drained` is a safe-drain result, and it must be observed on the current
serving Leader immediately before termination. `remaining_blocks` and
`remaining_bytes` are scan estimates, not a durable percentage. `null` means the
estimate is unavailable; it must not be interpreted as zero.

| State | Meaning and operator action |
| --- | --- |
| `Disabled` | The drain feature is off. Do not terminate; enable it on all participating Masters first. |
| `PendingScan` | The current Leader has not completed a valid scan for every registration. Wait and query again. |
| `Draining` | Copies or guarded releases are in progress. Wait for `Drained`. |
| `Paused` | New drain work is intentionally stopped. Resume or keep the Worker running. |
| `Blocked` | Progress cannot currently be proven, for example because of no target capacity, a writing block, admission limits, pending release, stale inventory, or an unavailable Worker. Do not terminate. |
| `Drained` | Every current registration passed the inventory, session, release, and in-flight checks. This is the only state that authorizes planned termination. |

Common blockers:

- **No target capacity:** keep the Worker running, add/repair capacity, or reduce
  the drain rate. Never solve this by deleting the source Worker.
- **Writing:** wait for the file/block writer to close and for the next scan.
- **AdmissionLimit:** wait for active copies to settle; this is bounded backpressure.
- **PendingRelease / NoSafeWitness:** wait for the replacement commit and current
  Live witnesses to be confirmed.
- **InventoryUnconfirmed / StaleLocation:** wait for a fresh heartbeat/full
  inventory report. Do not infer that an empty scan means an empty Worker.
- **WorkerUnavailable:** the Worker is lost, stale, or has no valid current
  session. Its replicas have not been safely drained; investigate and recover it.

A Worker restart changes its session. The old drain attempt is fenced and its old
replica is retained until a later scan and retry establish a valid replacement.
Heartbeat loss is not a successful drain signal.

## 4. Pause, resume, and failure handling

Pause is durable and prevents new drain work; it does not cancel a copy already
sent to a Worker and cannot recall a deletion command already acknowledged by the
source Worker.

```sh
cv node --pause-decommission worker-a
cv node --decommission-status=worker-a
cv node --resume-decommission worker-a
```

The equivalent endpoints are:

```sh
curl --fail --get 'http://MASTER_WEB_HOST:MASTER_WEB_PORT/pause-dcm' \
  --data-urlencode 'workers=worker-a'
curl --fail --get 'http://MASTER_WEB_HOST:MASTER_WEB_PORT/resume-dcm' \
  --data-urlencode 'workers=worker-a'
```

Requests are idempotent. Invalid host syntax returns `400`, an unmarked/unknown
host returns `404`, and a non-serving Master returns `503`. These endpoints use the
existing admin HTTP model and do not add authentication; restrict the admin
listener to trusted operators and networks.

If a drain is blocked or times out:

1. Leave the source Worker running and pause it if an operational change is
   needed.
2. Inspect `blockers`, `inflight`, Worker heartbeat age, capacity, and the Master
   logs/metrics. `decommission_failed_total` counts submission/result errors and
   is process-local, not a count of unique blocks.
3. Resolve the cause (restore a Worker, add target capacity, finish a write, or
   wait for reconciliation), then resume and continue polling.
4. If the Worker is irrecoverably lost, follow the separate lost-Worker recovery
   procedure. Do not claim `Drained`; the drain workflow is intentionally not a
   forced data-recovery or cache-dropping mechanism.
5. Remove the intent only when the operational plan is to recommission the host,
   not as a shortcut for terminating it:

   ```sh
   cv node --remove-decommission -- worker-a:8997
   ```

## 5. Kubernetes scale-in example

The preferred workflow is an external controller or deployment pipeline that starts
the drain before reducing the replica count. A `preStop` hook is a last-chance
safety net, not a controller: Kubernetes starts termination, runs the hook, and
still sends the termination signal when `terminationGracePeriodSeconds` expires.
The Pod may therefore be killed before `Drained` even if the hook is still polling.

The following script is an example for a Worker Pod. It requires `curl` and `jq` in
the image (or replace the JSON extraction with an equivalent tool). It uses the Master web Service DNS name and the Worker hostname from the
container environment. It exits non-zero on timeout or any state other than
`Drained`; the non-zero exit is deliberately not treated as proof that termination
is safe.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: curvine-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: curvine-worker
  template:
    metadata:
      labels:
        app: curvine-worker
    spec:
      terminationGracePeriodSeconds: 1800
      containers:
        - name: curvine-worker
          image: docker.io/curvine:latest
          args: ["worker"]
          env:
            - name: CURVINE_MASTER_WEB
              value: "curvine-master.curvine.svc.cluster.local:9000"
          lifecycle:
            preStop:
              exec:
                command: ["/bin/sh", "-c", "/opt/curvine/bin/worker-decommission-prestop.sh"]
```

The script defaults to the container's `HOSTNAME`, which must match the Worker
registration hostname. Set `CURVINE_DRAIN_HOST` explicitly when the registration
uses a different stable name (for example, an FQDN supplied by the deployment).
Do not substitute the Kubernetes node name unless it is also the registered Worker
hostname.
#!/bin/sh
set -eu

master_web="${CURVINE_MASTER_WEB:?set CURVINE_MASTER_WEB to the Master web host:port}"
host="${CURVINE_DRAIN_HOST:?set CURVINE_DRAIN_HOST to the Worker registration hostname}"
timeout_seconds="${CURVINE_DRAIN_TIMEOUT_SECONDS:-1500}"
poll_seconds="${CURVINE_DRAIN_POLL_SECONDS:-10}"
deadline=$(( $(date +%s) + timeout_seconds ))

url="http://${master_web}"
echo "requesting Curvine decommission for ${host}"
if ! curl --fail --silent --show-error --get "${url}/add-dcm" \
    --data-urlencode "workers=${host}" >/dev/null; then
  echo "could not commit decommission intent; keep the Worker in service" >&2
  exit 1
fi

while :; do
  body=$(curl --fail --silent --show-error --get "${url}/api/decommission" \
    --data-urlencode "host=${host}") || {
      echo "could not read drain status; termination is not confirmed safe" >&2
      exit 1
    }
  state=$(printf '%s' "$body" | jq -r '.[0].state // "Unknown"')
  echo "${host}: state=${state}"
  if [ "$state" = Drained ]; then
    echo "${host}: drain completed; termination may proceed"
    exit 0
  fi
  if [ "$state" = Blocked ] || [ "$state" = Disabled ] || [ "$state" = Paused ]; then
    echo "${host}: state=${state}; waiting without declaring it safe"
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "${host}: drain timeout; do not treat this Pod as safely drained" >&2
    exit 1
  fi
  sleep "$poll_seconds"
done
```

Use a grace period longer than the expected drain time, but do not describe it as
a guarantee. A large grace period reduces the chance of interruption; it cannot
prevent node loss, kubelet failure, eviction, preemption, network failure, or a
slow drain from exceeding the deadline. For a guaranteed workflow, drain and
verify from a controller or administrator **before** changing the Deployment or
StatefulSet replica count, then use the hook only as a fallback.

A `preStop` hook must not call `cv node --remove-decommission` after a timeout.
Removing the intent would make a later replacement Worker eligible for ordinary
service without proving that the old Worker was drained. Keep the intent until the
operator has decided how to recover the blocked/lost Worker.

## 6. Operational checklist

Before termination:

- [ ] All Masters and Workers are on the compatible release.
- [ ] The serving Leader has `decommission_drain_enabled = true`.
- [ ] In a Raft cluster, `decommission_intent_enabled = true` on every Master.
- [ ] The exact Worker hostname is marked and status is queried from the serving Leader.
- [ ] The status is `Drained`, not merely `PendingScan`, `Blocked`, or timed out.
- [ ] `remaining_blocks` and `remaining_bytes` are known and consistent with the
      final scan; `null` is not accepted as zero.
- [ ] No active drain copies, pending releases, unavailable registrations, or
      unresolved blockers remain.

After termination, keep the intent while validating the replacement and cluster
health. Remove it only when the hostname is intentionally recommissioned.

## Related documentation

- [Durable decommission intent](decommission-intent.md)
- [Elastic Cache design, Discussion #2409](https://github.com/CurvineIO/curvine/discussions/2409)
- [T8 progress/pause/resume issue #2438](https://github.com/CurvineIO/curvine/issues/2438)
- [T11 documentation issue #2441](https://github.com/CurvineIO/curvine/issues/2441)
