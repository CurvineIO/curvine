# curvine-fault

`curvine-fault` is a feature-gated fault-injection runtime for Rust services.
It combines call-site fault points, structured rules, queryable execution
evidence, in-process control, and an optional authenticated HTTP router.

The crate does not depend on Curvine Client, Master, Worker, or their error
types. Curvine uses it as the common engine for local tests and distributed
cluster fault injection.

## Architecture

```text
fault_point! call sites
        │
        ├── linkme registrations ──> complete linked-point catalog
        │
        └── evaluate context ──────> process or isolated FaultRuntime
                                           │
                         ┌─────────────────┴─────────────────┐
                         │                                   │
               FaultLocalController             FaultHttpControl::mount
                                                             │
                                                   FaultHttpController
```

Each OS process has one lazily initialized process Runtime. It contains every
fault point linked into that binary and one shared rule/evidence table. An
isolated Runtime uses the same complete catalog but owns an independent rule
table for unit tests.

Every linked point is available for configuration. A rule that the workload
does not reach remains at `executions = 0`, so tests must observe evidence
after running the workload.

## Feature flags

| Feature | Effect |
| --- | --- |
| default | `fault_point!` expands to an empty block |
| `fault-injection` | Compiles point registration and Runtime evaluation |
| `http-client` | Enables `FaultHttpController` and `FaultTestSession` |
| `http-server` | Enables the embeddable authenticated Axum router |

The Cargo feature decides whether fault points exist. A host's runtime
`fault_injection.enabled` setting controls only whether its HTTP route is
exposed; it does not initialize or disable the in-process Runtime.

Hosts normally forward their own feature:

```toml
[dependencies]
curvine-fault = { path = "../curvine-fault" }

[features]
fault-injection = ["curvine-fault/fault-injection"]
```

Because Cargo unifies dependency features, a library that controls its own
instrumentation feature should export a local macro alias:

```rust
#[cfg(feature = "fault-injection")]
pub(crate) use curvine_fault::fault_point;

#[cfg(not(feature = "fault-injection"))]
pub(crate) use curvine_fault::__noop_fault_point as fault_point;
```

## Declaring a point

One macro call declares, registers, and evaluates a point:

```rust
fn dispatch(msg: &Message) -> Result<Response, MyError> {
    crate::fault_point! {
        sync,
        name: "worker.rpc.before_dispatch",
        description: "Before a Worker RPC is dispatched",
        context: {
            "req_id" => msg.req_id(),
            "rpc_code" => msg.code() as i32,
        },
        return_error: |fault| Err(MyError::injected(fault.message)),
    }

    // Normal business path.
}
```

The macro derives the catalog contract from the call site:

- `sync` or `async` defines Delay execution;
- context keys become available matcher keys;
- `Record`, `Delay`, and `Crash` are available on every point;
- a `return_error` closure additionally enables `ReturnError`;
- identical declarations of one name merge into one catalog entry;
- conflicting same-name declarations fail Runtime construction and report
  both source locations.

Point names are opaque non-empty strings to the Runtime. Curvine uses readable
hierarchical names such as `worker.rpc.before_dispatch`, but the prefix is not
a Runtime selector.

### ReturnError

The HTTP rule carries only an error message. The call-site closure converts it
to the surrounding function's natural return value, and Rust checks the type:

```rust
return_error: |fault| Err(FsError::common(fault.message)),
```

An async point uses an async closure when required by its return contract.
Resource rollback remains business logic: a point placed after acquiring a
lease, reserving space, or opening a writing block must call the same cleanup
helper as the normal error path.

### Non-returning point

Omit the closure when the point supports only `Record`, `Delay`, and `Crash`:

```rust
crate::fault_point! {
    async,
    name: "worker.heartbeat.before_send",
    description: "Before a Worker heartbeat is sent",
    context: {"worker_id" => worker_id},
}
```

## Runtime

The crate owns one lazily initialized process Runtime. Business handlers do
not initialize it or carry Runtime fields: the default `fault_point!` form
uses it automatically. Local setup code that needs to configure rules can get
the same Runtime directly:

```rust
let runtime = FaultRuntime::process();
```

The first access constructs the complete linked catalog. Conflicting
declarations of one point name are programming errors and fail immediately
with both source locations. Hosts that mount `FaultHttpControl` initialize the
Runtime while building their Web router; processes without a Web control plane
initialize it at the first point evaluation or local configuration.

Tests that need independent rule tables use:

```rust
let runtime = FaultRuntime::isolated()?;
```

An in-process MiniCluster naturally shares the process Runtime and rule table.
Use isolated Runtimes with the explicit `runtime:` macro argument only when a
test requires per-instance isolation.

## Rules

A rule has a required `point` and `action`. `description`, `matcher`, and
`trigger` are optional:

```json
{
  "point": "worker.rpc.before_dispatch",
  "action": {
    "type": "return_error",
    "error": {"message": "injected Worker RPC failure"}
  }
}
```

Defaults are:

| Field | Default |
| --- | --- |
| `description` | `null` |
| `matcher` | `{}` (match every evaluation) |
| `trigger.after` | `0` |
| `trigger.max_hits` | `null` (unlimited) |
| `trigger.probability` | `1.0` |
| `trigger.seed` | `0` |

Matcher fields use exact equality with AND semantics. Context and matcher are
open scalar maps supporting integer, string, and Boolean values.

Rust tests can use `FaultRuleBuilder`:

```rust
let rule = FaultRuleBuilder::named("worker.rpc.before_dispatch")
    .matches("rpc_code", 80_i32)?
    .after(1)
    .times(2)?
    .return_error("injected failure")?;

runtime.configure("case.fail-write", rule)?;
```

`FaultRuleBuilder::named` performs best-effort validation against points linked
into the caller. The target Runtime remains authoritative for remote rules.

## Actions

| Action | Behavior |
| --- | --- |
| `record` | Records evidence and continues evaluating later rules |
| `return_error` | Invokes the call-site closure and returns from the business function |
| `delay` | Sleeps before the business path continues |
| `crash` | Calls `std::process::abort()` |

A sync point implements Delay with `std::thread::sleep` and blocks its caller
thread. An async point uses `tokio::time::sleep` and suspends only its future.
The Runtime does not limit Delay duration or Crash; expose the control plane
only in an isolated test environment.

## Evidence

Every rule reports:

- `evaluations`: calls reaching the point;
- `matches`: contexts satisfying the matcher;
- `executions`: actions selected after trigger evaluation;
- `last_evaluated_context`: latest evaluated context;
- `last_context`: latest context that executed the action.

Interpretation:

```text
evaluations = 0  → workload did not reach this linked point
matches = 0      → point ran, but matcher did not match
executions = 0   → matcher ran, but after/probability/max_hits did not select it
executions > 0   → action was selected
```

Configuration success alone never proves that a fault occurred.

## HTTP control plane

Hosts with an Axum server use `FaultHttpControl::mount`. Curvine Master and
Worker mount it on their existing Web port. Standalone Client tests normally
use `FaultLocalController` and do not require a Web server. The lower-level
`fault_router` API remains available when a host intentionally wants to expose
an isolated Runtime instead of the process Runtime.

`FaultHttpConfig` and `FaultHttpControl` are reusable by any Axum host. The
host embeds the configuration in its own model, constructs the control once,
and passes its router through `mount`:

```rust
let fault_http = FaultHttpControl::from_env(&config.fault_injection)?;

let router = Router::new().route("/health", get(health));
let router = fault_http.mount(router);
```

```toml
[fault_injection]
enabled = true
auth_token_env = "FAULT_HTTP_TOKEN"
```

When disabled, `mount` returns the original router and does not read the
environment. Enabling the configuration without the `http-server` feature,
without an environment-variable name, or without a non-empty token fails
construction instead of silently exposing an unusable control plane.

Every HTTP request requires `Authorization: Bearer <token>`:

| Method | Path | Behavior |
| --- | --- | --- |
| GET | `/api/v1/debug/faults` | Status, complete catalog, rules, evidence |
| PUT | `/api/v1/debug/faults/rules/:rule_id` | Install or replace a rule |
| GET | `/api/v1/debug/faults/rules/:rule_id` | Read one rule |
| DELETE | `/api/v1/debug/faults/rules/:rule_id` | Remove one rule |
| DELETE | `/api/v1/debug/faults/rules` | Clear all rules |

```bash
curl -X PUT http://worker-2:9001/api/v1/debug/faults/rules/case.fail-write \
  -H "Authorization: Bearer ${CURVINE_FAULT_TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{
    "point":"worker.rpc.before_dispatch",
    "matcher":{"rpc_code":80,"request_status":2},
    "trigger":{"max_hits":1},
    "action":{"type":"return_error","error":{"message":"injected failure"}}
  }'
```

An empty configured token rejects every request. The router provides no TLS or
network policy and must be exposed only on loopback or a trusted test network.
The crate provides an embeddable router rather than a standalone listener.

## Test lifecycle

Distributed tests use:

```text
Preflight → Configure → Workload → Observe → Cleanup
```

1. **Preflight** verifies that the controller can reach the target and that its
   rule set is empty.
2. **Configure** installs rules and confirms their initial counters.
3. **Workload** runs the real Client API, RPC, or IO operation.
4. **Observe** asserts the workload outcome and execution evidence.
5. **Cleanup** quiesces the workload, clears all rules, verifies the target is
   clean, and runs a fault-free recovery check.

Rules are in memory and have no TTL or generation. Tests must execute Cleanup
on normal and error-return paths. Cleanup does not cancel actions that already
started.

## Curvine configuration

Build Server instrumentation explicitly:

```bash
cargo build -p curvine-server --features fault-injection
```

Enable the authenticated HTTP route:

```toml
[fault_injection]
enabled = true
auth_token_env = "CURVINE_FAULT_TOKEN"
```

Enabling this configuration in a binary built without the corresponding
feature fails server startup instead of silently omitting the requested HTTP
route.

## Examples and tests

```bash
cargo run -p curvine-fault --example in_process \
  --features fault-injection

cargo run -p curvine-fault --example http_remote \
  --features http-server,http-client

cargo test -p curvine-fault --all-features
```
