# Transfer kind allowlist

`transfer.enabled_kinds` controls which transfer kinds a TransferServer accepts for new submissions and explicit retries. The default is `["load", "export"]` (both kinds available in this release). Use `enabled_kinds = ["load"]` to disallow new exports or `enabled_kinds = []` to disallow all new submissions and retries. Use lowercase names; unknown kinds are rejected during configuration validation. Existing transfers can still run, be queried, and be canceled. Configure the same list on all TransferServer instances sharing a database.

## Upgrade order for a future kind

Perform the rollout in this order. Keep the default `enabled_kinds = ["load", "export"]` until the final step.

1. Upgrade **every TransferServer sharing the database** to a build containing [T4's](https://github.com/CurvineIO/curvine/issues/2382) strict stored-kind decoding and the planner/worker compatibility changes needed for the future kind. Do not enable the kind while old TransferServers are still serving: an old server may not understand the persisted kind and must not interpret it as Load. A local configuration check cannot verify the versions of other server replicas.
2. Stop submissions that would race the migration, back up the store, and run the backend-specific schema migration. The required floors for non-legacy kinds are SQLite **v3**, MySQL **v5**, and PostgreSQL **v3** (or later as supported by the binary). The unversioned memory store cannot be used for a future kind.
3. Upgrade the Worker fleet. New workers may report additional heartbeat fields and task capabilities; old TransferServers ignore the added protobuf fields. Keep the future kind disabled until all workers that must execute it are upgraded and report its task type.
4. Add the kind to `enabled_kinds` on **all** TransferServer instances and restart/reload them consistently. A schema migration does not add planner support or enable a kind automatically.

If rollback is required, do not point an old TransferServer at a migrated store unless that version explicitly supports the resulting schema version. The compatibility guarantee covered here is that old readers can continue to interpret existing Load/Export columns and protobuf messages; it does not claim that an old binary can open every newer, version-gated database schema.
