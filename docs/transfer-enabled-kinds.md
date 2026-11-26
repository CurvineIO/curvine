# Transfer kind allowlist

`transfer.enabled_kinds` controls which transfer kinds a TransferServer accepts for new submissions and explicit retries. The default is `["load", "export"]` (both kinds available in this release). Use `enabled_kinds = ["load"]` to disallow new exports or `enabled_kinds = []` to disallow all new submissions and retries. Use lowercase names; unknown kinds are rejected during configuration validation. Existing transfers can still run, be queried, and be canceled. Configure the same list on all TransferServer instances sharing a database.

## Upgrade order for a future kind

Perform the rollout in this order. Keep the default `enabled_kinds = ["load", "export"]` until the final step.

1. Pause submissions and quiesce all writers to the transfer store, including schedulers and task reporters. Stop incompatible TransferServer replicas and take a consistent pre-upgrade backup before starting any upgraded replica. An upgraded TransferServer can migrate the store as soon as it opens it during startup.
2. Start the upgraded build on **every TransferServer sharing the database**. It must include [T4's](https://github.com/CurvineIO/curvine/issues/2382) strict stored-kind decoding and the planner/worker support for the future kind. Migration runs when the upgraded server opens the store. Verify the resulting schema is at least SQLite **v3**, MySQL **v5**, or PostgreSQL **v3**, as supported by the binary. The unversioned memory store does not support future kinds. Verify all replicas are upgraded before enabling the kind; a local configuration check cannot verify other replicas' versions.
3. Upgrade the Worker fleet. Transfer-capable workers that advertise the base capabilities but omit task types are eligible for Load and Export. Workers that predate the base transfer capabilities are not eligible for transfer tasks; upgrade them before relying on them for dispatch. Keep the future kind disabled until the workers needed to execute it advertise its task type.
4. Add the kind to `enabled_kinds` on **all** TransferServer instances and restart/reload them consistently. A schema migration does not add planner support or enable a kind automatically.

Compatibility tests cover capability decoding and routing, protobuf field tolerance using legacy schema projections, and a legacy SQL projection over v3 Load/Export rows. They do not run mixed-version binaries or establish how older handlers populate the capability table, accept task reports, or retain results. Upgrade the relevant servers before relying on new task kinds or result fields.

If rollback is required, do not point an old TransferServer at a migrated store unless that version explicitly supports the resulting schema version. A successful legacy SQL projection does not show that an older binary can open a newer, version-gated database schema.
