# Transfer kind allowlist

`transfer.enabled_kinds` controls which transfer kinds a TransferServer accepts for new submissions and explicit retries. The default is `["load", "export"]` (both kinds available in this release). Use `enabled_kinds = ["load"]` to disallow new exports or `enabled_kinds = []` to disallow all new submissions and retries. Use lowercase names; unknown kinds are rejected during configuration validation. Existing transfers can still run, be queried, and be canceled. Configure the same list on all TransferServer instances sharing a database.

## Before enabling a future kind

1. Upgrade **every TransferServer sharing the database** to a build with strict stored-kind decoding ([T4](https://github.com/CurvineIO/curvine/issues/2382)) and support for that kind. Do not enable a new kind during a mixed-version rollout; an old server might misread it as Load. A local configuration check cannot verify the versions running on other instances.
2. Ensure the store has been upgraded to the schema with spec and result fields: SQLite **v3**, MySQL **v5**, or PostgreSQL **v3**, or later as supported by the binary. These are backend-specific version numbers. A future, non-legacy kind cannot be enabled with an older schema or with the unversioned memory store. Take a backup before migration if you need the option to roll back to an older binary.
3. Add the kind to `enabled_kinds` on all TransferServer instances. A schema upgrade does not add support for a new kind or enable it automatically.
