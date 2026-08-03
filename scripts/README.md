# Scripts

Test-related scripts (build-server, regression, dailytest, Portal) have been moved to **curvine-tests/regression**. See [curvine-tests/README-regression.md](../curvine-tests/README-regression.md) for how to run the regression test server locally or via Docker.

This directory may still contain other non-test scripts (e.g. perf).

## Dependency boundaries

`check-deps.sh` reports or enforces dependency boundary checks for the issue
#1243 crate/module reorganization plan.

```bash
scripts/check-deps.sh --mode report
scripts/check-deps.sh --mode ci
scripts/check-deps.sh --mode final
```

`report` mode is useful during migration because known violations are printed as
warnings. `ci` mode is the current enforceable gate for already-migrated client
paths: `curvine-client-core`, minimal `curvine-cli`, minimal Java/Python SDKs,
the SPDK/RDMA feature-unification check, and production reverse dependencies on
`curvine-tests`. Remaining `curvine-common` / `orpc` facade users and the
current `curvine-fuse` final-tree debt stay visible as warnings until the P7
cleanup removes them. `final` mode is the P7 gate and fails if internal
dependencies on `curvine-common` / `orpc` or forbidden heavy paths remain.

`check-minimal-artifact-deps.sh` inspects built client artifacts with `readelf`,
`llvm-readelf`, or `otool` and fails if minimal client artifacts dynamically
link RDMA/SPDK, Jindo/HDFS/JVM, or native storage libraries.

```bash
scripts/check-minimal-artifact-deps.sh
scripts/check-minimal-artifact-deps.sh --artifact curvine-cli=target/debug/curvine-cli
```
