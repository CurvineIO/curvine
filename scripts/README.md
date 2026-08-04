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

`report` mode prints known violations as warnings. `ci` mode enforces client-side
gates: `curvine-client-core`, minimal `curvine-cli`, minimal Java/Python SDKs,
the SPDK/RDMA feature-isolation check, and production reverse dependencies on
`curvine-tests`. `final` mode additionally requires that the `orpc` and
`curvine-common` facade packages are gone from the workspace and that the
`curvine-fuse` default tree stays free of those facades and heavy UFS paths.

`check-minimal-artifact-deps.sh` inspects built client artifacts with `readelf`,
`llvm-readelf`, or `otool` and fails if minimal client artifacts dynamically
link RDMA/SPDK, Jindo/HDFS/JVM, or native storage libraries.

```bash
scripts/check-minimal-artifact-deps.sh
scripts/check-minimal-artifact-deps.sh --artifact curvine-cli=target/debug/curvine-cli
scripts/check-minimal-artifact-deps.sh --allow-rdma-spdk --artifact curvine-fuse-rdma=target/release/curvine-fuse
```

Use `--allow-rdma-spdk` only for explicitly RDMA/SPDK-enabled client or FUSE
artifacts. The default mode remains the guard for minimal/non-RDMA client
artifacts.
