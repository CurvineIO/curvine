# Scripts

Test-related scripts (build-server, regression, dailytest, Portal) have been moved to **curvine-tests/regression**. See [curvine-tests/README-regression.md](../curvine-tests/README-regression.md) for how to run the regression test server locally or via Docker.

This directory may still contain other non-test scripts (e.g. perf).

## Dependency boundaries

`check-deps.sh` reports or enforces dependency boundary checks for the issue
#1243 crate/module reorganization plan.

```bash
scripts/check-deps.sh --mode report
scripts/check-deps.sh --mode final
```

`report` mode is useful during migration because known violations are printed as
warnings. `final` mode is the P7 gate and fails if internal dependencies on
`curvine-common` / `orpc` or forbidden heavy paths remain.
