# Workflow Catalog

Paths relative to repo root: `.github/workflows/`

Dispatchable (`gh workflow run`):

| File | Trigger | Notes |
| ---- | ------- | ----- |
| `build.yml` | push to `main`, PR, `workflow_dispatch` | Main compile + smoke test; no inputs |
| `build-compile-image.yml` | `workflow_dispatch` | Matrix: rocky9, ubuntu22.04, ubuntu24.04, amzn2 (amd64 + arm64), centos7 (amd64); inputs: `push_image`, `build_types` (comma list or `all`), `platforms` |
| `build-runtime-image.yml` | `workflow_dispatch`, called by `release.yml` | Runtime Docker images; inputs: `tag`, `push_image` |
| `build-tests-image.yml` | `workflow_dispatch` | curvine-tests image; input: `push_image` |
| `build-csi-image.yml` | `workflow_dispatch`, called by `release.yml` | CSI driver image; inputs: `tag`, `push_image` |
| `build-java-sdk.yml` | `workflow_dispatch`, called by `release.yml` | Java SDK; inputs: `tag`, `publish` |
| `csi-unit-test.yml` | PR / push on `curvine-csi/**`, `workflow_dispatch` | Go tests and gofmt for curvine-csi; no inputs |
| `java-sdk-unit-test.yml` | PR / push on `curvine-libsdk/java/**` and the proto files, `workflow_dispatch` | Java SDK unit tests; no inputs |
| `webui-build.yml` | PR / push on `curvine-web/webui/**`, `workflow_dispatch` | Web UI lint + build; no inputs |
| `fluid-config-test.yml` | PR / push on `curvine-docker/fluid/**`, `workflow_dispatch` | Unit tests for the Fluid runtime scripts; no inputs |
| `daily-ut-coverage.yml` | schedule (daily), `workflow_dispatch` | UT coverage; no inputs |
| `prepare-release.yml` | `workflow_dispatch` | Version bump PR and release tracking issue; run before the `v*` tag; inputs: `version`, `base_branch`, `create_issue` |
| `release.yml` | push of a `v*` tag, `workflow_dispatch` | Release pipeline (binaries, images, Java SDK, Helm dispatch); input: `tag` |
| `trigger-helm-release.yml` | `workflow_dispatch`, called by `release.yml` | Helm chart release; inputs: `tag`, `commit_sha` |
| `gfi-stale.yml` | schedule (daily), `workflow_dispatch` | Releases good-first-issue claims with no open PR after 7 days; no inputs |
| `scorecard.yml` | schedule (weekly), push to `main`, branch protection changes, `workflow_dispatch` | OpenSSF Scorecard; no inputs |

Event-driven only (not manually dispatched):

| File | Trigger | Notes |
| ---- | ------- | ----- |
| `pr-title-check.yml` | PR events | Validates PR title format |
| `dco.yml` | PR events | Checks `Signed-off-by` on every commit |
| `first-pr-comment.yml` | PR opened (`pull_request_target`) | One-time comment on a contributor's first PR |
| `gfi-pr-guard.yml` | PR opened/edited (`pull_request_target`) | Good-first-issue eligibility check on PRs (comments only) |
| `issue-self-assign.yml` | issue comment | `/assign` and `/unassign` comments |

Re-read the YAML before dispatch — inputs and matrix change over time.
