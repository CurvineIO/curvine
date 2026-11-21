# GitHub Actions Workflows

This directory holds every GitHub Actions workflow for the repository. The
tables below are generated from each file's `name:` and `on:` fields — those
files remain the source of truth, so follow the links when you need the exact
steps, inputs, or permissions.

Secrets are referenced by name only. Configure them under
**Settings → Secrets and variables → Actions**; never commit their values.

## 1. Workflow index

| Workflow | Name | Trigger | Purpose | Secrets |
| -------- | ---- | ------- | ------- | ------- |
| [`build.yml`](build.yml) | Compile and Smoke Test | push (`main`, `release/**`), pull_request (`**`), dispatch | Compile the Rust workspace, run `cargo fmt`/Clippy (deny), and run the POSIX smoke test. | — |
| [`java-sdk-unit-test.yml`](java-sdk-unit-test.yml) | Java SDK Unit Test | pull_request / push on Java SDK & proto paths, dispatch | Run the Java SDK unit tests when the SDK or protobuf definitions change. | — |
| [`fluid-config-test.yml`](fluid-config-test.yml) | Fluid Config Test | pull_request / push on `curvine-docker/fluid/**`, dispatch | Run the Fluid runtime script tests with the Python of both runtime images. | — |
| [`csi-unit-test.yml`](csi-unit-test.yml) | CSI Unit Test | pull_request / push on `curvine-csi/**`, dispatch | Run `gofmt` and the CSI driver unit tests when the driver changes. | — |
| [`webui-build.yml`](webui-build.yml) | WebUI Build | pull_request / push on `curvine-web/webui/**`, dispatch | Install, lint and build the master web UI when it or its dependencies change. | — |
| [`dco.yml`](dco.yml) | DCO | pull_request (opened, synchronize, reopened, edited) | Verify every commit carries a matching `Signed-off-by:` line. | — |
| [`pr-title-check.yml`](pr-title-check.yml) | Pull Request Title Check | pull_request (opened, synchronize, reopened, edited) | Enforce the Conventional Commits PR title format. | — |
| [`first-pr-comment.yml`](first-pr-comment.yml) | First PR Comment | pull_request_target (opened) | Leave a one-time welcome comment on a contributor's first PR. | — |
| [`issue-self-assign.yml`](issue-self-assign.yml) | Issue Self Assign | issue_comment (created) | Let contributors claim/release an issue with `/assign` and `/unassign`; enforce `good first issue` eligibility. | — |
| [`gfi-pr-guard.yml`](gfi-pr-guard.yml) | GFI PR Guard | pull_request_target (opened, edited, reopened) | Comment when a pull request references a `good first issue` and its author is not a first-time contributor. | — |
| [`gfi-stale.yml`](gfi-stale.yml) | GFI Stale Claims | schedule (daily `17 3 * * *`), dispatch | Release `good first issue` assignments that have no open pull request after 7 days. | — |
| [`daily-ut-coverage.yml`](daily-ut-coverage.yml) | Daily UT and Coverage | schedule (daily `0 0 * * *`), dispatch | Run the full unit-test suite with coverage and publish a summary. | — |
| [`scorecard.yml`](scorecard.yml) | Scorecard supply-chain security | branch_protection_rule, schedule (weekly `30 1 * * 1`), push (`main`), dispatch | Run the OSSF Scorecard supply-chain security checks. | — |
| [`prepare-release.yml`](prepare-release.yml) | Prepare Release | dispatch | Bump the workspace version, open the version-bump PR, and open a release tracking issue. | `RELEASE_BOT_TOKEN` |
| [`release.yml`](release.yml) | Release | push tag `v*`, dispatch | Orchestrate the full release chain (see [§3](#3-release-flow)). | `GITHUB_TOKEN` |
| [`build-runtime-image.yml`](build-runtime-image.yml) | Build Runtime Docker Image | workflow_call, dispatch | Build and push the multi-arch runtime image. | `GHCR_USERNAME`, `PAT_DOCKERIO_TOKEN`, `PAT_GITHUB_TOKEN` |
| [`build-csi-image.yml`](build-csi-image.yml) | Build CSI Docker Image | workflow_call, dispatch | Build and push the multi-arch CSI driver image. | `GHCR_USERNAME`, `PAT_DOCKERIO_TOKEN`, `PAT_GITHUB_TOKEN` |
| [`build-java-sdk.yml`](build-java-sdk.yml) | Build Java SDK | workflow_call, dispatch | Build the native `.so` + JAR and attach them to the GitHub Release. | `GITHUB_TOKEN` |
| [`trigger-helm-release.yml`](trigger-helm-release.yml) | Trigger Helm Release | workflow_call, dispatch | Dispatch a `curvine-release` event to `CurvineIO/curvine-helm`. | `CURVINE_HELM_DISPATCH_TOKEN` |
| [`build-compile-image.yml`](build-compile-image.yml) | Build Compile Docker Image | dispatch | Build the Rust compile-environment images (rocky9, ubuntu, amzn2, …). | `GHCR_USERNAME`, `PAT_DOCKERIO_TOKEN`, `PAT_GITHUB_TOKEN` |
| [`build-tests-image.yml`](build-tests-image.yml) | Build Tests Docker Image | dispatch | Build the image used by the POSIX/FUSE smoke tests. | `GHCR_USERNAME`, `PAT_DOCKERIO_TOKEN`, `PAT_GITHUB_TOKEN` |

## 2. Pull request checks

These run automatically on pull requests:

- **[`build.yml`](build.yml)** — compiles the workspace, runs `cargo fmt` and
  Clippy (level `deny`), runs the unit tests of selected crates and the
  `build/tests` launch and packaging script tests, and runs the POSIX smoke
  test. Also runs on pushes to `main` and `release/**`.
- **[`java-sdk-unit-test.yml`](java-sdk-unit-test.yml)** — runs the Java SDK
  unit tests, but only when files under `curvine-libsdk/java/**`,
  `crates/common/curvine-proto/proto/**`, or the workflow itself change.
- **[`fluid-config-test.yml`](fluid-config-test.yml)** — runs the `unittest`
  tests in `curvine-docker/fluid/tests` with Python 3.9 (Rocky Linux 9) and
  3.12 (Ubuntu 24.04), but only when files under `curvine-docker/fluid/**` or
  the workflow itself change.
- **[`csi-unit-test.yml`](csi-unit-test.yml)** — fails on files `gofmt` would
  change and runs the CSI driver unit tests, but only when files under
  `curvine-csi/**` or the workflow itself change.
- **[`webui-build.yml`](webui-build.yml)** — runs `npm ci`, the Vue CLI lint
  and `npm run build` for the master web UI, but only when files under
  `curvine-web/webui/**` (Dependabot npm updates included) or the workflow
  itself change.
- **[`dco.yml`](dco.yml)** — fails if any commit is missing a `Signed-off-by:`
  line. Sign commits with `git commit -s`.
- **[`pr-title-check.yml`](pr-title-check.yml)** — fails if the PR title does
  not follow Conventional Commits (`feat:`, `fix:`, `docs:`, …).
- **[`first-pr-comment.yml`](first-pr-comment.yml)** — posts a one-time welcome
  comment on a contributor's first PR. It never checks out or runs PR code.
- **[`gfi-pr-guard.yml`](gfi-pr-guard.yml)** — comments on a PR that references
  a `good first issue` when the author is a maintainer or already has a merged
  PR. A maintainer can add the `gfi-override` label to the issue to allow it.
  It only comments; it does not block the PR.

## 3. Release flow

Pushing a `v*` tag makes **[`release.yml`](release.yml)** the single entry
point that drives the whole release chain:

```
prepare (parse tag + verify version)
  ├─ binaries -> create-release (create the GitHub Release, upload binary tar.gz)
  ├─ runtime-image / csi-image (reusable: build and push multi-arch images)
  ├─ java-sdk (reusable: native .so + JAR attached to the Release; no Maven publish)
  ├─ verify (confirm the GHCR images and Release assets are all present)
  └─ helm-dispatch (reusable: dispatch to curvine-helm)
```

The sub-workflows — [`build-runtime-image.yml`](build-runtime-image.yml),
[`build-csi-image.yml`](build-csi-image.yml),
[`build-java-sdk.yml`](build-java-sdk.yml), and
[`trigger-helm-release.yml`](trigger-helm-release.yml) — are reusable
(`workflow_call`). They no longer trigger on tag push; each keeps a
`workflow_dispatch` entry only as a rerun/rescue channel.

`trigger-helm-release.yml` verifies the release tag matches the Cargo workspace
version and that the GHCR images for the tag exist, then sends a
`repository_dispatch` (`curvine-release`) event to `CurvineIO/curvine-helm`,
which tags `main` and packages the chart.

### Preparing a release

Before tagging, run **[`prepare-release.yml`](prepare-release.yml)** (manual
dispatch) to bump the workspace version, open the version-bump PR, and open the
release tracking issue. `release.yml` rejects a `v*` tag whose version does not
match `[workspace.package].version`.

## 4. Scheduled and security workflows

- **[`daily-ut-coverage.yml`](daily-ut-coverage.yml)** — runs the full unit-test
  suite with coverage on a daily schedule (and on demand) in the upstream repo,
  publishing a Markdown summary to the job's Step Summary.
- **[`scorecard.yml`](scorecard.yml)** — runs the OSSF Scorecard supply-chain
  security analysis weekly, on `main` pushes, on branch-protection changes, and
  on demand.
- **[`gfi-stale.yml`](gfi-stale.yml)** — runs daily and releases `good first
  issue` claims that have no open pull request after 7 days.

## 5. Manual dispatch

Run these from **Actions → select the workflow → Run workflow**:

| Workflow | When to run it |
| -------- | -------------- |
| [`prepare-release.yml`](prepare-release.yml) | Start a release: bump the version and open the bump PR / tracking issue. |
| [`release.yml`](release.yml) | Re-run the whole release chain for a tag (fill in the tag, e.g. `v0.3.0`). |
| [`build-compile-image.yml`](build-compile-image.yml) | Rebuild the compile-environment images after changing their Dockerfiles. |
| [`build-tests-image.yml`](build-tests-image.yml) | Rebuild the smoke-test image. |
| [`build-runtime-image.yml`](build-runtime-image.yml), [`build-csi-image.yml`](build-csi-image.yml), [`build-java-sdk.yml`](build-java-sdk.yml), [`trigger-helm-release.yml`](trigger-helm-release.yml) | Re-run a single release segment for a tag when the full chain does not need to repeat. |

## 6. Secrets reference

| Secret | Used by | Purpose |
| ------ | ------- | ------- |
| `RELEASE_BOT_TOKEN` | `prepare-release.yml` | PAT (contents, issues, pull-requests write) so the bump PR triggers PR workflows. |
| `CURVINE_HELM_DISPATCH_TOKEN` | `trigger-helm-release.yml` | PAT with read/write on `curvine-helm` for the cross-repo dispatch. |
| `GHCR_USERNAME`, `PAT_DOCKERIO_TOKEN`, `PAT_GITHUB_TOKEN` | the image builders | Registry login for pushing images to GHCR / Docker Hub. |

The built-in `GITHUB_TOKEN` covers the remaining workflows; the three GFI
workflows (`issue-self-assign.yml`, `gfi-pr-guard.yml`, and `gfi-stale.yml`)
only request `issues: write` and need no extra secret.
