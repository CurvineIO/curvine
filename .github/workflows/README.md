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
| [`dco.yml`](dco.yml) | DCO | pull_request (opened, synchronize, reopened, edited) | Verify every commit carries a matching `Signed-off-by:` line. | — |
| [`pr-title-check.yml`](pr-title-check.yml) | Pull Request Title Check | pull_request (opened, synchronize, reopened, edited) | Enforce the Conventional Commits PR title format. | — |
| [`first-pr-comment.yml`](first-pr-comment.yml) | First PR Comment | pull_request_target (opened) | Leave a one-time welcome comment on a contributor's first PR. | — |
| [`issue-self-assign.yml`](issue-self-assign.yml) | Issue Self Assign | issue_comment (created) | Let contributors claim/release an issue with `/assign` and `/unassign`. | — |
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

These run automatically on pull requests and gate merges:

- **[`build.yml`](build.yml)** — compiles the workspace, runs `cargo fmt` and
  Clippy (level `deny`), and runs the POSIX smoke test. Also runs on pushes to
  `main` and `release/**`.
- **[`java-sdk-unit-test.yml`](java-sdk-unit-test.yml)** — runs the Java SDK
  unit tests, but only when files under `curvine-libsdk/java/**`,
  `crates/common/curvine-proto/proto/**`, or the workflow itself change.
- **[`dco.yml`](dco.yml)** — fails if any commit is missing a `Signed-off-by:`
  line. Sign commits with `git commit -s`.
- **[`pr-title-check.yml`](pr-title-check.yml)** — fails if the PR title does
  not follow Conventional Commits (`feat:`, `fix:`, `docs:`, …).
- **[`first-pr-comment.yml`](first-pr-comment.yml)** — posts a one-time welcome
  comment on a contributor's first PR. It never checks out or runs PR code.

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

The built-in `GITHUB_TOKEN` covers the remaining workflows; `issue-self-assign.yml`
only requests `issues: write` and needs no extra secret.
