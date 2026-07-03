---
name: cv-run-workflow
description: Trigger GitHub Actions workflows for Curvine when workflow YAML or dependent Dockerfiles change, with support for specifying branch, workflow file, and inputs. Use when user asks to run a workflow, dispatch CI, or test workflow/Dockerfile changes.
---

# cv-run-workflow

Manually dispatch `.github/workflows/` jobs via `gh workflow run`, with branch and input selection.

## When to Use

- Changes to `.github/workflows/*.yml`
- Changes to Dockerfiles referenced by workflows (e.g. `curvine-docker/`)
- User asks to run / trigger / dispatch CI or a specific workflow
- User invokes `/cv-run-workflow` or `/run-workflow-actions`

## Step 1: Identify Workflow

List available workflows:

```bash
gh workflow list --repo CurvineIO/curvine
```

Read the YAML to discover `workflow_dispatch` inputs:

```bash
# Example: inspect inputs
head -30 .github/workflows/build-compile-image.yml
```

Common workflows in this repo:

| Workflow file | Purpose |
| ------------- | ------- |
| `build.yml` | Compile and smoke test |
| `build-compile-image.yml` | Compile Docker images |
| `build-runtime-image.yml` | Runtime images |
| `build-tests-image.yml` | Test images |
| `build-csi-image.yml` | CSI images |
| `build-fluid-image.yml` | Fluid images |
| `build-java-sdk.yml` | Java SDK build |
| `daily-ut-coverage.yml` | UT coverage |
| `release.yml` | Release |
| `trigger-helm-release.yml` | Helm release trigger |

## Step 2: Confirm Parameters with User

Before dispatching, confirm:

| Parameter | Description |
| --------- | ----------- |
| `workflow` | YAML filename, e.g. `build-compile-image.yml` |
| `--ref` | Branch or tag to run on |
| `-f key=value` | `workflow_dispatch` inputs from YAML |

Show the exact command for user approval.

## Step 3: Dispatch

### Generic form

```bash
gh workflow run <workflow-file> \
  --repo CurvineIO/curvine \
  --ref <branch-or-tag> \
  -f <input_name>=<value>
```

### Example: compile image (from project convention)

```bash
gh workflow run build-compile-image.yml \
  --repo CurvineIO/curvine \
  --ref build/docker-centos7-compile-image \
  -f push_image=true
```

Note: inspect the workflow file for actual input names (`push_image`, `platforms`, etc.) — they vary per workflow.

### Example: build smoke test on a branch

```bash
gh workflow run build.yml \
  --repo CurvineIO/curvine \
  --ref <your-branch>
```

## Step 4: Monitor Run

```bash
# Latest run for a workflow
gh run list --workflow=<workflow-file> --repo CurvineIO/curvine --limit 5

# Watch specific run
gh run watch <run-id> --repo CurvineIO/curvine

# View logs on failure
gh run view <run-id> --repo CurvineIO/curvine --log-failed
```

## Invocation Alias

User may say:

```text
/run-workflow-actions [compile,deploy] <branch>
```

Map intent to workflow files:

| Intent keyword | Typical workflow |
| -------------- | ---------------- |
| `compile` | `build-compile-image.yml` |
| `test` / `build` | `build.yml` |
| `runtime` | `build-runtime-image.yml` |
| `csi` | `build-csi-image.yml` |
| `deploy` / `helm` | `trigger-helm-release.yml` |
| `release` | `release.yml` |

Always verify inputs in the YAML before dispatching.

## Rules

- Default repo: `CurvineIO/curvine` unless user specifies another
- Never dispatch without user confirmation of branch + inputs
- After workflow YAML edits, run the affected workflow on a **feature branch** before merging
- Report run URL and conclusion to user

## Related

- PR for workflow changes → [cv-create-pr](../cv-create-pr/SKILL.md)

See [references/workflow-catalog.md](references/workflow-catalog.md) for workflow file paths.
