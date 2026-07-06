# Workflow Catalog

Paths relative to repo root: `.github/workflows/`

| File | Trigger | Notes |
| ---- | ------- | ----- |
| `build.yml` | push, PR, `workflow_dispatch` | Main compile + smoke test |
| `build-compile-image.yml` | `workflow_dispatch` | Matrix: rocky9, ubuntu22.04, ubuntu24.04; inputs: `push_image`, `platforms` |
| `build-runtime-image.yml` | `workflow_dispatch` | Runtime Docker images |
| `build-tests-image.yml` | `workflow_dispatch` | curvine-tests image |
| `build-csi-image.yml` | `workflow_dispatch` | CSI driver image |
| `build-fluid-image.yml` | `workflow_dispatch` | Fluid integration image |
| `build-java-sdk.yml` | `workflow_dispatch` | Java SDK |
| `daily-ut-coverage.yml` | schedule / manual | UT coverage |
| `release.yml` | `workflow_dispatch` | Release pipeline |
| `trigger-helm-release.yml` | `workflow_dispatch` | Helm chart release |
| `pr-title-check.yml` | PR events | Validates PR title format (not manually dispatched) |

Re-read the YAML before dispatch — inputs and matrix change over time.
