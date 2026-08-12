# Curvine Agent Rules

Cross-agent workflow rules. Cursor-specific rules live in `.cursor/rules/` (`.mdc`).

## Workflow skills

| Step | Skill |
| ---- | ----- |
| Add / update cv skill | `cv-add-skills` |
| Decompose plan | `cv-tasks-breakdown` |
| Create issue | `cv-create-issue` |
| Fix issue | `cv-handle-issue` |
| Create / update PR | `cv-create-pr` |
| Review comments | `cv-address-pr-review` |
| Run CI workflow | `cv-run-workflow` |

## Issue → PR flow

See [workflow.md](workflow.md).
