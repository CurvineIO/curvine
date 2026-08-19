# Full-chain daily report structure

Public Markdown uses **four H2 groups**. Related content is H3 (and H4 under perf). Do not add a fifth H2 for environment or evidence.

Body language is **English only**. Headings below are canonical.

The post contains **results only**. No test paths, host accounts, IPs, usernames, cluster names, machine types, image digests, harness revisions, run IDs, or archive locations.

## Front matter

| Field | Required | Notes |
| ----- | -------- | ----- |
| `title` | yes | Calendar date only, no clock and no host |
| `linkTitle` | yes | Short sidebar label, e.g. `YYYY-MM-DD full-chain` |
| `date` | yes | `YYYY-MM-DDT00:00:00Z` (must not be in the future) |
| `weight` | yes | `-YYYYMMDD` so Hextra lists newest first |
| `tags` | yes | `full-chain` or profile name, plus `go` or `no-go` |

## Heading tree

The page H1 comes from front matter `title` only. Body starts at H2. Hextra draws the right-hand TOC from H2–H4.

```text
## Quality conclusion
  ### Executive summary
  ### Quality gates
  ### Conclusion
## Test results
  ### Profile summary
  ### LTP          (omit if ltp not run)
  ### Performance  (omit if perf not run)
    #### Metadata performance (this run)
    #### FIO read/write (this run)
    #### Metadata performance baseline
    #### FIO read/write baseline
## Failures and attribution  (omit entire H2 if all required profiles passed)
  ### Failure analysis
    #### {profile}   (one per failed required profile)
  ### Failed case summary
  ### Failed case reconciliation
  ### Common root-cause groups
  ### All failed cases
## Follow-up
  ### Defects and fixes
  ### Risks
  ### Next actions
```

| H2 | Required | Omit when |
| -- | -------- | --------- |
| Quality conclusion | yes | never |
| Test results | yes | never |
| Failures and attribution | if any required profile failed | all required profiles passed |
| Follow-up | yes | never |

Do **not** create: Test scope and environment, Evidence index, Execution nodes, Cluster info.

Do **not** use backticks. Do **not** write a body `#` heading. Do **not** use CJK. Tables must be GFM with matching column counts (see SKILL.md “Markdown must render”).

## Quality conclusion

### Executive summary

Lead with a GitHub alert, then counts.

```markdown
> [!CAUTION]
> Release decision: **NO-GO**. Pipeline **FAIL**; ran N profiles, P passed, F failed.
```

Use `> [!TIP]` when the decision is **GO**.

- **GO**: every required profile passed (perf `report_only` yellow/red does not block).
- **NO-GO**: any required profile failed, or cleanup failed when cleanup is a gate.

No wall-clock window, no commit list, no where it ran.

### Quality gates

| Gate | Criterion | Actual | Verdict |
| ---- | --------- | ------ | ------- |
| Full-chain result | All required profiles passed | `{passed}/{total}` passed | PASS / FAIL |
| Failure attribution | Failures classified | `{n}` failures | PASS / pending per-item |
| Resource cleanup | All profile cleanups succeeded | `{passed}/{total}` | PASS / FAIL |

### Conclusion

One short paragraph. List unattributed failed profiles by **profile name** only.

## Test results

### Profile summary

`Profile | Preflight | Result | Duration | Class | Cleanup`

No Run ID column. Result is PASS/FAIL. Class is `passed` / `unknown_failure` / `product_regression` / …

### LTP

Suite status plus `passed / real failed / skipped / report-consistency errors`, then per-suite counts. No summary-file path.

### Performance

Gate mode (`report_only` vs blocking), then number tables only. No client, server, instance type, pipeline SHA, or file names.

## Failures and attribution

### Failure analysis / #### {profile}

Keep: Goal, Expected, Actual (exit code + symptom), Impact, Class, Failure layer, Root-cause confidence, Cleanup, Next step (role, not a person).

Drop: Evidence, Fingerprint, minimal repro that names commits/images/hosts, log excerpts that contain paths or IPs.

### Failed case summary / All failed cases

`Case | Suite/Package | Status | Key error | Root group`

No log / Fingerprint columns. Error text must not contain paths or addresses.

### Failed case reconciliation

`Profile | Reported failures | Source failures | Delta | Notes`

### Common root-cause groups

Coverage `{attributed}/{total}`. Each group: profiles, hypothesis (no host/disk paths), recommendation, unique count, class, Issue (`needs_human` / `#n`), verification plan, member **case names**.

## Follow-up

### Defects and fixes

Public GitHub Issue/PR numbers only, on [`CurvineIO/curvine`](https://github.com/CurvineIO/curvine). No internal ticket hosts, no operator names.

### Risks

NO-GO cannot be waived by a green subset. Name unattributed **profiles**. Do not say evidence is node-local or how to fetch logs.

### Next actions

`Priority | Role | Action | Done when` — Role is a function (`fuse-owner`, `test-owner`), never a username.
