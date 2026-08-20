---
title: "Curvine full-chain daily test report - YYYY-MM-DD"
linkTitle: "YYYY-MM-DD full-chain"
date: YYYY-MM-DDT00:00:00Z
weight: -YYYYMMDD
tags: [full-chain, daily, no-go]
---

## Quality conclusion

### Executive summary

> [!CAUTION]
> Release decision: **GO / NO-GO**. Pipeline **PASS / FAIL**; ran N profiles, P passed, F failed.

A blocking failure exists. This revision is not releasable. Finish attribution, fix, and targeted regression, then rerun the full-chain tests.

### Quality gates

| Gate | Criterion | Actual | Verdict |
| --- | --- | --- | --- |
| Full-chain result | All required profiles passed | P/N passed | PASS / FAIL |
| Failure attribution | Failures classified | F failures | PASS / pending per-item |
| Resource cleanup | All profile cleanups succeeded | C/N | PASS / FAIL |

### Conclusion

This full-chain run did not pass. Route by failure class into product fix, harness fix, or environment work.

Unattributed failed profile: fuse.

## Test results

### Profile summary

| Profile | Preflight | Result | Duration | Class | Cleanup |
| --- | --- | --- | --- | --- | --- |
| fast | PASS | PASS | 1m 35s | passed | passed |
| fuse | PASS | FAIL | 2m 56s | unknown_failure | passed |

### LTP

- Status: **completed**
- Suites completed: N
- Suites remaining: 0
- Stats: P passed / F real failed / S skipped / E report-consistency errors

| Suite | Status | Passed | Real failed | Skipped | Report errors | Return code |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| smoketest | passed | 12 | 0 | 1 | 0 | 0 |

#### Failed and abnormal cases

No TFAIL/TBROK parsed.

### Performance

> [!NOTE]
> Gate policy: **report only, non-blocking** for the full-chain result. Mark yellow/red vs baseline for human follow-up.

- Status: **failed**
- Gate mode: **report_only**

#### Metadata performance (this run)

| ITEM | VALUE | AVG COST | P50 ms | P95 ms | P99 ms | MAX ms | SAMPLES | ERRORS | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Create file | 21392.01 ops/s | 1.86 ms/op | 2.05 | 4.09 | 4.09 | 162.66 | 200000 | 0 | pass |

#### FIO read/write (this run)

| ITEM | SPEED GiB/s | IOPS | AVG COST | P50 ms | P95 ms | P99 ms | MAX ms | SAMPLES | ERRORS | Status |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Sequential write 64KB | 1.70 | 27840.27 | 9.00 ms/op | 8.98 | 10.55 | 11.47 | 18.43 | 262144 | 0 | pass |

#### Metadata performance baseline

| ITEM | VALUE | AVG COST | P50 ms | P95 ms | P99 ms | MAX ms | SAMPLES | ERRORS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Create file | 21668.74 ops/s | 1.84 ms/op | 2.05 | 4.09 | 4.09 | 188.88 | 200000 | 0 |

#### FIO read/write baseline

| ITEM | SPEED GiB/s | IOPS | AVG COST | P50 ms | P95 ms | P99 ms | MAX ms | SAMPLES | ERRORS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Sequential write 64KB | 1.71 | 28084.85 | 8.89 ms/op | 8.85 | 10.29 | 10.94 | 16.58 | 262144 | 0 |

## Failures and attribution

### Failure analysis

#### fuse

- Goal: FUSE mount, file I/O, and FIO regression
- Expected: Mount is readable/writable; I/O semantics are correct; no EIO
- Actual: exit code **40**; sustained or preallocated writes returned EIO or ENOSPC
- Impact: Blocks the full-chain quality gate
- Class: **unknown_failure**
- Failure layer: **test**
- Root-cause confidence: low
- Cleanup: **passed**
- Next step: fuse-owner completes attribution

### Failed case summary

| Case | Suite / Package | Status | Key error | Root group |
| --- | --- | --- | --- | --- |
| FIO Sequential Write Test (256KB blocks) | fio / fuse | FAIL | FIO Sequential Write test failed | g-fuse-write-eio |

### Failed case reconciliation

| Profile | Reported failures | Source failures | Delta | Notes |
| --- | ---: | ---: | ---: | --- |
| fuse | 6 | 6 | +0 | Counts match |

### Common root-cause groups

Attribution coverage: **6/6 (100.0%)**.

#### P1 g-fuse-write-eio

- Profiles: fuse
- Hypothesis: FUSE or backend storage returned EIO or ENOSPC
- Recommendation: Align the first EIO timestamp and check worker and master ERROR logs
- Unique logical failures: 6
- Model class: **unknown_failure**; confidence: **medium**; Issue: **needs_human**
- Verification: Rerun the fuse profile after the fix
- FIO Sequential Write Test (256KB blocks) (fuse): FIO Sequential Write test failed

### All failed cases

| Case | Suite / Package | Status | Key error | Root group |
| --- | --- | --- | --- | --- |
| FIO Sequential Write Test (256KB blocks) | fio / fuse | FAIL | FIO Sequential Write test failed | g-fuse-write-eio |

## Follow-up

### Defects and fixes

- GitHub Issue: **needs_human**
- GitHub PR: **pending_fix_review**

### Risks

- A subset of green profiles does not override a full-chain NO-GO.
- Unattributed failed profile: fuse.

### Next actions

| Priority | Role | Action | Done when |
| --- | --- | --- | --- |
| P0 | fuse-owner | Finish attribution, file an Issue, fix, and run targeted regression | fuse passes; Issue and PR complete |
| P0 | test-owner | Rerun the full chain and update the report | All required profiles pass |
