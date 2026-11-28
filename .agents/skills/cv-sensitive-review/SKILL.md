---
name: cv-sensitive-review
description: Mandatory pre-submit leak gate for issues, PRs, review comments and commits; classifies credentials, internal endpoints, PII and business-confidential material with provider patterns, history search and entropy heuristics, then returns PASS/BLOCK with a redaction plan. Use when about to create or update an issue or PR, post a review comment, commit or push, or when asked to review text or code for sensitive-data leakage (desensitization / redaction review).
---

# cv-sensitive-review

Single gate that every GitHub-facing payload must pass before it is published:
issue body, PR title/body, review comment, reply, commit message, and the code itself.

## When to Use

- Before `gh issue create`, `gh pr create`, `gh pr edit`, `gh pr review`, `gh pr comment`
- Before `git commit`, `git commit --amend`, `git push` (especially `git push -f`)
- When asked "is this leaking anything", "脱敏审查", "check for secrets", "is this safe to post"
- When writing a report, blog post, test report or issue that quotes logs, configs or cluster output
- Mandatory sub-step of [cv-create-issue](../cv-create-issue/SKILL.md),
  [cv-create-pr](../cv-create-pr/SKILL.md),
  [cv-submit-pr-review](../cv-submit-pr-review/SKILL.md),
  [cv-address-pr-review](../cv-address-pr-review/SKILL.md)

## Non-negotiable rules

| Rule | Detail |
| ---- | ------ |
| Payload first, publish second | Write the draft to a temp file (`/tmp/pr_body.md`), scan it, then pass it via `--body-file`. Never inline `--body "$(cat <<EOF ... )"` before scanning |
| Never echo the value | Report `rule`, `path:line`, commit SHA. Never the matched text — not masked, not truncated, not "just the prefix" |
| A live credential is a BLOCK | Not a warning, not a follow-up TODO |
| Deletion is not remediation | A secret in a pushed commit is compromised forever: containment = rotate, then remove, then decide on history rewrite |
| No public restatement | Do not open a public issue or comment quoting the leaked value; reference `path:line` + SHA only |
| Fail closed | Scanner crash, missing `python3`, or an unreadable payload → treat as BLOCK until re-run manually |

## Step 1: Build the payload and scanner targets

| Payload | Target |
| ------- | ------ |
| Issue / PR / comment draft | the temp file holding the body |
| Commit | `--staged` (or the file list from `git diff --cached --name-only`) |
| Branch / PR diff | `--range origin/main...HEAD` |
| Repo audit (advisory) | `--repo` |
| Already-known literal to trace | `--history '<literal>'` |

## Step 2: Run the scanner

```bash
SCAN=".agents/skills/cv-sensitive-review/scripts/scan.sh"

bash "$SCAN" /tmp/pr_body.md                    # payload
bash "$SCAN" --staged                          # commit gate
bash "$SCAN" --range origin/main...HEAD        # PR diff gate
bash "$SCAN" --repo                            # whole-repo audit (advisory)
bash "$SCAN" --history '<literal from finding>' # was it ever pushed?
```

Exit codes: `0` PASS, `1` BLOCK (S0/S1 found), `3` REVIEW (S2 only), `2` usage error.

`--repo` audits the whole tree and therefore also reports pre-existing debt; it is
informational for a PR (the blocking scope is the payload plus the diff), but it is the
input for cleanup tracking.

Export org-internal markers once per shell — the gate is only as good as these:

```bash
export CV_SENSITIVE_DOMAIN_SUFFIXES="internal,corp,local,lan"   # + org UAT/prod/CDN suffixes
export CV_INTERNAL_EMAIL_DOMAINS="oppo.com"
```

## Step 3: Manual pass the scanner cannot do

Regex misses the highest-impact leaks. Read the payload and answer each item:

- **Internal topology described in prose** — cluster names, master/worker/pod names, namespaces, node counts, capacity, ACL model, VPC/VSwitch, region+AZ pairs.
- **Identifiers** — internal ticket IDs, employee names/handles/emails, org charts, on-call rotations, internal repo/wiki/dashboard links.
- **Customer and business data** — customer names, bucket/dir names owned by a customer, order/tenant IDs, revenue, pricing, capacity commitments, SLA numbers, perf numbers not yet published.
- **Unreleased material** — roadmap, unmerged design, security findings before disclosure, benchmark results of unannounced hardware.
- **Artifacts quoted wholesale** — `kubectl get/describe -o yaml`, `.kube/config`, `env`/`printenv`, curl with `-H "Authorization: ..."`, browser URLs with query tokens, screenshots (EXIF, tabs, account avatars, other windows).
- **Placeholders that look real** — an example that mirrors a real endpoint (`http://<real-host>` minus a character) is still a leak; use `example.com`, `127.0.0.1`, `<access-key>`.

## Step 4: Verdict and action

| Severity | Class | Action |
| -------- | ----- | ------ |
| S0 | Live credential, private key, token, prod data sample, customer PII | **BLOCK**. Rotate first, then remove, then gate re-run |
| S1 | Internal endpoint/host/IP, DB URI with password, presigned URL, code/config that embeds any of the above | **BLOCK**. Redact, or justify as public-safe with explicit user approval |
| S2 | Internal ticket ID, corporate email, internal-only reference, PII-lite, high-entropy hint | **REVIEW**. Redact by default; never ship without an explicit decision |
| S3 | Entropy/lint false positive | Note it, continue |

Record the verdict in the PR body (see template below). `cv-create-pr` requires it.

## Step 5: Redaction patterns

| Leak | Replace with |
| ---- | ------------ |
| Endpoint / URL | `<internal-object-storage-endpoint>`, `<internal-cdn-host>` |
| Access key / secret | `<access-key>`, `<secret-key>`; in tests read env (`MOUNT_TEST_S3_ACCESS_KEY`) |
| Token / password | `<token>`, `<password>` |
| IP | `<node-ip>` or a documentation range (`192.0.2.0/24`) |
| Internal ticket | delete, or `internal ticket` |
| Person | role (`a maintainer`, `the reporter`), not the handle |
| Log excerpt | keep levels/errors, drop headers, hostnames, paths with account IDs |

When a finding must be identified precisely but must not be repeated, cite
`sha256:<first 12 hex of the value>` — stable, non-reversible, enough to correlate.

## Step 6: When the leak is already public

1. **Contain** — the value is burned. Rotation/revocation owner must be named before the PR merges.
2. **Remove** — delete from the tree (this is the code half of the fix).
3. **Trace history** — `--history` / `git log -S`; deletion does not remove it from clones, forks or mirrors.
4. **Decide history rewrite** — `git filter-repo` is a maintainer decision with fork-notification cost; never the default.
5. **Report** — reference `path:line` + SHA only, or use the `SECURITY.md` channel.

Curvine precedent (do not repeat the values): the mount/load test credential pair,
committed in `7ff7b914c` and still present on `main` until #2478.

## Step 7: Record the verdict

Append to the issue/PR body:

```markdown
## Sensitive data review

- Scope: <payload / diff range / files>
- Scanner: `bash .agents/skills/cv-sensitive-review/scripts/scan.sh ...` -> PASS
- Manual pass: endpoints, topology, identifiers, customer data — none found
- Verdict: PASS (no S0/S1/S2 findings)
```

Any non-PASS verdict blocks publication; if the user overrides, state the accepted
finding and the rotation owner in the body.

## Checklist

- [ ] Draft written to a temp file, scanned before publishing
- [ ] `--staged` or `--range` gate run for the code half
- [ ] Payload scanned for the text half (title, body, comments)
- [ ] History searched for anything removed in this change
- [ ] Org internal suffixes/domains exported for the run
- [ ] Verdict + scope recorded in the PR body
- [ ] Rotation owner named for any S0/S1 finding

## Related

- Pattern catalogue → [references/sensitive-patterns.md](references/sensitive-patterns.md)
- Workflow rules → `.agents/rules/workflow.md`
- [cv-create-issue](../cv-create-issue/SKILL.md) · [cv-create-pr](../cv-create-pr/SKILL.md) ·
  [cv-submit-pr-review](../cv-submit-pr-review/SKILL.md) · [cv-address-pr-review](../cv-address-pr-review/SKILL.md)
