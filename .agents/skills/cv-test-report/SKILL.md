---
name: cv-test-report
description: Publish Curvine full-chain daily test reports to the Hextra Hugo site at CurvineIO/test-reports, using a standard Markdown template with no environment or secret leakage. Use when the user asks to publish a test report, convert harness output to Markdown, or update CurvineIO/test-reports.
---

# cv-test-report

Turn a full-chain / daily harness report into one Markdown post and publish it to [`CurvineIO/test-reports`](https://github.com/CurvineIO/test-reports). Live site: https://curvineio.github.io/test-reports/ (Hugo **Hextra**). Product GitHub: [`CurvineIO/curvine`](https://github.com/CurvineIO/curvine).

One report = one Markdown file. Hextra builds the sidebar and in-page TOC. Posts older than 30 days are pruned on deploy.

**Public posts express test results only, in English.** They must not describe or identify the test environment. Do not use Chinese or other CJK text.

## When to Use

- User asks to publish / upload / post a test report
- User pastes a full-chain daily report and wants it on GH Pages
- User asks to convert harness output into the standard report format
- User mentions `test-reports`, `cv-test-report`, or the report site URL

## Target repo

| Item | Value |
| ---- | ----- |
| Product GitHub | [`CurvineIO/curvine`](https://github.com/CurvineIO/curvine) |
| Pages repo | `CurvineIO/test-reports` |
| Pages URL | `https://curvineio.github.io/test-reports/` |
| Theme | Hextra (`github.com/imfing/hextra`, Hugo module) |
| Post path | `content/reports/YYYY-MM-DD-<slug>.md` |
| Retention | 30 calendar days (filename / front-matter date) |

```bash
gh repo clone CurvineIO/test-reports /path/to/test-reports
```

Do not put the site inside the `curvine` tree. Site chrome GitHub links must point to `https://github.com/CurvineIO/curvine`.

## Forbidden in the published post (hard stop)

If any of the following appear, strip them or **do not publish**. Do not replace with aliases that still identify the environment (`reserved`, `internal-archive`, host roles, etc.).

| Class | Examples (never publish) |
| ----- | ------------------------ |
| Paths | `/data/...`, `/var/lib/...`, `file://`, UNC, home dirs, `~/...` |
| Accounts | Unix users, Git authors as operators, kube contexts, SSO names |
| Network | IPv4/IPv6, hostnames, DNS, CIDR, VIP, `10.x` / `192.168.x` / `172.16.x` |
| Environment | cluster / CSI context names, machine types, client/server roles+addresses, image digests, harness SHAs, run directories, archive roots |
| Secrets | tokens, kubeconfigs, passwords, keys |
| Evidence handles | Run IDs, fingerprints, artifact URLs, log path columns |
| Non-English copy | Chinese headings, table headers, alerts, or body text |

**Allowed:** profile names, PASS/FAIL, counts, durations of a profile, GO/NO-GO, failure **case names**, one-line error text with paths/IPs removed, LTP suite stats, perf **numbers**, public GitHub Issue/PR numbers, Curvine **product** commit only if the user explicitly asks to name the tested revision (otherwise omit).

Title may include the report calendar date. Do not include start/end timestamps, total wall time, or where it ran.

## Step 1: Collect the source (private)

Read the raw harness report locally. Use env fields only to decide the post date and GO/NO-GO. Those fields stay out of the Markdown. Translate source text into English before publishing.

## Step 2: Write the standard post

Copy [assets/daily-full-chain.md](assets/daily-full-chain.md). Hierarchy rules: [references/report-structure.md](references/report-structure.md).

```yaml
---
title: "Curvine full-chain daily test report - YYYY-MM-DD"
linkTitle: "YYYY-MM-DD full-chain"
date: YYYY-MM-DDT00:00:00Z
weight: -YYYYMMDD
tags: [full-chain, daily, no-go]
---
```

- `date` must be in the past (Hugo skips future posts).
- `weight` is `-YYYYMMDD` so the sidebar lists newest first.
- Filename: `content/reports/YYYY-MM-DD-full-chain.md`.
- Keep the H2/H3 order. Omit optional H3s that have no data.
- Never drop H2 **Quality conclusion** or **Test results**.
- **Do not put an H1 in the body.** Hextra already renders `title` as the page H1.
- Lead GO / NO-GO with a GitHub alert: `> [!TIP]` for GO, `> [!CAUTION]` for NO-GO.
- Table cells that are exactly `PASS` / `FAIL` / `pass` / `fail` / `degraded` / `NOT_RECORDED` get colored status dots automatically. Do not wrap them in backticks.

## Markdown must render (hard stop)

Do not publish until a local Hugo build proves the HTML is valid. Goldmark + GFM tables are strict.

| Rule | Why |
| ---- | --- |
| No `#` heading in the body | Duplicates the Hextra H1 |
| No backticks anywhere in the body | Inline code next to CJK or `\|` often stays literal or eats spaces |
| No CJK characters | Site and skills are English-only |
| Tokens use **bold**: **NO-GO**, **failed**, **40** | Renders in lists and tables |
| Tables: spaces around every `\|` | `\| ITEM \|` not `\|ITEM\|` |
| Header cells = separator cells = every row | One missing `\| --- \|` dumps the whole table as a paragraph |
| No `/` or `()` in table headers | Use `SPEED GiB/s`, `P50 ms`, `Sequential write 64KB` |
| Blank line before and after each table | Required by GFM |
| Table before/after is a heading or paragraph, not a list item | Lists swallow tables |

Validate before commit (from `test-reports`):

```bash
python3 - <<'PY'
from pathlib import Path
import re, sys
p = Path("content/reports/YYYY-MM-DD-full-chain.md")
body = p.read_text().split("---", 2)[-1]
if re.search(r"^# ", body, re.M):
    sys.exit("H1 in body")
if "`" in body:
    sys.exit("backticks in body")
if re.search(r"[\u4e00-\u9fff]", body):
    sys.exit("CJK in body")
lines = body.splitlines()
i = 0
while i < len(lines):
    if re.match(r"^\| .+\|$", lines[i]) and i + 1 < len(lines) and re.match(r"^\|[-: |]+\|$", lines[i + 1]):
        cells = lambda s: s.strip().strip("|").split("|")
        h, sep = cells(lines[i]), cells(lines[i + 1])
        if len(h) != len(sep):
            sys.exit(f"header/sep {len(h)} vs {len(sep)}: {lines[i]}")
        j = i + 2
        while j < len(lines) and lines[j].startswith("|"):
            if len(cells(lines[j])) != len(h):
                sys.exit(f"row {len(cells(lines[j]))} vs {len(h)}: {lines[j]}")
            j += 1
        i = j
        continue
    i += 1
print("markdown ok")
PY
hugo --gc --minify
# built HTML must have exactly one H1, real <table>s, hextra-toc, and no leftover "| ITEM |"
```

If Hugo is not on PATH, use the repo `.tools/hugo`. First build needs Go (`hugo mod get`). Fail the publish if the script exits non-zero or the HTML contains `<p>\|`.

## Step 3: Redact, then scan

After filling the template, search the file for leaks:

```bash
rg -n -i \
  -e '/data|/var/lib|file://|home/' \
  -e '[0-9]{1,3}(\.[0-9]{1,3}){3}' \
  -e 'sha256:|ecs\.|kube|CSI context' \
  content/reports/YYYY-MM-DD-full-chain.md
```

Fix every hit that is not an explicit product commit the user asked to publish. Re-run until clean.

Do not add `static/files/` attachments (they usually carry paths or env dumps).

## Step 4: Publish

From `test-reports` (confirm with the user before commit if this session is in `curvine`):

```bash
git checkout main
git pull --ff-only
git add content/reports/YYYY-MM-DD-full-chain.md
git commit -m "$(cat <<'EOF'
docs: add full-chain report YYYY-MM-DD

EOF
)"
git push origin main
```

## Step 5: Verify

```bash
gh run watch --repo CurvineIO/test-reports --exit-status
curl -sS -o /dev/null -w '%{http_code}\n' \
  https://curvineio.github.io/test-reports/reports/YYYY-MM-DD-full-chain/
```

Expect `200`. Posts older than 30 days are removed on the next deploy.

## Do Not

- Publish environment, paths, accounts, IPs, usernames, run IDs, or evidence URLs
- Write a Test scope and environment or Evidence index section
- Put `#` title, backticks, or CJK in the Markdown body
- Publish a table whose column counts do not match
- Attach raw JSON/logs
- Commit reports into `curvine` or `curvineio.github.io`
- Point site GitHub chrome at `CurvineIO/test-reports` (use `CurvineIO/curvine`)
- Write the post for the old Paper theme (no body H1 is still required; Hextra adds `linkTitle`, `weight`, and GO/NO-GO alerts)
- Treat a subset of green profiles as GO when any required profile failed

## Related

- [cv-create-issue](../cv-create-issue/SKILL.md) — product regression after attribution
- [cv-csi-test](../cv-csi-test/SKILL.md) — CSI profile meaning
- [cv-add-skills](../cv-add-skills/SKILL.md) — skill layout

## Checklist

- [ ] Front matter has `linkTitle` and `weight: -YYYYMMDD`
- [ ] GO/NO-GO uses `> [!TIP]` or `> [!CAUTION]`
- [ ] H2/H3 order matches the template; no body H1
- [ ] English only; no CJK; no backticks; tokens are **bold**
- [ ] Every table column count matches; Hugo HTML has `<table>` and `hextra-toc`, not `\| ITEM \|`
- [ ] Results only; no env / path / IP / account / username
- [ ] `rg` leak scan clean
- [ ] No attachments
- [ ] Pushed to `CurvineIO/test-reports` `main`
- [ ] Pages URL returns 200
