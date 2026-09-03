---
name: cv-codegraph
description: Use CodeGraph for Curvine code exploration when the MCP tool or local index is already available; prefer it over repo-wide grep when locating or understanding code. Human one-time setup covers CLI install, MCP wiring, and indexing. Use when exploring the codebase, tracing call paths, or finding symbols.
---

# cv-codegraph

[CodeGraph](https://github.com/colbymchenry/codegraph) indexes this repository into a local knowledge graph (`.codegraph/` at repo root). It returns symbol source, call paths (including dynamic-dispatch hops grep cannot follow), and blast-radius context in one call.

## When to Use

- Exploring or locating code in Curvine
- Tracing how symbols connect across files or crates
- Answering "where is X" or "how does X work" before broad repo scans
- Any task where you would reach for `grep`, `find`, or reading many files

**Priority rule:** When `.codegraph/` exists or a `codegraph_explore` MCP tool is available, use CodeGraph **first**. Fall back to grep/read when CodeGraph is unavailable, not indexed, does not cover the target (configs, docs), or may be stale (see Limitations).

## Human One-Time Setup

Developers run this once per machine. **Agents must not run these steps mid-task** — if the index or MCP tool is missing, fall back to grep/read and note that setup is needed.

### 1. Install the CLI

Download the installer, review it if desired, then run it:

```bash
# macOS / Linux
curl -fsSL https://raw.githubusercontent.com/colbymchenry/codegraph/main/install.sh \
  -o /tmp/codegraph-install.sh
# optional: inspect /tmp/codegraph-install.sh
sh /tmp/codegraph-install.sh
```

```powershell
# Windows (PowerShell)
Invoke-WebRequest https://raw.githubusercontent.com/colbymchenry/codegraph/main/install.ps1 `
  -OutFile codegraph-install.ps1
# optional: inspect .\codegraph-install.ps1
.\codegraph-install.ps1
```

### 2. Wire the MCP server

Installing the CLI alone does not connect your agent. Run:

```bash
codegraph install
```

Review any changes it proposes to agent/MCP config (this skill remains the canonical usage guide).

### 3. Initialize indexing

At the repository root (once per clone/machine):

```bash
codegraph init
```

The `.codegraph/` directory is gitignored and stays local to each developer machine.

Verify:

```bash
codegraph --version
ls .codegraph/
```

## Agent Usage

Agents use CodeGraph **only when it is already available**:

- **MCP:** look for a `codegraph_explore` tool among available MCP tools (namespace names vary by environment — match by tool name, not a hard-coded namespace)
- **CLI:** run `codegraph explore` when `codegraph` is on `PATH` and `.codegraph/` exists

Do **not** download installers, run `codegraph install`, or run `codegraph init` during a task. If neither MCP nor `.codegraph/` is present, use grep/read as usual.

### MCP tool (preferred in Cursor)

When `codegraph_explore` is available:

- One query usually answers symbol location, verbatim source, and call paths
- Name a file or symbol to load line-numbered source safe for edits
- If a symbol is listed but deferred, load it by name in a follow-up query
- If the response flags files as stale since last sync, **Read those files directly** before editing

### Shell CLI

```bash
codegraph explore "<symbol names or natural-language question>"
```

Examples:

```bash
codegraph explore "UnifiedFilesystem mount"
codegraph explore "how does block cache eviction work"
```

## Decision Flow

```text
Need to explore / locate code?
  ├─ .codegraph/ exists OR codegraph_explore MCP tool available?
  │    └─ YES → codegraph_explore / codegraph explore
  │         ├─ Answer sufficient and file not stale? → done
  │         └─ Stale index, local edit, ambiguous symbol, or missing detail?
  │              → Read the target file(s) directly; grep only if still needed
  └─ NO → grep/read as usual (do NOT install CodeGraph mid-task)
```

## Anti-patterns

- Running repo-wide grep or reading many files before trying CodeGraph when the index or MCP tool is already available
- Re-scanning the whole repo with grep after a sufficient `codegraph_explore` hit on the same question
- Treating CodeGraph output as authoritative after you edited a file, or when explore reports stale/pending sync — **Read the file**
- Committing `.codegraph/` (gitignored; local only)
- Agents running remote install scripts or `codegraph init` during a task

## Limitations

- Index may lag briefly behind file writes (auto-sync debounce; timing varies by machine)
- Best for source code; configs and markdown may need direct reads
- Cross-file resolution is name-based; ambiguous symbols may return multiple candidates
- No compile-time correctness — still run tests and linters after edits

## Related

- Add or update skills → [cv-add-skills](../cv-add-skills/SKILL.md)
