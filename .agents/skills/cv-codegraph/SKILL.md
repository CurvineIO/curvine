---
name: cv-codegraph
description: Install, initialize, and use CodeGraph for Curvine code exploration. Prefer CodeGraph before grep, find, or manual file reads when locating or understanding code. Use when exploring the codebase, tracing call paths, finding symbols, or when CodeGraph MCP or CLI is available.
---

# cv-codegraph

[CodeGraph](https://github.com/colbymchenry/codegraph) indexes this repository into a local knowledge graph (`.codegraph/` at repo root). It returns symbol source, call paths (including dynamic-dispatch hops grep cannot follow), and blast-radius context in one call.

## When to Use

- Exploring or locating code in Curvine
- Tracing how symbols connect across files or crates
- Answering "where is X" or "how does X work" before opening files
- Any task where you would reach for `grep`, `find`, or broad file reads

**Priority rule:** When `.codegraph/` exists or the CodeGraph MCP tool is available, use CodeGraph **first**. Fall back to grep/read only when CodeGraph is unavailable, not indexed, or does not cover the target (configs, docs, non-indexed files).

## Setup

Install the CLI if `codegraph` is not on `PATH`:

```bash
# macOS / Linux
curl -fsSL https://raw.githubusercontent.com/colbymchenry/codegraph/main/install.sh | sh
```

```powershell
# Windows (PowerShell)
irm https://raw.githubusercontent.com/colbymchenry/codegraph/main/install.ps1 | iex
```

Initialize indexing at the repository root (once per machine):

```bash
codegraph init
```

The `.codegraph/` directory is gitignored and stays local to each developer machine.

Verify:

```bash
codegraph --version
ls .codegraph/
```

## Usage

### MCP tool (preferred in Cursor)

When the `user-codegraph` MCP namespace is available, call `codegraph_explore`:

- One query usually answers symbol location, verbatim source, and call paths
- Name a file or symbol to load line-numbered source safe for edits
- If a symbol is listed but deferred, load it by name in a follow-up query

### Shell CLI (always available after install)

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
  ├─ .codegraph/ exists OR MCP available?
  │    └─ YES → codegraph_explore / codegraph explore
  │         ├─ Answer sufficient? → done
  │         └─ Missing detail (config, doc, stale file)? → grep/read that target only
  └─ NO → grep/read as usual
       └─ Consider: install + codegraph init for future sessions
```

## Anti-patterns

- Running grep or reading many files before trying CodeGraph when the index exists
- Re-verifying CodeGraph AST results with grep (slower, less accurate on call paths)
- Committing `.codegraph/` (gitignored; local only)
- Skipping `codegraph init` and then ignoring CodeGraph entirely

## Limitations

- Index lags file writes by ~1 second
- Best for source code; configs and markdown may need direct reads
- Cross-file resolution is name-based; ambiguous symbols may return multiple candidates
- No compile-time correctness — still run tests and linters after edits

## Related

- Add or update skills → [cv-add-skills](../cv-add-skills/SKILL.md)
