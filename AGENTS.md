

<skills_system priority="1">

## Available Skills

<!-- SKILLS_TABLE_START -->
<usage>
When users ask you to perform tasks, check if any of the available skills below can help complete the task more effectively. Skills provide specialized capabilities and domain knowledge.

How to use skills:
- Read the skill file at `.agents/skills/<skill-name>/SKILL.md`
- Base directory provided in output for resolving bundled resources (references/, scripts/, assets/)

Usage notes:
- For Curvine project workflow tasks, prefer the cv-* skills listed in <available_skills> below
- When exploring or locating code, read and follow [cv-codegraph](.agents/skills/cv-codegraph/SKILL.md) — prefer CodeGraph (`codegraph_explore` MCP or `codegraph explore` CLI) when `.codegraph/` exists or the MCP tool is available; **Read source files directly** after local edits or when explore reports stale index sync
- User-installed global skills (e.g. ~/.cursor/skills/, ~/.claude/skills/) remain available when the agent tool exposes them
- Do not invent or invoke project skills that are not listed in <available_skills>
- Do not invoke a skill that is already loaded in your context
- Each skill invocation is stateless
- Canonical project skill location: `.agents/skills/` (symlinked from `.cursor/skills`, `.claude/skills`, `.github/skills`)
- Only `cv-*` skills are versioned in this repository
</usage>

<available_skills>

<skill>
<name>cv-add-skills</name>
<description>Add or update Curvine cv-* project skills under .agents/skills/, following naming and layout conventions, and sync the AGENTS.md registry. Use when creating a new cv skill, updating an existing cv skill structure, or registering skills after changes.</description>
<location>project</location>
</skill>

<skill>
<name>cv-codegraph</name>
<description>Use CodeGraph for Curvine code exploration when the MCP tool or local index is already available; prefer it over repo-wide grep when locating or understanding code. Human one-time setup covers CLI install, MCP wiring, and indexing. Use when exploring the codebase, tracing call paths, or finding symbols.</description>
<location>project</location>
</skill>

<skill>
<name>cv-create-issue</name>
<description>Create a GitHub issue for Curvine using repository templates, with extended Goal/Not Goal/Test Plan sections for features. Use when user asks to create an issue, report a serious bug found during testing, or when agent should suggest filing an issue for the current problem.</description>
<location>project</location>
</skill>

<skill>
<name>cv-create-pr</name>
<description>Create or update a Curvine pull request with standardized title and body sections (Summary, Issue/Design, Changes table, Test verified, Dependencies). Use when user asks to create, submit, update, or push a PR.</description>
<location>project</location>
</skill>

<skill>
<name>cv-csi-test</name>
<description>End-to-end Curvine CSI driver testing guide for Kubernetes deployment, StorageClass and MountPod setup, resilience scenarios, volume expansion, and scripted test suites. Use when validating CSI code changes, running integration tests, debugging mount failures, or verifying MountPod recovery after node or CSI restarts.</description>
<location>project</location>
</skill>

<skill>
<name>cv-handle-issue</name>
<description>Fix a Curvine GitHub issue from analysis through plan approval to implementation, routing large features to cv-tasks-breakdown. Use when user asks to fix, resolve, or implement a specific issue URL or issue number.</description>
<location>project</location>
</skill>

<skill>
<name>cv-submit-pr-review</name>
<description>Perform a direct code review of a Curvine PR against live base/head by reconstructing intent, reading tests first, mapping dirty crates, and checking correctness, safety, lifecycle, contracts, and performance on data I/O, RPC, and metadata paths. Use when user asks to review a PR's code, do a code review, or check a PR before merge.</description>
<location>project</location>
</skill>

<skill>
<name>cv-address-pr-review</name>
<description>Review an existing Curvine PR, analyze review comments one by one with an accept/reject table, fix after user approval, update the PR, reply to comments, and resolve threads. Use when user asks to review a PR, handle review comments, or provides a PR URL.</description>
<location>project</location>
</skill>

<skill>
<name>cv-run-workflow</name>
<description>Trigger GitHub Actions workflows for Curvine when workflow YAML or dependent Dockerfiles change, with support for specifying branch, workflow file, and inputs. Use when user asks to run a workflow, dispatch CI, or test workflow/Dockerfile changes.</description>
<location>project</location>
</skill>

<skill>
<name>cv-tasks-breakdown</name>
<description>Break a design or implementation plan into small, dependency-ordered tasks with test coverage per task, commit-sized deliverables, and sub-issue tracking. Use when user asks to decompose a plan, split a large feature, create a task breakdown, or prepare incremental commits for a complex change.</description>
<location>project</location>
</skill>

<skill>
<name>cv-test-report</name>
<description>Publish Curvine full-chain daily test reports to the Hextra Hugo site at CurvineIO/test-reports, using a standard Markdown template with no environment or secret leakage. Use when the user asks to publish a test report, convert harness output to Markdown, or update CurvineIO/test-reports.</description>
<location>project</location>
</skill>

</available_skills>
<!-- SKILLS_TABLE_END -->

</skills_system>
