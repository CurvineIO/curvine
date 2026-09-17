---
name: cv-create-issue
description: Create a GitHub issue for Curvine using repository templates, with extended Goal/Not Goal/Test Plan sections for features. Use when user asks to create an issue, report a serious bug found during testing, or when agent should suggest filing an issue for the current problem.
---

# cv-create-issue

按 `.github/ISSUE_TEMPLATE/` 与 Curvine 约定创建结构化 GitHub issue。

Issue 标题与正文**必须使用中文**（模板文件本身已是中文；`[BUG]:` / `[FEATURE]` 前缀与 labels 保持英文原样，机器可解析）。

## 使用时机 (When to Use)

- 用户明确要求创建 / 提交 / 发起 issue
- 测试中发现**严重缺陷**（数据丢失、崩溃、安全、CI 中断）
- Agent 发现值得跟踪的未解决问题——先**问用户**是否建 issue

## 步骤一：分类 (Step 1: Classify)

| 类型 | 模板 | Label | 标题前缀 |
| ---- | -------- | ----- | ------------ |
| 缺陷 | `bug_report.md` | `bug` | `[BUG]:` |
| 功能 | `feature_request.md` | `enhancement` | `[FEATURE]` |

先搜索是否已有重复：

```bash
gh issue list --search "<keywords>" --state open --limit 10
```

## 步骤二：起草正文 (Step 2: Draft Body)

### 缺陷（按模板）

`.github/ISSUE_TEMPLATE/bug_report.md` 的必填章节（中文填写）:

- **问题描述**
- **复现步骤** (numbered steps)
- **期望行为**
- **环境信息** (OS, Curvine version/branch, relevant config)
- **补充信息** (logs, screenshots)

### 功能（模板 + 扩展）

`.github/ISSUE_TEMPLATE/feature_request.md` 的基础章节（中文填写）:

- **关联的问题是什么？**
- **期望的方案**
- **考虑过的替代方案**
- **补充信息**

**功能类 issue 必备的扩展章节（中文填写）:**

```markdown
# 目标 (Goal)

设计范围：列出子需求及其解决的问题。

| 子需求 | 解决的问题 |
| --------------- | -------------- |
| ... | ... |

# 非目标 (Not Goal)

本 issue **不**涉及的模块、功能或边界。

- ...

# 测试计划 (Test Plan)

| 测试用例 | 测试目标 | 涉及模块 |
| --------- | -------------- | ---------------- |
| ... | ... | ... |
```

## 步骤三：与用户确认 (Step 3: Confirm with User)

展示标题 + 完整正文预览。`gh issue create` 之前等待批准。

## 步骤四：创建 (Step 4: Create)

```bash
# 缺陷
gh issue create \
  --title "[BUG]: <中文概述>" \
  --label bug \
  --body "$(cat <<'EOF'
...
EOF
)"

# 功能
gh issue create \
  --title "[FEATURE] <中文概述>" \
  --label enhancement \
  --body "$(cat <<'EOF'
...
EOF
)"
```

## 步骤五：输出 (Step 5: Output)

返回 issue 号、URL 和建议的下一步：

- 修缺陷 → [cv-handle-issue](../cv-handle-issue/SKILL.md)
- 大功能 → [cv-tasks-breakdown](../cv-tasks-breakdown/SKILL.md)

## 规则 (Rules)

- 标题与正文必须使用中文（仅 `[BUG]:` / `[FEATURE]` 前缀与 labels 例外）
- 一事一 issue
- 父 issue 的子任务：标题加 `[SUB]` 前缀，正文链接 `Parent: #<n>`
