---
name: cv-create-pr
description: Create or update a Curvine pull request with standardized title and body sections (Summary, Issue/Design, Changes table, Test verified, Dependencies). Use when user asks to create, submit, update, or push a PR.
---

# cv-create-pr

按 Curvine 标题与正文规范创建或更新 PR。

## 使用时机 (When to Use)

- 用户要求创建 / 提交 / 发起 PR
- 用户要求更新 PR 标题或正文
- `cv-handle-issue` 之后，或任务提交已就绪待发布

## 准备工作 (Pre-flight)

```bash
make format
git status
git diff
```

只暂存本次任务/会话改动的文件。提交前须经用户确认。

## 语言要求 (Language)

Issue 与 PR **必须使用中文**：标题、正文一律中文。例外仅两处机器可解析的前缀，保持英文原样：

- PR 标题的 `<type>(<module>):` 前缀（如 `fix(transfer):`）和 issue 标题的 `[BUG]:` / `[FEATURE]` 前缀；
- 正文章节标题采用中英双语（如 `# 摘要 (Summary)`），过渡期内旧检索仍能命中。

## PR 标题格式 (PR Title Format)

```
<type>(<module>): <中文描述> (#<issueNumber>)
```

**示例：** `fix(transfer): 修复分段任务重试丢失进度 (#223)`

**支持的 type：** `feat`、`fix`、`docs`、`style`、`refactor`、`perf`、`test`、`chore`、`ci`、`build`、`revert`

向用户展示标题预览并获得批准。

## 提交与推送 (Commit Before PR)

```bash
git add <confirmed-files>
git commit -m "$(cat <<'EOF'
<type>(<module>): <subject>

<optional body>
EOF
)"
git push -u origin HEAD
```

## PR 正文模板 (PR Body Template)

正文必须使用**中文**，章节标题使用中英双语。用以下结构：

```markdown
# 摘要 (Summary)

<1-3 句话：这个 PR 做了什么、为什么做>

# 问题描述 / 设计 (Issue / Design)

Closes #<issueNumber>（或 "Related to #<n>"）

**缺陷修复**需包含：
- 根因
- 复现步骤
- 修复思路

**新功能**需包含：
- 设计决策与权衡
- 父 issue / 子 issue 链接（如有）

# 变更 (Changes)

| 模块 / 文件 | 变更内容 | 对现有行为的影响 |
| ------------- | ------ | --------------------------- |
| `path/to/file` | ... | 无 / 行为变化：... |

# 测试验证 (Test verified)

| 测试用例 | 结果 | 备注 |
| --------- | ------ | ----- |
| `cargo test -p ...` | 通过 | |
| ... | | |

# 依赖 (Dependencies)

- 相关 PR：#<n> 或无
- 相关 issue：#<n> 或无
- 阻塞 / 被阻塞：或无
```

不要带工具水印（如 "Made with Cursor"）。

## 创建 PR (Create PR)

先展示完整标题 + 正文预览，等待用户批准。

```bash
gh pr create \
  --title "<type>(<module>): <中文描述> (#<issueNumber>)" \
  --body "$(cat <<'EOF'
# 摘要 (Summary)
...
EOF
)"
```

默认开 **draft**，除非用户说 `ready`：

```bash
gh pr create --draft --title "..." --body "..."
```

## 更新已有 PR (Update Existing PR)

```bash
gh pr edit <number> \
  --title "<type>(<module>): <中文描述> (#<issueNumber>)" \
  --body "$(cat <<'EOF'
...
EOF
)"
```

或更新当前分支的 PR：

```bash
gh pr view --json number
gh pr edit <number> --title "..." --body "..."
```

## 检查清单 (Checklist)

- [ ] `make format` 通过
- [ ] 测试已跑，结果记入**测试验证 (Test verified)**
- [ ] 标题符合格式且带 issue 号
- [ ] 变更表覆盖关键模块
- [ ] 正文为中文（标题保留英文前缀，章节标题中英双语）
- [ ] 用户已批准提交与 PR 内容

## 相关 (Related)

- 评审反馈 → [cv-address-pr-review](../cv-address-pr-review/SKILL.md)
- 修 issue 流程 → [cv-handle-issue](../cv-handle-issue/SKILL.md)
