# Full-chain daily report structure

Public Markdown uses **four H2 groups**. Related content is H3 (and H4 under perf). Do not add a fifth H2 for environment or evidence.

Body language follows the source (usually Chinese). Headings below are canonical.

The post contains **results only**. No test paths, host accounts, IPs, usernames, cluster names, machine types, image digests, harness revisions, run IDs, or archive locations.

## Front matter

| Field | Required | Notes |
| ----- | -------- | ----- |
| `title` | yes | Calendar date only, no clock and no host |
| `linkTitle` | yes | Short sidebar label, e.g. `YYYY-MM-DD 全链路` |
| `date` | yes | `YYYY-MM-DDT00:00:00Z` (must not be in the future) |
| `weight` | yes | `-YYYYMMDD` so Hextra lists newest first |
| `tags` | yes | `full-chain` or profile name, plus `go` or `no-go` |

## Heading tree

The page H1 comes from front matter `title` only. Body starts at H2. Hextra draws the right-hand TOC from H2–H4.

```text
## 质量结论
  ### 执行摘要
  ### 质量门禁
  ### 结论
## 测试结果
  ### Profile 汇总
  ### LTP          (omit if ltp not run)
  ### 性能基准      (omit if perf not run)
    #### 元数据性能（本次）
    #### FIO 读写性能（本次）
    #### 元数据性能基准
    #### FIO 读写性能基准
## 失败与归因       (omit entire H2 if all required profiles passed)
  ### 失败分析
    #### {profile}   (one per failed required profile)
  ### 失败用例摘要
  ### 失败用例对账
  ### 共性根因组
  ### 全部失败用例
## 闭环
  ### 缺陷与修复
  ### 风险
  ### 后续行动
```

| H2 | Required | Omit when |
| -- | -------- | --------- |
| 质量结论 | yes | never |
| 测试结果 | yes | never |
| 失败与归因 | if any required profile failed | all required profiles passed |
| 闭环 | yes | never |

Do **not** create: 测试范围与环境, 证据索引, 执行节点, 集群信息.

Do **not** use backticks. Do **not** write a body `#` heading. Tables must be GFM with matching column counts (see SKILL.md “Markdown must render”).

## 质量结论

### 执行摘要

Lead with a GitHub alert, then counts.

```markdown
> [!CAUTION]
> 发布决策：**NO-GO**。流水线结果 **FAIL**；执行 N 个 profile，P 个通过，F 个失败。
```

Use `> [!TIP]` when the decision is **GO**.

- **GO**: every required profile passed (perf `report_only` yellow/red does not block).
- **NO-GO**: any required profile failed, or cleanup failed when cleanup is a gate.

No wall-clock window, no commit list, no “where it ran”.

### 质量门禁

| 门禁 | 标准 | 实际 | 结论 |
| ---- | ---- | ---- | ---- |
| 全链路结果 | 所有必跑 profile 通过 | `{passed}/{total}` 通过 | PASS / FAIL |
| 失败归因 | 失败项已分类 | `{n}` 个失败 | PASS / 待逐项确认 |
| 资源清理 | 所有 profile cleanup 成功 | `{passed}/{total}` | PASS / FAIL |

### 结论

One short paragraph. List unattributed failed profiles by **profile name** only.

## 测试结果

### Profile 汇总

`Profile | Preflight | 结果 | 耗时 | 分类 | Cleanup`

No Run ID column. 结果 is PASS/FAIL. 分类 is `passed` / `unknown_failure` / `product_regression` / …

### LTP

Suite status plus `passed / real failed / skipped / report-consistency errors`, then per-suite counts. No summary-file path.

### 性能基准

Gate mode (`report_only` vs blocking), then number tables only. No client, server, instance type, pipeline SHA, or file names.

## 失败与归因

### 失败分析 / #### {profile}

Keep: 测试目标, 预期, 实际（exit code + symptom）, 业务影响, 分类, 失败层, 根因置信度, Cleanup, 下一步（role, not a person）.

Drop: Evidence, Fingerprint, 最小复现 that names commits/images/hosts, log excerpts that contain paths or IPs.

### 失败用例摘要 / 全部失败用例

`用例 | Suite/Package | 状态 | 关键错误 | 根因组`

No 日志 / Fingerprint columns. Error text must not contain paths or addresses.

### 失败用例对账

`Profile | 报告失败数 | 源失败数 | 差异 | 解释`

### 共性根因组

Coverage `{attributed}/{total}`. Each group: profiles, hypothesis (no host/disk paths), 建议, unique count, class, Issue (`needs_human` / `#n`), 验证方案, member **case names**.

## 闭环

### 缺陷与修复

Public GitHub Issue/PR numbers only. No internal ticket hosts, no operator names.

### 风险

NO-GO cannot be waived by a green subset. Name unattributed **profiles**. Do not say evidence is node-local or how to fetch logs.

### 后续行动

`优先级 | 角色 | 行动 | 完成标准` — 角色 is a function (`fuse-owner`, `测试负责人`), never a username.
