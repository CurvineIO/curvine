---
title: "Curvine 全链路每日测试报告 - YYYY-MM-DD"
linkTitle: "YYYY-MM-DD 全链路"
date: YYYY-MM-DDT00:00:00Z
weight: -YYYYMMDD
tags: [full-chain, daily, no-go]
---

## 质量结论

### 执行摘要

> [!CAUTION]
> 发布决策：**GO / NO-GO**。流水线结果 **PASS / FAIL**；执行 N 个 profile，P 个通过，F 个失败。

存在阻断性失败，当前提交不得作为可发布版本；需完成归因、修复和定向回归后重新执行全链路测试。

### 质量门禁

| 门禁 | 标准 | 实际 | 结论 |
| --- | --- | --- | --- |
| 全链路结果 | 所有必跑 profile 通过 | P/N 通过 | PASS / FAIL |
| 失败归因 | 失败项已分类 | F 个失败 | PASS / 待逐项确认 |
| 资源清理 | 所有 profile cleanup 成功 | C/N | PASS / FAIL |

### 结论

本次全链路测试未通过，按失败分类进入产品修复、Harness 修复或环境治理。

未完成归因的 profile：fuse。

## 测试结果

### Profile 汇总

| Profile | Preflight | 结果 | 耗时 | 分类 | Cleanup |
| --- | --- | --- | --- | --- | --- |
| fast | PASS | PASS | 1m 35s | passed | passed |
| fuse | PASS | FAIL | 2m 56s | unknown_failure | passed |

### LTP

- 状态：**completed**
- 已完成 suite：N
- 待运行 suite：0
- 测试统计：P passed / F real failed / S skipped / E report-consistency errors

| Suite | 状态 | Passed | Real failed | Skipped | Report errors | Return code |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| smoketest | passed | 12 | 0 | 1 | 0 | 0 |

#### 失败与异常用例

未解析到 TFAIL/TBROK。

### 性能基准

> [!NOTE]
> 门禁策略：**仅报告，不阻断** 全链路结果；低于 baseline 时标黄/标红供人工跟进。

- 状态：**failed**
- 门禁模式：**report_only**

#### 元数据性能（本次）

| ITEM | VALUE | AVG COST | P50 ms | P95 ms | P99 ms | MAX ms | SAMPLES | ERRORS | 状态 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Create file | 21392.01 ops/s | 1.86 ms/op | 2.05 | 4.09 | 4.09 | 162.66 | 200000 | 0 | pass |

#### FIO 读写性能（本次）

| ITEM | SPEED GiB/s | IOPS | AVG COST | P50 ms | P95 ms | P99 ms | MAX ms | SAMPLES | ERRORS | 状态 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Sequential write 64KB | 1.70 | 27840.27 | 9.00 ms/op | 8.98 | 10.55 | 11.47 | 18.43 | 262144 | 0 | pass |

#### 元数据性能基准

| ITEM | VALUE | AVG COST | P50 ms | P95 ms | P99 ms | MAX ms | SAMPLES | ERRORS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Create file | 21668.74 ops/s | 1.84 ms/op | 2.05 | 4.09 | 4.09 | 188.88 | 200000 | 0 |

#### FIO 读写性能基准

| ITEM | SPEED GiB/s | IOPS | AVG COST | P50 ms | P95 ms | P99 ms | MAX ms | SAMPLES | ERRORS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Sequential write 64KB | 1.71 | 28084.85 | 8.89 ms/op | 8.85 | 10.29 | 10.94 | 16.58 | 262144 | 0 |

## 失败与归因

### 失败分析

#### fuse

- 测试目标：FUSE 挂载、文件 I/O 与 FIO 回归
- 预期结果：挂载可读写，I/O 语义正确且无 EIO
- 实际结果：exit code **40**；持续或预分配写入出现 EIO 或 ENOSPC
- 业务影响：阻断全链路质量门禁
- 分类：**unknown_failure**
- 失败层：**test**
- 根因置信度：低
- Cleanup：**passed**
- 下一步：由 fuse-owner 完成归因

### 失败用例摘要

| 用例 | Suite / Package | 状态 | 关键错误 | 根因组 |
| --- | --- | --- | --- | --- |
| FIO Sequential Write Test (256KB blocks) | fio / fuse | FAIL | FIO Sequential Write test failed | g-fuse-write-eio |

### 失败用例对账

| Profile | 报告失败数 | 源失败数 | 差异 | 解释 |
| --- | ---: | ---: | ---: | --- |
| fuse | 6 | 6 | +0 | 数量一致 |

### 共性根因组

归因覆盖率：**6/6（100.0%）**。

#### P1 g-fuse-write-eio

- Profiles：fuse
- 根因语义：假设 FUSE 或后端存储返回 EIO 或 ENOSPC
- 建议：对齐首次 EIO 时间点并核对 worker 与 master ERROR
- 唯一逻辑失败：6
- 模型分类：**unknown_failure**；置信度：**medium**；Issue：**needs_human**
- 验证方案：修复后重跑 fuse profile
- FIO Sequential Write Test (256KB blocks)（fuse）：FIO Sequential Write test failed

### 全部失败用例

| 用例 | Suite / Package | 状态 | 关键错误 | 根因组 |
| --- | --- | --- | --- | --- |
| FIO Sequential Write Test (256KB blocks) | fio / fuse | FAIL | FIO Sequential Write test failed | g-fuse-write-eio |

## 闭环

### 缺陷与修复

- GitHub Issue：**needs_human**
- GitHub PR：**pending_fix_review**

### 风险

- 局部 profile 通过不能替代全链路 NO-GO。
- 未完成归因的 profile：fuse。

### 后续行动

| 优先级 | 角色 | 行动 | 完成标准 |
| --- | --- | --- | --- |
| P0 | fuse-owner | 完成归因、建 Issue、修复并定向回归 | fuse 通过，Issue 与 PR 完整 |
| P0 | 测试负责人 | 重跑全链路并更新报告 | 必跑 profile 全部通过 |
