# UFS 元数据信任边界：cache_mode / fs_mode

日期：2026-10-10
状态：客户端 D1/D2 已实现并通过本地验收；未部署生产

## 1. 问题与证据边界

本次处理的问题是：CV 中已有缓存目录，但 `cache_mode` 的目录 stat 和列举
仍访问 UFS，可能在多客户端并发下放大 OCS/S3 请求。

已核实的证据：

- 故障挂载为 `cache_mode + read_only + auto_cache=yes`，`read_verify_ufs=false`。
- 查询时，模型目录及其子文件在 CV 中存在，条目 TTL 为 30 天；目录
  `ufs_mtime=0`、`state=Cv`，文件有 UFS fingerprint。
- `FileStatus::cv_valid()` 拒绝 `ufs_mtime=0`。旧目录 stat 即使关闭 verify，
  也会因这一条件回源。旧 cache_mode 列举直接访问 UFS。
- 故障日志出现 `LIST max-keys=1` 的目录探测及 429。不能由此断言每次 stat
  固定产生两次 HTTP 请求；探测顺序、分页和重试会影响实际请求数。

当前 inode 和时间戳不能完整证明历史预热、TTL 删除、故障时全部读取是否命中，
也不能证明 OCS 限流的作用范围。首次截断查询中的 133 个挂载不是全量挂载数。
本设计不以这些未证实的结论作为前提。

## 2. 模式边界

| 操作 | cache_mode | fs_mode |
|---|---|---|
| 文件 stat/open | 保留现有 fingerprint、TTL、可选 UFS verify 和数据 fallback | 元数据来自 CV；数据 fallback 保持现状 |
| 目录 stat | D1：满足信任条件时来自 CV，否则回源 | 原本就来自 CV，不改 |
| 目录列举 | D2：满足信任条件时返回 CV 视图，否则保留 UFS 路径 | 原本就来自 CV，不改 |
| `exists()` | 原本直连 UFS，本次不改 | CV，不改 |
| 写入 | 保留原有访问权限、写入和失效逻辑 | 保留原有 CV 写入及 UFS 同步逻辑 |

代码依据：`unified_filesystem.rs` 的 `get_status()` 对 fs_mode 提前返回 CV；
`get_mount_checked()` 只放行 cache_mode，因此 fs_mode 的五种列举 API 已经走 CV。
Master 目录的 `is_complete` 为 false，不是完整 UFS listing 的标志。

`read_only` 只约束经该挂载的写入，不代表 UFS 不会被外部修改。
本次接受的是 **TTL 内信任已有 CV 目录视图**，不是“只读挂载必然不可变”。

## 3. 本次实现范围

仅 D1、D2，以及对应回归验证。不增加配置、协议、Master 状态或存储字段。

### D1：目录 stat 信任 CV

统一信任条件：

```text
status.is_dir
  && status.cv_exists()
  && mount.is_read_only_cache_mode()
  && !mount.read_verify_ufs
  && !status.is_expired()
```

满足时不访问 UFS，原样返回 CV status，保留 inode ID、mtime、children_num。
不调用 `apply_ufs_fields()`，避免目录的 `ufs_mtime=0` 覆盖真实 mtime。

- 不修改共享 `FileStatus::cv_valid()`，保护文件和 Transfer 的缓存校验语义。
- 缺失、过期、UFS-only、read_write、显式 verify 保留原探测路径。
- TTL 使用现有 `is_expired()`，按目录条目 mtime 计算；不能等同于精确预热时间。
  未配置有效 TTL 时继续使用既有“不自动过期”语义。
- stat 回源本身不会刷新 CV 条目，也不保证“每次过期仅探测一次”。

### D2：目录列举使用同一信任条件

适用入口：`list_status`、`list_status_bytes`、`list_options`、
`list_options_bytes`、`list_stream`。这些入口复用同一选择逻辑。

- 信任条件满足：返回 CV 目录视图，包括空目录，不探测 UFS。
- CV 缺失、目录过期或 UFS-only：回退 UFS；启用 auto-cache 时调用已有
  `async_cache()`，提交建议性的 load 请求。后台失败不阻塞前台 UFS 读取。
- read_write 或 `read_verify_ufs=true`：保持原有 UFS 列举，不使用新信任分支。
- 非目录条目：不套用目录规则，保留原有 UFS 列举路径。
- CV 错误仅对 FileNotFound/Expired 作冷回退；其他错误直接返回，避免将控制面
  或权限等错误悄悄转换为 UFS 列举。
- 原有后端能力不扩展：OpenDAL 当前没有非流式 `list_options` 实现，因此冷回退
  的 `list_options` / `list_options_bytes` 仍返回原“不支持”错误；流式列举不受此限制。

`AsyncCachePending` 只在同一客户端实例及其 clones 内约束提交，不跨独立客户端
进程协调，也不覆盖整个 load 执行期。目录 load 后续可能产生额外 UFS 请求。
本设计不承诺跨进程请求合并或全局“只提交一次”。

## 4. 目录视图的兼容边界

**目录存在不等于已经完整同步 UFS 子项。** 本次采用此前确认的“存在即信任”边界，
不新增完整性标记或目录同步协议：

- CV 已有但部分缓存的目录，仅返回当前 CV 子项；空目录如实返回空。
- 已有目录中 UFS 外部新增/删除的子项，不会在每次列举时自动发现。
- CV 尚不存在的新路径仍能回源发现；按精确路径打开未缓存文件的原有 read-through
  行为不变。仅依赖父目录列举，不保证发现所有未缓存的新路径。
- 需要同步已有目录的 UFS 变化时，使用现有显式 load，或既有缓存释放/TTL 路径。
  不声称 warm readdir 会触发增量 load，也不声称后台 load 一定刷新目录 TTL。
- 返回的子项是否仍可从 CV 读取，由文件原有校验和数据 fallback 判断；父目录
  被信任不能代替文件内容校验。

这是对 cache_mode 只读挂载列举契约的可见变更，发版时必须说明；需要实时 UFS
列举的用户可使用显式 verify。fs_mode、cache_mode read_write 的列举契约不变。

## 5. 验证与验收

### 单测

位置：`crates/client/curvine-unified-fs/src/unified/tests/metadata_trust_test.rs`。

覆盖 D1 的 RO/RW、verify、过期/无 TTL、CV/UFS-only、文件 fingerprint 条件，
以及共享 `cv_valid()` 不放宽。实际 status 的 ID/mtime 保留由集成测试断言。

### MiniCluster 集成测试

位置：`curvine-server/tests/unified_fs/metadata_trust_test.rs`；入口
`curvine-server/tests/unified_fs_test.rs`。

使用真实 Master/Worker RPC、真实 OpenDAL S3 HTTP 适配器和本地计数服务：

- 暖目录 stat、五种列举 API、空目录、分页与流式分页的 UFS HTTP 请求数为 0。
- 两个独立 `UnifiedFileSystem` 实例均读取共享 CV 视图；这不等同真实多进程压测。
- 成功 CV status 中的过期条目仍回源；冷缺失、verify、RW 保留 UFS 路径。
- FM 的 stat/列举和不存在路径查询不访问 UFS。
- 冷/过期路径的 UFS 429（SlowDown）继续返回，不伪装为空目录或成功；
  同样的 UFS 限流不会影响已信任的暖目录视图。
- 本地 `file://` 冷目录列举能触发真实 legacy load，文件完成后移除 UFS 源，
  后续目录 stat 和五种列举仍能从 CV 返回。该项不声称覆盖独立 Transfer 服务。

命令均有外部超时：

```bash
timeout --kill-after=30s 300s "$HOME/.cargo/bin/cargo" test -p curvine-unified-fs --lib
timeout --kill-after=30s 600s "$HOME/.cargo/bin/cargo" test -p curvine-server --test unified_fs_test
timeout --kill-after=30s 1800s "$HOME/.cargo/bin/cargo" clippy --all-targets -- --deny warnings
timeout --kill-after=30s 120s "$HOME/.cargo/bin/cargo" fmt --all -- --check
git diff --check
```

本地验收结果（2026-10-11）：

| 检查 | 结果 |
|---|---|
| unified-fs 全部 lib 单测 | 26 passed，0 failed |
| MiniCluster + OpenDAL HTTP 计数集成测试 | 1 passed，覆盖上述矩阵 |
| 暖目录 stat + 五种列举 API | UFS HTTP 请求数 0（有显式计数断言） |
| workspace clippy all-targets、deny warnings | 通过 |
| workspace fmt check、git diff check | 通过 |

未部署生产，不承诺生产 429 已消失，也不估算减少百分比。

## 6. 不做与回滚

- 删除原 D3/D5 和 P1 排期：进程内单飞、客户端负缓存不能合并跨进程首次访问，
  当前没有证据需要增加它们。
- 不增加 Master 负缓存、共享协调中心、目录 watch 或新计数器。
- 不改重试参数，不扩展 UFS 后端接口，不改线上挂载或业务任务。
- 回滚仅撤销本次客户端分支；没有协议、持久化格式或 Master 状态迁移。

## 7. 自我 review 记录

1. 代码事实：FM 的列举本来走 CV；目录 `is_complete` 不代表同步完成，不能用它
   判断 warm/cold；共享 `cv_valid()` 必须保持原样。
2. 契约与实现：D1/D2 共用目录信任条件；补 D2 的 TTL/verify/CV 存在性检查；
   CV 非 miss 错误不吞；明确部分目录、空目录和 UFS 外部修改的可见性边界。
3. 验证与范围：以真实 API 路径和 UFS HTTP 请求计数验收，而非仅测条件函数；
   记录后端原有不支持项；移除未经证实的生产归因和跨进程去重设计。
