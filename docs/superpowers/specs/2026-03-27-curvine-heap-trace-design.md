# Curvine 通用 Heap Trace 设计

## 背景

当前 curvine 已有 Prometheus `/metrics` 能力，且底层进程级指标已经可用，例如 `process_resident_memory_bytes`。master 侧也已有部分内存相关指标，如 `used_memory_bytes`、`rocksdb_used_memory_bytes`。但这些指标只能回答“进程内存高了没有”，不能回答“到底是哪类对象、哪条调用路径造成了高内存占用”。

本设计目标是提供一套 **debug-only、可高精度定位对象热点、对未来新增结构天然生效** 的通用 heap trace 方案。该方案不是 master 专属能力，而是适用于整个 curvine 系统中的任意 Rust 进程，包括但不限于 master、worker、fuse、s3-gateway，以及后续新增模块。

## 目标

1. 在显式开启时，支持对任意 curvine 进程进行高精度 heap profiling。
2. 能以“对象热点 / 调用路径热点”的粒度定位高内存占用来源，而不是固定锚定某些手工埋点结构。
3. 对后续新增结构零适配：只要它们走堆分配，就能进入这套观测体系。
4. 将聚合后的热点结果以 Prometheus metrics 暴露，便于在 Grafana 中持续追踪。
5. 支持手动触发 profile，并提供一个 HTTP 地址直接查看当前抓取出的 flamegraph。
6. 支持低频周期采样，便于形成时间序列趋势。
7. 在关闭功能时，不引入额外 profile 任务或明显运行时负担。

## 非目标

1. 不追求在默认生产环境自动启用。
2. 不在首版中处理安全访问控制；首版以“方便 trace”优先。
3. 不做阈值触发作为默认策略。
4. 不将完整调用栈高基数写入 Prometheus。
5. 不要求替换现有普通业务指标体系；该能力是内存追踪增强层。

## 推荐方案

采用 **方案 A：debug-only jemalloc profiling + 通用 heap trace runtime + 聚合后的热点 metrics + flamegraph 展示**。

核心思路：

- 在显式开启 heap trace 功能的 curvine 进程中，使用 jemalloc profiling 对堆分配进行采样。
- 周期性或手动触发生成 heap profile。
- 对 profile 做解析、符号化、调用栈归一化和热点聚合。
- 输出三类结果：
  - Prometheus top-N hotspot metrics
  - 最近一次及历史若干次 profile 对应的 flamegraph
  - 原始 profile 和 normalized summary
- metrics 不区分 master / worker / fuse 等角色，交由采集端口、job、instance、pod 等外部 labels 区分。

## 总体架构

方案分为四层：

### 1. 通用 Profiling Runtime 层

职责：

- 初始化 jemalloc profiling。
- 根据配置决定是否启用 heap trace。
- 管理低频周期抓取任务。
- 提供手动触发接口。
- 管理 profile 生命周期、保留窗口、并发控制。
- 维护“最近一次结果”的内存视图供 metrics / HTTP endpoint 读取。

设计要求：

- 与具体进程角色无关。
- 对 master、worker、fuse、s3-gateway 使用相同实现。
- 各二进制只负责接入，不复制实现。

### 2. Heap Profile Parser / Aggregator 层

职责：

- 读取 jemalloc heap profile。
- 完成符号化。
- 将调用栈归一化成稳定可读的热点签名。
- 聚合出当前 top-N bytes / objects / growth 热点。
- 产出 machine-readable summary 供 metrics 和 HTTP 查询使用。

### 3. Metrics Export 层

职责：

- 将最近一次 profile 的聚合结果转换为 Prometheus metrics。
- 仅导出有边界的 top-N 热点，避免高基数失控。
- 同时暴露 profile 执行状态、时长、时间戳、错误计数等运行指标。

### 4. Debug HTTP 展示层

职责：

- 提供手动触发 profile 的入口。
- 返回最近一次 profile 摘要。
- 直接提供当前 flamegraph 的 HTTP 地址。
- 支持查看历史 profile 的 flamegraph / 原始 profile。

## 统一接入原则

该能力必须以“系统级公共能力”存在，而不是挂在 `master_metrics` 之类的局部实现中。

接入原则如下：

1. 通用 runtime / parser / metrics 作为公共模块实现。
2. 每个 curvine 进程只在启动时决定是否启用，并完成一次初始化接线。
3. 每个进程继续复用自身现有 `/metrics` 路径，heap trace 指标自动并入该输出。
4. 不在指标命名中编码 master / worker / fuse 身份。

## 为什么采用 jemalloc profiling

之所以选 jemalloc profiling，而不是手工埋点模块内存估算或自定义 allocator 包装，是因为目标是“对象热点级、对未来新增结构零适配”。

jemalloc profiling 的优点：

- 能从 allocator 视角看到热点分配调用路径。
- 对新结构天然生效，无需再补埋点。
- 能接近 heap profiler 视角，而不是仅停留在模块统计。
- 比自研 allocator trace 风险更低、维护成本更可控。

## 触发模型

### 默认状态

- 功能默认关闭。
- 若未显式开启，则不存在周期 profile 任务、手动 profile endpoint、额外 profile 解析开销。

### 启用后支持的触发方式

首版支持两种：

1. **低频周期触发**
   - 用于形成趋势数据，服务 Grafana 持续观察。
   - 间隔可配置，例如数十秒到数分钟。

2. **手动触发**
   - 用于现场排障。
   - 触发后生成一份新的 profile、summary 与 flamegraph。

### 不采用的默认策略

- **阈值触发** 不作为首版默认方案。
- 原因：即使在已开启 tracing 的环境中，阈值触发仍可能导致行为时机不可预测，不利于控制现场开销。

推荐默认组合：

- 平时启用低频周期触发。
- 异常时由人工通过 HTTP endpoint 立即抓取。

## 调用栈归一化与热点聚合

完整调用栈是事实来源，但不能直接作为 Prometheus label 导出。

因此需要两层表示：

### 1. 原始层

保存在原始 profile 文件中，保留完整调用栈与符号信息，供线下精查与火焰图生成使用。

### 2. 指标层

将调用栈归一化为稳定的热点键：

- `site_id`：稳定短 ID，用于唯一标识热点位置。
- `site_name`：人类可读名称，通常由最有代表性的前 1~3 个关键栈帧组成。
- `rank`：当前 top-N 排名。

归一化原则：

1. 优先选取 curvine 自身栈帧。
2. 若没有 curvine 自身栈帧，则回退到 rocksdb / tokio / std / libc 等上层可解释调用点。
3. 去除地址、行号、泛型单态化噪声，保证 `site_name` 稳定。
4. 保证新增结构在不改代码的前提下也能形成新热点条目。

### 聚合输出

至少输出以下三个视角：

- 当前占用最大的 top-N hotspots by bytes
- 当前对象数最多的 top-N hotspots by objects
- 相比上一次快照增长最快的 top-N hotspots by growth bytes

其余未进入 top-N 的热点聚合为 `__other__`，避免时序膨胀。

## 统一指标方案

指标前缀统一为：

- `curvine_heap_profile_*`
- `curvine_heap_hotspot_*`

建议包含以下指标：

### 运行状态指标

- `curvine_heap_profile_enabled`
- `curvine_heap_profile_last_success_unixtime`
- `curvine_heap_profile_last_duration_ms`
- `curvine_heap_profile_runs_total{status}`
- `curvine_heap_profile_parse_errors_total`

### 热点指标

- `curvine_heap_hotspot_bytes{rank,site_id,site_name}`
- `curvine_heap_hotspot_objects{rank,site_id,site_name}`
- `curvine_heap_hotspot_growth_bytes{rank,site_id,site_name}`

这些指标不区分进程角色。master / worker / fuse / s3-gateway 的区分由 scrape target、job、instance、pod、namespace、port 等外部 labels 完成。

## Debug HTTP Endpoint 设计

为了方便 trace，首版直接提供易用的调试地址。

建议 endpoint：

### 手动触发

- `POST /debug/heap/profile`
  - 立即触发一次 heap profile。
  - 返回 profile id、状态、生成时间等摘要。

### 最近结果摘要

- `GET /debug/heap/latest`
  - 返回最近一次 profile 的摘要信息。
  - 包括时间、状态、top hotspots、关联 flamegraph id。

### 当前火焰图

- `GET /debug/heap/flamegraph.svg`
  - 直接返回最近一次 profile 生成的 flamegraph SVG。
  - 浏览器可直接访问查看。

### 历史火焰图

- `GET /debug/heap/flamegraph?id=<profile_id>`
  - 查看指定历史 profile 的 flamegraph。

### 原始 profile / 中间产物

- `GET /debug/heap/pprof?id=<profile_id>`
  - 返回原始 profile 或兼容中间格式，供进一步分析。

## 结果产物设计

每次 profile 建议保留三类产物：

### 1. Raw Profile

- 原始 jemalloc dump。
- 作为权威事实来源。

### 2. Normalized Summary

- 结构化 JSON。
- 包含时间戳、profile id、top bytes / objects / growth、site_id / site_name、解析状态等。
- 供 `/debug/heap/latest` 与 metrics exporter 使用。

### 3. Flamegraph SVG

- 浏览器直开。
- 用于人工排障。

保留最近 K 份，超出窗口自动淘汰旧文件。

## 并发与运行控制

为避免 trace runtime 自己造成系统混乱，需要以下控制：

1. 同一时刻只允许一个 profile 任务运行。
2. 若上一次 profile 尚未完成解析，则新周期任务跳过。
3. 手动触发可优先于周期任务，但也必须遵守单飞约束。
4. 所有成功、失败、跳过都需要反映到运行指标中。

## 配置模型

建议定义统一配置项：

- `heap_trace_enabled`
- `heap_trace_interval_secs`
- `heap_trace_manual_endpoint_enabled`
- `heap_trace_keep_last`
- `heap_trace_topn`
- `heap_trace_output_dir`
- `heap_trace_sampling_rate`
- `heap_trace_symbolize_inline`

说明：

- `heap_trace_enabled`：是否启用整套 runtime。
- `heap_trace_interval_secs`：低频周期采样间隔。
- `heap_trace_manual_endpoint_enabled`：是否注册手动触发和查看 endpoint。
- `heap_trace_keep_last`：保留最近多少次 profile 结果。
- `heap_trace_topn`：导出多少个热点到 metrics。
- `heap_trace_output_dir`：profile / summary / flamegraph 输出目录。
- `heap_trace_sampling_rate`：控制 jemalloc profiling 采样强度。
- `heap_trace_symbolize_inline`：是否在进程内完成符号化。

## 组件落点建议

建议将 heap trace runtime 设计为公共能力，而非挂在 master 局部实现中。

### 推荐落点

- 继续复用 `orpc::common::Metrics` 作为指标导出基础。
- heap trace runtime / parser / artifact 管理逻辑单独实现为公共模块。
- 各二进制在启动时统一接入。

无论最终落在 `orpc` 内部还是新建独立 observability crate，原则都相同：

- 公共实现只写一份。
- 进程角色只做初始化接线。
- `/metrics` 输出链路沿用各自现有实现。

## 设计权衡

### 相比手工结构埋点

本方案的优势：

- 不依赖预先枚举“值得观测的结构”。
- 后续新增结构天然纳入观测。
- 更接近“真实 heap 热点”，而非人工估算。

代价：

- 需要绑定 jemalloc profiling。
- profile 解析和 flamegraph 生成存在额外 CPU 开销。
- 只能在显式开启时使用，不适合默认常驻生产。

### 相比完整栈直接写入 metrics

本方案的优势：

- 避免 Prometheus label 高基数爆炸。
- Grafana 中可以稳定做 panel 和趋势分析。
- 需要精查时仍可通过原始 profile 与 flamegraph 回看完整栈。

## 测试策略

### 单元测试

验证：

- site_id / site_name 归一化稳定性
- top-N 裁剪和 `__other__` 聚合正确性
- profile 历史淘汰逻辑
- growth 计算逻辑
- 无 curvine 栈帧时的回退规则

### 集成测试

验证：

- 启用后 `/metrics` 中出现统一 heap 指标
- `POST /debug/heap/profile` 可成功生成新 profile
- `GET /debug/heap/latest` 返回最近一次摘要
- `GET /debug/heap/flamegraph.svg` 返回有效 SVG
- 周期任务能持续更新最新 profile 时间戳

### 演示 / 压测验证

构造已知 heap-heavy 场景，例如：

- 大量 inode / metadata 对象
- 大量 cache entry
- 大量 buffer / block 元数据

验证：

1. top hotspot metrics 能稳定出现对应热点。
2. flamegraph 主热点能落到对应调用路径。
3. 新增一种新的 heap-heavy 结构时，无需加埋点也能进入 profile 结果。

### Task 10 验证结果补充（2026-03-30）

- 目标测试 / 编译矩阵已通过。
- 本地 manual smoke flow 已到达 `POST /debug/heap/profile`、`GET /debug/heap/latest`、`GET /debug/heap/flamegraph.svg` 与 `/metrics`。
- 在当前环境中，启用 capture / flamegraph generation 未能完成端到端验证：jemalloc profiling 处于 disabled 状态，且运行时注入的 `MALLOC_CONF` profiling keys 被拒绝。
- 指标命名已确认保持 role-agnostic，统一使用 `curvine_heap_*` 前缀。

## 验收标准

首版验收定义如下：

1. 任意启用该功能的 curvine 进程都能输出统一的 heap profile / hotspot metrics。
2. 手动触发后能够生成最新 flamegraph，并通过固定 HTTP 地址直接访问。
3. 低频周期采样能够持续刷新热点 metrics，Grafana 可展示 top-N bytes / objects / growth 趋势。
4. 对新增结构无需补埋点，仍可在 profile / flamegraph 中反映其内存热点。
5. 功能关闭时，不存在 profile 任务、热点文件生成、热点指标更新行为。

## 后续计划衔接

本设计文档只定义方案与边界，不展开到具体代码改动步骤。

下一步应基于本设计撰写 implementation plan，明确：

1. 公共 runtime / parser / metrics 模块的具体落点
2. jemalloc profiling 的编译与运行时启用方式
3. 各二进制的统一接入点
4. debug endpoints 的注册方式
5. flamegraph 生成链路与 artifact 生命周期管理
6. 测试与演示用例的落地顺序
