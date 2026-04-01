# Curvine Heap Trace Framework 设计文档

## 1. 背景

Curvine 当前已经通过 `/metrics` 暴露 Prometheus 指标，也能看到进程级内存指标，例如 resident memory。一些角色还暴露了业务内存相关指标，但这些信号只能回答 **“内存是不是变高了”**，无法回答更关键的调试问题：**究竟是哪类对象、哪条分配路径导致了堆内存增长**。

本文档描述一套面向 Curvine 的统一 Heap Trace Framework，用于提供：

- 仅在 debug 场景启用的 heap tracing
- allocator 级别的内存可见性
- 面向热点对象/热点调用路径的分析能力
- 统一的 metrics 导出
- 通过 HTTP 直接查看 flamegraph
- 在所有 Curvine Rust 进程中复用

这套框架面向 `curvine-server` 的 master、worker，`curvine-fuse`，`curvine-s3-gateway`，以及后续新增的 Curvine 进程，且不依赖某些特定数据结构的手工埋点。

## 2. 设计目标

这套框架的设计目标如下：

1. **高精度 heap tracing**
  - 基于 allocator profiling 观察真实堆分配行为，而不是依赖少量手工挑选的数据结构做内存估算。
2. **仅在 debug 场景启用**
  - 必须显式开启。默认运行路径不应承担 profile 采集开销。
3. **热点级定位能力**
  - 输出应当能够定位到对象热点/调用路径热点，而不是只依赖固定的一组手工指标。
4. **对新增结构天然生效**
  - 后续新增的 heap-heavy 结构不需要额外改代码埋点，也能自动出现在 profile 和热点摘要中。
5. **统一观测面**
  - Heap trace 数据既能被 Prometheus/Grafana 消费，也能通过 HTTP debug endpoint 直接查看。
6. **跨角色复用**
  - 同一套框架可以用于 master、worker、fuse、s3-gateway 和未来新增的 Curvine 进程，只保留极薄的角色接线代码。
7. **可控的触发方式**
  - 同时支持低频周期采集和手动即时抓取，便于排障与趋势观测。

## 3. 非目标

首版设计明确**不**覆盖以下内容：

1. **默认常驻生产 profiling**
  - 该框架不面向默认开启的生产常驻 profile。
2. **debug endpoint 的安全加固**
  - 首版优先保证 trace 使用方便，不优先处理访问控制。后续如有需要，可在 HTTP 集成层补充鉴权。
3. **阈值触发的自动 profiling**
  - 阈值触发不是首要模型，因为它会让 tracing 开销变得不可预测，也会增加运维控制复杂度。
4. **把完整原始调用栈直接导出为 Prometheus labels**
  - 完整栈的 label 基数过高，不适合作为稳定指标输出。
5. **替代现有业务指标或进程指标**
  - Heap trace 是对现有 observability 能力的补强层，而不是替代品。

## 4. 总体技术架构

该框架是一个共享的 heap-trace 子系统，由四层组成：

1. **Allocator profiling 边界层**
  - 从 jemalloc profiling 捕获原始 heap profile 数据。
2. **Runtime 编排层**
  - 负责触发控制、single-flight 执行、artifact 生成、周期调度以及最新结果状态管理。
3. **归一化与导出层**
  - 负责调用栈归一化、稳定 hotspot identity 生成、top-N 收敛、metrics/summary 导出。
4. **Debug 访问层**
  - 提供手动抓取、最近摘要、raw profile 获取和 flamegraph 查看等 HTTP 接口。

系统层面的结构如下：

```text
Curvine 进程
  ├─ heap-trace 配置 / feature gate
  ├─ HeapTraceRuntime
  │    ├─ HeapProfiler（jemalloc 驱动）
  │    ├─ artifact writer
  │    ├─ flamegraph generator
  │    ├─ latest summary state
  │    └─ periodic trigger task
  ├─ Prometheus metrics export
  └─ debug HTTP endpoints
         ├─ POST /debug/heap/profile
         ├─ GET  /debug/heap/latest
         ├─ GET  /debug/heap/flamegraph.svg
         ├─ GET  /debug/heap/flamegraph
         └─ GET  /debug/heap/pprof
```

实现集中在共享代码中，各进程只负责启动时接线。

## 5. 为什么选择 Jemalloc Profiling

本方案选择 jemalloc profiling，而不是依赖手工维护的结构内存计数，或者自定义 tracing allocator。

### 5.1 为什么它适合这个问题

- 能在 allocator 边界观察真实 heap 分配行为。
- 对新增结构天然生效，不需要额外改代码。
- 能按调用路径暴露热点，而不是受限于人工设计的 metric key。
- 天然适合生成 flamegraph 和 raw profile 供后续分析。

### 5.2 为什么不能只依赖手工 metrics

手工内存指标适合已知子系统，但有两个根本限制：

- 它只能测量“事先已经想到要测”的对象。
- 它不擅长解释新增结构或隐式持有路径造成的 heap 增长。

对于“精确定位是谁占了内存”这个问题，allocator 侧 profiling 更匹配需求。

## 6. 核心设计原则

这套框架遵循以下原则：

1. **共享实现只写一份**
  - runtime、normalization、artifact 管理、metrics 逻辑、debug response 逻辑都应只实现一次并复用。
2. **角色无关指标**
  - 指标名中不编码 `master`、`worker`、`fuse`、`s3`。角色区分交由 scrape target 和外部 labels 完成。
3. **有界基数**
  - 只导出归一化后的 top-N hotspot 指标，避免 label 爆炸。
4. **single-flight capture**
  - 同一进程任意时刻只允许一个 capture 执行。
5. **手动 + 周期并存**
  - 周期采集负责趋势，手动抓取负责现场分析。
6. **latest 结果快速读取**
  - HTTP 和 metrics 应优先读取最近一次已处理结果，而不是每次读取时重新触发昂贵计算。
7. **runtime 行为必须真实**
  - 如果 allocator profiling 在当前环境下不可用，系统应明确暴露限制，而不是伪装为抓取成功。

## 7. 关键模块与职责

### 7.1 共享 heap-trace 模块

共享 heap-trace 逻辑位于 `orpc` 侧公共模块，供所有进程角色统一复用。

职责包括：

- 配置模型
- 原始 profile capture 边界
- normalization 逻辑
- metrics 集成
- artifact 持久化
- HTTP helper 逻辑
- runtime 编排

这是整套框架的核心。

### 7.2 Profile capture 边界

该层负责把一个已启用的 runtime 转换为原始 heap profile artifact。

职责：

- 触发 jemalloc profile dump
- 读取生成的 profile payload
- 以统一内部格式返回 captured profile
- 在 feature 关闭或 allocator profiling 不可用时提供受控 fallback/报错路径

这一层隔离了 allocator 相关细节。

### 7.3 HeapTraceRuntime

`HeapTraceRuntime` 是整个 heap tracing 的执行协调器。

职责：

- 判断 tracing 是否启用
- 通过 single-flight 保证串行 capture
- 按需触发 profile capture
- 启停周期 capture
- 写出 profile、flamegraph、summary artifact
- 维护 latest profile/flamegraph 路径及内存态结果
- 更新共享 summary 和 metric 状态

这是进程接入和 debug handler 使用的主入口。

### 7.4 调用栈归一化与热点收敛

normalization 负责把原始调用栈转换成稳定、可控、适合 metrics 导出的 hotspot identity。

职责：

- 优先用 Curvine 自身栈帧给热点命名
- 如果没有 Curvine 栈帧，则回退到可解释的上游/运行时栈帧
- 去除地址、行号及其它不稳定噪声
- 计算稳定的 `site_id`
- 把原始热点结果收敛成 top-N
- 把尾部热点聚合为 `__other__`

这一层是 Prometheus 可用性的关键。

### 7.5 Metrics 导出层

metrics 层负责把最近一次处理后的 heap-trace 结果转换为统一的、角色无关的 Prometheus 指标。

职责：

- 暴露 runtime 状态和执行计数
- 暴露 bytes / objects / growth 三类有界热点视图
- 存储并刷新 latest summary，供 metrics 与 debug response 共用
- 避免角色特定的 metric family 分叉

### 7.6 Artifact 管理层

每次成功 capture 都会产生持久化 artifact，便于查看当前结果和最近历史。

职责：

- 生成稳定的 artifact 命名
- 写出 raw profile
- 写出 normalized summary JSON
- 写出 flamegraph SVG
- 仅保留最近配置数量的 artifacts
- 清理旧文件

### 7.7 Debug HTTP 接口层

HTTP 层为 heap tracing 提供一组直接可用的调试接口。

职责：

- 手动 capture endpoint
- latest summary endpoint
- latest flamegraph endpoint
- raw profile 获取 endpoint
- 提供共享 response helper，保证各角色行为一致

### 7.8 各进程接入层

各进程角色只负责最小化接线。

典型职责：

- 启动时构造 `HeapTraceRuntime`
- 在启用时启动低频 periodic capture
- 把 debug routes 合并到自身 HTTP surface
- 继续通过已有 `/metrics` 路径暴露指标

这样可以保证角色代码足够薄，避免逻辑复制。

## 8. 端到端数据流

### 8.1 周期 capture 流程

1. 进程以 heap-trace feature/config 启动。
2. 构造 `HeapTraceRuntime`。
3. 启动 periodic capture task。
4. 每次 tick 时，runtime 先检查是否已有 capture 正在运行。
5. 如果没有运行中的 capture，则触发 profile capture。
6. 生成并读取 raw profile。
7. 生成 flamegraph 与 summary artifact。
8. 更新 latest 内存态结果和 metric summary。
9. Prometheus `/metrics` 输出刷新后的热点结果。

### 8.2 手动 capture 流程

1. 运维或开发者发起 `POST /debug/heap/profile`。
2. HTTP handler 调用 runtime capture 路径。
3. runtime 执行 single-flight 校验。
4. 重新生成 profile、summary 与 flamegraph。
5. 成功时返回最新 profile artifact，失败时返回错误。
6. latest summary 与 flamegraph endpoint 立即反映新结果。

### 8.3 latest 查询流程

1. 请求 `GET /debug/heap/latest` 或 `GET /debug/heap/flamegraph.svg`。
2. handler 直接读取最近一次缓存的状态/artifact。
3. 返回响应，不重新触发 capture。

这样可以把“读取”保持为低成本操作，而把“capture”保持为显式行为。

## 9. 触发模型

### 9.1 默认状态

- Heap trace 默认关闭。
- 关闭时不应有 periodic task。
- 关闭时不应发生意外 capture。

### 9.2 手动触发

手动触发用于现场排障时立即查看当前 heap 状态。

优点：

- 运维控制明确
- 能即时刷新 artifacts
- 可直接通过浏览器或 HTTP client 使用

### 9.3 低频周期触发

周期触发用于形成 Grafana 趋势和保留滚动历史样本。

特点：

- 频率刻意较低
- 面向 debug 或受控环境
- 若有 capture 正在执行，则跳过重叠 tick

### 9.4 默认不采用阈值自动触发

阈值触发没有作为首版主模型，因为它会让 tracing 成本更加不可预测，也容易在最不希望增加负载的时候触发额外采样。

## 10. 调用栈归一化与热点模型

原始 allocator 调用栈噪声较大、基数过高，不能直接导出成 Prometheus 指标，因此需要一个标准化的 hotspot model。

### 10.1 Hotspot identity

每个热点至少由以下字段表示：

- `site_id`：稳定的机器可读标识
- `site_name`：人类可读的归一化位置名称
- `rank`：当前 top-N 结果中的排名

### 10.2 归一化规则

归一化目标是让多次 capture 之间的结果尽量稳定。

规则包括：

1. 优先选择 Curvine 自身栈帧命名热点。
2. 如果没有 Curvine 栈帧，则回退到有解释性的上游/运行时栈帧。
3. 去掉地址、格式噪声等不稳定因素。
4. 对语义相同的热点在多次采样中维持稳定 identity。

### 10.3 有界导出

框架只导出各视角下的 top-N 热点。

主要视角包括：

- bytes
- objects
- growth bytes

其它热点统一归并到 `__other__`，确保指标基数可控。

## 11. Metrics 设计

整个 metrics 设计对所有进程角色保持统一。

### 11.1 运行状态指标

当前已经实现的运行状态指标如下：


| Metric                                       | 类型         | Labels   | 含义                                                      | 更新时机                             |
| -------------------------------------------- | ---------- | -------- | ------------------------------------------------------- | -------------------------------- |
| `curvine_heap_profile_enabled`               | Gauge      | 无        | 当前 heap-trace runtime 是否处于 enabled 状态。`1` 表示启用，`0` 表示关闭 | runtime 初始化或状态刷新时                |
| `curvine_heap_profile_last_success_unixtime` | Gauge      | 无        | 最近一次成功 capture 的 Unix 秒级时间戳                             | 每次成功 capture 后                   |
| `curvine_heap_profile_last_duration_ms`      | Gauge      | 无        | 最近一次成功 capture 的耗时，单位毫秒                                 | 每次成功 capture 后                   |
| `curvine_heap_profile_runs_total`            | CounterVec | `status` | heap-trace 运行次数计数                                       | 每次 attempt / success / failure 时 |
| `curvine_heap_profile_parse_errors_total`    | Counter    | 无        | profile 解析失败总次数                                         | 每次 parse error 时                 |


其中 `curvine_heap_profile_runs_total` 的 `status` 当前有三种值：

- `attempt`：开始尝试执行一次 capture
- `success`：一次 capture 成功完成
- `failure`：一次 capture 执行失败

这些指标主要回答下面几类问题：

- 这个进程当前有没有真正启用 heap trace
- 最近一次成功 capture 是什么时候
- 最近一次 capture 大概要跑多久
- 最近一段时间是成功多还是失败多
- profile 读取/解析是否经常出错

### 11.2 热点指标

当前已经实现的热点指标如下：


| Metric                              | 类型       | Labels                         | 含义                                |
| ----------------------------------- | -------- | ------------------------------ | --------------------------------- |
| `curvine_heap_hotspot_bytes`        | GaugeVec | `rank`, `site_id`, `site_name` | 最近一次 capture 中，各热点按归一化站点聚合后的占用字节数 |
| `curvine_heap_hotspot_objects`      | GaugeVec | `rank`, `site_id`, `site_name` | 最近一次 capture 中，各热点对应的对象数          |
| `curvine_heap_hotspot_growth_bytes` | GaugeVec | `rank`, `site_id`, `site_name` | 最近一次 capture 中，各热点的增长字节数，可为负值     |


这些指标提供最近一次 profile 的有界、可做趋势分析的热点视图。

labels 说明：

- `rank`：当前 top-N 排名，从 `1` 开始
- `site_id`：稳定的机器可读标识，适合做聚合和告警
- `site_name`：可读性更好的热点名字，适合 dashboard 直接展示

当前实现还约定了下面几个行为：

- 热点导出上限固定为 top `10`
- 超出 top `10` 的尾部热点会被聚合成一条 `site_id="__other__"` / `site_name="__other__"` 记录
- 每次刷新热点前，上一轮热点 gauge 会先整体 reset，再写入最新结果
- 因此这些 hotspot metrics 表示的是“最近一次 capture 的快照”，不是累积计数

如果从 Prometheus 输出角度看，可以理解成：

- `curvine_heap_hotspot_bytes` 看谁当前最占堆
- `curvine_heap_hotspot_objects` 看谁对象数量最多
- `curvine_heap_hotspot_growth_bytes` 看谁最近增长最快

### 11.3 角色区分策略

指标名本身不区分进程角色。

角色区分依赖外围观测系统中的 labels，例如：

- job
- instance
- pod
- namespace
- scrape port

这样可以让 dashboard 保持统一，也避免 metric family 碎片化。

### 11.4 当前没有导出成 Prometheus 指标的字段

有几个字段虽然和 heap trace 相关，但当前不在 `/metrics` 中，而是在 `GET /debug/heap/latest` 的 summary 里返回：

- `runtime_enabled`
- `sample_interval_bytes`
- `capture_count`
- `last_capture_epoch_ms`

也就是说：

- `/metrics` 侧重点是监控和趋势
- `/debug/heap/latest` 侧重点是最近一次结果摘要

## 12. HTTP Debug 接口设计

框架提供一组专门面向 heap 调试的 HTTP 接口。

### 12.1 `POST /debug/heap/profile`

触发一次即时 capture。

行为：

- 调用 runtime 执行 capture
- 成功返回 profile 数据，失败返回错误
- 成功时刷新 latest artifacts

### 12.2 `GET /debug/heap/latest`

返回最近一次 capture 的摘要。

典型内容包括：

- runtime 是否启用
- 当前采样间隔配置
- capture 次数
- 最近一次 capture 时间戳
- 最近热点摘要元数据

### 12.3 `GET /debug/heap/flamegraph.svg`

直接返回最近一次 flamegraph SVG。

该接口面向浏览器直接打开查看。

### 12.4 `GET /debug/heap/flamegraph`

通过通用 artifact 路径返回最近的 flamegraph artifact。

### 12.5 `GET /debug/heap/pprof`

返回最近一次 raw profile artifact。

适合用于离线分析或后续工具集成。

## 13. Artifact 模型

每次成功 capture 都会产出多类 artifacts。

### 13.1 Raw profile

raw profile 是最权威的 capture 结果表示。

用途：

- 离线分析
- 后续重新处理
- 作为 flamegraph 生成输入

### 13.2 Summary JSON

summary JSON 是 machine-readable 的 normalized 结果，用于 metrics 和 latest-summary HTTP 返回。

用途：

- 暴露稳定的结构化热点数据
- 支撑 latest summary endpoint
- 保存 capture 元数据与 top-N 结果

### 13.3 Flamegraph SVG

flamegraph 是最快速的人类可视化排障入口。

用途：

- 浏览器直接打开
- 支撑现场调试
- 直观展示热点调用路径

### 13.4 保留策略

runtime 仅保留最近配置数量的 capture artifacts，并自动清理更老结果。

这样可以避免磁盘无界增长。

## 14. 并发与运行控制

这套框架需要一些 runtime 约束，避免 tracing 自身引入混乱。

### 14.1 Single-flight capture

同一时刻只允许一个 capture 执行。

如果已有 capture 正在运行：

- 重叠的 periodic tick 直接跳过
- 并发的手动请求快速失败，而不是再启动一个昂贵 capture

### 14.2 Periodic task 生命周期

periodic task 的持有方式必须避免自引用导致的 task cycle。

这样可以保证：

- runtime shutdown 时后台任务能被正确停止
- runtime drop 后不会遗留孤儿 capture loop

### 14.3 关闭态和不可用态必须真实反映

如果 tracing 被关闭，或者 allocator profiling 在当前环境中实际上不可用，capture 应返回真实错误，而不是伪造成功结果。

## 15. 多进程接入模型

### 15.1 `curvine-server`

master 和 worker 在进程启动时接入共享 runtime，并把 heap debug endpoints 合并到各自 HTTP surface。

### 15.2 `curvine-fuse`

fuse 在启动 web server 之前构造 runtime，并在 metrics/health endpoint 旁边暴露 heap debug routes。

### 15.3 `curvine-s3-gateway`

s3-gateway 同样接入 heap tracing，但由于共享 `orpc` 代码与 s3-gateway 使用的 axum 版本不同，因此不能直接 merge 同一个 router 类型，只能保留一个本地薄适配层。

这里的关键设计是：

- heap-trace response-building 和业务行为仍然下沉到共享代码
- s3-gateway 仅保留最小的 axum 兼容适配层

这样既能保证行为一致，也避免业务逻辑复制。

## 16. 如何完整启用 Heap Trace（重点）

**这是最容易踩坑的部分。** Heap trace 必须同时满足三层条件才能真正生效：

1. **编译期**：打开 `heap-trace` feature
2. **配置层**：在集群配置中启用 `[heap_trace]`
3. **运行环境层**：进程启动前设置正确的 `MALLOC_CONF`

**三层缺一不可。** 每一层缺失都会导致 runtime 最终处于 disabled 状态。

### 16.1 第一层：编译期 feature 开关

当前推荐的构建方式：

```bash
# 使用 --perf 参数构建
make build ARGS="--perf -p server"

# 或者直接走 build/build.sh
build/build.sh --perf
```

`--perf` 会把 `heap-trace` feature 编进二进制。

**重要：编译时不要设置 MALLOC_CONF**

编译时如果设置了 `MALLOC_CONF`，会报类似错误：

```text
<jemalloc>: Invalid conf pair: prof:true
<jemalloc>: Invalid conf pair: prof_active:true
```

这是因为编译过程中 jemalloc 会被初始化，但此时 profiling 功能还未编译进去。**`MALLOC_CONF` 只应该在运行时设置，不要在编译时设置。**

正确做法：

```bash
# 编译前确保 MALLOC_CONF 未设置（或设为空）
unset MALLOC_CONF
make build ARGS="--perf -p server"
```

**如果这一层没做：**

- 相关代码、路由、jemalloc allocator 都不会编译进二进制
- `/debug/heap/`* 路由根本不存在
- `/metrics` 中也不会有 `curvine_heap_`* 指标族

**验证方法：**

启动后直接访问 `/debug/heap/latest`，如果返回 404，说明这一层没打开。

### 16.2 第二层：运行期配置开关

运行期配置位于集群配置文件的 `[heap_trace]` 段：

```toml
[heap_trace]
runtime_enabled = true
sample_interval_bytes = 8388608
periodic_interval_secs = 60
```

字段说明：


| 字段                       | 类型      | 默认值                 | 说明                                     |
| ------------------------ | ------- | ------------------- | -------------------------------------- |
| `runtime_enabled`        | `bool`  | `false`             | 是否启用 heap-trace runtime。默认关闭。          |
| `sample_interval_bytes`  | `usize` | `8388608` (`8 MiB`) | 采样间隔字节数的配置值。当前主要用于 runtime summary 输出。 |
| `periodic_interval_secs` | `u64`   | `60`                | 周期采集间隔，单位秒。设为 `0` 可禁用周期采集，仅保留手动触发。     |


补充说明：

- `runtime_enabled = false` 时，不会创建 heap-trace runtime，也不会挂载 `/debug/heap/*` 路由
- 当前 `etc/curvine-cluster.toml` 默认没有显式写出 `[heap_trace]` 段；如果要开启，需要手动补上该配置块
- **注意：配置只是"请求开启"，真正生效还要看第三层**

**如果这一层没做但第一层做了：**

- `/debug/heap/`* 路由存在
- 但访问 `/debug/heap/latest` 会返回 `runtime_enabled: false`

### 16.3 第三层：MALLOC_CONF 环境变量（最容易漏）

这是 **最容易被忽略但最关键的一层**。

**重要：只在运行时设置，不要在编译时设置**

进程**启动前**必须设置 `MALLOC_CONF` 环境变量，至少包含：

- `prof:true` — 启用 jemalloc profiling
- `prof_active:true` — 激活 profiling

典型写法：

```bash
# 运行进程前设置
export MALLOC_CONF="prof:true,prof_active:true,lg_prof_sample:23"
./curvine-server master --config your-config.toml
```

或者一行写完：

```bash
MALLOC_CONF="prof:true,prof_active:true,lg_prof_sample:23" ./curvine-server master --config your-config.toml
```

说明：

- `lg_prof_sample:23` 表示采样间隔为 `2^23 = 8 MiB`，可根据需求调整
- 这个环境变量必须在进程启动前设置，进程启动后再设置无效
- jemalloc allocator 在进程启动时就初始化，之后无法再改变 profiling 状态

**如果这一层没做但前两层都做了：**

- `/debug/heap/`* 路由存在
- 配置 `[heap_trace]` 也打开了
- 但访问 `/debug/heap/latest` 返回 `runtime_enabled: false`
- 调用 `POST /debug/heap/profile` 会返回错误：`jemalloc heap profiling is disabled`
- master/worker 启动日志中会打印 warning：

```text
Heap trace requested for master, but MALLOC_CONF did not enable jemalloc profiling at process start; runtime capture disabled
```

**这就是你刚才遇到的问题。**

### 16.4 完整启用示例

假设要为 master 启用 heap trace，完整步骤如下：

**Step 1：编译（不要设置 MALLOC_CONF）**

```bash
# 确保编译时没有设置 MALLOC_CONF
unset MALLOC_CONF
make build ARGS="--perf -p server"
```

**Step 2：修改配置文件**

在 `etc/curvine-cluster.toml` 或你使用的配置文件中增加：

```toml
[heap_trace]
runtime_enabled = true
sample_interval_bytes = 8388608
periodic_interval_secs = 60
```

**Step 3：设置环境变量并启动**

```bash
# 运行时设置 MALLOC_CONF
export MALLOC_CONF="prof:true,prof_active:true,lg_prof_sample:23"
./curvine-server master --config etc/curvine-cluster.toml
```

**Step 4：验证**

```bash
# 检查 runtime 状态
curl http://localhost:9000/debug/heap/latest

# 应该返回 runtime_enabled: true

# 手动触发一次 capture
curl -X POST http://localhost:9000/debug/heap/profile

# 查看 flamegraph
curl http://localhost:9000/debug/heap/flamegraph.svg
```

### 16.5 各进程端口对应关系

heap-trace 没有独立监听端口，它挂载在各进程现有的 web/metrics HTTP 服务上。


| 进程                      | 端口配置项                 | 默认值    | 访问示例                                   |
| ----------------------- | --------------------- | ------ | -------------------------------------- |
| `curvine-server master` | `master.web_port`     | `9000` | `http://<host>:9000/debug/heap/latest` |
| `curvine-server worker` | `worker.web_port`     | `9001` | `http://<host>:9001/debug/heap/latest` |
| `curvine-fuse`          | `fuse.web_port`       | `9002` | `http://<host>:9002/debug/heap/latest` |
| `curvine-s3-gateway`    | `s3_gateway.web_port` | `9003` | `http://<host>:9003/debug/heap/latest` |


**特别注意 s3-gateway：**

- `s3_gateway.listen` 默认是 `0.0.0.0:9900`，这是 S3 API 对外监听端口
- heap-trace 和 `/metrics` 走的是 `s3_gateway.web_port`，默认 `9003`
- 不要访问 `9900`，要访问 `9003`

### 16.6 三层条件缺失时的表现总结


| 条件缺失                                 | 表现                                                                                            |
| ------------------------------------ | --------------------------------------------------------------------------------------------- |
| 编译期没打开 `heap-trace` feature          | `/debug/heap/`* 返回 404                                                                        |
| 配置没打开 `[heap_trace].runtime_enabled` | `/debug/heap/latest` 返回 `runtime_enabled: false`                                              |
| `MALLOC_CONF` 没设置或设置不正确              | `/debug/heap/latest` 返回 `runtime_enabled: false`，日志有 warning，capture 返回 profiling disabled 错误 |


### 16.7 配置与代码对齐说明

当前实现已确保配置项与代码行为完全对齐：

- `periodic_interval_secs` 控制周期采集间隔，默认 `60` 秒，设为 `0` 可禁用周期采集
- `sample_interval_bytes` 会正确传入 runtime，并在 `/debug/heap/latest` 中展示
- master/worker/fuse/s3-gateway 的 runtime 初始化行为已统一，都从配置读取参数

说明：

- `sample_interval_bytes` 目前主要用于 runtime summary 输出，真正的 jemalloc 采样行为仍由 `MALLOC_CONF` 的 `lg_prof_sample` 控制
- 如需调整实际采样精度，请修改 `MALLOC_CONF` 中的 `lg_prof_sample` 值

## 17. 使用方式

### 17.1 验证 metrics

启动后访问目标进程的 web 端口 `/metrics`，确认 heap 指标族已出现：

```bash
# master
curl http://localhost:9000/metrics | grep curvine_heap

# worker
curl http://localhost:9001/metrics | grep curvine_heap
```

典型检查：

- `curvine_heap_profile_enabled` 存在且值为 1
- 发生 capture 后 `curvine_heap_profile_last_success_unixtime` 更新
- 成功 profile 后 `curvine_heap_hotspot_*` 指标出现

### 17.2 手动触发 capture

```bash
curl -X POST http://localhost:9000/debug/heap/profile
```

成功会返回 profile artifact 信息，失败会返回错误原因。

### 17.3 查看最近摘要

```bash
curl http://localhost:9000/debug/heap/latest
```

返回结构化摘要，包括：

- `runtime_enabled` — 当前是否真正启用
- `sample_interval_bytes` — 配置值
- `periodic_interval_secs` — 周期采集间隔
- `capture_count` — 已执行 capture 次数
- `last_capture_epoch_ms` — 最近一次成功 capture 时间

### 17.4 直接打开 flamegraph

浏览器访问：

```text
http://localhost:9000/debug/heap/flamegraph.svg
```

这是查看当前分配热点布局的最直接方式。

### 17.5 获取 raw profile

```bash
curl http://localhost:9000/debug/heap/pprof -o heap.pb.gz
```

适合进一步离线分析。

## 18. 典型排障流程

1. 确认三层条件都满足：编译 `--perf`、配置 `[heap_trace]`、环境变量 `MALLOC_CONF`
2. 启动进程，检查日志是否有 warning
3. 访问 `/debug/heap/latest` 确认 `runtime_enabled: true`
4. 访问 `/metrics` 确认 heap 指标族存在
5. 手动触发 `POST /debug/heap/profile`
6. 浏览器打开 `/debug/heap/flamegraph.svg` 查看热点
7. 在 Grafana 中通过 `site_id`/`site_name` 观察热点趋势

## 19. 测试与验证策略

### 19.1 单元测试

单元测试主要验证：

- stack normalization 稳定性
- stable hotspot identity 生成
- top-N reduction 行为
- `__other__` 聚合逻辑
- flamegraph 生成行为
- runtime single-flight 语义
- artifact retention 行为

### 19.2 集成测试

集成测试主要验证：

- debug routes 是否正确注册
- latest-summary endpoint 是否工作正常
- flamegraph endpoint 是否返回 SVG 内容
- metrics export 是否暴露统一 heap-trace 指标
- periodic capture 是否更新 runtime 状态

### 19.3 端到端 smoke 验证

smoke 验证主要检查真实二进制中的接入链路。

包括：

- 启动接线是否正确
- 路由是否可达
- metrics 是否存在
- 在 allocator profiling 可用环境下，artifact 流程是否完整跑通

## 20. 当前验证状态与已知限制

当前实现已经验证了框架接线、共享逻辑、路由集成以及 metrics surface，但仍有一个重要环境限制：

- 当前运行环境下，jemalloc profiling 还没有真正成功启用
- profiling 相关 allocator 配置在运行时被拒绝
- 因此在该环境里无法完整证明真实 profile capture 成功

已经验证的内容包括：

- debug endpoints 的路由接线
- latest summary 行为
- 统一 metrics 命名
- 各进程角色的接入方式
- 除 allocator 环境限制外的整体框架行为

仍依赖环境能力的部分包括：

- jemalloc profiling 驱动下的真实 profile capture 成功
- 在 profiling 真正可用环境中生成真实 flamegraph 数据

这属于部署/runtime 能力限制，不是框架设计本身的缺陷。

## 21. 运维建议

1. 该框架应优先用于受控 debug 或诊断环境。
2. 需要持续趋势时，优先使用低频 periodic capture。
3. 现场排障时，优先结合手动 capture 使用。
4. dashboard 侧应重点围绕 top-N hotspot 指标构建。
5. `site_id` 应视为稳定 identity，`site_name` 应视为展示标签。
6. 如果 capture 一开始就失败，优先检查 allocator profiling 支持情况，而不是先怀疑上层逻辑。

## 22. 后续演进方向

当前设计为后续增强保留了清晰扩展空间，例如：

- debug endpoint 的访问控制
- 更丰富的历史 artifact 浏览能力
- 更完善的 profiling 环境检查与启动期诊断
- 更强的跨 capture diff/growth 分析
- 围绕 raw profile 的离线分析工具能力

由于 runtime 控制、artifact、normalization 和 debug delivery 已经清晰分层，这些增强都可以在当前架构上逐步演进。

## 23. 总结

Curvine 的 Heap Trace Framework 是一套面向 debug 场景、基于 allocator profiling 的 observability 子系统，用来回答普通 metrics 难以回答的问题：**到底是什么占用了堆内存，以及这些内存是从哪里分配出来的**。

其设计中心包括：

- 共享 runtime 编排
- allocator 驱动 capture
- 归一化热点收敛
- 统一 Prometheus metrics
- HTTP 直接查看 flamegraph
- 跨 Curvine 各类进程角色复用

最终得到的是一套统一框架：既能在 Grafana 中做趋势追踪，也能通过 HTTP 做交互式 heap 调试，并且对未来新增的 Curvine 结构天然具备观测能力，而不需要再次添加结构级别的专用埋点。