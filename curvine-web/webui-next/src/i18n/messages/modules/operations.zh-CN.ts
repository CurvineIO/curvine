export default {
  operations: {
    summary: {
      coverage: '覆盖率',
      status: '状态',
      checksPassed: '检查通过',
      needAttention: '{count} 项需关注',
      targetsCovered: '目标覆盖',
      masterWorker: '{master} 个 Master · {worker} 个 Worker',
      masterWorkerTransfer: '{master} 个 Master · {worker} 个 Worker · {transfer} 个 Transfer',
      activeAlerts: '活跃告警',
      criticalWarning: '{critical} 严重 · {warning} 警告',
      providerMode: 'Provider 模式',
      discoveredTargets: '发现 {count} 个目标',
      providerPending: 'Provider 待定'
    },
    status: {
      healthy: '健康',
      critical: '严重',
      degraded: '降级',
      pending: '待定',
      headlineFail: '需要立即处理',
      headlineWarning: '平台健康已降级',
      headlinePass: '所有系统运行正常',
      headlinePending: '等待运营数据',
      descFail: '{critical} 条严重告警和 {actionable} 项诊断检查需要关注。',
      descWarning: '集群仍可用，但有一项或多项检查需要排查。',
      descPass: '全部 {count} 项检查已通过或安全跳过。',
      descPending: '目标发现或 Provider 评估尚未完成。'
    },
    checks: {
      title: '诊断检查',
      subtitle: 'Provider 就绪、发现与采集覆盖校验。',
      passed: '{passed}/{total} 通过',
      empty: '未返回诊断检查。',
      howToFix: '修复方法',
      items: {
        'observability-enabled': {
          title: '可观测性已启用',
          summary: {
            pass: '可观测性 API 与采集已启用。',
            fail: '可观测性在配置中被禁用。'
          },
          detail: {
            fail: '设置 web.observability.enabled=true 并重启 curvine-web。'
          }
        },
        'master-targets': {
          title: '已发现 Curvine Master 目标',
          summary: '已发现 {count} 个 Master 指标目标。',
          detail: 'Master 发现使用活跃 Master 与配置的指标 Web 端口。',
          suggested_fix: '请检查 Master RPC 连通性与 master.web_port，然后刷新诊断。'
        },
        'worker-targets': {
          title: '已发现 Curvine Worker 目标',
          summary: '已发现 {count} 个在线 Worker 指标目标。',
          detail: 'Prometheus 覆盖仅要求在线 Worker。',
          suggested_fix: '请检查 Worker 注册与上报的 Web 端口。'
        },
        'fuse-targets': {
          title: '已发现 Curvine FUSE 目标',
          summary: {
            pass: '已发现 {count} 个 FUSE 指标目标。',
            warning: '已发现 {count} 个 FUSE 指标目标。',
            skipped: '配置中已禁用 FUSE 指标采集。'
          },
          detail: {
            pass: 'FUSE 目标来自 web.observability.fuse 或本地 fuse.web_port 默认值。',
            skipped: 'FUSE 可观测性需通过 web.observability.fuse.enabled 显式开启。'
          },
          suggested_fix: {
            skipped: '需要 FUSE 可观测性时，请设置 web.observability.fuse.enabled=true 并配置目标。',
            warning: '请配置 web.observability.fuse.targets，或确保 curvine-fuse 运行并暴露 /metrics。'
          }
        },
        'transfer-targets': {
          title: '已发现 Curvine Transfer 目标',
          summary: {
            pass: '已发现 {count} 个 Transfer 指标目标。',
            warning: '已发现 {count} 个 Transfer 指标目标。',
            skipped: '配置中已禁用 Transfer 指标采集。'
          },
          detail: {
            pass: 'Transfer 目标来自 transfer.hostname 与 transfer.web_port。',
            skipped: 'Transfer 可观测性跟随 transfer.enabled 开关。'
          },
          suggested_fix: {
            skipped: '需要 Transfer 可观测性时，请设置 transfer.enabled=true。',
            warning: '请检查 transfer.enabled、transfer.hostname 与 transfer.web_port，并确保 curvine-transfer 暴露 /metrics。'
          }
        },
        'builtin-coverage': {
          title: 'Builtin 采集覆盖',
          summary: {
            pass: 'Builtin 采集器覆盖 {covered}/{expected} 个必需目标。',
            warning: 'Builtin 采集器覆盖 {covered}/{expected} 个必需目标。',
            fail: 'Builtin 采集器覆盖 {covered}/{expected} 个必需目标。',
            skipped: '当前有其他 Provider 处于活跃状态，Builtin 覆盖不作为权威结果。'
          },
          detail: '覆盖基于每个已发现 Master、在线 Worker、FUSE 与 Transfer 目标的最近一次 Builtin 采集结果。',
          suggested_fix: '请检查各端点与指标 Web 端口，并在可观测性 / 数据源中查看采集错误。'
        },
        'prometheus-reachable': {
          title: 'Prometheus 可达',
          summary: {
            pass: 'Prometheus 已接受健康检查请求。',
            fail: '无法访问配置的 Prometheus。'
          },
          suggested_fix: '请检查 Prometheus URL、网络策略与 /api/v1/targets 访问权限。'
        },
        'prometheus-coverage': {
          title: 'Prometheus 采集覆盖',
          summary: {
            pass: 'Prometheus 覆盖 {covered}/{expected} 个必需目标。',
            warning: 'Prometheus 覆盖 {covered}/{expected} 个必需目标。',
            fail: 'Prometheus 覆盖 {covered}/{expected} 个必需目标。',
            skipped: '当前未使用 Prometheus 作为活跃 Provider。'
          },
          suggested_fix: '请检查 Prometheus 目标配置、服务发现与抓取状态。'
        }
      }
    },
    alerts: {
      title: '活跃告警',
      subtitle: '由诊断与缺失目标衍生的可操作问题。',
      active: '{count} 条活跃',
      allClear: '一切正常',
      allClearDesc: '无活跃告警。Provider 与采集目标状态健康。',
      recommendedAction: '建议操作',
      reviewChecks: '请查看左侧诊断检查。',
      suggestedFix: {
        builtin: '请检查目标端点与指标 Web 端口，并在可观测性 / 数据源中查看采集错误。',
        prometheus: '请检查目标端点、组件标签与 Prometheus monitor 选择器。',
        discovery: '请检查 Master RPC 连通性与 Worker 注册，然后刷新平台健康。',
        prometheusUrl: '请检查 Prometheus URL、网络策略、monitor 选择器与 /api/v1/targets 访问权限。'
      },
      targetTitle: {
        master: 'Master 指标目标不可用',
        worker: 'Worker 指标目标不可用',
        fuse: 'FUSE 指标目标不可用',
        transfer: 'Transfer 指标目标不可用'
      }
    },
    targets: {
      title: '不可用目标',
      subtitle: '缺少健康匹配采集的必需端点。',
      missing: '{count} 个缺失',
      source: '来源',
      component: '组件',
      instance: '实例',
      endpoint: '端点',
      reason: '原因',
      reasons: {
        'connection refused': '连接被拒绝',
        'request timed out': '请求超时',
        'scrape failed': '采集失败',
        'target health is down': '目标健康状态为 down',
        'no matching scrape target': '无匹配的采集目标',
        noHealthyScrape: '{provider} 没有健康的匹配采集目标'
      }
    },
    severity: {
      critical: '严重',
      warning: '警告'
    },
    checkStatus: {
      pass: '通过',
      fail: '失败',
      warning: '警告',
      skipped: '跳过'
    },
    sources: {
      diagnostics: '诊断',
      builtin: 'builtin',
      prometheus: 'prometheus'
    },
    time: {
      notAvailable: '不可用'
    }
  }
}
