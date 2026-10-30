export default {
  workers: {
    metrics: {
      liveWorkers: '在线 Worker',
      managed: '受管',
      totalCapacity: '总容量',
      lostWorkers: '丢失 Worker',
      healthyHeartbeat: '心跳正常',
      blacklistDecommission: '黑名单 / 下线',
      activeWorkers: '活跃 Worker',
      usedPercent: '已用 {percent}%',
      requiresAttention: '需要关注',
      unhealthy: '不健康节点',
      unhealthyMeta: '无法接受写入'
    },
    list: {
      title: 'Worker 节点',
      subtitle: 'Worker 状态、容量、心跳与管理操作',
      searchPlaceholder: '搜索主机名或 IP',
      headerWorker: 'Worker',
      headerStatus: '状态',
      headerHealth: '健康',
      headerVersion: '版本',
      headerUsage: '使用率',
      headerBlocks: '块',
      headerHeartbeat: '心跳',
      headerActions: '操作',
      rpc: 'RPC',
      empty: '没有匹配的 Worker。'
    },
    filters: {
      all: '全部',
      live: '在线',
      blacklist: '黑名单',
      decommission: '下线',
      lost: '丢失'
    },
    health: {
      healthy: '健康',
      suspect: '可疑',
      unusable: '不可用',
      unhealthy: '不健康',
      notAllocatable: '不可分配',
      reason: '原因',
      since: '持续',
      fault: '故障',
      state: '状态'
    },
    actions: {
      detail: '详情',
      addBlacklist: '加入黑名单',
      allow: '允许',
      decommission: '下线',
      recommission: '重新上线',
      profile: '火焰图',
      confirm: '{action} Worker {worker}？',
    },
    actionVerb: {
      blacklist: '加入黑名单',
      allow: '允许',
      decommission: '下线',
      recommission: '重新上线'
    },
    detail: {
      title: 'Worker 详情',
      runtime: {
        title: '运行时',
        subtitle: '地址、心跳与上报计数',
        hostname: '主机名',
        workerId: 'Worker ID',
        rpcAddress: 'RPC 地址',
        webPort: 'Web 端口',
        ip: 'IP',
        heartbeat: '心跳',
        version: '版本',
        versionCommit: 'Commit',
        files: '文件',
        blocks: '块',
        notReported: '未上报',
        versionMismatch: '与 Active Master 版本不一致'
      },
      storage: {
        title: '存储',
        subtitle: '该 Worker 上报的聚合存储',
        curvineUsed: 'Curvine 已用',
        nonFsUsed: '非文件系统已用',
        reserved: '预留',
        storageDirs: '存储目录数',
        directoriesTitle: '存储目录',
        directoriesSubtitle: '各磁盘容量与块分布',
        type: '类型',
        empty: '未上报存储目录。'
      }
    },
    profile: {
      titleWithTarget: 'CPU 火焰图 · {target}',
      title: 'CPU 火焰图',
      description: '正在采样 {seconds} 秒。采样是同步的，窗口结束后才会出图。',
      kind: '剖面类型',
      kindCpu: 'CPU',
      kindMemory: '内存',
      capabilityUnreachable: '无法探测 {host} 的 profiling 能力（可能是跨域或网络不通），内存视图不可用。',
      capabilityUnsupported: '该进程未编译 profiling 支持，内存视图不可用。',
      memoryDescription: '按分配点展示仍持有的内存，宽度即字节数。jemalloc 自进程启动起持续采样，无需等待。',
      fullscreen: '全屏',
      exitFullscreen: '退出全屏',
      duration: '采样时长',
      seconds: '{count} 秒',
      sampling: '采样中，约 {seconds} 秒…',
      openInNewTab: '在新标签页打开',
      unsupported: '该进程无法提供火焰图。',
      noWebPort: '未上报 web 端口，无法访问其 profiling 端点。'
    }
  }
}
