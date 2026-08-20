export default {
  overview: {
    metrics: {
      master: 'Master',
      haNodes: 'HA 节点',
      workers: 'Worker',
      capacity: '容量',
      namespace: '命名空间',
      liveLost: '{live} 在线，{lost} 丢失',
      availableMeta: '可用 {amount}',
      usedDelta: '已用 {percent}',
      filesMeta: '{count} 个文件',
      dirsDelta: '{count} 个目录',
      standbyMeta: '{standby} 备用，{active} 主用',
      notConfigured: '未配置'
    },
    capacity: {
      title: '集群容量',
      subtitle: 'Master 上报的存储使用情况',
      used: '已用',
      fsUsed: '文件系统已用',
      available: '可用',
      reserved: '预留'
    },
    masterDetails: {
      title: 'Master 详情',
      subtitle: '运行时标识与启动状态',
      clusterId: '集群 ID',
      configuredMaster: '配置的 Master',
      activeMaster: '当前 Master',
      version: '版本',
      startTime: '启动时间',
      blockTotal: '块总数'
    },
    ha: {
      title: 'Master 高可用',
      subtitle: '从集群配置发现的 Active 与 Standby Master',
      master: 'Master',
      journal: 'Journal',
      role: '角色',
      version: '版本',
      startTime: '启动时间',
      reachable: '可达性',
      current: '当前',
      action: '操作',
      activeTarget: 'Active 目标',
      empty: '尚未上报 HA Master 节点。',
      failoverApi: '故障转移 API：',
      enabled: '已启用',
      unavailable: '不可用'
    },
    role: {
      active: 'Active',
      standby: 'Standby'
    },
    tooltips: {
      failoverUnavailable: '故障转移 API 不可用',
      switchingTo: '正在切换到 {target}',
      alreadyActive: '该 Master 已是 Active',
      targetUnreachable: '目标 Master 不可达'
    },
    confirm: {
      switchMaster: '将 Active Master 从 {from} 切换到 {to}？'
    },
    errors: {
      switchTimeout: 'Master 切换请求已接受，但 {target} 在超时前未变为 Active。{suffix}',
      lastError: ' 最后错误：{error}'
    }
  }
}
