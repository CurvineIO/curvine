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
      requiresAttention: '需要关注'
    },
    list: {
      title: 'Worker 节点',
      subtitle: 'Worker 状态、容量、心跳与管理操作',
      searchPlaceholder: '搜索主机名或 IP',
      headerWorker: 'Worker',
      headerStatus: '状态',
      headerUsage: '使用率',
      headerBlocks: '块',
      headerHeartbeat: '心跳',
      headerActions: '操作',
      rpc: 'RPC',
      available: '可用 {amount}',
      empty: '没有匹配的 Worker。'
    },
    filters: {
      all: '全部',
      live: '在线',
      blacklist: '黑名单',
      decommission: '下线',
      lost: '丢失'
    },
    actions: {
      detail: '详情',
      addBlacklist: '加入黑名单',
      allow: '允许',
      decommission: '下线',
      recommission: '重新上线',
      confirm: '{action} Worker {worker}？',
      failed: 'Worker 操作失败：{error}'
    },
    actionVerb: {
      blacklist: '加入黑名单',
      allow: '允许',
      decommission: '下线',
      recommission: '重新上线'
    },
    detail: {
      title: 'Worker 详情',
      subtitle: 'Worker 状态与存储详情',
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
        files: '文件',
        blocks: '块',
        notReported: '未上报'
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
    }
  }
}
