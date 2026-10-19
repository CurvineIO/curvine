export default {
  syncJobs: {
    list: {
      title: 'UFS 同步进度',
      subtitle: 'FS 模式逐路径同步到 UFS 的进度，含本地跟踪与 master 上报',
      searchPlaceholder: '搜索路径、任务 ID 或消息',
      searchAria: '搜索同步任务',
      pollActive: '轮询进行中',
      headerPath: 'CV 路径',
      headerTarget: 'UFS 目标',
      headerStatus: '状态',
      headerProgress: '进度',
      headerFiles: '文件数',
      headerSizes: '已同步 / 总大小',
      headerUpdated: '更新时间',
      headerActions: '操作',
      emptyNoJobs: '暂无 UFS 同步任务。fs 模式挂载下的上传与 fs_mode 自动导出会出现在这里。',
      emptyFiltered: '当前筛选条件下没有匹配的同步任务。{suffix}',
      activeFilters: ' 当前筛选（{filters}）。',
      hintBucket: '状态：{value}',
      hintSearch: '搜索：{value}',
      truncated: '列表窗口上限 500 行，请通过筛选缩小范围。'
    },
    filters: {
      all: '全部',
      active: '进行中',
      completed: '已完成',
      failed: '失败',
      attention: '需关注',
      ariaStatus: '同步状态筛选'
    },
    metrics: {
      active: '进行中',
      activeMeta: '同步中或等待中',
      completed: '已完成',
      completedMeta: '已完成的同步',
      attention: '需关注',
      attentionMeta: '失败或存在跳过',
      local: '本地跟踪中',
      localMeta: '上传进行中的任务'
    },
    detail: {
      jobId: '任务 ID',
      mount: '挂载',
      triggerEvent: '触发事件',
      message: '消息',
      tasksHeader: '子任务',
      taskId: '子任务 ID',
      source: '源路径',
      target: '目标路径',
      worker: 'Worker',
      state: '状态',
      taskProgress: '进度',
      taskMessage: '消息',
      memberHeader: '本次上传包含的路径',
      waitingForJob: '等待 master 创建任务…',
      loadFailed: '加载任务详情失败：{error}'
    },
    actions: {
      relatedJob: '关联任务',
      expand: '展开详情',
      collapse: '收起详情',
      refresh: '刷新'
    },
    alerts: {
      loadFailed: '加载同步任务失败：{error}',
      pollFailed: '同步任务轮询失败：{error}'
    }
  }
}
