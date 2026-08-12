export default {
  browse: {
    title: '文件浏览器',
    subtitle: '浏览 Curvine 命名空间并查看文件块分布',
    pathPlaceholder: '/path',
    cacheOnly: '仅缓存',
    mkdir: '新建目录',
    uploadFile: '上传文件',
    uploadFolder: '上传文件夹',
    free: '释放缓存',
    copyPath: '复制绝对路径',
    goUpTitle: '打开上级目录',
    view: {
      list: '列表',
      blocks: '块分布'
    },
    itemType: {
      directory: '目录',
      file: '文件'
    },
    table: {
      name: '名称',
      type: '类型',
      size: '大小',
      modified: '修改时间'
    },
    empty: {
      cacheOnly: '此路径下没有缓存文件。',
      directory: '目录为空。'
    },
    upload: {
      progress: '上传进度',
      clearCompleted: '清除已完成',
      clear: '清除',
      uploadedCount: '已上传 {uploaded} / {total}',
      uploadingFiles: '正在上传 {count} 个文件到 Curvine…',
      uploaded: '已上传：{path}',
      uploadedFolder: '已从文件夹上传 {count} 个文件。',
      folderUpload: '文件夹上传',
      failed: '上传失败：{error}',
      folderFailed: '文件夹上传失败：{error}'
    },
    ufsSync: {
      title: 'FS 模式 UFS 同步进度',
      collapse: '折叠',
      expand: '展开',
      collapseAria: '折叠 UFS 同步面板',
      expandAria: '展开 UFS 同步面板',
      viewJobs: '在同步任务中查看详情',
      activeTotal: '{active} 活跃 · 共 {total}',
      tracked: '跟踪 {total} 项',
      ufsSynced: 'UFS 已同步 {synced} / {total}',
      states: {
        queued: '排队中',
        completed: '已完成',
        needsAttention: '需要关注',
        syncingToUfs: '正在同步到 UFS',
        waitingForUfsSync: '等待 UFS 同步',
        uploadingToCurvine: '正在上传到 Curvine',
        finishedWithSkipped: '已完成（部分跳过）',
        uploadedToCurvine: '已上传到 Curvine',
        checkingUfsSync: '检查 UFS 同步',
        uploadFailed: '上传失败',
        interrupted: '已中断',
        timedOut: '超时',
        unsupported: '不支持',
        notMounted: '未挂载',
        notStarted: '未开始',
        pending: '等待中',
        loading: '加载中',
        failed: '失败',
        canceled: '已取消',
        unknown: '未知'
      },
      messages: {
        interrupted: '上传进度停止更新。浏览器上传已中断或页面已刷新。',
        timedOut: 'UFS 同步状态在 6 分钟内未完成。请刷新状态或检查加载任务。',
        unsupported: '当前运行的 Web API 未暴露 /v1/fs/ufs-sync。请重新构建或重启 Web 服务。',
        statusCheckFailed: '状态检查失败（第 {attempt} 次）：{message}',
        timedOutRecovery: 'UFS 同步状态在多次错误后仍未恢复。请刷新或检查加载任务。',
        waitingForJob: '等待任务',
        filesPending: '文件待处理',
        filesCount: '{completed} / {total} 个文件'
      },
      tooltip: {
        cv: 'CV：{path}',
        ufs: 'UFS：{path}',
        job: '任务：{id}',
        mount: '挂载 {path}'
      }
    },
    download: {
      file: '正在下载 {path}…',
      folder: '正在将 {path} 打包为 tar 下载。大文件夹可能需要较长时间。'
    },
    freeAction: {
      currentTitle: '释放此挂载路径下的缓存数据',
      onlyMount: '仅 UFS 挂载路径下的数据可以释放',
      itemDir: '释放此挂载目录下的缓存数据',
      itemFile: '释放此文件的缓存数据',
      confirmDir: '递归释放 {path} 下的缓存数据？',
      confirmFile: '释放 {path} 的缓存数据？',
      confirmCurrent: '递归释放 {path} 下的缓存数据？',
      result: 'inode：{inodes}，空间：{space}',
      failed: '释放失败：{error}'
    },
    delete: {
      mountRoot: '请在挂载页面删除挂载点',
      mounted: '从 Curvine 及底层 UFS 删除',
      directory: '递归删除目录',
      file: '删除文件',
      confirmDirMounted: '删除目录 {path}？这将同时删除底层 UFS 数据。',
      confirmFileMounted: '删除文件 {path}？这将同时删除底层 UFS 数据。',
      confirmDir: '删除目录 {path}？',
      confirmFile: '删除文件 {path}？',
      success: '已删除：{path}',
      failed: '删除失败：{error}'
    },
    mkdirAction: {
      prompt: '目录路径',
      defaultName: 'new-dir',
      success: '已创建目录：{path}',
      failed: '创建目录失败：{error}'
    },
    copy: {
      failed: '复制失败：{error}'
    }
  }
}
