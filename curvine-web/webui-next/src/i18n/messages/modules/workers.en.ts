export default {
  workers: {
    metrics: {
      liveWorkers: 'Live Workers',
      managed: 'Managed',
      totalCapacity: 'Total Capacity',
      lostWorkers: 'Lost Workers',
      healthyHeartbeat: 'healthy heartbeat',
      blacklistDecommission: 'blacklist / decommission',
      activeWorkers: 'active workers',
      usedPercent: '{percent}% used',
      requiresAttention: 'requires attention'
    },
    list: {
      title: 'Worker Nodes',
      subtitle: 'Worker status, capacity, heartbeat and management actions',
      searchPlaceholder: 'Search hostname or ip',
      headerWorker: 'Worker',
      headerStatus: 'Status',
      headerVersion: 'Version',
      headerUsage: 'Usage',
      headerBlocks: 'Blocks',
      headerHeartbeat: 'Heartbeat',
      headerActions: 'Actions',
      rpc: 'RPC',
      empty: 'No workers matched.'
    },
    filters: {
      all: 'All',
      live: 'Live',
      blacklist: 'Blacklist',
      decommission: 'Decommission',
      lost: 'Lost'
    },
    actions: {
      detail: 'Detail',
      addBlacklist: 'AddBlacklist',
      allow: 'Allow',
      decommission: 'Decommission',
      recommission: 'Recommission',
      profile: 'Profile',
      confirm: '{action} worker {worker}?',
    },
    actionVerb: {
      blacklist: 'blacklist',
      allow: 'allow',
      decommission: 'decommission',
      recommission: 'recommission'
    },
    detail: {
      title: 'Worker Detail',
      runtime: {
        title: 'Runtime',
        subtitle: 'Address, heartbeat and reported counters',
        hostname: 'Hostname',
        workerId: 'Worker ID',
        rpcAddress: 'RPC Address',
        webPort: 'Web Port',
        ip: 'IP',
        heartbeat: 'Heartbeat',
        version: 'Version',
        versionCommit: 'Commit',
        files: 'Files',
        blocks: 'Blocks',
        notReported: 'Not reported',
        versionMismatch: 'Differs from Active Master version'
      },
      storage: {
        title: 'Storage',
        subtitle: 'Aggregated storage reported by this worker',
        curvineUsed: 'Curvine Used',
        nonFsUsed: 'Non-FS Used',
        reserved: 'Reserved',
        storageDirs: 'Storage Dirs',
        directoriesTitle: 'Storage Directories',
        directoriesSubtitle: 'Per-disk capacity and block distribution',
        type: 'Type',
        empty: 'No storage directories reported.'
      }
    },
    profile: {
      titleWithTarget: 'CPU Flame Graph · {target}',
      title: 'CPU Flame Graph',
      description:
        'Sampling for {seconds}s. The graph appears when the window ends — sampling is synchronous by design.',
      kind: 'Profile type',
      kindCpu: 'CPU',
      kindMemory: 'Memory',
      memoryDescription: 'Bytes still held, attributed to the allocating call site. jemalloc samples continuously from startup, so there is no window to wait for.',
      fullscreen: 'Fullscreen',
      exitFullscreen: 'Exit fullscreen',
      duration: 'Duration',
      seconds: '{count}s',
      sampling: 'Sampling, ~{seconds}s…',
      openInNewTab: 'Open in a new tab',
      unsupported: 'This process cannot serve a profile.',
      noWebPort: 'This process did not report a web port, so its profiling endpoint is unreachable.'
    }
  }
}
