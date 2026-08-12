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
      headerUsage: 'Usage',
      headerBlocks: 'Blocks',
      headerHeartbeat: 'Heartbeat',
      headerActions: 'Actions',
      rpc: 'RPC',
      available: '{amount} available',
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
      confirm: '{action} worker {worker}?',
      failed: 'Worker action failed: {error}'
    },
    actionVerb: {
      blacklist: 'blacklist',
      allow: 'allow',
      decommission: 'decommission',
      recommission: 'recommission'
    },
    detail: {
      title: 'Worker Detail',
      subtitle: 'Worker status and storage details',
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
        files: 'Files',
        blocks: 'Blocks',
        notReported: 'Not reported'
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
    }
  }
}
