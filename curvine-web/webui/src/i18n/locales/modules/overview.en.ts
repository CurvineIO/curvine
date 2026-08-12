export default {
  overview: {
    metrics: {
      master: 'Master',
      haNodes: 'HA Nodes',
      workers: 'Workers',
      capacity: 'Capacity',
      namespace: 'Namespace',
      liveLost: '{live} live, {lost} lost',
      availableMeta: '{amount} available',
      usedDelta: '{percent} used',
      filesMeta: '{count} files',
      dirsDelta: '{count} dirs',
      standbyMeta: '{standby} standby, {active} active',
      notConfigured: 'not configured'
    },
    capacity: {
      title: 'Cluster Capacity',
      subtitle: 'Storage usage reported by Master',
      used: 'Used',
      fsUsed: 'Filesystem Used',
      available: 'Available',
      reserved: 'Reserved'
    },
    masterDetails: {
      title: 'Master Details',
      subtitle: 'Runtime identity and startup state',
      clusterId: 'Cluster ID',
      configuredMaster: 'Configured Master',
      activeMaster: 'Active Master',
      startTime: 'Start Time',
      blockTotal: 'Block Total'
    },
    ha: {
      title: 'Master HA',
      subtitle: 'Active and standby masters discovered from cluster configuration',
      master: 'Master',
      journal: 'Journal',
      role: 'Role',
      reachable: 'Reachable',
      current: 'Current',
      action: 'Action',
      activeTarget: 'Active target',
      empty: 'No HA master nodes reported yet.',
      failoverApi: 'Failover API:',
      enabled: 'enabled',
      unavailable: 'unavailable'
    },
    role: {
      active: 'Active',
      standby: 'Standby'
    },
    tooltips: {
      failoverUnavailable: 'Failover API is unavailable',
      switchingTo: 'Switching to {target}',
      alreadyActive: 'This master is already active',
      targetUnreachable: 'Target master is unreachable'
    },
    confirm: {
      switchMaster: 'Switch active master from {from} to {to}?'
    },
    errors: {
      switchTimeout: 'Master switch request was accepted, but {target} did not become active before timeout.{suffix}',
      lastError: ' Last error: {error}'
    }
  }
}
