export default {
  observability: {
    toolbar: {
      timeRange: 'Time range',
      targetFilter: 'Target filter',
      worker: 'Worker',
      fuse: 'FUSE',
      transfer: 'Transfer',
      instance: 'Instance',
      allWorkers: 'All workers',
      allFuseMounts: 'All FUSE mounts',
      allTransferInstances: 'All Transfer instances',
      selectInstance: 'Select instance…',
      showEmptyPanels: 'Show empty panels',
      providerMode: 'Provider mode',
      applyMode: 'Apply mode',
      status: 'Status',
      disabled: 'disabled',
      zoomed: 'Zoomed',
      resetZoom: 'Reset zoom',
      zoomHint: 'Drag on a chart to zoom the time range; double-click a chart to reset.'
    },
    provider: {
      builtin: 'Builtin',
      prometheus: 'Prometheus',
      auto: 'Auto',
      configured: 'Configured',
      endpoint: 'Endpoint',
      lastCollect: 'Last collect',
      lastDiscovery: 'Last discovery'
    },
    collector: {
      title: 'Collector & Data Sources',
      settings: 'Settings',
      settingsBadge: 'scrape & retention',
      scrapeInterval: 'Scrape interval',
      retention: 'Retention',
      action: 'Action',
      seriesSamples: '{series} series · {samples} samples',
      lastScrape: 'last scrape {time}',
      issues: 'Issues',
      metricNames: 'Metric Names',
      loadingTargets: 'Loading targets…',
      noMetrics: 'No metrics collected yet.',
      emptyPanel: 'No samples in this range (currently zero).',
      selectInstanceHint: 'Select an instance from the dropdown to view filtered metrics.',
      targetsUp: '{up}/{total} targets up',
      collectorLabel: 'collector',
      alertsCount: '{count} alerts',
      namesCount: '{count} names',
      settingsSaved: 'Settings saved.'
    },
    chart: {
      charts: 'charts',
      expanded: 'Expanded',
      collapsed: 'Collapsed',
      series: 'Series',
      current: 'Current',
      min: 'Min',
      max: 'Max',
      avg: 'Avg',
      loading: 'Loading…',
      emptyDefault: 'No metric series available for the selected range.',
      showLess: 'Show less',
      showMore: '+{count} more',
      showAllSeries: 'Show all series'
    },
    targets: {
      up: 'Up',
      down: 'Down'
    },
    sections: {
      trends: 'Trends',
      performance: 'Performance',
      capacity: 'Capacity',
      process: 'Process',
      runtime: 'Runtime',
      io: 'IO',
      latency: 'Latency',
      queue: 'Queue',
      cache: 'Cache',
      health: 'Health',
      risks: 'Risks',
      status: 'Status',
      flow: 'Flow',
      store: 'Store',
      snapshot: 'Snapshot',
      transferTasks: 'Transfer Tasks',
      other: 'Other'
    },
    timeRanges: {
      '15m': '15m',
      '1h': '1h',
      '6h': '6h',
      '24h': '24h',
      '3d': '3d'
    },
    targetFilters: {
      all: 'All',
      master: 'Master',
      worker: 'Worker',
      fuse: 'FUSE',
      transfer: 'Transfer',
      instance: 'Instance'
    },
    hints: {
      retentionExceeded: 'Selected range exceeds collector retention ({retention}); charts show at most retained data.',
      sparseData: 'Only ~{coverage} of data collected for the selected {range} window; charts zoom to available samples until history fills in.'
    },
    groups: {
      master: 'Master',
      workers: 'Workers',
      fuse: 'FUSE',
      transfer: 'Transfer',
      overview: 'Overview',
      cluster: 'Cluster'
    },
    messages: {
      modeSet: 'Provider mode set to {mode}.'
    },
    providerHealth: {
      healthy: 'healthy',
      unhealthy: 'unhealthy',
      stopped: 'stopped',
      warning: 'warning'
    }
  }
}
