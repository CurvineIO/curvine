export default {
  syncJobs: {
    list: {
      title: 'UFS Sync Progress',
      subtitle: 'Per-path FS mode sync to UFS, tracked locally and reported by the master',
      searchPlaceholder: 'Search path, job id or message',
      searchAria: 'Search sync jobs',
      pollActive: 'Poll active',
      headerPath: 'CV Path',
      headerTarget: 'UFS Target',
      headerStatus: 'Status',
      headerProgress: 'Progress',
      headerFiles: 'Files',
      headerSizes: 'Synced / Total',
      headerUpdated: 'Updated',
      headerActions: 'Actions',
      emptyNoJobs: 'No UFS sync jobs tracked yet. Uploads on fs mode mounts and fs_mode auto exports appear here.',
      emptyFiltered: 'No sync jobs matched the current filters.{suffix}',
      activeFilters: ' Active filters ({filters}).',
      hintBucket: 'status: {value}',
      hintSearch: 'search: {value}',
      truncated: 'List window is limited to 500 rows; refine the filters to see the rest.'
    },
    filters: {
      all: 'All',
      active: 'Active',
      completed: 'Completed',
      failed: 'Failed',
      attention: 'Needs attention',
      ariaStatus: 'Sync status filter'
    },
    metrics: {
      active: 'Active',
      activeMeta: 'syncing or waiting',
      completed: 'Completed',
      completedMeta: 'finished syncs',
      attention: 'Needs attention',
      attentionMeta: 'failed or skipped',
      local: 'Locally tracked',
      localMeta: 'uploads in progress'
    },
    detail: {
      jobId: 'Job ID',
      mount: 'Mount',
      triggerEvent: 'Trigger',
      message: 'Message',
      tasksHeader: 'Tasks',
      taskId: 'Task ID',
      source: 'Source',
      target: 'Target',
      worker: 'Worker',
      state: 'State',
      taskProgress: 'Progress',
      taskMessage: 'Message',
      memberHeader: 'Paths in this upload',
      waitingForJob: 'Waiting for the job to be created on the master…',
      loadFailed: 'Failed to load job detail: {error}'
    },
    actions: {
      relatedJob: 'Related job',
      expand: 'Expand details',
      collapse: 'Collapse details',
      refresh: 'Refresh'
    },
    alerts: {
      loadFailed: 'Failed to load sync jobs: {error}',
      pollFailed: 'Sync job polling failed: {error}'
    }
  }
}
