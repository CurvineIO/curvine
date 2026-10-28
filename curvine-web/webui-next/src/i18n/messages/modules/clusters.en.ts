export default {
  clusters: {
    table: {
      id: 'Cluster ID',
      displayName: 'Display name',
      masters: 'Master addresses',
      status: 'Status',
      source: 'Source',
      actions: 'Actions'
    },
    status: {
      ready: 'Ready',
      invalid: 'Invalid',
      unreachable: 'Unreachable'
    },
    source: {
      default: 'Default',
      file: 'File'
    },
    /** Actions column. */
    switch: 'Switch to this cluster',
    current: 'Current',
    probing: 'Probing…',
    probe: 'Probe',
    import: 'Import cluster',
    importTitle: 'Import cluster',
    importHint: 'Paste the cluster configuration (TOML). It is validated before anything is written.',
    editTitle: 'Replace cluster configuration',
    /**
     * The stored document is preloaded, so editing starts from the real file
     * instead of asking the user to retype it.
     */
    editHint:
      'The stored configuration is loaded below. Change it, validate, then save — saving replaces the whole file with what is on screen.',
    configLoading: 'Reading the stored configuration…',
    configLoadingHint: 'The editor unlocks once the stored file has been read.',
    configLoadFailed:
      'Could not read the stored configuration — paste the complete TOML document to replace it.',
    editUnavailable: 'This cluster cannot be edited.',
    tomlLabel: 'Cluster TOML',
    tomlPlaceholder: '[cluster]\nname = "analytics"\n\n[master]\nhost = "127.0.0.1"\nrpc_port = 8995',
    tomlHint: 'The payload is stored as-is once validation passes.',
    validate: 'Validate',
    validating: 'Validating…',
    save: 'Save',
    saving: 'Saving…',
    validationOk: 'Configuration is valid',
    validationErrors: 'Validation errors',
    validationWarnings: 'Warnings',
    summaryId: 'Cluster ID: {id}',
    summaryName: 'Display name: {name}',
    summaryMasters: 'Master addresses: {masters}',
    saved: 'Cluster {id} saved',
    switched: 'Switched to cluster {id}',
    rescan: 'Rescan directory',
    rescanning: 'Rescanning…',
    rescanned: 'Clusters directory rescanned — {count} clusters',
    rescanFailed: 'Rescan failed — {error}',
    probeReachable: 'Reachable — active master {master}, latency {latency} ms',
    probeUnreachable: 'Unreachable — {message}',
    probeFailed: 'Probe failed — {error}',
    deleteTitle: 'Delete cluster',
    delete: 'Delete',
    deleteConfirmHint:
      'The cluster file is removed from the clusters directory. Type the cluster ID to confirm.',
    deleteInputLabel: 'Cluster ID',
    deleteMismatch: 'The typed ID does not match this cluster.',
    deleted: 'Cluster {id} deleted',
    deleteFailed: 'Delete failed — {error}',
    importFailed: 'Import failed — {error}',
    validateFailed: 'Validation request failed — {error}',
    switchFailed: 'Switch failed — {error}',
    empty: 'No clusters registered',
    emptyHint: 'The default cluster and every file in the clusters directory are listed here.',
    loadFailed: 'Clusters could not be loaded'
  }
}
