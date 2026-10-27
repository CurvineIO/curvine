export default {
  config: {
    metrics: {
      entries: 'Entries',
      entriesMeta: 'matching current filter',
      groups: 'Groups',
      groupsMeta: 'top-level prefixes',
      settings: 'Settings',
      settingsMeta: 'loaded config values',
      tunable: 'Hot-tunable',
      tunableMeta: 'editable without a restart'
    },
    title: 'Configuration',
    subtitle: 'The effective configuration of the process serving this console',
    searchLabel: 'Search',
    searchPlaceholder: 'Search by property name',
    groupLabel: 'Group',
    allGroups: 'All groups ({count})',
    groupOption: '{group} ({count})',
    sourceLabel: 'Source',
    allSources: 'All sources ({count})',
    sourceOption: '{source} ({count})',
    property: 'Property',
    source: 'Source',
    sources: {
      default: 'Default',
      file: 'Config File',
      env: 'Environment',
      cli: 'CLI',
      derived: 'Derived',
      runtime: 'Runtime'
    },
    value: 'Value',
    actions: 'Actions',
    empty: 'No configuration matched.',
    node: 'Node',
    tunableOnly: 'Hot-tunable only',
    editHint: 'Rows marked with a pencil can be changed here and take effect immediately.',
    edit: 'Edit',
    revert: 'Revert to file value',
    readonly: 'Read-only',
    runtimeSaved: '{key} updated.',
    runtimeReverted: '{key} reverted to the file value.',
    runtimeFile: 'File',
    legendTitle: 'What the provenance labels mean',
    legendDerived:
      'Derived — the process computed this value after reading the file, so nothing you write to the file is the value in effect. Master port splitting and hostname resolution produce these.',
    legendRuntime:
      'Runtime — an operator overrode this value on the running process. It is not persisted and disappears on restart, so the file remains the source of truth.',
    unresolvedTitle: 'Unresolved runtime keys',
    unresolvedBody:
      'These keys are registered but their configuration path is missing from the published tree, so they are hidden everywhere else. The backend and the configuration schema have drifted.',
    volatileNote: 'Runtime overrides are not persisted and are lost when the process restarts.'
  }
}
