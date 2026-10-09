export default {
  config: {
    metrics: {
      entries: 'Entries',
      entriesMeta: 'matching current filter',
      groups: 'Groups',
      groupsMeta: 'top-level prefixes',
      settings: 'Settings',
      settingsMeta: 'loaded config values',
      api: 'API',
      apiMeta: 'preferred endpoint'
    },
    title: 'Configuration',
    subtitle: 'Searchable readonly cluster configuration',
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
      cli: 'CLI'
    },
    value: 'Value',
    empty: 'No configuration matched.'
  }
}
