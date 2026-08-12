<template>
  <div class="admin-page" v-loading="loading">
    <section class="metric-grid">
      <MetricCard :label="$t('config.metrics.entries')" :value="filteredConfig.length" :meta="$t('config.metrics.entriesMeta')" :delta="deltaLabel('readonly')" tone="good" />
      <MetricCard :label="$t('config.metrics.groups')" :value="groups.length" :meta="$t('config.metrics.groupsMeta')" :delta="deltaLabel('auto')" tone="neutral" />
      <MetricCard :label="$t('config.metrics.settings')" :value="configItems.length" :meta="$t('config.metrics.settingsMeta')" :delta="deltaLabel('cluster')" tone="good" />
      <MetricCard :label="$t('config.metrics.api')" value="/api/v1" :meta="$t('config.metrics.apiMeta')" :delta="deltaLabel('enabled')" tone="good" />
    </section>

    <section class="admin-panel table-panel">
      <div class="panel-heading config-heading">
        <div>
          <h2>{{ $t('config.title') }}</h2>
          <p>{{ $t('config.subtitle') }}</p>
        </div>
      </div>

      <div class="config-toolbar">
        <label class="config-field">
          {{ $t('config.searchLabel') }}
          <input v-model="query" class="admin-input config-search" :placeholder="$t('config.searchPlaceholder')">
        </label>
        <label class="config-field">
          {{ $t('config.groupLabel') }}
          <select v-model="groupFilter" class="admin-input config-select">
            <option value="all">{{ $t('config.allGroups', { count: configItems.length }) }}</option>
            <option v-for="group in groups" :key="group" :value="group">{{ $t('config.groupOption', { group, count: groupCount(group) }) }}</option>
          </select>
        </label>
      </div>

      <table class="admin-table config-table">
        <thead>
          <tr><th>{{ $t('config.property') }}</th><th>{{ $t('config.value') }}</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in filteredConfig" :key="item.key">
            <td><strong>{{ item.key }}</strong></td>
            <td><pre v-if="item.multiline" class="config-value-block">{{ item.value }}</pre><span v-else>{{ item.value }}</span></td>
          </tr>
        </tbody>
      </table>
      <div v-if="filteredConfig.length === 0" class="empty-state">{{ $t('config.empty') }}</div>
    </section>
  </div>
</template>

<script>
import MetricCard from '@/components/admin/MetricCard.vue'
import { fetchConfigData } from '@/api/client'
import { translateDelta } from '@/utils/i18nLabels'
import eventBus from '@/utils/eventBus'

export default {
  name: 'ConfigPage',
  components: { MetricCard },
  data() {
    return {
      loading: false,
      configItems: [],
      query: '',
      groupFilter: 'all'
    }
  },
  computed: {
    groups() {
      return [...new Set(this.configItems.map((item) => this.groupOf(item.key)).filter(Boolean))].sort()
    },
    filteredConfig() {
      const q = this.query.toLowerCase()
      return this.configItems.filter((item) => {
        const matchedGroup = this.groupFilter === 'all' || this.groupOf(item.key) === this.groupFilter
        return matchedGroup && item.key.toLowerCase().includes(q)
      })
    }
  },
  created() { this.fetchData() },
  mounted() { eventBus.on('admin-refresh', this.fetchData) },
  beforeUnmount() { eventBus.off('admin-refresh', this.fetchData) },
  methods: {
    deltaLabel(value) { return translateDelta(this.$t, value) },
    async fetchData() {
      this.loading = true
      try {
        const data = await fetchConfigData()
        this.configItems = []
        this.flattenData(data || {})
      } finally { this.loading = false }
    },
    groupOf(key) { return String(key || '').split('.')[0] },
    groupCount(group) { return this.configItems.filter((item) => this.groupOf(item.key) === group).length },
    formatValue(value) {
      if (Array.isArray(value)) {
        if (value.length === 0) return { value: '[]', multiline: false }
        const lines = value.map((item) => this.formatArrayItem(item))
        return { value: lines.join('\n'), multiline: lines.length > 1 || lines.some((line) => line.length > 80) }
      }
      if (value && typeof value === 'object') return { value: JSON.stringify(value), multiline: false }
      if (value === null || value === undefined) return { value: '', multiline: false }
      return { value: String(value), multiline: false }
    },
    formatArrayItem(item) {
      if (!item || typeof item !== 'object') return String(item)
      if ('hostname' in item && 'port' in item) {
        const prefix = 'id' in item ? `${item.id}: ` : ''
        return `${prefix}${item.hostname}:${item.port}`
      }
      return Object.entries(item).map(([key, value]) => `${key}=${value}`).join(', ')
    },
    flattenData(data, parent = '') {
      Object.keys(data || {}).forEach((key) => {
        const name = parent ? `${parent}.${key}` : key
        const value = data[key]
        if (value && typeof value === 'object' && !Array.isArray(value)) {
          this.flattenData(value, name)
        } else {
          const formatted = this.formatValue(value)
          this.configItems.push({ key: name, value: formatted.value, multiline: formatted.multiline })
        }
      })
    }
  }
}
</script>

<style scoped>
.config-heading {
  margin-bottom: 12px;
}

.config-toolbar {
  display: grid;
  grid-template-columns: minmax(260px, 1fr) minmax(220px, 320px);
  gap: 12px;
  align-items: end;
  margin-bottom: 14px;
  padding-bottom: 12px;
  border-bottom: 1px solid #edf4e9;
}

.config-field {
  display: grid;
  gap: 5px;
  color: var(--admin-muted);
  font-size: 12px;
  font-weight: 700;
  margin: 0;
}

.config-search,
.config-select {
  width: 100%;
  height: 38px;
}

.config-table {
  table-layout: fixed;
}

.config-table th:first-child,
.config-table td:first-child {
  width: 38%;
}

.config-table td {
  overflow-wrap: anywhere;
  white-space: normal;
}

@media (max-width: 900px) {
  .config-toolbar {
    grid-template-columns: 1fr;
  }
}
</style>
