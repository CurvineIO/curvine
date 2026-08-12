<template>
  <div class="admin-page observability-page" v-loading="loading">
    <section class="admin-panel obs-toolbar-panel">
      <div class="toolbar obs-toolbar">
        <div class="obs-toolbar-filters">
          <div class="toolbar-group">
            <span class="toolbar-label">{{ $t('observability.toolbar.timeRange') }}</span>
            <div class="segmented">
              <button
                v-for="item in timeRanges"
                :key="item.value"
                type="button"
                :class="{ active: selectedRange === item.value }"
                @click="selectedRange = item.value"
              >
                {{ item.label }}
              </button>
            </div>
          </div>
          <div class="toolbar-group">
            <span class="toolbar-label">{{ $t('observability.toolbar.targetFilter') }}</span>
            <div class="segmented">
              <button
                v-for="item in targetFilters"
                :key="item.value"
                type="button"
                :class="{ active: targetFilter === item.value }"
                @click="targetFilter = item.value"
              >
                {{ item.label }}
              </button>
            </div>
          </div>
          <div v-if="showWorkerFilter" class="toolbar-group">
            <span class="toolbar-label">{{ $t('observability.toolbar.worker') }}</span>
            <select v-model="workerFilter" class="admin-input obs-instance-select">
              <option value="">{{ $t('observability.toolbar.allWorkers') }}</option>
              <option v-for="item in workerOptions" :key="item.value" :value="item.value">{{ item.label }}</option>
            </select>
          </div>
          <div v-if="showFuseFilter" class="toolbar-group">
            <span class="toolbar-label">{{ $t('observability.toolbar.fuse') }}</span>
            <select v-model="fuseFilter" class="admin-input obs-instance-select">
              <option value="">{{ $t('observability.toolbar.allFuseMounts') }}</option>
              <option v-for="item in fuseOptions" :key="item.value" :value="item.value">{{ item.label }}</option>
            </select>
          </div>
          <div v-if="showTransferFilter" class="toolbar-group">
            <span class="toolbar-label">{{ $t('observability.toolbar.transfer') }}</span>
            <select v-model="transferFilter" class="admin-input obs-instance-select">
              <option value="">{{ $t('observability.toolbar.allTransferInstances') }}</option>
              <option v-for="item in transferOptions" :key="item.value" :value="item.value">{{ item.label }}</option>
            </select>
          </div>
          <div v-if="targetFilter === 'instance'" class="toolbar-group">
            <span class="toolbar-label">{{ $t('observability.toolbar.instance') }}</span>
            <select v-model="instanceFilter" class="admin-input obs-instance-select">
              <option value="">{{ $t('observability.toolbar.selectInstance') }}</option>
              <option v-for="item in instanceOptions" :key="item" :value="item">{{ item }}</option>
            </select>
          </div>
          <label class="obs-empty-toggle admin-toggle">
            <input v-model="showEmptyPanels" type="checkbox">
            {{ $t('observability.toolbar.showEmptyPanels') }}
          </label>
        </div>
        <div class="obs-toolbar-provider-section">
          <div class="obs-provider-mode-row">
            <span class="toolbar-label">{{ $t('observability.toolbar.providerMode') }}</span>
            <select v-model="draftMode" class="admin-input obs-mode-select" :disabled="modeSaving">
              <option value="builtin">{{ $t('observability.provider.builtin') }}</option>
              <option value="prometheus" :disabled="!health?.prometheus_configured">{{ $t('observability.provider.prometheus') }}</option>
              <option value="auto" :disabled="!health?.prometheus_configured">{{ $t('observability.provider.auto') }}</option>
            </select>
            <button
              type="button"
              class="admin-button primary obs-mode-apply"
              :disabled="modeSaving || !modeDirty"
              @click="applyMode"
            >
              {{ modeSaving ? $t('common.applying') : $t('observability.toolbar.applyMode') }}
            </button>
          </div>
          <div class="obs-provider-status-row" v-if="health">
            <div class="provider-status-head">
              <span class="toolbar-label">{{ $t('observability.toolbar.status') }}</span>
              <span :class="['status-pill', providerStatusClass]">
                {{ providerLabel(health.active_provider || 'builtin') }}
              </span>
              <span
                v-if="health.provider_health"
                :class="['status-pill', providerHealthStatusClass]"
              >
                {{ translateProviderHealthLabel(health.provider_health) }}
              </span>
              <span v-if="!health.enabled" class="provider-tag provider-tag-muted">{{ $t('observability.toolbar.disabled') }}</span>
            </div>
            <div v-if="providerStatusDetails.length" class="provider-status-details">
              <div
                v-for="item in providerStatusDetails"
                :key="item.label"
                class="provider-status-metric"
              >
                <span class="provider-status-metric-label">{{ item.label }}</span>
                <span class="provider-status-metric-value">{{ item.value }}</span>
              </div>
            </div>
          </div>
        </div>
        <div v-if="hasProviderMessages" class="obs-toolbar-messages">
          <p v-if="modeError" class="obs-message obs-message-error">{{ modeError }}</p>
          <p v-else-if="modeNotice" class="obs-message obs-message-ok">{{ modeNotice }}</p>
          <p v-if="health?.fallback_reason" class="obs-message obs-message-warn">{{ health.fallback_reason }}</p>
        </div>
        <p v-if="retentionRangeHint" class="retention-range-hint">{{ retentionRangeHint }}</p>
        <p v-if="sparseDataHint" class="sparse-data-hint">{{ sparseDataHint }}</p>
        <div v-if="chartZoomActive" class="toolbar-group obs-zoom-group">
          <span class="toolbar-label">{{ $t('observability.toolbar.zoomed') }}</span>
          <span class="obs-zoom-range">{{ chartZoomLabel }}</span>
          <button type="button" class="obs-zoom-reset" @click="resetChartZoom">{{ $t('observability.toolbar.resetZoom') }}</button>
        </div>
        <p v-if="!chartZoomActive" class="obs-zoom-hint">{{ $t('observability.toolbar.zoomHint') }}</p>
      </div>
    </section>

    <section v-if="dashboardError" class="admin-panel obs-inline-error-panel">
      <p class="obs-inline-error">{{ dashboardError }}</p>
    </section>

    <section v-if="needsInstanceSelection" class="admin-panel obs-filter-hint-panel">
      <div class="empty-state">{{ $t('observability.collector.selectInstanceHint') }}</div>
    </section>

    <CollectorPanel
      :health="health"
      :health-error="healthError"
      :settings="collectorSettings"
      :targets="filteredTargets"
      :targets-loading="targetsLoading"
      :targets-error="targetsError"
      :metric-names="metricNames"
      :names-error="namesError"
      @settings-updated="onSettingsUpdated"
    />

    <template v-for="group in chartGroups" :key="group.id">
      <section class="obs-group-section">
        <div class="obs-group" :class="{ 'is-expanded': isGroupExpanded(group.id), 'is-collapsed': !isGroupExpanded(group.id) }">
            <button
              type="button"
              class="obs-group-header"
              :class="{ 'is-expanded': isGroupExpanded(group.id), 'is-collapsed': !isGroupExpanded(group.id) }"
              :aria-expanded="isGroupExpanded(group.id)"
              @click="toggleGroup(group.id)"
            >
              <span
                class="obs-collapse-icon obs-collapse-icon--group"
                :class="{ 'is-open': isGroupExpanded(group.id) }"
                aria-hidden="true"
              />
              <span class="obs-group-title">{{ groupTitle(group) }}</span>
              <span class="obs-count-badge">{{ groupChartCount(group) }} {{ $t('observability.chart.charts') }}</span>
              <span class="obs-collapse-label">{{ isGroupExpanded(group.id) ? $t('observability.chart.expanded') : $t('observability.chart.collapsed') }}</span>
            </button>
            <div v-show="isGroupExpanded(group.id)" class="obs-group-body">
              <div
                v-for="section in group.sections"
                :key="`${group.id}-${section.id}`"
                class="obs-section"
                :class="{ 'is-expanded': isSectionExpanded(group.id, section.id), 'is-collapsed': !isSectionExpanded(group.id, section.id) }"
              >
                <button
                  type="button"
                  class="obs-section-header"
                  :class="{ 'is-expanded': isSectionExpanded(group.id, section.id), 'is-collapsed': !isSectionExpanded(group.id, section.id) }"
                  :aria-expanded="isSectionExpanded(group.id, section.id)"
                  @click="toggleSection(group.id, section.id)"
                >
                  <span
                    class="obs-collapse-icon obs-collapse-icon--section"
                    :class="{ 'is-open': isSectionExpanded(group.id, section.id) }"
                    aria-hidden="true"
                  />
                  <span class="obs-section-title">{{ section.title }}</span>
                  <span class="obs-count-badge obs-count-badge--section">{{ section.panels.length }} {{ $t('observability.chart.charts') }}</span>
                  <span class="obs-collapse-label obs-collapse-label--section">{{ isSectionExpanded(group.id, section.id) ? $t('observability.chart.expanded') : $t('observability.chart.collapsed') }}</span>
                </button>
                <div v-show="isSectionExpanded(group.id, section.id)" class="chart-grid">
                  <TimeSeriesChart
                    v-for="panel in section.panels"
                    :key="`${panel.id}-${selectedRange}`"
                    :title="panelTitle(panel)"
                    :subtitle="panel.subtitle"
                    :series="panel.series"
                    :range="selectedRange"
                    :window-start-ms="chartWindowStartMs"
                    :window-end-ms="chartWindowEndMs"
                    :zoom-start-ms="chartZoomStartMs"
                    :zoom-end-ms="chartZoomEndMs"
                    :loading="dashboardLoading"
                    :error="panel.error"
                    :value-formatter="panel.valueFormatter"
                    :empty-message="showEmptyPanels ? emptyPanelMessage : undefined"
                    :panel-id="panel.id"
                    compact
                    @zoom-change="onChartZoomChange"
                  />
                </div>
              </div>
            </div>
          </div>
      </section>
    </template>
  </div>
</template>

<script>
import CollectorPanel from '@/components/observability/CollectorPanel.vue'
import TimeSeriesChart from '@/components/observability/TimeSeriesChart.vue'
import {
  fetchObservabilityDashboard,
  fetchObservabilityHealth,
  fetchObservabilityNames,
  fetchObservabilitySettings,
  fetchObservabilityTargets,
  updateObservabilityMode
} from '@/api/client'
import eventBus from '@/utils/eventBus'
import {
  formatObservabilityRelative,
  translateGroupTitle,
  translatePanelTitle,
  translateProviderHealth
} from '@/utils/observabilityI18n'
import {
  TIME_RANGES,
  TARGET_FILTERS,
  DASHBOARD_SECTION_DEFS,
  stepForRange,
  rangeToMs,
  seriesInstanceKey,
  groupMatchesTargetFilter,
  matchesTargetFilter,
  matchesWorkerFilter,
  matchesFuseFilter,
  matchesTransferFilter,
  resolveInstanceComponent,
  workerFilterOptionLabel,
  fuseFilterOptionLabel,
  transferFilterOptionLabel,
  shortenInstanceLabel,
  panelValueFormatter,
  shouldShowDashboardPanel
} from '@/views/observability/metrics'

const SHOW_EMPTY_PANELS_KEY = 'observability-show-empty-panels'

function normalizePoints(points) {
  if (!Array.isArray(points)) return []
  return points
    .map((point) => {
      if (Array.isArray(point) && point.length >= 2) {
        return [Number(point[0]), Number(point[1])]
      }
      if (point && typeof point === 'object') {
        return [Number(point.timestamp_ms), Number(point.value)]
      }
      return null
    })
    .filter((point) => point && Number.isFinite(point[0]) && Number.isFinite(point[1]))
}

function normalizeSummary(summary) {
  if (!summary || typeof summary !== 'object') return null
  return {
    current: summary.current ?? null,
    min: summary.min ?? null,
    max: summary.max ?? null,
    avg: summary.avg ?? null
  }
}

export default {
  name: 'ObservabilityPage',
  components: { CollectorPanel, TimeSeriesChart },
  data() {
    return {
      loading: false,
      health: null,
      healthError: '',
      collectorSettings: null,
      targets: [],
      targetsLoading: false,
      targetsError: '',
      metricNames: [],
      namesError: '',
      dashboard: null,
      dashboardError: '',
      dashboardLoading: false,
      dashboardRequestId: 0,
      selectedRange: '15m',
      targetFilter: 'all',
      instanceFilter: '',
      workerFilter: '',
      fuseFilter: '',
      transferFilter: '',
      showEmptyPanels: false,
      chartWindowStartMs: 0,
      chartWindowEndMs: Date.now(),
      chartZoomStartMs: 0,
      chartZoomEndMs: 0,
      collapsedGroups: {},
      collapsedSections: {},
      draftMode: 'builtin',
      modeSaving: false,
      modeError: '',
      modeNotice: ''
    }
  },
  computed: {
    timeRanges() {
      return TIME_RANGES.map((item) => ({ ...item, label: this.$t(item.labelKey) }))
    },
    targetFilters() {
      return TARGET_FILTERS.map((item) => ({ ...item, label: this.$t(item.labelKey) }))
    },
    filteredTargets() {
      return (this.targets || []).filter((target) => this.matchesRowFilter({
        component: target.component,
        instance: target.instance || ''
      }))
    },
    instanceOptions() {
      const instances = new Set()
      this.targets.forEach((target) => {
        if (target.instance) instances.add(target.instance)
      })
      return Array.from(instances).sort()
    },
    workerOptions() {
      return (this.targets || [])
        .filter((target) => target.component === 'worker' && target.instance)
        .map((target) => ({
          value: target.instance,
          label: workerFilterOptionLabel(target.instance, target.worker_id)
        }))
        .sort((left, right) => left.label.localeCompare(right.label))
    },
    showWorkerFilter() {
      return this.targetFilter === 'all' || this.targetFilter === 'worker'
    },
    fuseOptions() {
      return (this.targets || [])
        .filter((target) => target.component === 'fuse' && target.instance)
        .map((target) => ({
          value: target.instance,
          label: fuseFilterOptionLabel(target.instance)
        }))
        .sort((left, right) => left.label.localeCompare(right.label))
    },
    showFuseFilter() {
      return this.targetFilter === 'all' || this.targetFilter === 'fuse'
    },
    transferOptions() {
      return (this.targets || [])
        .filter((target) => target.component === 'transfer' && target.instance)
        .map((target) => ({
          value: target.instance,
          label: transferFilterOptionLabel(target.instance)
        }))
        .sort((left, right) => left.label.localeCompare(right.label))
    },
    showTransferFilter() {
      return this.targetFilter === 'all' || this.targetFilter === 'transfer'
    },
    filteredInstanceComponent() {
      return resolveInstanceComponent(this.targets, this.instanceFilter)
    },
    emptyPanelMessage() {
      return this.$t('observability.collector.emptyPanel')
    },
    chartGroups() {
      const groups = Array.isArray(this.dashboard?.groups) ? this.dashboard.groups : []
      return groups
        .map((group) => {
          const timeseriesPanels = (group.panels || [])
            .filter((panel) => panel.kind === 'timeseries')
            .map((panel) => ({
              id: panel.id,
              title: panel.title,
              subtitle: panel.subtitle,
              section: panel.section || null,
              series: this.filterDashboardSeries(panel.series || []),
              error: panel.error || '',
              valueFormatter: panelValueFormatter(panel.id)
            }))
            .filter((panel) => shouldShowDashboardPanel(panel, {
              showEmptyPanels: this.showEmptyPanels,
              dashboardLoading: this.dashboardLoading
            }))
          const sections = this.buildGroupSections(group.id, timeseriesPanels)
          return { ...group, sections, timeseriesPanels }
        })
        .filter((group) => {
          if (!groupMatchesTargetFilter(
            group.id,
            this.targetFilter,
            this.instanceFilter,
            this.targets
          )) {
            return false
          }
          return group.sections.length > 0
        })
    },
    needsInstanceSelection() {
      return this.targetFilter === 'instance' && !this.instanceFilter
    },
    providerStatusClass() {
      if (!this.health?.enabled) return 'failed'
      const status = (this.health.provider_health || '').toLowerCase()
      if (status === 'healthy') return 'healthy'
      if (status === 'unhealthy' || status === 'stopped') return 'failed'
      if (status === 'warning') return 'warning'
      return this.health.fallback_reason ? 'warning' : 'healthy'
    },
    providerHealthStatusClass() {
      const status = (this.health?.provider_health || '').toLowerCase()
      if (status === 'healthy') return 'healthy'
      if (status === 'unhealthy' || status === 'stopped') return 'failed'
      if (status === 'warning') return 'warning'
      return 'healthy'
    },
    providerStatusDetails() {
      if (!this.health) return []
      const details = []
      const active = this.health.active_provider || 'builtin'
      const configured = this.health.mode || ''
      if (configured && configured !== active) {
        details.push({ label: this.$t('observability.provider.configured'), value: configured })
      }
      if (this.health.prometheus_url && (active === 'prometheus' || configured === 'prometheus' || configured === 'auto')) {
        details.push({ label: this.$t('observability.provider.endpoint'), value: this.health.prometheus_url })
      }
      if (this.health.last_collect_ms) {
        details.push({ label: this.$t('observability.provider.lastCollect'), value: this.formatRelative(this.health.last_collect_ms) })
      }
      if (this.health.last_discovery_ms) {
        details.push({ label: this.$t('observability.provider.lastDiscovery'), value: this.formatRelative(this.health.last_discovery_ms) })
      }
      return details
    },
    hasProviderMessages() {
      return Boolean(this.modeError || this.modeNotice || this.health?.fallback_reason)
    },
    modeDirty() {
      if (!this.health?.mode) return false
      return this.draftMode !== this.health.mode
    },
    retentionRangeHint() {
      if (!this.health || this.health.active_provider === 'prometheus') return ''
      const windowMs = rangeToMs(this.selectedRange)
      if (windowMs <= this.health.retention_ms) return ''
      return this.$t('observability.hints.retentionExceeded', {
        retention: this.formatDuration(this.health.retention_ms)
      })
    },
    dataCoverageMs() {
      const groups = this.dashboard?.groups
      if (!Array.isArray(groups)) return 0
      let minTs = Infinity
      let maxTs = -Infinity
      groups.forEach((group) => {
        (group.panels || []).forEach((panel) => {
          if (panel.kind !== 'timeseries') return
          ;(panel.series || []).forEach((series) => {
            ;(series.points || []).forEach((point) => {
              const ts = Array.isArray(point) ? Number(point[0]) : Number(point?.timestamp_ms)
              if (!Number.isFinite(ts)) return
              minTs = Math.min(minTs, ts)
              maxTs = Math.max(maxTs, ts)
            })
          })
        })
      })
      if (!Number.isFinite(minTs) || !Number.isFinite(maxTs) || maxTs <= minTs) return 0
      return maxTs - minTs
    },
    sparseDataHint() {
      const window = this.dashboard?.window
      const coverage = this.dataCoverageMs
      if (!window || !coverage) return ''
      const querySpan = Number(window.end_ms) - Number(window.start_ms)
      if (querySpan <= 0 || coverage >= querySpan * 0.25) return ''
      const rangeLabel = this.timeRanges.find((item) => item.value === (window.range || this.selectedRange))?.label
        || window.range
        || this.selectedRange
      return this.$t('observability.hints.sparseData', {
        coverage: this.formatDuration(coverage),
        range: rangeLabel
      })
    },
    chartZoomActive() {
      const start = Number(this.chartZoomStartMs)
      const end = Number(this.chartZoomEndMs)
      return Number.isFinite(start) && start > 0
        && Number.isFinite(end) && end > start
    },
    chartZoomLabel() {
      if (!this.chartZoomActive) return ''
      return `${this.formatTs(this.chartZoomStartMs)} – ${this.formatTs(this.chartZoomEndMs)}`
    }
  },
  watch: {
    selectedRange() {
      this.resetChartZoom()
      this.fetchDashboard()
    },
    targetFilter(value) {
      if (value !== 'instance') {
        this.instanceFilter = ''
      } else {
        this.ensureInstanceSelection()
      }
      if (value === 'master' || value === 'instance') {
        this.workerFilter = ''
      }
      if (value === 'fuse' || value === 'instance') {
        this.fuseFilter = ''
      }
      if (value !== 'all' && value !== 'transfer') {
        this.transferFilter = ''
      }
    },
    instanceOptions() {
      this.ensureInstanceSelection()
    },
    workerOptions() {
      this.ensureWorkerSelection()
    },
    fuseOptions() {
      this.ensureFuseSelection()
    },
    transferOptions() {
      this.ensureTransferSelection()
    },
    showEmptyPanels(value) {
      try {
        localStorage.setItem(SHOW_EMPTY_PANELS_KEY, value ? '1' : '0')
      } catch {
        // ignore storage failures
      }
    }
  },
  created() {
    try {
      this.showEmptyPanels = localStorage.getItem(SHOW_EMPTY_PANELS_KEY) === '1'
    } catch {
      this.showEmptyPanels = false
    }
    this.fetchAll()
  },
  mounted() {
    eventBus.on('admin-refresh', this.fetchAll)
  },
  beforeUnmount() {
    eventBus.off('admin-refresh', this.fetchAll)
  },
  methods: {
    groupTitle(group) {
      return translateGroupTitle(this.$t, this.$te, group.id, group.title)
    },
    panelTitle(panel) {
      return translatePanelTitle(this.$t, this.$te, panel.id, panel.title)
    },
    translateProviderHealthLabel(status) {
      return translateProviderHealth(this.$t, status)
    },
    providerLabel(value) {
      const key = `observability.provider.${String(value || 'builtin').toLowerCase()}`
      return this.$te(key) ? this.$t(key) : value
    },
    ensureInstanceSelection() {
      if (this.targetFilter !== 'instance') return
      if (this.instanceFilter && this.instanceOptions.includes(this.instanceFilter)) return
      this.instanceFilter = this.instanceOptions[0] || ''
    },
    ensureWorkerSelection() {
      if (!this.showWorkerFilter) {
        this.workerFilter = ''
        return
      }
      if (!this.workerFilter) return
      const values = this.workerOptions.map((item) => item.value)
      if (!values.includes(this.workerFilter)) {
        this.workerFilter = ''
      }
    },
    ensureFuseSelection() {
      if (!this.showFuseFilter) {
        this.fuseFilter = ''
        return
      }
      if (!this.fuseFilter) return
      const values = this.fuseOptions.map((item) => item.value)
      if (!values.includes(this.fuseFilter)) {
        this.fuseFilter = ''
      }
    },
    ensureTransferSelection() {
      if (!this.showTransferFilter) {
        this.transferFilter = ''
        return
      }
      if (!this.transferFilter) return
      const values = this.transferOptions.map((item) => item.value)
      if (!values.includes(this.transferFilter)) {
        this.transferFilter = ''
      }
    },
    sectionKey(groupId, sectionId) {
      return `${groupId}:${sectionId}`
    },
    isGroupExpanded(groupId) {
      return !this.collapsedGroups[groupId]
    },
    toggleGroup(groupId) {
      this.collapsedGroups[groupId] = !this.collapsedGroups[groupId]
    },
    groupChartCount(group) {
      return (group.sections || []).reduce(
        (sum, section) => sum + (section.panels?.length || 0),
        0
      )
    },
    isSectionExpanded(groupId, sectionId) {
      const key = this.sectionKey(groupId, sectionId)
      return !this.collapsedSections[key]
    },
    toggleSection(groupId, sectionId) {
      const key = this.sectionKey(groupId, sectionId)
      this.collapsedSections[key] = !this.collapsedSections[key]
    },
    buildGroupSections(groupId, panels) {
      const defs = DASHBOARD_SECTION_DEFS[groupId] || []
      const buckets = new Map()
      panels.forEach((panel) => {
        const sectionId = panel.section || 'other'
        if (!buckets.has(sectionId)) buckets.set(sectionId, [])
        buckets.get(sectionId).push(panel)
      })
      const sections = defs
        .map((def) => ({
          id: def.id,
          title: this.$t(def.titleKey),
          panels: buckets.get(def.id) || []
        }))
        .filter((section) => section.panels.length > 0)
      const otherPanels = buckets.get('other') || []
      if (otherPanels.length) {
        sections.push({ id: 'other', title: this.$t('observability.sections.other'), panels: otherPanels })
      }
      return sections
    },
    matchesRowFilter(row) {
      if (!matchesTargetFilter(
        this.targetFilter,
        this.instanceFilter,
        row.component,
        row.instance || '',
        this.filteredInstanceComponent,
        this.fuseFilter
      )) {
        return false
      }
      return matchesWorkerFilter(
        this.workerFilter,
        row.component,
        row.instance || '',
        {
          instance: row.instance,
          worker_id: row.worker_id != null ? String(row.worker_id) : undefined
        }
      ) && matchesFuseFilter(
        this.fuseFilter,
        row.component,
        row.instance || '',
        {
          instance: row.instance
        }
      ) && matchesTransferFilter(
        this.transferFilter,
        row.component,
        row.instance || '',
        {
          instance: row.instance
        }
      )
    },
    filterDashboardSeries(series) {
      return (series || [])
        .map((item) => ({
          name: item.name,
          labels: item.labels || {},
          points: normalizePoints(item.points),
          summary: normalizeSummary(item.summary)
        }))
        .filter((item) => {
          const labels = item.labels || {}
          const seriesKey = seriesInstanceKey(item)
          if (!matchesTargetFilter(
            this.targetFilter,
            this.instanceFilter,
            labels.component,
            seriesKey,
            this.filteredInstanceComponent,
            this.fuseFilter
          )) {
            return false
          }
          return matchesWorkerFilter(
            this.workerFilter,
            labels.component,
            seriesKey,
            labels
          ) && matchesFuseFilter(
            this.fuseFilter,
            labels.component,
            seriesKey,
            labels
          ) && matchesTransferFilter(
            this.transferFilter,
            labels.component,
            seriesKey,
            labels
          )
        })
    },
    formatTs(value) {
      if (!value) return '-'
      const date = new Date(Number(value))
      if (Number.isNaN(date.getTime())) return '-'
      return date.toLocaleString()
    },
    formatRelative(value) {
      return formatObservabilityRelative(this.$t, value)
    },
    formatDuration(ms) {
      const value = Number(ms || 0)
      if (!value) return '-'
      if (value < 60_000) return `${Math.round(value / 1000)}s`
      if (value < 3_600_000) return `${Math.round(value / 60_000)}m`
      if (value < 86_400_000) return `${Math.round(value / 3_600_000)}h`
      return `${Math.round(value / 86_400_000)}d`
    },
    onChartZoomChange(range) {
      if (!range) {
        this.resetChartZoom()
        return
      }
      const start = Number(range.startMs)
      const end = Number(range.endMs)
      if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) return
      this.chartZoomStartMs = start
      this.chartZoomEndMs = end
    },
    resetChartZoom() {
      this.chartZoomStartMs = 0
      this.chartZoomEndMs = 0
    },
    async onSettingsUpdated(settings) {
      this.collectorSettings = settings
      await Promise.all([
        this.fetchHealth(),
        this.fetchDashboard(),
        this.fetchNames(),
      ])
    },
    syncChartWindowEnd() {
      const window = this.dashboard?.window
      const fromStart = Number(window?.start_ms)
      const fromEnd = Number(window?.end_ms)
      const fromHealth = Number(this.health?.last_collect_ms)
      if (Number.isFinite(fromStart) && fromStart > 0) {
        this.chartWindowStartMs = fromStart
      } else if (Number.isFinite(fromEnd) && fromEnd > 0) {
        this.chartWindowStartMs = fromEnd - rangeToMs(this.selectedRange)
      }
      const endCandidate = Number.isFinite(fromEnd) && fromEnd > 0
        ? fromEnd
        : fromHealth
      this.chartWindowEndMs = Number.isFinite(endCandidate) && endCandidate > 0
        ? endCandidate
        : Date.now()
    },
    async fetchAll() {
      const prevCollectMs = this.health?.last_collect_ms
      this.loading = true
      try {
        await Promise.all([
          this.fetchSettings(),
          this.fetchNames(),
          this.fetchHealth(),
          this.fetchTargets()
        ])
        const collectChanged = this.health?.last_collect_ms !== prevCollectMs
        if (collectChanged || !this.dashboard) {
          await this.fetchDashboard()
        }
      } finally {
        this.loading = false
      }
    },
    async fetchSettings() {
      try {
        this.collectorSettings = await fetchObservabilitySettings()
      } catch (err) {
        this.collectorSettings = null
      }
    },
    async fetchTargets() {
      this.targetsLoading = true
      try {
        this.targets = await fetchObservabilityTargets()
        this.targetsError = ''
      } catch (err) {
        this.targets = []
        this.targetsError = String(err?.message || err)
      } finally {
        this.targetsLoading = false
      }
    },
    async fetchHealth() {
      try {
        this.health = await fetchObservabilityHealth()
        this.healthError = ''
        if (!this.modeSaving && this.health?.mode) {
          this.draftMode = this.health.mode
        }
      } catch (err) {
        this.health = null
        this.healthError = String(err?.message || err)
      }
    },
    async applyMode() {
      if (!this.modeDirty) return
      this.modeSaving = true
      this.modeError = ''
      this.modeNotice = ''
      try {
        this.health = await updateObservabilityMode(this.draftMode)
        this.healthError = ''
        this.modeNotice = this.$t('observability.messages.modeSet', { mode: this.draftMode })
        await Promise.all([
          this.fetchNames(),
          this.fetchTargets(),
          this.fetchDashboard(),
          fetchObservabilitySettings().then((settings) => {
            this.collectorSettings = settings
          }).catch(() => {
            this.collectorSettings = null
          })
        ])
      } catch (err) {
        this.modeError = String(err?.message || err)
        if (this.health?.mode) {
          this.draftMode = this.health.mode
        }
      } finally {
        this.modeSaving = false
      }
    },
    async fetchNames() {
      try {
        this.metricNames = await fetchObservabilityNames()
        this.namesError = ''
      } catch (err) {
        this.metricNames = []
        this.namesError = String(err?.message || err)
      }
    },
    async fetchDashboard() {
      const requestId = ++this.dashboardRequestId
      this.dashboardLoading = true
      this.dashboardError = ''
      try {
        const payload = await fetchObservabilityDashboard({
          range: this.selectedRange,
          step: stepForRange(this.selectedRange)
        })
        if (requestId !== this.dashboardRequestId) return
        this.dashboard = payload
        this.syncChartWindowEnd()
        this.ensureInstanceSelection()
        this.ensureWorkerSelection()
      } catch (err) {
        if (requestId !== this.dashboardRequestId) return
        this.dashboard = null
        this.dashboardError = String(err?.message || err)
      } finally {
        if (requestId === this.dashboardRequestId) {
          this.dashboardLoading = false
        }
      }
    }
  }
}
</script>

<style scoped>
.observability-page { min-width: 0; }
.obs-toolbar-panel { padding: 10px 16px 12px; }
.obs-toolbar {
  margin-bottom: 0;
  padding: 0;
  background: transparent;
  border: 0;
  display: flex;
  flex-direction: column;
  gap: 8px;
  align-items: stretch;
}
.obs-toolbar-filters {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  width: 100%;
}
.obs-empty-toggle {
  margin-left: auto;
  white-space: nowrap;
}
.obs-toolbar-provider-section {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 16px 32px;
  width: 100%;
  padding: 8px 0 2px;
  border-top: 1px solid var(--admin-border);
}
.obs-provider-mode-row {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 18px;
  min-width: 0;
}
.obs-provider-status-row {
  display: flex;
  flex-direction: column;
  gap: 6px;
  justify-self: end;
  align-items: flex-end;
  text-align: right;
  max-width: min(100%, 560px);
}
.provider-status-head {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 8px;
}
.provider-status-details {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 6px 24px;
}
.provider-status-metric {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 2px;
  min-width: 88px;
}
.provider-status-metric-label {
  color: var(--admin-muted);
  font-size: 11px;
  font-weight: 700;
  line-height: 1.3;
}
.provider-status-metric-value {
  color: var(--admin-text);
  font-size: 12px;
  line-height: 1.35;
  overflow-wrap: anywhere;
}
.obs-toolbar .admin-input.obs-mode-select {
  min-width: 108px;
  max-width: 124px;
  width: 124px;
  height: 32px;
  padding: 0 8px;
  font-size: 12px;
}
.obs-mode-apply {
  min-width: 84px;
  height: 32px;
  padding: 0 10px;
  font-size: 12px;
  font-weight: 650;
}
.provider-tag {
  display: inline-flex;
  align-items: center;
  min-height: 22px;
  padding: 0 7px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.02em;
}
.provider-tag-muted {
  color: var(--admin-muted);
  background: #eef2ea;
}
.obs-toolbar-messages {
  display: flex;
  flex-direction: column;
  gap: 6px;
  width: 100%;
}
.obs-message {
  margin: 0;
  padding: 6px 12px;
  border-radius: var(--admin-radius);
  font-size: 12px;
  line-height: 1.4;
}
.obs-message-error {
  color: var(--admin-danger, #c53030);
  background: var(--admin-danger-soft, #fde8e8);
  border: 1px solid #f5c2c2;
}
.obs-message-ok {
  color: var(--admin-success, #2f855a);
  background: #edf7f0;
  border: 1px solid #c6e7d0;
}
.obs-message-warn {
  color: var(--admin-warning);
  background: var(--admin-warning-soft);
  border: 1px solid #ead9a8;
}
.toolbar-group { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.toolbar-label { color: var(--admin-muted); font-size: 12px; font-weight: 700; white-space: nowrap; }
.obs-toolbar .admin-input.obs-instance-select { min-width: 210px; max-width: 340px; }
.retention-range-hint {
  flex: 1 1 100%;
  margin: 0;
  color: var(--admin-warning);
  font-size: 12px;
}
.sparse-data-hint {
  flex: 1 1 100%;
  margin: 0;
  color: var(--admin-muted);
  font-size: 12px;
}
.obs-zoom-group {
  flex: 1 1 100%;
  margin-top: 2px;
}
.obs-zoom-range {
  font-size: 12px;
  font-weight: 650;
  color: var(--admin-text);
}
.obs-zoom-reset {
  border: 1px solid var(--admin-border);
  border-radius: 6px;
  background: #fff;
  color: var(--admin-accent);
  font-size: 12px;
  font-weight: 650;
  padding: 4px 10px;
  cursor: pointer;
}
.obs-zoom-reset:hover {
  background: var(--admin-surface-soft);
  border-color: #c5d8b8;
}
.obs-zoom-hint {
  flex: 1 1 100%;
  margin: 0;
  color: var(--admin-muted);
  font-size: 12px;
}
.obs-metric-grid { margin-bottom: 4px; }
.obs-group-section {
  display: grid;
  gap: 16px;
  margin-top: 14px;
}
.obs-group-section:first-of-type { margin-top: 10px; }
.obs-group {
  border: 1px solid var(--admin-border-strong);
  border-radius: calc(var(--admin-radius) + 2px);
  background: var(--admin-surface);
  overflow: hidden;
  transition: box-shadow 0.2s ease, border-color 0.2s ease;
}
.obs-group.is-collapsed {
  box-shadow: 0 1px 3px rgba(27, 38, 31, 0.05);
}
.obs-group.is-expanded {
  border-color: #c5d8b8;
  box-shadow: 0 4px 14px rgba(79, 143, 52, 0.08);
}
.obs-group-header,
.obs-section-header {
  width: 100%;
  display: flex;
  align-items: center;
  gap: 12px;
  border: 0;
  color: var(--admin-text);
  cursor: pointer;
  text-align: left;
  transition: background 0.15s ease, border-color 0.15s ease;
}
.obs-group-header {
  padding: 16px 18px;
}
.obs-group-header.is-collapsed {
  background: linear-gradient(180deg, #ffffff 0%, var(--admin-surface-soft) 100%);
}
.obs-group-header.is-expanded {
  background: #fff;
  border-bottom: 1px solid var(--admin-border);
  box-shadow: inset 3px 0 0 var(--admin-primary);
}
.obs-group-header:hover,
.obs-section-header:hover {
  background: #f3f8ee;
}
.obs-group-header.is-expanded:hover {
  background: #f8fbf4;
}
.obs-group-title {
  font-size: 18px;
  font-weight: 700;
  letter-spacing: -0.01em;
}
.obs-group-body {
  display: grid;
  gap: 10px;
  padding: 12px;
  background: linear-gradient(180deg, #f9fbf7 0%, var(--admin-bg) 100%);
}
.obs-section {
  border: 1px solid var(--admin-border);
  border-radius: var(--admin-radius);
  background: var(--admin-surface);
  overflow: hidden;
  transition: box-shadow 0.2s ease, border-color 0.2s ease;
}
.obs-section.is-collapsed {
  background: #fcfdfb;
}
.obs-section.is-expanded {
  border-color: #d4e2cb;
  box-shadow: 0 2px 8px rgba(27, 38, 31, 0.04);
}
.obs-section-header {
  padding: 12px 14px;
}
.obs-section-header.is-collapsed {
  background: var(--admin-surface-soft);
}
.obs-section-header.is-expanded {
  background: #fff;
  border-bottom: 1px solid var(--admin-border);
  box-shadow: inset 3px 0 0 #9bc77a;
}
.obs-section-title {
  font-size: 15px;
  font-weight: 700;
}
.obs-count-badge {
  margin-left: auto;
  padding: 4px 10px;
  border-radius: 999px;
  background: var(--admin-primary-soft);
  border: 1px solid #d4e8c4;
  color: var(--admin-primary-strong);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.02em;
  white-space: nowrap;
}
.obs-count-badge--section {
  background: #f4f7f1;
  border-color: var(--admin-border);
  color: #4d6248;
}
.obs-collapse-label {
  padding: 4px 8px;
  border-radius: 6px;
  background: #eef2ea;
  color: var(--admin-muted);
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  white-space: nowrap;
}
.obs-collapse-label--section {
  background: transparent;
  border: 1px dashed var(--admin-border);
}
.obs-group-header.is-expanded .obs-collapse-label,
.obs-section-header.is-expanded .obs-collapse-label {
  background: var(--admin-primary-soft);
  color: var(--admin-primary-strong);
  border-color: transparent;
}
.obs-collapse-icon {
  flex: 0 0 auto;
  position: relative;
  border-radius: 8px;
  border: 1px solid var(--admin-border-strong);
  background: #fff;
  transition: transform 0.2s ease, background 0.15s ease, border-color 0.15s ease;
}
.obs-collapse-icon--group {
  width: 32px;
  height: 32px;
}
.obs-collapse-icon--section {
  width: 28px;
  height: 28px;
}
.obs-collapse-icon::before {
  content: '';
  position: absolute;
  left: 50%;
  top: 50%;
  width: 7px;
  height: 7px;
  border-right: 2px solid #5f7358;
  border-bottom: 2px solid #5f7358;
  /* Collapsed: chevron points right (▸) */
  transform: translate(-60%, -50%) rotate(-45deg);
  transition: transform 0.2s ease, border-color 0.15s ease;
}
.obs-collapse-icon.is-open {
  background: var(--admin-primary-soft);
  border-color: #b8d6a2;
}
.obs-collapse-icon.is-open::before {
  border-color: var(--admin-primary-strong);
  /* Expanded: chevron points down (▾) */
  transform: translate(-50%, -40%) rotate(45deg);
}
.obs-group-header:focus-visible,
.obs-section-header:focus-visible {
  outline: none;
  box-shadow: inset 0 0 0 2px rgba(111, 174, 63, 0.35);
}
.obs-section .chart-grid {
  padding: 12px;
  background: #fff;
}
.chart-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  min-width: 0;
}
.obs-filter-hint-panel { padding: 16px; }
.obs-filter-hint-panel .empty-state { margin: 0; }
.obs-inline-error-panel { padding: 12px 16px; }
.obs-inline-error {
  padding: 12px;
  border-radius: var(--admin-radius);
  background: var(--admin-danger-soft);
  color: var(--admin-danger);
  font-size: 13px;
  margin: 0;
}
@media (max-width: 900px) {
  .obs-collapse-label { display: none; }
}
@media (max-width: 1500px) {
  .chart-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
}
@media (max-width: 1100px) {
  .chart-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
@media (max-width: 760px) {
  .chart-grid { grid-template-columns: 1fr; }
  .obs-toolbar-provider-section {
    grid-template-columns: 1fr;
    gap: 10px;
  }
  .obs-provider-status-row {
    justify-self: stretch;
    align-items: flex-start;
    text-align: left;
    max-width: none;
  }
  .provider-status-head {
    justify-content: flex-start;
  }
  .provider-status-details {
    justify-content: flex-start;
  }
  .provider-status-metric {
    align-items: flex-start;
  }
}
</style>
