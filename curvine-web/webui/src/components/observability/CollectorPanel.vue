<template>
  <section class="obs-group-section">
    <div class="obs-group" :class="{ 'is-expanded': expanded, 'is-collapsed': !expanded }">
      <button
        type="button"
        class="obs-group-header"
        :class="{ 'is-expanded': expanded, 'is-collapsed': !expanded }"
        :aria-expanded="expanded"
        @click="expanded = !expanded"
      >
        <span
          class="obs-collapse-icon obs-collapse-icon--group"
          :class="{ 'is-open': expanded }"
          aria-hidden="true"
        />
        <span class="obs-group-title">{{ $t('observability.collector.title') }}</span>
        <span class="obs-count-badge">{{ targetsUpLabel }}</span>
        <span class="obs-collapse-label">{{ expanded ? $t('observability.chart.expanded') : $t('observability.chart.collapsed') }}</span>
      </button>

      <div v-show="expanded" class="obs-group-body">
        <div
          v-if="showBuiltinSettings"
          class="obs-section"
          :class="{ 'is-expanded': settingsOpen, 'is-collapsed': !settingsOpen }"
        >
          <button
            type="button"
            class="obs-section-header"
            :class="{ 'is-expanded': settingsOpen, 'is-collapsed': !settingsOpen }"
            :aria-expanded="settingsOpen"
            @click="settingsOpen = !settingsOpen"
          >
            <span
              class="obs-collapse-icon obs-collapse-icon--section"
              :class="{ 'is-open': settingsOpen }"
              aria-hidden="true"
            />
            <span class="obs-section-title">{{ $t('observability.collector.settings') }}</span>
            <span class="obs-count-badge obs-count-badge--section">{{ $t('observability.collector.settingsBadge') }}</span>
            <span class="obs-collapse-label obs-collapse-label--section">{{ settingsOpen ? $t('observability.chart.expanded') : $t('observability.chart.collapsed') }}</span>
          </button>
          <div v-show="settingsOpen" class="collector-section-body">
            <form class="collector-settings" @submit.prevent="applySettings">
              <div class="collector-settings-field">
                <span class="collector-field-label">{{ $t('observability.collector.scrapeInterval') }}</span>
                <select v-model.number="draftScrapeIntervalMs" class="admin-input" :disabled="settingsSaving">
                  <option
                    v-for="value in settings?.scrape_interval_options_ms || []"
                    :key="`scrape-${value}`"
                    :value="value"
                  >
                    {{ formatDuration(value) }}
                  </option>
                </select>
              </div>
              <div class="collector-settings-field">
                <span class="collector-field-label">{{ $t('observability.collector.retention') }}</span>
                <select v-model.number="draftRetentionMs" class="admin-input" :disabled="settingsSaving">
                  <option
                    v-for="value in settings?.retention_options_ms || []"
                    :key="`retention-${value}`"
                    :value="value"
                  >
                    {{ formatDuration(value) }}
                  </option>
                </select>
              </div>
              <div class="collector-settings-field collector-settings-action">
                <span class="collector-field-label" aria-hidden="true">{{ $t('observability.collector.action') }}</span>
                <button type="submit" class="admin-button primary settings-apply-btn" :disabled="settingsSaving || !settingsDirty">
                  {{ settingsSaving ? $t('common.saving') : $t('common.apply') }}
                </button>
              </div>
            </form>
            <p v-if="health" class="collector-meta">
              {{ $t('observability.collector.seriesSamples', {
                series: health.series_count,
                samples: formatSampleCount(health.sample_count)
              }) }}
              · {{ $t('observability.collector.lastScrape', { time: formatRelative(health.last_collect_ms) }) }}
            </p>
            <p v-if="settingsError" class="collector-hint error">{{ settingsError }}</p>
            <p v-else-if="settingsNotice" class="collector-hint ok">{{ settingsNotice }}</p>
            <p v-if="healthError" class="collector-hint error">{{ healthError }}</p>
          </div>
        </div>

        <div
          v-if="targetsLoading || targetsError || problemTargets.length"
          class="obs-section"
          :class="{ 'is-expanded': issuesOpen, 'is-collapsed': !issuesOpen }"
        >
          <button
            type="button"
            class="obs-section-header"
            :class="{ 'is-expanded': issuesOpen, 'is-collapsed': !issuesOpen }"
            :aria-expanded="issuesOpen"
            @click="issuesOpen = !issuesOpen"
          >
            <span
              class="obs-collapse-icon obs-collapse-icon--section"
              :class="{ 'is-open': issuesOpen }"
              aria-hidden="true"
            />
            <span class="obs-section-title">{{ $t('observability.collector.issues') }}</span>
            <span class="obs-count-badge obs-count-badge--section">{{ $t('observability.collector.alertsCount', { count: problemTargets.length }) }}</span>
            <span class="obs-collapse-label obs-collapse-label--section">{{ issuesOpen ? $t('observability.chart.expanded') : $t('observability.chart.collapsed') }}</span>
          </button>
          <div v-show="issuesOpen" class="collector-section-body">
            <p v-if="targetsError" class="collector-hint error">{{ targetsError }}</p>
            <div v-else-if="targetsLoading" class="collector-empty">{{ $t('observability.collector.loadingTargets') }}</div>
            <ul v-else class="target-list">
              <li
                v-for="row in problemTargets"
                :key="row.id"
                class="target-row"
                :class="{ down: !row.scrape_up }"
              >
                <span class="target-instance" :title="row.instance">{{ shortInstance(row) }}</span>
                <span :class="['status-pill', row.scrape_up ? 'healthy' : 'failed']">
                  {{ row.scrape_up ? $t('observability.targets.up') : $t('observability.targets.down') }}
                </span>
                <span v-if="row.worker_state && row.component === 'worker'" class="target-tag">
                  {{ workerStateLabel(row.worker_state) }}
                </span>
                <span v-if="row.detail" class="target-detail" :title="row.detail">{{ row.detail }}</span>
              </li>
            </ul>
          </div>
        </div>

        <div
          v-if="metricNames.length || namesError"
          class="obs-section"
          :class="{ 'is-expanded': metricNamesOpen, 'is-collapsed': !metricNamesOpen }"
        >
          <button
            type="button"
            class="obs-section-header"
            :class="{ 'is-expanded': metricNamesOpen, 'is-collapsed': !metricNamesOpen }"
            :aria-expanded="metricNamesOpen"
            @click="metricNamesOpen = !metricNamesOpen"
          >
            <span
              class="obs-collapse-icon obs-collapse-icon--section"
              :class="{ 'is-open': metricNamesOpen }"
              aria-hidden="true"
            />
            <span class="obs-section-title">{{ $t('observability.collector.metricNames') }}</span>
            <span class="obs-count-badge obs-count-badge--section">{{ $t('observability.collector.namesCount', { count: metricNames.length }) }}</span>
            <span class="obs-collapse-label obs-collapse-label--section">{{ metricNamesOpen ? $t('observability.chart.expanded') : $t('observability.chart.collapsed') }}</span>
          </button>
          <div v-show="metricNamesOpen" class="collector-section-body">
            <p v-if="namesError" class="collector-hint error">{{ namesError }}</p>
            <div v-else-if="!metricNames.length" class="collector-empty">{{ $t('observability.collector.noMetrics') }}</div>
            <div v-else class="metric-names-list">
              <span v-for="name in metricNames" :key="name" class="metric-tag">{{ name }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  </section>
</template>

<script>
import { updateObservabilitySettings } from '@/api/client'
import { translateStatus } from '@/utils/i18nLabels'
import { formatObservabilityRelative } from '@/utils/observabilityI18n'

export default {
  name: 'CollectorPanel',
  props: {
    health: { type: Object, default: null },
    healthError: { type: String, default: '' },
    settings: { type: Object, default: null },
    targets: { type: Array, default: () => [] },
    targetsLoading: { type: Boolean, default: false },
    targetsError: { type: String, default: '' },
    metricNames: { type: Array, default: () => [] },
    namesError: { type: String, default: '' }
  },
  emits: ['settings-updated'],
  data() {
    return {
      expanded: false,
      settingsOpen: true,
      issuesOpen: true,
      metricNamesOpen: false,
      draftScrapeIntervalMs: 60_000,
      draftRetentionMs: 24 * 60 * 60 * 1000,
      settingsSaving: false,
      settingsError: '',
      settingsNotice: ''
    }
  },
  computed: {
    dataSourceRows() {
      return (this.targets || []).map((target) => ({
        id: target.id,
        component: target.component,
        instance: target.instance,
        scrape_up: target.up,
        worker_id: target.worker_id,
        worker_state: target.worker_state,
        detail: [target.last_warning, target.last_error].filter(Boolean).join('; ')
      }))
    },
    targetsUpLabel() {
      const total = this.dataSourceRows.length
      const up = this.dataSourceRows.filter((row) => row.scrape_up).length
      return total
        ? this.$t('observability.collector.targetsUp', { up, total })
        : this.$t('observability.collector.collectorLabel')
    },
    problemTargets() {
      return this.dataSourceRows.filter((row) => !row.scrape_up || !!row.detail)
    },
    showBuiltinSettings() {
      const active = (this.health?.active_provider || '').toLowerCase()
      return active === 'builtin' || active === ''
    },
    settingsDirty() {
      if (!this.settings) return false
      return this.draftScrapeIntervalMs !== this.settings.scrape_interval_ms
        || this.draftRetentionMs !== this.settings.retention_ms
    }
  },
  watch: {
    settings: {
      immediate: true,
      handler(value) {
        if (!value) return
        this.draftScrapeIntervalMs = value.scrape_interval_ms
        this.draftRetentionMs = value.retention_ms
      }
    }
  },
  methods: {
    shortInstance(row) {
      const instance = row.instance || ''
      const host = instance.split(':')[0] || instance
      if (row.component === 'worker') {
        const workerNum = row.id?.startsWith('worker:') ? row.id.slice('worker:'.length) : null
        return workerNum ? `worker-${workerNum}` : host
      }
      return host
    },
    formatRelative(value) {
      return formatObservabilityRelative(this.$t, value)
    },
    workerStateLabel(state) {
      return translateStatus(this.$t, state)
    },
    formatSampleCount(value) {
      const n = Number(value || 0)
      if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
      if (n >= 1000) return `${(n / 1000).toFixed(1)}k`
      return String(n)
    },
    formatDuration(ms) {
      const value = Number(ms || 0)
      if (!value) return '-'
      if (value < 60_000) return `${Math.round(value / 1000)}s`
      if (value < 3_600_000) return `${Math.round(value / 60_000)}m`
      if (value < 86_400_000) return `${Math.round(value / 3_600_000)}h`
      return `${Math.round(value / 86_400_000)}d`
    },
    async applySettings() {
      if (!this.settingsDirty) return
      this.settingsSaving = true
      this.settingsError = ''
      this.settingsNotice = ''
      try {
        const updated = await updateObservabilitySettings({
          scrape_interval_ms: this.draftScrapeIntervalMs,
          retention_ms: this.draftRetentionMs
        })
        this.$emit('settings-updated', updated)
        this.settingsNotice = this.$t('observability.collector.settingsSaved')
      } catch (err) {
        this.settingsError = String(err?.message || err)
      } finally {
        this.settingsSaving = false
      }
    }
  }
}
</script>

<style scoped>
.obs-group-section {
  display: grid;
  gap: 16px;
  margin-top: 20px;
}
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
  transform: translate(-60%, -50%) rotate(-45deg);
  transition: transform 0.2s ease, border-color 0.15s ease;
}
.obs-collapse-icon.is-open {
  background: var(--admin-primary-soft);
  border-color: #b8d6a2;
}
.obs-collapse-icon.is-open::before {
  border-color: var(--admin-primary-strong);
  transform: translate(-50%, -40%) rotate(45deg);
}
.collector-section-body {
  padding: 12px;
  background: #fff;
}
.collector-settings {
  display: flex;
  flex-wrap: wrap;
  align-items: start;
  gap: 12px;
}
.collector-settings-field {
  display: grid;
  gap: 6px;
  min-width: 160px;
}
.collector-settings-action {
  min-width: 100px;
}
.collector-field-label {
  font-size: 12px;
  font-weight: 700;
  line-height: 1.2;
  color: var(--admin-muted);
  min-height: 14.4px;
}
.collector-settings-action .collector-field-label {
  visibility: hidden;
}
.settings-apply-btn {
  width: 100%;
  margin: 0;
}
.collector-settings .admin-input,
.collector-settings .settings-apply-btn {
  height: 38px;
  box-sizing: border-box;
}
.collector-meta {
  margin: 10px 0 0;
  color: var(--admin-muted);
  font-size: 12px;
}
.collector-hint {
  margin: 8px 0 0;
  font-size: 12px;
}
.collector-hint.error { color: var(--admin-danger); }
.collector-hint.ok { color: var(--admin-primary-strong); }
.collector-empty {
  color: var(--admin-muted);
  font-size: 12px;
}
.target-list {
  list-style: none;
  margin: 0;
  padding: 0;
  border: 1px solid var(--admin-border);
  border-radius: 6px;
  overflow: hidden;
}
.target-row {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 10px;
  font-size: 12px;
  border-bottom: 1px solid #eef2ea;
}
.target-row:last-child { border-bottom: 0; }
.target-row.down { background: #fff8f6; }
.target-instance {
  font-weight: 650;
  min-width: 120px;
}
.target-tag {
  padding: 2px 6px;
  border-radius: 999px;
  background: #f4f7f1;
  color: #4d6248;
  font-size: 11px;
}
.target-detail {
  margin-left: auto;
  min-width: 0;
  color: var(--admin-danger);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.metric-names-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  max-height: 160px;
  overflow: auto;
}
.metric-tag {
  padding: 4px 10px;
  border-radius: 999px;
  background: var(--admin-surface-soft);
  border: 1px solid var(--admin-border);
  font-size: 12px;
  color: #36506c;
}
@media (max-width: 900px) {
  .obs-collapse-label { display: none; }
}
</style>
