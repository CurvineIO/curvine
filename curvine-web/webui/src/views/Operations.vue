<template>
  <div class="admin-page operations-page" v-loading="loading">
    <div v-if="operationsError" class="ph-errors">
      <div class="ph-error-banner">
        <strong>{{ $t('operations.errors.dataUnavailable') }}</strong>
        <span>{{ operationsError }}</span>
      </div>
    </div>

    <section :class="['admin-panel', 'ph-summary', `ph-summary--${overallStatus}`]">
      <div class="ph-banner">
        <div class="ph-banner-top">
          <div class="ph-banner-leading">
            <div class="ph-coverage-ring" :style="coverageRingStyle" aria-hidden="true">
              <div>
                <strong>{{ coverageLabel }}</strong>
                <span>{{ $t('operations.summary.coverage') }}</span>
              </div>
            </div>
            <div class="ph-banner-copy">
              <div class="ph-kicker">
                <span class="ph-kicker-provider">{{ formatProvider(activeProvider) }}</span>
                <span class="ph-kicker-sep">·</span>
                <time class="ph-kicker-time">{{ formatTime(generatedMs) }}</time>
              </div>
              <h2>{{ statusHeadline }}</h2>
              <p>{{ statusDescription }}</p>
            </div>
          </div>
          <div :class="['ph-health-badge', `ph-health-badge--${overallStatus}`]">
            <span class="ph-health-badge-label">{{ $t('operations.summary.status') }}</span>
            <strong>{{ statusChipLabel }}</strong>
          </div>
        </div>

        <div class="ph-stat-strip">
          <div class="ph-stat-item">
            <span>{{ $t('operations.summary.checksPassed') }}</span>
            <strong>{{ passedCheckCount }}/{{ checks.length }}</strong>
            <small>{{ $t('operations.summary.needAttention', { count: actionableCheckCount }) }}</small>
          </div>
          <div class="ph-stat-item">
            <span>{{ $t('operations.summary.targetsCovered') }}</span>
            <strong>{{ coveredTargets }}/{{ expectedTargets }}</strong>
            <small>{{ transferCount
              ? $t('operations.summary.masterWorkerTransfer', { master: masterCount, worker: workerCount, transfer: transferCount })
              : $t('operations.summary.masterWorker', { master: masterCount, worker: workerCount }) }}</small>
          </div>
          <div class="ph-stat-item">
            <span>{{ $t('operations.summary.activeAlerts') }}</span>
            <strong :class="{ 'ph-stat-alert': alerts.length }">{{ operationsError ? $t('common.notAvailable') : alerts.length }}</strong>
            <small>{{ $t('operations.summary.criticalWarning', { critical: criticalCount, warning: warningCount }) }}</small>
          </div>
          <div class="ph-stat-item">
            <span>{{ $t('operations.summary.providerMode') }}</span>
            <strong class="ph-stat-provider">{{ formatProvider(activeProvider) }}</strong>
            <small>{{ $t('operations.summary.discoveredTargets', { count: expectedTargets || 0 }) }}</small>
          </div>
        </div>
      </div>
    </section>

    <section class="content-grid ph-columns">
      <div class="admin-panel ph-panel">
        <div class="panel-heading">
          <div>
            <h2>{{ $t('operations.checks.title') }}</h2>
            <p>{{ $t('operations.checks.subtitle') }}</p>
          </div>
          <span :class="['ph-badge', actionableCheckCount ? 'ph-badge--warn' : 'ph-badge--ok']">
            {{ $t('operations.checks.passed', { passed: passedCheckCount, total: checks.length }) }}
          </span>
        </div>

        <div v-if="!operationsError && !loading && orderedChecks.length === 0" class="ph-empty">
          {{ $t('operations.checks.empty') }}
        </div>

        <ul v-else class="ph-check-list">
          <li
            v-for="item in orderedChecks"
            :key="item.id"
            :class="['ph-check-item', `ph-check-item--${item.status}`]"
          >
            <span :class="['ph-check-mark', `ph-check-mark--${item.status}`]">{{ checkIcon(item.status) }}</span>
            <div class="ph-check-body">
              <div class="ph-check-head">
                <h3>{{ checkTitle(item) }}</h3>
                <span :class="['status-pill', checkPillClass(item.status)]">{{ translateCheckStatusLabel(item.status) }}</span>
              </div>
              <p>{{ checkSummary(item) }}</p>
              <small v-if="item.detail">{{ checkDetail(item) }}</small>
              <div v-if="item.suggested_fix && isActionable(item.status)" class="ph-fix">
                <strong>{{ $t('operations.checks.howToFix') }}</strong>
                <span>{{ checkSuggestedFix(item) }}</span>
              </div>
            </div>
          </li>
        </ul>
      </div>

      <div class="admin-panel ph-panel">
        <div class="panel-heading">
          <div>
            <h2>{{ $t('operations.alerts.title') }}</h2>
            <p>{{ $t('operations.alerts.subtitle') }}</p>
          </div>
          <span :class="['ph-badge', alerts.length ? 'ph-badge--warn' : 'ph-badge--ok']">
            {{ $t('operations.alerts.active', { count: alerts.length }) }}
          </span>
        </div>

        <div v-if="!operationsError && !loading && alerts.length === 0" class="ph-empty ph-empty--success">
          <span class="ph-empty-icon">✓</span>
          <div>
            <strong>{{ $t('operations.alerts.allClear') }}</strong>
            <p>{{ $t('operations.alerts.allClearDesc') }}</p>
          </div>
        </div>

        <ul v-else class="ph-alert-list">
          <li
            v-for="alert in alerts"
            :key="alert.id"
            :class="['ph-alert-item', `ph-alert-item--${alert.severity}`]"
          >
            <div class="ph-alert-head">
              <span :class="['status-pill', alert.severity === 'critical' ? 'failed' : 'warning']">
                {{ translateSeverityLabel(alert.severity) }}
              </span>
              <span class="ph-alert-source">{{ formatSource(alert.source) }}</span>
            </div>
            <h3>{{ alertTitle(alert) }}</h3>
            <p>{{ alertSummary(alert) }}</p>
            <div class="ph-fix">
              <strong>{{ $t('operations.alerts.recommendedAction') }}</strong>
              <span>{{ alertSuggestedFix(alert) }}</span>
            </div>
          </li>
        </ul>
      </div>
    </section>

    <section v-if="missingTargets.length" class="admin-panel table-panel">
      <div class="panel-heading table-heading">
        <div>
          <h2>{{ $t('operations.targets.title') }}</h2>
          <p>{{ $t('operations.targets.subtitle') }}</p>
        </div>
        <span class="status-pill failed">{{ $t('operations.targets.missing', { count: missingTargets.length }) }}</span>
      </div>
      <div class="table-wrap">
        <table class="admin-table ph-targets-table">
          <thead>
            <tr>
              <th>{{ $t('operations.targets.source') }}</th>
              <th>{{ $t('operations.targets.component') }}</th>
              <th>{{ $t('operations.targets.instance') }}</th>
              <th>{{ $t('operations.targets.endpoint') }}</th>
              <th>{{ $t('operations.targets.reason') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="target in missingTargets" :key="`${target.source}:${target.id}`">
              <td><span class="status-pill">{{ formatSource(target.source) }}</span></td>
              <td><strong>{{ target.component }}</strong></td>
              <td>{{ target.instance }}</td>
              <td><code>{{ target.url }}</code></td>
              <td class="ph-reason">{{ targetReason(target.reason) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  </div>
</template>

<script>
import { fetchObservabilityOperations } from '@/api/client'
import eventBus from '@/utils/eventBus'
import {
  translateAlertField,
  translateCheckField,
  translateCheckStatus,
  translateOperationsSource,
  translateSeverity,
  translateSuggestedFix,
  translateTargetAlertTitle,
  translateTargetReason
} from '@/utils/operationsI18n'

const STATUS_ORDER = { fail: 0, warning: 1, pass: 2, skipped: 3 }

export default {
  name: 'OperationsPage',
  props: {
    operationsLoader: { type: Function, default: null }
  },
  data() {
    return { loading: false, operationsError: '', payload: null, alerts: [] }
  },
  computed: {
    checks() { return Array.isArray(this.payload?.checks) ? this.payload.checks : [] },
    orderedChecks() { return [...this.checks].sort((a, b) => (STATUS_ORDER[a.status] ?? 9) - (STATUS_ORDER[b.status] ?? 9)) },
    missingTargets() { return Array.isArray(this.payload?.missing_targets) ? this.payload.missing_targets : [] },
    overallStatus() { return this.payload?.overall_status || (this.alerts.length ? 'warning' : 'skipped') },
    activeProvider() { return this.payload?.active_provider || 'provider pending' },
    expectedTargets() { return Number(this.payload?.expected_targets || 0) },
    coveredTargets() { return Number(this.payload?.active_covered_targets || 0) },
    masterCount() { return Number(this.payload?.discovered_master_targets || 0) },
    workerCount() { return Number(this.payload?.discovered_live_worker_targets || 0) },
    transferCount() { return Number(this.payload?.discovered_transfer_targets || 0) },
    criticalCount() { return this.alerts.filter((item) => item.severity === 'critical').length },
    warningCount() { return this.alerts.filter((item) => item.severity === 'warning').length },
    passedCheckCount() { return this.checks.filter((item) => item.status === 'pass').length },
    actionableCheckCount() { return this.checks.filter((item) => this.isActionable(item.status)).length },
    generatedMs() { return Number(this.payload?.generated_ms || 0) },
    coveragePercent() { return this.expectedTargets ? Math.round((this.coveredTargets / this.expectedTargets) * 100) : 0 },
    coverageLabel() { return this.expectedTargets ? `${this.coveragePercent}%` : '—' },
    coverageRingStyle() {
      const pct = this.expectedTargets ? this.coveragePercent : 0
      const color = pct >= 100 ? '#6fae3f' : pct > 0 ? '#a8791f' : '#cfdcc9'
      return { background: `conic-gradient(${color} 0 ${pct}%, #edf4e9 ${pct}% 100%)` }
    },
    statusChipLabel() {
      if (this.overallStatus === 'pass') return this.$t('operations.status.healthy')
      if (this.overallStatus === 'fail') return this.$t('operations.status.critical')
      if (this.overallStatus === 'warning') return this.$t('operations.status.degraded')
      return this.$t('operations.status.pending')
    },
    statusHeadline() {
      if (this.overallStatus === 'fail') return this.$t('operations.status.headlineFail')
      if (this.overallStatus === 'warning') return this.$t('operations.status.headlineWarning')
      if (this.overallStatus === 'pass') return this.$t('operations.status.headlinePass')
      return this.$t('operations.status.headlinePending')
    },
    statusDescription() {
      if (this.overallStatus === 'fail') {
        return this.$t('operations.status.descFail', { critical: this.criticalCount, actionable: this.actionableCheckCount })
      }
      if (this.overallStatus === 'warning') {
        return this.$t('operations.status.descWarning')
      }
      if (this.overallStatus === 'pass') {
        return this.$t('operations.status.descPass', { count: this.checks.length })
      }
      return this.$t('operations.status.descPending')
    }
  },
  created() { this.fetchData() },
  mounted() { eventBus.on('admin-refresh', this.fetchData) },
  beforeUnmount() { eventBus.off('admin-refresh', this.fetchData) },
  methods: {
    isActionable(status) { return status === 'fail' || status === 'warning' },
    checkSummaryParams(item) {
      if (item.id === 'builtin-coverage' || item.id === 'prometheus-coverage') {
        return { covered: this.coveredTargets, expected: this.expectedTargets }
      }
      const match = String(item.summary || '').match(/(\d+)/)
      return { count: match ? Number(match[1]) : 0 }
    },
    checkTitle(item) {
      return translateCheckField(this.$t, this.$te, item, 'title', item.title)
    },
    checkSummary(item) {
      return translateCheckField(this.$t, this.$te, item, 'summary', item.summary, this.checkSummaryParams(item))
    },
    checkDetail(item) {
      return translateCheckField(this.$t, this.$te, item, 'detail', item.detail, this.checkSummaryParams(item))
    },
    checkSuggestedFix(item) {
      const translated = translateCheckField(this.$t, this.$te, item, 'suggested_fix', item.suggested_fix, this.checkSummaryParams(item))
      return translateSuggestedFix(this.$t, this.$te, translated)
    },
    translateCheckStatusLabel(status) {
      return translateCheckStatus(this.$t, status)
    },
    translateSeverityLabel(severity) {
      return translateSeverity(this.$t, severity)
    },
    alertTitle(alert) {
      if (alert.target_id) {
        const component = String(alert.title || '').split(' ')[0]
        return translateTargetAlertTitle(this.$t, this.$te, component, alert.title)
      }
      return translateAlertField(this.$t, this.$te, alert, 'title', alert.title)
    },
    alertSummary(alert) {
      return translateAlertField(this.$t, this.$te, alert, 'summary', alert.summary, this.checkSummaryParams({ id: alert.id?.replace('diagnostic:', ''), summary: alert.summary }))
    },
    alertSuggestedFix(alert) {
      return translateAlertField(
        this.$t,
        this.$te,
        alert,
        'suggested_fix',
        alert.suggested_fix || this.$t('operations.alerts.reviewChecks')
      )
    },
    targetReason(reason) {
      return translateTargetReason(this.$t, this.$te, reason)
    },
    checkIcon(status) {
      if (status === 'pass') return '✓'
      if (status === 'fail') return '✕'
      if (status === 'warning') return '!'
      return '−'
    },
    checkPillClass(status) {
      if (status === 'pass') return 'healthy'
      if (status === 'fail') return 'failed'
      if (status === 'warning') return 'warning'
      return ''
    },
    async fetchData() {
      this.loading = true
      try {
        const result = this.operationsLoader
          ? await this.operationsLoader()
          : await fetchObservabilityOperations()
        this.payload = result?.diagnostics || null
        this.alerts = Array.isArray(result?.alerts) ? result.alerts : []
        this.operationsError = ''
      } catch (err) {
        this.payload = null
        this.alerts = []
        this.operationsError = String(err?.message || err)
      } finally {
        this.loading = false
      }
    },
    formatSource(value) { return translateOperationsSource(this.$t, value) },
    formatProvider(value) {
      const label = String(value || '').trim()
      if (!label || label === 'provider pending') return this.$t('operations.summary.providerPending')
      return label.charAt(0).toUpperCase() + label.slice(1)
    },
    formatTime(value) {
      if (!value) return this.$t('operations.time.notAvailable')
      const date = new Date(Number(value))
      return Number.isNaN(date.getTime()) ? this.$t('operations.time.notAvailable') : date.toLocaleString()
    }
  }
}
</script>

<style scoped>
.operations-page {
  gap: 16px;
}


.ph-errors {
  display: grid;
  gap: 8px;
}

.ph-error-banner {
  display: grid;
  gap: 4px;
  padding: 12px 14px;
  border: 1px solid color-mix(in srgb, var(--admin-danger) 28%, var(--admin-border));
  border-radius: var(--admin-radius);
  background: var(--admin-danger-soft);
}

.ph-error-banner strong {
  color: var(--admin-danger);
  font-size: 13px;
}

.ph-error-banner span {
  color: var(--admin-muted);
  font-size: 12px;
  line-height: 1.45;
}

.ph-summary {
  padding: 0;
  overflow: hidden;
}

.ph-summary--pass { box-shadow: inset 3px 0 0 var(--admin-primary); }
.ph-summary--warning { box-shadow: inset 3px 0 0 var(--admin-warning); }
.ph-summary--fail { box-shadow: inset 3px 0 0 var(--admin-danger); }

.ph-banner {
  display: grid;
  gap: 0;
}

.ph-banner-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 20px;
  padding: 18px 20px;
  background: linear-gradient(135deg, var(--admin-surface-soft) 0%, #fff 55%);
}

.ph-banner-leading {
  display: flex;
  align-items: center;
  gap: 18px;
  min-width: 0;
}

.ph-coverage-ring {
  flex: 0 0 88px;
  width: 88px;
  aspect-ratio: 1;
  border-radius: 50%;
  display: grid;
  place-items: center;
  position: relative;
}

.ph-coverage-ring::after {
  content: '';
  position: absolute;
  inset: 10px;
  border-radius: 50%;
  background: #fff;
}

.ph-coverage-ring div {
  position: relative;
  z-index: 1;
  text-align: center;
}

.ph-coverage-ring strong {
  display: block;
  font-size: 18px;
  line-height: 1.1;
  font-weight: 760;
}

.ph-coverage-ring span {
  display: block;
  margin-top: 2px;
  color: var(--admin-muted);
  font-size: 10px;
  font-weight: 650;
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.ph-banner-copy {
  min-width: 0;
}

.ph-kicker {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
  margin-bottom: 6px;
  color: var(--admin-muted);
  font-size: 12px;
}

.ph-kicker-provider {
  color: var(--admin-text);
  font-weight: 650;
}

.ph-kicker-sep {
  color: var(--admin-border-strong);
}

.ph-banner-copy h2 {
  margin: 0 0 6px;
  font-size: 22px;
  line-height: 1.2;
  font-weight: 760;
  letter-spacing: -0.02em;
}

.ph-banner-copy p {
  margin: 0;
  color: var(--admin-muted);
  font-size: 13px;
  line-height: 1.5;
  max-width: 58ch;
}

.ph-health-badge {
  flex: 0 0 auto;
  display: grid;
  gap: 2px;
  min-width: 108px;
  padding: 12px 16px;
  border-radius: 12px;
  border: 1px solid var(--admin-border);
  background: #fff;
  text-align: center;
}

.ph-health-badge-label {
  color: var(--admin-muted);
  font-size: 11px;
  font-weight: 650;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.ph-health-badge strong {
  font-size: 18px;
  line-height: 1.2;
  font-weight: 760;
}

.ph-health-badge--pass {
  border-color: color-mix(in srgb, var(--admin-primary) 35%, var(--admin-border));
  background: var(--admin-primary-soft);
  color: var(--admin-primary-strong);
}

.ph-health-badge--warning {
  border-color: color-mix(in srgb, var(--admin-warning) 35%, var(--admin-border));
  background: var(--admin-warning-soft);
  color: var(--admin-warning);
}

.ph-health-badge--fail {
  border-color: color-mix(in srgb, var(--admin-danger) 35%, var(--admin-border));
  background: var(--admin-danger-soft);
  color: var(--admin-danger);
}

.ph-stat-strip {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  border-top: 1px solid var(--admin-border);
  background: #fff;
}

.ph-stat-item {
  display: grid;
  gap: 3px;
  padding: 14px 18px;
  border-right: 1px solid var(--admin-border);
}

.ph-stat-item:last-child {
  border-right: 0;
}

.ph-stat-item > span {
  color: var(--admin-muted);
  font-size: 12px;
  font-weight: 650;
}

.ph-stat-item strong {
  font-size: 20px;
  line-height: 1.1;
  font-weight: 760;
  color: var(--admin-text);
}

.ph-stat-item small {
  color: var(--admin-muted);
  font-size: 11px;
  line-height: 1.35;
}

.ph-stat-alert {
  color: var(--admin-warning) !important;
}

.ph-stat-provider {
  font-size: 17px !important;
  text-transform: capitalize;
}

.ph-columns {
  align-items: stretch;
}

.ph-panel {
  display: flex;
  flex-direction: column;
  min-height: 100%;
}

.ph-badge {
  display: inline-flex;
  align-items: center;
  min-height: 28px;
  padding: 0 10px;
  border-radius: 999px;
  background: var(--admin-primary-soft);
  color: var(--admin-primary-strong);
  font-size: 12px;
  font-weight: 760;
  white-space: nowrap;
}

.ph-badge--warn {
  background: var(--admin-warning-soft);
  color: var(--admin-warning);
}

.ph-badge--ok {
  background: var(--admin-primary-soft);
  color: var(--admin-primary-strong);
}

.ph-empty {
  flex: 1;
  display: grid;
  place-items: center;
  min-height: 180px;
  padding: 24px;
  border: 1px dashed var(--admin-border-strong);
  border-radius: var(--admin-radius);
  background: var(--admin-surface-soft);
  color: var(--admin-muted);
  font-size: 13px;
  text-align: center;
}

.ph-empty--success {
  display: flex;
  align-items: center;
  gap: 14px;
  text-align: left;
}

.ph-empty-icon {
  flex: 0 0 42px;
  width: 42px;
  height: 42px;
  display: grid;
  place-items: center;
  border-radius: 12px;
  background: var(--admin-primary-soft);
  color: var(--admin-primary-strong);
  font-size: 18px;
  font-weight: 800;
}

.ph-empty strong {
  display: block;
  color: var(--admin-text);
  font-size: 14px;
}

.ph-empty p {
  margin: 4px 0 0;
  font-size: 13px;
  line-height: 1.45;
}

.ph-check-list,
.ph-alert-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: grid;
  gap: 10px;
  flex: 1;
}

.ph-check-item {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  gap: 12px;
  padding: 14px;
  border: 1px solid var(--admin-border);
  border-radius: var(--admin-radius);
  background: var(--admin-surface-soft);
}

.ph-check-item--fail {
  border-color: color-mix(in srgb, var(--admin-danger) 35%, var(--admin-border));
  background: color-mix(in srgb, var(--admin-danger-soft) 35%, var(--admin-surface));
}

.ph-check-item--warning {
  border-color: color-mix(in srgb, var(--admin-warning) 35%, var(--admin-border));
  background: color-mix(in srgb, var(--admin-warning-soft) 40%, var(--admin-surface));
}

.ph-check-mark {
  width: 32px;
  height: 32px;
  display: grid;
  place-items: center;
  border-radius: 10px;
  font-size: 15px;
  font-weight: 800;
  background: #eef5e8;
  color: var(--admin-muted);
}

.ph-check-mark--pass { background: var(--admin-primary-soft); color: var(--admin-primary-strong); }
.ph-check-mark--warning { background: var(--admin-warning-soft); color: var(--admin-warning); }
.ph-check-mark--fail { background: var(--admin-danger-soft); color: var(--admin-danger); }

.ph-check-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 10px;
}

.ph-check-head h3 {
  margin: 0;
  font-size: 14px;
  line-height: 1.35;
  font-weight: 760;
}

.ph-check-body > p {
  margin: 6px 0 0;
  color: var(--admin-muted);
  font-size: 13px;
  line-height: 1.5;
}

.ph-check-body small {
  display: block;
  margin-top: 4px;
  color: var(--admin-muted);
  font-size: 12px;
  line-height: 1.4;
  overflow-wrap: anywhere;
}

.ph-alert-item {
  padding: 14px 16px;
  border: 1px solid var(--admin-border);
  border-radius: var(--admin-radius);
  background: var(--admin-surface-soft);
  border-left: 4px solid var(--admin-warning);
}

.ph-alert-item--critical {
  border-left-color: var(--admin-danger);
  background: color-mix(in srgb, var(--admin-danger-soft) 45%, var(--admin-surface));
}

.ph-alert-item--warning {
  border-left-color: var(--admin-warning);
  background: color-mix(in srgb, var(--admin-warning-soft) 45%, var(--admin-surface));
}

.ph-alert-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 10px;
  margin-bottom: 8px;
}

.ph-alert-source {
  color: var(--admin-muted);
  font-size: 12px;
  text-transform: capitalize;
}

.ph-alert-item h3 {
  margin: 0;
  font-size: 15px;
  line-height: 1.35;
}

.ph-alert-item > p {
  margin: 6px 0 0;
  color: var(--admin-muted);
  font-size: 13px;
  line-height: 1.5;
}

.ph-fix {
  margin-top: 10px;
  padding: 10px 12px;
  border-radius: var(--admin-radius);
  background: rgba(255, 255, 255, 0.88);
  border: 1px solid var(--admin-border);
}

.ph-fix strong {
  display: block;
  font-size: 12px;
  color: var(--admin-text);
}

.ph-fix span {
  display: block;
  margin-top: 4px;
  color: var(--admin-muted);
  font-size: 12px;
  line-height: 1.45;
}

.ph-targets-table {
  min-width: 760px;
}

.ph-targets-table code {
  font-size: 12px;
  color: var(--admin-text);
}

.ph-reason {
  color: var(--admin-danger);
  overflow-wrap: anywhere;
}

@media (max-width: 1100px) {
  .ph-stat-strip {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .ph-stat-item:nth-child(2) {
    border-right: 0;
  }

  .ph-stat-item:nth-child(-n+2) {
    border-bottom: 1px solid var(--admin-border);
  }
}

@media (max-width: 760px) {
  .ph-banner-top {
    flex-direction: column;
    align-items: stretch;
  }

  .ph-health-badge {
    width: 100%;
  }

  .ph-stat-strip {
    grid-template-columns: 1fr;
  }

  .ph-stat-item {
    border-right: 0;
    border-bottom: 1px solid var(--admin-border);
  }

  .ph-stat-item:last-child {
    border-bottom: 0;
  }

  .ph-check-head {
    flex-direction: column;
    align-items: flex-start;
  }
}
</style>
