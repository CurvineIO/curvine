<template>
  <div class="admin-panel table-panel target-health-panel">
    <div class="panel-heading table-heading">
      <div>
        <h2>Data Sources</h2>
        <p>{{ subtitle }}</p>
      </div>
    </div>
    <div v-if="error" class="obs-chart-error">{{ error }}</div>
    <div v-else-if="loading" class="empty-state">Loading scrape targets…</div>
    <div v-else-if="!targets.length" class="empty-state">No scrape targets discovered yet.</div>
    <table v-else class="admin-table target-health-table">
      <thead>
        <tr>
          <th>ID</th>
          <th>Component</th>
          <th>Instance</th>
          <th>Scrape</th>
          <th>Worker</th>
          <th>Last Scrape</th>
          <th>Last Success</th>
          <th>Warning / Error</th>
          <th>Samples / Parse Errors</th>
          <th>URL</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="target in targets" :key="target.id" :class="{ 'target-down': !target.up }">
          <td><strong :title="target.id">{{ target.id }}</strong></td>
          <td><span>{{ target.component }}</span></td>
          <td><span :title="target.instance">{{ target.instance }}</span></td>
          <td>
            <span :class="['status-pill', target.up ? 'healthy' : 'failed']">
              {{ target.up ? 'Up' : 'Down' }}
            </span>
          </td>
          <td>
            <template v-if="target.component === 'worker'">
              <span class="worker-meta">{{ workerIdLabel(target) }}</span>
              <span :class="['status-pill', workerStateTone(target.worker_state)]">
                {{ target.worker_state || 'unknown' }}
              </span>
            </template>
            <span v-else class="muted-cell">—</span>
          </td>
          <td><span :title="formatTs(target.last_scrape_ms)">{{ formatRelative(target.last_scrape_ms) }}</span></td>
          <td>
            <span :class="successAgeClass(target)" :title="formatTs(target.last_success_ms)">
              {{ formatRelative(target.last_success_ms) }}
            </span>
          </td>
          <td>
            <span v-if="target.last_warning" class="warning-text" :title="target.last_warning">
              {{ target.last_warning }}
            </span>
            <span v-if="target.last_error" class="error-text" :title="target.last_error">
              {{ target.last_error }}
            </span>
            <span v-if="!target.last_warning && !target.last_error" class="muted-cell">—</span>
          </td>
          <td>
            <span>samples {{ target.sample_count }} / parse errors {{ target.parse_error_count ?? 0 }}</span>
          </td>
          <td><span :title="target.url">{{ target.url }}</span></td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script>
export default {
  name: 'TargetHealthTable',
  props: {
    targets: { type: Array, default: () => [] },
    loading: { type: Boolean, default: false },
    error: { type: String, default: '' },
    subtitle: {
      type: String,
      default: 'Scrape target health from built-in collector discovery'
    }
  },
  methods: {
    workerIdLabel(target) {
      return target.worker_id != null ? `ID ${target.worker_id}` : '—'
    },
    workerStateTone(state) {
      const normalized = String(state || '').toLowerCase()
      if (normalized === 'live') return 'live'
      if (normalized === 'lost') return 'lost'
      if (normalized === 'blacklist') return 'warning'
      if (normalized === 'decommission') return 'waiting'
      return 'neutral'
    },
    formatTs(value) {
      if (!value) return '-'
      const date = new Date(Number(value))
      if (Number.isNaN(date.getTime())) return '-'
      return date.toLocaleString()
    },
    formatRelative(value) {
      if (!value) return 'never'
      const delta = Date.now() - Number(value)
      if (!Number.isFinite(delta) || delta < 0) return this.formatTs(value)
      if (delta < 60_000) return `${Math.max(1, Math.round(delta / 1000))}s ago`
      if (delta < 3_600_000) return `${Math.round(delta / 60_000)}m ago`
      if (delta < 86_400_000) return `${Math.round(delta / 3_600_000)}h ago`
      return `${Math.round(delta / 86_400_000)}d ago`
    },
    successAgeClass(target) {
      if (!target.last_success_ms) return 'error-text'
      if (!target.up && target.last_scrape_ms && target.last_success_ms < target.last_scrape_ms - 30_000) {
        return 'error-text'
      }
      return ''
    }
  }
}
</script>

<style scoped>
.target-health-panel { padding: 16px; min-width: 0; }
.target-health-table th:nth-child(1) { width: 12%; }
.target-health-table th:nth-child(2) { width: 8%; }
.target-health-table th:nth-child(3) { width: 14%; }
.target-health-table th:nth-child(4) { width: 7%; }
.target-health-table th:nth-child(5) { width: 10%; }
.target-health-table th:nth-child(6),
.target-health-table th:nth-child(7) { width: 9%; }
.target-health-table th:nth-child(8) { width: 16%; }
.target-health-table th:nth-child(9) { width: 8%; }
.target-health-table th:nth-child(10) { width: 12%; }
.target-down td { background: #fff8f6; }
.worker-meta { display: block; color: var(--admin-muted); font-size: 11px; margin-bottom: 4px; }
.warning-text, .error-text, .muted-cell { display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 12px; }
.warning-text { color: var(--admin-warning); }
.error-text { color: var(--admin-danger); }
.muted-cell { color: var(--admin-muted); }
.parse-errors { display: block; font-size: 11px; margin-top: 2px; }
.obs-chart-error {
  padding: 16px;
  border-radius: var(--admin-radius);
  background: var(--admin-danger-soft);
  color: var(--admin-danger);
  font-size: 13px;
  margin-bottom: 12px;
}
@media (max-width: 1100px) {
  .target-health-table { min-width: 980px; }
}
</style>
