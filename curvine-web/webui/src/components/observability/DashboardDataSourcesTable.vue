<template>
  <div :class="['dashboard-sources-panel', { embedded }]">
    <div v-if="!embedded" class="panel-heading table-heading">
      <div>
        <h2>{{ title }}</h2>
        <p>{{ subtitle }}</p>
      </div>
    </div>
    <div v-else-if="subtitle" class="embedded-subtitle">{{ subtitle }}</div>
    <div v-if="error" class="obs-inline-error">{{ error }}</div>
    <div v-else-if="loading" class="empty-state">Loading dashboard data sources…</div>
    <div v-else-if="!rows.length" class="empty-state">No scrape targets in dashboard payload.</div>
    <table v-else class="admin-table dashboard-sources-table">
      <thead>
        <tr>
          <th>ID</th>
          <th>Component</th>
          <th>Instance</th>
          <th>Scrape</th>
          <th>Worker</th>
          <th>Detail</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="row.id" :class="{ 'target-down': !row.scrape_up }">
          <td><strong :title="row.id">{{ row.id }}</strong></td>
          <td>{{ row.component }}</td>
          <td><span :title="row.instance">{{ row.instance }}</span></td>
          <td>
            <span :class="['status-pill', row.scrape_up ? 'healthy' : 'failed']">
              {{ row.scrape_up ? 'Up' : 'Down' }}
            </span>
          </td>
          <td>
            <template v-if="row.component === 'worker'">
              <span class="worker-meta">{{ workerIdLabel(row) }}</span>
              <span :class="['status-pill', workerStateTone(row.worker_state)]">
                {{ row.worker_state || 'unknown' }}
              </span>
            </template>
            <span v-else class="muted-cell">—</span>
          </td>
          <td>
            <span v-if="row.detail" class="detail-text" :title="row.detail">{{ row.detail }}</span>
            <span v-else class="muted-cell">—</span>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script>
export default {
  name: 'DashboardDataSourcesTable',
  props: {
    title: { type: String, default: 'Data Sources' },
    subtitle: { type: String, default: '' },
    rows: { type: Array, default: () => [] },
    loading: { type: Boolean, default: false },
    error: { type: String, default: '' },
    embedded: { type: Boolean, default: false }
  },
  methods: {
    workerIdLabel(row) {
      return row.worker_id != null ? `ID ${row.worker_id}` : '—'
    },
    workerStateTone(state) {
      const normalized = String(state || '').toLowerCase()
      if (normalized === 'live') return 'live'
      if (normalized === 'lost') return 'lost'
      if (normalized === 'blacklist') return 'warning'
      if (normalized === 'decommission') return 'waiting'
      return 'neutral'
    }
  }
}
</script>

<style scoped>
.dashboard-sources-panel { min-width: 0; }
.dashboard-sources-panel:not(.embedded) {
  padding: 16px;
  background: var(--admin-surface);
  border: 1px solid var(--admin-border);
  border-radius: var(--admin-radius);
}
.dashboard-sources-panel.embedded { padding: 0; }
.embedded-subtitle {
  margin: 0 0 10px;
  color: var(--admin-muted);
  font-size: 13px;
}
.dashboard-sources-table {
  table-layout: fixed;
  width: 100%;
}
.dashboard-sources-table th:nth-child(1) { width: 18%; }
.dashboard-sources-table th:nth-child(2) { width: 10%; }
.dashboard-sources-table th:nth-child(3) { width: 24%; }
.dashboard-sources-table th:nth-child(4) { width: 8%; }
.dashboard-sources-table th:nth-child(5) { width: 14%; }
.dashboard-sources-table th:nth-child(6) { width: 26%; }
.target-down td { background: #fff8f6; }
.worker-meta { display: block; color: var(--admin-muted); font-size: 11px; margin-bottom: 4px; }
.detail-text, .muted-cell {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 12px;
}
.detail-text { color: var(--admin-text); }
.muted-cell { color: var(--admin-muted); }
.obs-inline-error {
  padding: 16px;
  border-radius: var(--admin-radius);
  background: var(--admin-danger-soft);
  color: var(--admin-danger);
  font-size: 13px;
  margin-bottom: 12px;
}
@media (max-width: 1100px) {
  .dashboard-sources-table { min-width: 760px; }
}
</style>
