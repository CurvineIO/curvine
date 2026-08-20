<template>
  <div class="admin-page" v-loading="loading">
    <section class="metric-grid">
      <MetricCard :label="$t('workers.metrics.liveWorkers')" :value="liveWorkers.length" :meta="$t('workers.metrics.healthyHeartbeat')" :delta="deltaLabel('online')" tone="good" />
      <MetricCard :label="$t('workers.metrics.managed')" :value="managedWorkers.length" :meta="$t('workers.metrics.blacklistDecommission')" :delta="`${blacklistWorkers.length} / ${decommissionWorkers.length}`" :tone="managedWorkers.length ? 'warn' : 'neutral'" />
      <MetricCard :label="$t('workers.metrics.totalCapacity')" :value="formatBytes(totalCapacity)" :meta="$t('workers.metrics.activeWorkers')" :delta="usageDelta" tone="neutral" />
      <MetricCard :label="$t('workers.metrics.lostWorkers')" :value="lostWorkers.length" :meta="$t('workers.metrics.requiresAttention')" :delta="deltaLabel(lostWorkers.length ? 'check' : 'none')" :tone="lostWorkers.length ? 'bad' : 'good'" />
    </section>

    <section class="admin-panel">
      <div class="panel-heading table-heading"><div><h2>{{ $t('workers.list.title') }}</h2><p>{{ $t('workers.list.subtitle') }}</p></div><div class="segmented"><button :class="{ active: statusFilter === 'all' }" @click="statusFilter = 'all'">{{ $t('workers.filters.all') }}</button><button :class="{ active: statusFilter === 'live' }" @click="statusFilter = 'live'">{{ $t('workers.filters.live') }}</button><button :class="{ active: statusFilter === 'blacklist' }" @click="statusFilter = 'blacklist'">{{ $t('workers.filters.blacklist') }}</button><button :class="{ active: statusFilter === 'decommission' }" @click="statusFilter = 'decommission'">{{ $t('workers.filters.decommission') }}</button><button :class="{ active: statusFilter === 'lost' }" @click="statusFilter = 'lost'">{{ $t('workers.filters.lost') }}</button></div></div>
      <div class="toolbar"><input v-model="query" class="admin-input" :placeholder="$t('workers.list.searchPlaceholder')"></div>

      <div class="worker-list">
        <div class="worker-row worker-header"><span>{{ $t('workers.list.headerWorker') }}</span><span>{{ $t('workers.list.headerStatus') }}</span><span>{{ $t('workers.list.headerUsage') }}</span><span>{{ $t('workers.list.headerBlocks') }}</span><span>{{ $t('workers.list.headerHeartbeat') }}</span><span>{{ $t('workers.list.headerActions') }}</span></div>
        <article v-for="worker in filteredWorkers" :key="workerSelector(worker)" class="worker-row">
          <div class="worker-main file-name-cell" @click="openDetail(worker)">
            <strong>{{ workerHostname(worker) }}</strong>
            <small>
              {{ $t('workers.list.rpc', 'RPC') }} {{ rpcPort(worker) }}
              <template v-if="displayIp(worker)"> · {{ $t('workers.detail.runtime.ip') }} {{ displayIp(worker) }}</template>
              · <span :class="versionClass(worker)" :title="versionTitle(worker)">{{ versionLabel(worker) }}</span>
            </small>
          </div>
          <div class="worker-state"><span :class="['status-pill', worker._status]">{{ statusLabel(worker._status) }}</span></div>
          <div class="worker-usage">
            <div class="usage-line"><strong>{{ workerUsage(worker) }}%</strong><span>{{ formatBytes(usedBytes(worker)) }} / {{ formatBytes(worker.capacity) }}</span></div>
            <div class="inline-meter"><span :style="{ width: workerUsage(worker) + '%' }"></span></div>
            <small>{{ $t('workers.list.available', { amount: formatBytes(worker.available) }) }}</small>
          </div>
          <div class="worker-meta"><span>{{ $t('workers.list.headerBlocks') }}</span><strong>{{ worker.block_num || 0 }}</strong></div>
          <div class="worker-meta"><span>{{ $t('workers.list.headerHeartbeat') }}</span><strong>{{ formatHeartbeat(worker) }}</strong></div>
          <div class="row-actions worker-actions"><button class="link-button" @click="openDetail(worker)">{{ $t('workers.actions.detail') }}</button><button v-if="worker._status !== 'lost' && worker._status !== 'blacklist'" class="link-button" @click="runAction(worker, 'blacklist')">{{ $t('workers.actions.addBlacklist') }}</button><button v-if="worker._status === 'blacklist'" class="link-button" @click="runAction(worker, 'allow')">{{ $t('workers.actions.allow') }}</button><button v-if="worker._status !== 'lost' && worker._status !== 'decommission'" class="link-button" @click="runAction(worker, 'decommission')">{{ $t('workers.actions.decommission') }}</button><button v-if="worker._status === 'decommission'" class="link-button" @click="runAction(worker, 'recommission')">{{ $t('workers.actions.recommission') }}</button></div>
        </article>
      </div>
      <div v-if="filteredWorkers.length === 0" class="empty-state">{{ $t('workers.list.empty') }}</div>
    </section>
  </div>
</template>

<script>
import MetricCard from '@/components/admin/MetricCard.vue'
import { fetchWorkersData, updateWorkerAction } from '@/api/client'
import { formatBytes, formatTime, versionsMismatch, workerAddress, workerHostname, workerId, workerShortVersion } from '@/utils/format'
import { translateDelta, translateStatus, translateWorkerAction } from '@/utils/i18nLabels'
import { useAppStore } from '@/stores/app'
import eventBus from '@/utils/eventBus'

export default {
  name: 'WorkersPage',
  components: { MetricCard },
  data() { return { loading: false, data: {}, statusFilter: 'all', query: '' } },
  computed: {
    appStore() { return useAppStore() },
    liveWorkers() { return (this.data.live_workers || []).map((w) => ({ ...w, _status: 'live' })) },
    blacklistWorkers() { return (this.data.blacklist_workers || []).map((w) => ({ ...w, _status: 'blacklist' })) },
    decommissionWorkers() { return (this.data.decommission_workers || []).map((w) => ({ ...w, _status: 'decommission' })) },
    lostWorkers() { return (this.data.lost_workers || []).map((w) => ({ ...w, _status: 'lost' })) },
    managedWorkers() { return [...this.blacklistWorkers, ...this.decommissionWorkers] },
    workers() { return [...this.liveWorkers, ...this.blacklistWorkers, ...this.decommissionWorkers, ...this.lostWorkers] },
    filteredWorkers() {
      const q = this.query.toLowerCase()
      return this.workers.filter((w) => (this.statusFilter === 'all' || w._status === this.statusFilter)
        && `${workerHostname(w)} ${workerId(w)} ${workerAddress(w)} ${this.ipAddress(w)} ${workerShortVersion(w)}`.toLowerCase().includes(q))
    },
    activeWorkers() { return [...this.liveWorkers, ...this.blacklistWorkers, ...this.decommissionWorkers] },
    totalCapacity() { return this.activeWorkers.reduce((sum, w) => sum + Number(w.capacity || 0), 0) },
    totalAvailable() { return this.activeWorkers.reduce((sum, w) => sum + Number(w.available || 0), 0) },
    usagePercent() { return this.totalCapacity ? Math.round(((this.totalCapacity - this.totalAvailable) / this.totalCapacity) * 100) : 0 },
    usageDelta() { return this.$t('workers.metrics.usedPercent', { percent: this.usagePercent }) }
  },
  created() { this.fetchData() },
  mounted() { eventBus.on('admin-refresh', this.fetchData) },
  beforeUnmount() { eventBus.off('admin-refresh', this.fetchData) },
  methods: {
    formatBytes, workerAddress, workerHostname, workerId,
    deltaLabel(value) { return translateDelta(this.$t, value) },
    statusLabel(status) { return translateStatus(this.$t, status) },
    async fetchData() { this.loading = true; try { this.data = await fetchWorkersData() || {} } finally { this.loading = false } },
    ipAddress(worker) { return worker.display_ip || (worker.address && worker.address.ip_addr) || worker.ip_addr || '-' },
    workerHost(worker) { return (worker.address && worker.address.hostname) || worker.hostname || '-' },
    rpcPort(worker) { return (worker.address && worker.address.rpc_port) || worker.rpc_port || '-' },
    displayIp(worker) { const ip = this.ipAddress(worker); return ip && ip !== '-' ? ip : '' },
    versionLabel(worker) {
      const version = workerShortVersion(worker)
      return version || this.$t('workers.detail.runtime.notReported')
    },
    versionMismatch(worker) {
      return versionsMismatch(workerShortVersion(worker), this.appStore.masterVersion)
    },
    versionClass(worker) {
      return this.versionMismatch(worker) ? 'warn' : ''
    },
    versionTitle(worker) {
      return this.versionMismatch(worker) ? this.$t('workers.detail.runtime.versionMismatch') : ''
    },
    usedBytes(worker) { return Math.max(0, Number(worker.capacity || 0) - Number(worker.available || 0)) },
    workerUsage(worker) { const cap = Number(worker.capacity || 0); return cap ? Math.max(0, Math.min(100, Math.round((this.usedBytes(worker) / cap) * 100))) : 0 },
    formatHeartbeat(worker) { return formatTime(worker.last_update_ms || worker.last_update) },
    workerSelector(worker) { return workerAddress(worker) },
    openDetail(worker) { this.$router.push({ path: `/workers/${encodeURIComponent(this.workerSelector(worker))}` }) },
    async runAction(worker, action) {
      const actionLabel = translateWorkerAction(this.$t, action)
      if (!window.confirm(this.$t('workers.actions.confirm', { action: actionLabel, worker: this.workerSelector(worker) }))) return
      this.loading = true
      try {
        await updateWorkerAction(this.workerSelector(worker), action)
        await this.fetchData()
      } catch (error) {
        window.alert(this.$t('workers.actions.failed', { error }))
      } finally { this.loading = false }
    }
  }
}
</script>
