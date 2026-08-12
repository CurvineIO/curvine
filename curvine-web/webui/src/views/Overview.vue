<template>
  <div class="admin-page" v-loading="loading">
    <section class="metric-grid">
      <MetricCard :label="$t('overview.metrics.master')" :value="masterStateLabel" :meta="activeMasterDisplay" :delta="overview.cluster_id || '-'" :tone="masterState === 'Active' ? 'good' : (masterState === 'Standby' ? 'warn' : 'neutral')" />
      <MetricCard :label="$t('overview.metrics.haNodes')" :value="masterNodes.length || '-'" :meta="standbyLabel" :delta="deltaLabel(masterHa.failover_supported ? 'switchable' : 'readonly')" :tone="masterNodes.length > 1 ? 'good' : 'neutral'" />
      <MetricCard :label="$t('overview.metrics.workers')" :value="workerTotal" :meta="workersMeta" :delta="deltaLabel(overview.lost_workers ? 'attention' : 'healthy')" :tone="overview.lost_workers ? 'warn' : 'good'" />
      <MetricCard :label="$t('overview.metrics.capacity')" :value="formatBytes(overview.capacity)" :meta="capacityMeta" :delta="capacityUsedDelta" tone="neutral" />
      <MetricCard :label="$t('overview.metrics.namespace')" :value="formatCompactNumber(namespaceTotal)" :meta="namespaceFilesMeta" :delta="namespaceDirsDelta" tone="good" />
    </section>

    <section class="content-grid">
      <div class="admin-panel">
        <div class="panel-heading"><div><h2>{{ $t('overview.capacity.title') }}</h2><p>{{ $t('overview.capacity.subtitle') }}</p></div></div>
        <div class="capacity-layout">
          <div class="capacity-donut" :style="capacityStyle"><div><strong>{{ capacityUsedLabel }}</strong><span>{{ $t('overview.capacity.used') }}</span></div></div>
          <div class="capacity-list">
            <div class="capacity-row"><div><span>{{ $t('overview.capacity.fsUsed') }}</span><strong>{{ formatBytes(overview.fs_used) }}</strong></div><div class="meter"><span :style="{ width: capacityUsedPercent + '%' }"></span></div></div>
            <div class="capacity-row"><div><span>{{ $t('overview.capacity.available') }}</span><strong>{{ formatBytes(overview.available) }}</strong></div><div class="meter"><span :style="{ width: availablePercent + '%' }"></span></div></div>
            <div class="capacity-row"><div><span>{{ $t('overview.capacity.reserved') }}</span><strong>{{ formatBytes(overview.reserved_bytes) }}</strong></div><div class="meter"><span :style="{ width: reservedPercent + '%' }"></span></div></div>
          </div>
        </div>
      </div>

      <div class="admin-panel">
        <div class="panel-heading"><div><h2>{{ $t('overview.masterDetails.title') }}</h2><p>{{ $t('overview.masterDetails.subtitle') }}</p></div></div>
        <dl class="detail-list">
          <div><dt>{{ $t('overview.masterDetails.clusterId') }}</dt><dd>{{ overview.cluster_id || '-' }}</dd></div>
          <div><dt>{{ $t('overview.masterDetails.configuredMaster') }}</dt><dd>{{ configuredMasterDisplay }}</dd></div>
          <div><dt>{{ $t('overview.masterDetails.activeMaster') }}</dt><dd>{{ activeMasterDisplayDetail || '-' }}</dd></div>
          <div><dt>{{ $t('overview.masterDetails.startTime') }}</dt><dd>{{ overview.start_time || '-' }}</dd></div>
          <div><dt>{{ $t('overview.masterDetails.blockTotal') }}</dt><dd>{{ formatNumber(overview.block_total) }}</dd></div>
        </dl>
      </div>
    </section>

    <section class="admin-panel table-panel">
      <div class="panel-heading table-heading">
        <div><h2>{{ $t('overview.ha.title') }}</h2><p>{{ $t('overview.ha.subtitle') }}</p></div>
        <button class="admin-button ghost compact" :disabled="haLoading" @click="fetchHa">{{ $t('common.refresh') }}</button>
      </div>
      <table v-if="masterNodes.length" class="admin-table master-ha-table">
        <thead>
          <tr><th>{{ $t('overview.ha.master') }}</th><th>{{ $t('overview.ha.journal') }}</th><th>{{ $t('overview.ha.role') }}</th><th>{{ $t('overview.ha.reachable') }}</th><th>{{ $t('overview.ha.current') }}</th><th>{{ $t('overview.ha.action') }}</th></tr>
        </thead>
        <tbody>
          <tr v-for="node in masterNodes" :key="node.addr">
            <td><strong>{{ node.display_addr || node.addr }}</strong></td>
            <td><span>{{ node.journal_addr || '-' }}</span></td>
            <td><span :class="['status-pill', roleTone(node.role)]">{{ roleLabel(node.role) }}</span></td>
            <td>
              <span
                :class="['status-pill', node.reachable ? 'healthy' : 'failed']"
                :title="node.reachable_error || ''"
              >
                {{ node.reachable ? $t('common.reachable') : $t('common.unreachable') }}
              </span>
            </td>
            <td><span>{{ node.current ? $t('overview.ha.activeTarget') : '-' }}</span></td>
            <td>
              <button
                class="admin-button ghost compact"
                :disabled="Boolean(switchingMaster) || !node.switchable"
                :title="switchTitle(node)"
                @click="switchMaster(node)"
              >
                {{ switchingMaster === node.addr ? $t('common.switching') : $t('common.switch') }}
              </button>
            </td>
          </tr>
        </tbody>
      </table>
      <div v-else class="empty-state">{{ $t('overview.ha.empty') }}</div>
      <div class="ha-note">
        <strong>{{ $t('overview.ha.failoverApi') }}</strong>
        <span>{{ masterHa.failover_supported ? $t('overview.ha.enabled') : $t('overview.ha.unavailable') }}</span>
      </div>
    </section>
  </div>
</template>

<script>
import MetricCard from '@/components/admin/MetricCard.vue'
import { fetchOverviewData, fetchMasterHa, requestMasterFailover } from '@/api/client'
import { displayMasterAddr, formatBytes, formatNumber, formatCompactNumber, isLoopbackHost, normalizeCount } from '@/utils/format'
import { translateDelta, translateRole } from '@/utils/i18nLabels'
import eventBus from '@/utils/eventBus'

export default {
  name: 'OverviewPage',
  components: { MetricCard },
  data() { return { loading: false, haLoading: false, switchingMaster: '', overview: {}, masterHa: {} } },
  computed: {
    masterNodes() { return Array.isArray(this.masterHa.nodes) ? this.masterHa.nodes : [] },
    activeMaster() {
      return this.masterHa.active_master_configured
        || this.masterHa.active_master
        || this.overview.active_master
        || ''
    },
    activeMasterDisplay() { return this.activeMasterDisplayDetail || this.masterHa.current_master || this.overview.master_addr || '-' },
    configuredMasterDisplay() {
      const raw = this.masterHa.configured_master || this.overview.master_addr || '-'
      return displayMasterAddr(raw, this.preferredMasterHostname)
    },
    activeMasterDisplayDetail() {
      const raw = this.overview.active_master_display
        || this.masterHa.active_master_display
        || this.activeMaster
        || ''
      return displayMasterAddr(raw, this.preferredMasterHostname)
    },
    preferredMasterHostname() {
      const candidates = [
        this.overview.local_hostname,
        this.masterHa.local_hostname,
        this.hostnameFromAddr(this.activeMaster),
        this.hostnameFromHaNodes()
      ]
      return candidates.find((host) => host && !isLoopbackHost(host)) || ''
    },
    masterState() {
      const state = this.overview.master_state || this.masterHa.master_state
      if (state === 'Active' || state === 'Standby') return state
      if (!this.activeMaster) return 'Unknown'
      return this.activeMaster === (this.masterHa.current_master || this.overview.master_addr || '') ? 'Active' : 'Standby'
    },
    masterStateLabel() {
      if (this.masterState === 'Unknown') return this.$t('common.unknown')
      return translateRole(this.$t, this.masterState)
    },
    standbyLabel() {
      const standby = this.masterNodes.filter((node) => node.role === 'Standby').length
      if (!this.masterNodes.length) return this.$t('overview.metrics.notConfigured')
      return this.$t('overview.metrics.standbyMeta', { standby, active: this.activeMasterDisplay })
    },
    workersMeta() {
      return this.$t('overview.metrics.liveLost', {
        live: this.overview.live_workers || 0,
        lost: this.overview.lost_workers || 0
      })
    },
    capacityMeta() {
      return this.$t('overview.metrics.availableMeta', { amount: formatBytes(this.overview.available) })
    },
    capacityUsedDelta() {
      return this.$t('overview.metrics.usedDelta', { percent: this.capacityUsedLabel })
    },
    namespaceFilesMeta() {
      return this.$t('overview.metrics.filesMeta', { count: formatCompactNumber(this.overview.files_total) })
    },
    namespaceDirsDelta() {
      return this.$t('overview.metrics.dirsDelta', { count: formatCompactNumber(this.overview.dir_total) })
    },
    workerTotal() { return (this.overview.live_workers || 0) + (this.overview.lost_workers || 0) },
    namespaceTotal() { return normalizeCount(this.overview.files_total) + normalizeCount(this.overview.dir_total) },
    capacityUsedPercent() { return this.percent(this.overview.fs_used, this.overview.capacity) },
    capacityUsedLabel() { return this.percentLabel(this.overview.fs_used, this.overview.capacity) },
    availablePercent() { return this.percent(this.overview.available, this.overview.capacity) },
    reservedPercent() { return this.percent(this.overview.reserved_bytes, this.overview.capacity) },
    capacityStyle() { return { background: `conic-gradient(#6fae3f 0 ${this.capacityUsedPercent}%, #e9f0e4 ${this.capacityUsedPercent}% 100%)` } }
  },
  created() { this.fetchData() },
  mounted() { eventBus.on('admin-refresh', this.fetchData) },
  beforeUnmount() { eventBus.off('admin-refresh', this.fetchData) },
  methods: {
    formatBytes, formatNumber, formatCompactNumber,
    deltaLabel(value) { return translateDelta(this.$t, value) },
    roleLabel(role) { return translateRole(this.$t, role) },
    hostnameFromAddr(addr) {
      const match = String(addr || '').match(/^([^:]+):/)
      return match ? match[1] : ''
    },
    hostnameFromHaNodes() {
      for (const node of this.masterNodes) {
        const host = this.hostnameFromAddr(node.display_addr || node.addr)
        if (host && !isLoopbackHost(host)) return host
      }
      return ''
    },
    percent(value, total) { return total ? Math.min(100, Math.round((Number(value || 0) / Number(total)) * 100)) : 0 },
    percentLabel(value, total) {
      if (!total || !value) return '0%'
      const percent = (Number(value) / Number(total)) * 100
      if (percent > 0 && percent < 0.01) return '<0.01%'
      if (percent < 1) return `${percent.toFixed(2)}%`
      return `${Math.min(100, Math.round(percent))}%`
    },
    roleTone(role) {
      if (role === 'Active') return 'healthy'
      if (role === 'Standby') return 'waiting'
      return 'neutral'
    },
    switchTitle(node) {
      if (!this.masterHa.failover_supported) return this.$t('overview.tooltips.failoverUnavailable')
      if (!node) return ''
      if (this.switchingMaster && this.switchingMaster !== node.addr) {
        return this.$t('overview.tooltips.switchingTo', { target: this.switchingMaster })
      }
      if (node.current || node.role === 'Active') return this.$t('overview.tooltips.alreadyActive')
      if (!node.reachable) return node.reachable_error || this.$t('overview.tooltips.targetUnreachable')
      return ''
    },
    async fetchHa() {
      this.haLoading = true
      try { this.masterHa = await fetchMasterHa() || {} } finally { this.haLoading = false }
    },
    async fetchData() {
      this.loading = true
      try {
        const [overviewResult, haResult] = await Promise.allSettled([fetchOverviewData(), fetchMasterHa()])
        if (overviewResult.status === 'fulfilled') this.overview = overviewResult.value || {}
        if (haResult.status === 'fulfilled') this.masterHa = haResult.value || {}
      } finally { this.loading = false }
    },
    async waitForActiveMaster(targetMaster) {
      let lastError = ''
      for (let index = 0; index < 30; index += 1) {
        await new Promise((resolve) => setTimeout(resolve, 1000))
        try {
          await this.fetchHa()
          if (this.isTargetMasterActive(targetMaster)) return
        } catch (err) {
          lastError = String(err)
        }
      }
      const suffix = lastError ? this.$t('overview.errors.lastError', { error: lastError }) : ''
      throw new Error(this.$t('overview.errors.switchTimeout', { target: targetMaster, suffix }))
    },
    isTargetMasterActive(targetMaster) {
      const node = this.masterNodes.find((item) => item.addr === targetMaster)
      if (node && (node.current || node.role === 'Active')) return true
      const configured = this.masterHa.active_master_configured || this.masterHa.active_master
      if (configured && configured === targetMaster) return true
      if (node && node.raft_id != null && this.masterHa.active_raft_id != null) {
        return node.raft_id === this.masterHa.active_raft_id
      }
      return false
    },
    async switchMaster(node) {
      if (!node || !node.addr) return
      const ok = window.confirm(this.$t('overview.confirm.switchMaster', {
        from: this.activeMasterDisplay,
        to: node.addr
      }))
      if (!ok) return
      this.switchingMaster = node.addr
      try {
        await requestMasterFailover(node.addr)
        await this.waitForActiveMaster(node.addr)
        await this.fetchData()
      } catch (err) {
        window.alert(String(err))
      } finally {
        this.switchingMaster = ''
      }
    }
  }
}
</script>

<style scoped>
.master-ha-table th:nth-child(1) { width: 28%; }
.master-ha-table th:nth-child(2) { width: 24%; }
.master-ha-table th:nth-child(3),
.master-ha-table th:nth-child(4),
.master-ha-table th:nth-child(5) { width: 14%; }
.master-ha-table th:nth-child(6) { width: 10%; }
.ha-note { display: flex; gap: 8px; align-items: center; margin-top: 12px; color: var(--admin-muted); font-size: 13px; }
.ha-note strong { color: var(--admin-text); }
@media (max-width: 760px) { .ha-note { display: grid; } }
</style>
