<template>
  <div class="admin-page" v-loading="loading">
    <section class="admin-panel">
      <div class="panel-heading table-heading"><div><h2>{{ $t('workers.detail.title') }}</h2><p>{{ title }}</p></div><div class="row-actions"><button class="admin-button ghost compact" @click="$router.push('/workers')">{{ $t('common.back') }}</button><button v-if="manageable && rawStatusName !== 'Blacklist'" class="admin-button ghost compact" @click="runAction('blacklist')">{{ $t('workers.actions.addBlacklist') }}</button><button v-if="rawStatusName === 'Blacklist'" class="admin-button ghost compact" @click="runAction('allow')">{{ $t('workers.actions.allow') }}</button><button v-if="manageable && rawStatusName !== 'Decommission'" class="admin-button ghost compact" @click="runAction('decommission')">{{ $t('workers.actions.decommission') }}</button><button v-if="rawStatusName === 'Decommission'" class="admin-button ghost compact" @click="runAction('recommission')">{{ $t('workers.actions.recommission') }}</button></div></div>
      <div class="detail-summary">
        <div><span>{{ $t('workers.list.headerStatus') }}</span><strong><span :class="['status-pill', statusClass]">{{ workerStatusLabel || '-' }}</span></strong></div>
        <div><span>{{ $t('workers.list.headerUsage') }}</span><strong>{{ usagePercent }}%</strong><div class="inline-meter"><span :style="{ width: usagePercent + '%' }"></span></div></div>
        <div><span>{{ $t('common.capacity') }}</span><strong>{{ formatBytes(worker.capacity) }}</strong></div>
        <div><span>{{ $t('common.available') }}</span><strong>{{ formatBytes(worker.available) }}</strong></div>
      </div>
    </section>

    <section class="content-grid">
      <div class="admin-panel">
        <div class="panel-heading"><div><h2>{{ $t('workers.detail.runtime.title') }}</h2><p>{{ $t('workers.detail.runtime.subtitle') }}</p></div></div>
        <dl class="detail-list">
          <div><dt>{{ $t('workers.detail.runtime.hostname') }}</dt><dd>{{ workerHostname(worker) }}</dd></div>
          <div><dt>{{ $t('workers.detail.runtime.workerId') }}</dt><dd>{{ workerId(worker) }}</dd></div>
          <div><dt>{{ $t('workers.detail.runtime.rpcAddress') }}</dt><dd>{{ workerAddress(worker) }}</dd></div>
          <div><dt>{{ $t('workers.detail.runtime.webPort') }}</dt><dd>{{ address.web_port || '-' }}</dd></div>
          <div><dt>{{ $t('workers.detail.runtime.ip') }}</dt><dd>{{ displayIp }}</dd></div>
          <div><dt>{{ $t('workers.detail.runtime.heartbeat') }}</dt><dd>{{ formatTime(worker.last_update) }}</dd></div>
          <div><dt>{{ $t('workers.detail.runtime.version') }}</dt><dd>{{ detail.version || $t('workers.detail.runtime.notReported') }}</dd></div>
          <div><dt>{{ $t('workers.detail.runtime.files') }}</dt><dd>{{ detail.file_count ?? $t('workers.detail.runtime.notReported') }}</dd></div>
          <div><dt>{{ $t('workers.detail.runtime.blocks') }}</dt><dd>{{ worker.block_num || 0 }}</dd></div>
        </dl>
      </div>

      <div class="admin-panel">
        <div class="panel-heading"><div><h2>{{ $t('workers.detail.storage.title') }}</h2><p>{{ $t('workers.detail.storage.subtitle') }}</p></div></div>
        <dl class="detail-list">
          <div><dt>{{ $t('workers.detail.storage.curvineUsed') }}</dt><dd>{{ formatBytes(worker.fs_used) }}</dd></div>
          <div><dt>{{ $t('workers.detail.storage.nonFsUsed') }}</dt><dd>{{ formatBytes(worker.non_fs_used) }}</dd></div>
          <div><dt>{{ $t('workers.detail.storage.reserved') }}</dt><dd>{{ formatBytes(worker.reserved_bytes) }}</dd></div>
          <div><dt>{{ $t('workers.detail.storage.storageDirs') }}</dt><dd>{{ storageItems.length }}</dd></div>
        </dl>
      </div>
    </section>

    <section class="admin-panel">
      <div class="panel-heading table-heading"><div><h2>{{ $t('workers.detail.storage.directoriesTitle') }}</h2><p>{{ $t('workers.detail.storage.directoriesSubtitle') }}</p></div></div>
      <div class="storage-list">
        <article v-for="item in storageItems" :key="item.storage_id" class="storage-row">
          <div class="storage-main"><strong>{{ item.dir_path }}</strong><span>{{ item.storage_id }}</span></div>
          <div class="worker-meta"><span>{{ $t('workers.detail.storage.type') }}</span><strong>{{ item.storage_type || '-' }}</strong></div>
          <div class="worker-usage"><div class="usage-line"><strong>{{ storageUsage(item) }}%</strong><span>{{ formatBytes(item.capacity - item.available) }} / {{ formatBytes(item.capacity) }}</span></div><div class="inline-meter"><span :style="{ width: storageUsage(item) + '%' }"></span></div><small>{{ $t('workers.list.available', { amount: formatBytes(item.available) }) }}</small></div>
          <div class="worker-meta"><span>{{ $t('workers.list.headerBlocks') }}</span><strong>{{ item.block_num || 0 }}</strong></div>
          <div class="worker-state"><span :class="['status-pill', item.failed ? 'failed' : 'healthy']">{{ storageStatusLabel(item.failed ? 'failed' : 'healthy') }}</span></div>
        </article>
      </div>
      <div v-if="storageItems.length === 0" class="empty-state">{{ $t('workers.detail.storage.empty') }}</div>
    </section>
  </div>
</template>

<script>
import { fetchWorkerDetail, updateWorkerAction } from '@/api/client'
import { formatBytes, formatTime, workerAddress, workerHostname, workerId } from '@/utils/format'
import { translateStatus, translateWorkerAction } from '@/utils/i18nLabels'

export default {
  name: 'WorkerDetailPage',
  data() { return { loading: false, detail: {}, worker: {} } },
  computed: {
    address() { return this.worker.address || {} },
    rawStatusName() { return this.detail.state || this.worker.status || '' },
    workerStatusLabel() { return this.rawStatusName ? translateStatus(this.$t, this.rawStatusName) : '' },
    statusClass() { return String(this.rawStatusName || 'unknown').toLowerCase() },
    title() {
      if (this.workerHostname(this.worker) === '-') {
        return this.$t('workers.detail.subtitle')
      }
      const parts = [this.workerHostname(this.worker), this.workerAddress(this.worker)]
      if (this.displayIp !== '-') {
        parts.push(`${this.$t('workers.detail.runtime.ip')} ${this.displayIp}`)
      }
      return parts.join(' - ')
    },
    manageable() { return this.rawStatusName !== 'Lost' },
    usagePercent() { return this.workerUsage(this.worker) },
    storageItems() { return Object.values(this.worker.storage_map || {}) },
    displayIp() { return this.worker.display_ip || this.address.ip_addr || '-' }
  },
  created() { this.fetchData() },
  methods: {
    formatBytes, formatTime, workerAddress, workerHostname, workerId,
    storageStatusLabel(status) { return translateStatus(this.$t, status) },
    async fetchData() {
      this.loading = true
      try {
        this.detail = await fetchWorkerDetail(this.$route.params.id)
        this.worker = this.detail.worker || {}
      } finally { this.loading = false }
    },
    workerSelector(worker) { return this.workerAddress(worker) },
    workerUsage(worker) { const cap = Number(worker.capacity || 0); const avail = Number(worker.available || 0); return cap ? Math.max(0, Math.min(100, Math.round(((cap - avail) / cap) * 100))) : 0 },
    storageUsage(item) { const cap = Number(item.capacity || 0); const avail = Number(item.available || 0); return cap ? Math.max(0, Math.min(100, Math.round(((cap - avail) / cap) * 100))) : 0 },
    async runAction(action) {
      const selector = this.workerSelector(this.worker)
      const actionLabel = translateWorkerAction(this.$t, action)
      if (!window.confirm(this.$t('workers.actions.confirm', { action: actionLabel, worker: selector }))) return
      this.loading = true
      try {
        await updateWorkerAction(selector, action)
        await this.fetchData()
      } catch (error) {
        window.alert(this.$t('workers.actions.failed', { error }))
      } finally { this.loading = false }
    }
  }
}
</script>
