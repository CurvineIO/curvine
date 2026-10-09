<template>
  <div class="admin-page transfers-page" v-loading="loading">
    <section class="metric-grid transfer-metric-grid">
      <MetricCard label="Transfer Jobs" :value="transfers.length" meta="transfer service jobs" delta="total" tone="neutral" />
      <MetricCard label="Active" :value="activeCount" meta="pending or executing" delta="active" tone="good" />
      <MetricCard label="Failed" :value="failedCount" meta="failed or partial success" :tone="failedCount ? 'bad' : 'good'" delta="review" />
      <MetricCard label="Completed" :value="completedCount" meta="finished transfers" delta="done" tone="good" />
    </section>

    <section class="content-grid transfers-grid">
      <div class="admin-panel table-panel">
        <div class="panel-heading table-heading">
          <div>
            <h2>Transfer Tasks</h2>
            <p>Load / export jobs owned by the transfer service</p>
          </div>
          <div class="row-actions">
            <button class="admin-button ghost compact" @click="refreshTransfers">Refresh</button>
            <button class="admin-button primary compact" @click="openSubmitForm">Submit Transfer</button>
          </div>
        </div>

        <div class="panel-heading table-heading transfer-filter-bar">
          <div class="segmented" role="tablist" aria-label="Transfer status filter">
            <button :class="{ active: statusFilter === 'all' }" @click="statusFilter = 'all'">All {{ transfers.length }}</button>
            <button :class="{ active: statusFilter === 'active' }" @click="statusFilter = 'active'">Active {{ activeCount }}</button>
            <button :class="{ active: statusFilter === 'completed' }" @click="statusFilter = 'completed'">Completed {{ completedCount }}</button>
            <button :class="{ active: statusFilter === 'failed' }" @click="statusFilter = 'failed'">Failed {{ failedCount }}</button>
            <button :class="{ active: statusFilter === 'canceled' }" @click="statusFilter = 'canceled'">Canceled {{ canceledCount }}</button>
          </div>
        </div>

        <div class="toolbar transfers-toolbar">
          <input
            v-model="query"
            class="admin-input transfer-search-input"
            placeholder="Search by id, path, submitter, tenant or kind"
          >
          <select v-model="kindFilter" class="admin-input compact-select">
            <option value="all">All kinds</option>
            <option value="load">Load</option>
            <option value="export">Export</option>
          </select>
          <select v-model="timeRange" class="admin-input compact-select">
            <option value="all">Any time</option>
            <option value="1h">Last 1h</option>
            <option value="24h">Last 24h</option>
            <option value="7d">Last 7d</option>
          </select>
          <label class="admin-toggle"><input v-model="polling" type="checkbox" @change="togglePolling"> Poll active</label>
          <label class="admin-toggle failed-only-toggle"><input v-model="failedOnly" type="checkbox"> Failed only</label>
        </div>
        <div v-if="notice" class="admin-notice">{{ notice }}</div>

        <div class="transfer-list">
          <div class="transfer-row transfer-header">
            <span>Job</span>
            <span>Kind</span>
            <span>Path</span>
            <span>Status</span>
            <span>Progress</span>
            <span>Files</span>
            <span>Updated</span>
            <span>Actions</span>
          </div>
          <article v-for="transfer in filteredTransfers" :key="jobId(transfer)" class="transfer-row" :class="{ selected: selectedTransferId === jobId(transfer) }">
            <div class="transfer-id-cell" :title="jobListTooltip(transfer)" @click.stop="copyText(jobId(transfer), 'Transfer id copied')">
              <strong class="mono transfer-list-title">{{ jobListTitle(transfer) }}</strong>
              <small class="transfer-display-name">{{ jobDisplayName(transfer) }}</small>
            </div>
            <div class="transfer-kind-cell">
              <span :class="['transfer-kind-badge', kindClass(transfer)]">{{ kindLabel(transfer) }}</span>
            </div>
            <div class="transfer-main" :title="`${sourcePath(transfer)}\n${targetPath(transfer)}`">
              <strong :title="sourcePath(transfer)">{{ sourcePath(transfer) }}</strong>
              <small :title="targetPath(transfer)">-> {{ targetPath(transfer) }}</small>
              <small v-if="transfer.message" class="transfer-hint" :title="transfer.message">{{ transfer.message }}</small>
            </div>
            <div class="transfer-state">
              <span :class="['status-pill', transferClass(transfer)]">{{ stateLabel(transfer) }}</span>
            </div>
            <div class="transfer-progress">
              <div class="usage-line"><strong>{{ progress(transfer) }}%</strong></div>
              <div class="inline-meter"><span :style="{ width: progress(transfer) + '%' }"></span></div>
            </div>
            <div class="transfer-meta transfer-meta-files"><span>Files</span><strong :title="formatFiles(transfer)">{{ formatFiles(transfer) }}</strong></div>
            <div class="transfer-meta transfer-meta-updated"><span>Updated</span><strong :title="formatTime(transfer.update_time_ms)">{{ relativeTime(transfer.update_time_ms) }}</strong></div>
            <div class="row-actions transfer-actions">
              <button class="link-button" @click="openDetails(transfer)">Details</button>
              <button
                v-if="canRetry(transfer)"
                class="link-button"
                :disabled="isTransferBusy(transfer)"
                @click="retryTransferJob(transfer)"
              >{{ busyLabel(transfer, 'retry', 'Retry') }}</button>
              <button
                v-if="!isFinal(transfer)"
                class="link-button danger-link"
                :disabled="isTransferBusy(transfer)"
                @click="cancelTransferJob(transfer)"
              >{{ busyLabel(transfer, 'cancel', 'Cancel') }}</button>
            </div>
          </article>
        </div>
        <div v-if="filteredTransfers.length === 0" class="empty-state">{{ emptyMessage }}</div>
      </div>

      <aside v-if="selectedTransfer" class="admin-panel transfer-detail-panel" v-loading="detailLoading">
        <div class="detail-panel-heading">
          <div>
            <h2>Transfer Diagnostics</h2>
            <p class="mono">{{ jobListTitle(selectedTransfer) }}</p>
            <p class="mono transfer-id-subtitle">{{ jobId(selectedTransfer) }}</p>
          </div>
          <button class="admin-button ghost compact" @click="closeDetails">Close</button>
        </div>

        <div class="detail-badges">
          <span :class="['status-pill', transferClass(selectedTransfer)]">{{ stateLabel(selectedTransfer) }}</span>
          <span :class="['transfer-kind-badge', kindClass(selectedTransfer)]">{{ kindLabel(selectedTransfer) }}</span>
          <button class="link-button" @click="copyText(jobId(selectedTransfer), 'Transfer id copied')">Copy Job ID</button>
          <button
            v-if="canRetry(selectedTransfer)"
            class="link-button"
            :disabled="isTransferBusy(selectedTransfer)"
            @click="retryTransferJob(selectedTransfer)"
          >{{ busyLabel(selectedTransfer, 'retry', 'Retry') }}</button>
        </div>

        <section class="detail-section">
          <h3>Transfer Overview</h3>
          <div class="detail-grid">
            <div><span>Kind</span><strong>{{ kindLabel(selectedTransfer) }}</strong></div>
            <div><span>State</span><strong>{{ stateLabel(selectedTransfer) }}</strong></div>
            <div><span>Submitter</span><strong>{{ selectedTransfer.submitter || 'User' }}</strong></div>
            <div><span>Tenant</span><strong>{{ selectedTransfer.tenant || '-' }}</strong></div>
            <div><span>Run ID</span><strong class="mono">{{ selectedTransfer.run_id ?? '-' }}</strong></div>
            <div><span>Updated At</span><strong>{{ formatTime(selectedTransfer.update_time_ms) }}</strong></div>
          </div>
          <div class="path-detail">
            <div><span>Source Path</span><strong :title="sourcePath(selectedTransfer)">{{ sourcePath(selectedTransfer) }}</strong><button class="link-button" @click="copyText(sourcePath(selectedTransfer), 'Source path copied')">Copy</button></div>
            <div><span>Target Path</span><strong :title="targetPath(selectedTransfer)">{{ targetPath(selectedTransfer) }}</strong><button class="link-button" @click="copyText(targetPath(selectedTransfer), 'Target path copied')">Copy</button></div>
          </div>
          <p v-if="selectedTransfer.message" :class="detailMessageClass(selectedTransfer)">{{ selectedTransfer.message }}</p>
        </section>

        <section class="detail-section">
          <h3>Task Counters</h3>
          <div class="counter-grid">
            <div><span>Total</span><strong>{{ displayTaskTotal(selectedTransfer) }}</strong></div>
            <div><span>Completed</span><strong>{{ taskCountLabel(selectedTransfer.completed_files) }}</strong></div>
            <div><span>Failed</span><strong class="bad-count">{{ failedTaskCount(selectedTransfer) }}</strong></div>
            <div><span>Running</span><strong>{{ runningTaskCount(selectedTransfer) }}</strong></div>
            <div><span>Loaded</span><strong>{{ sizeLabel(selectedTransfer) }}</strong></div>
            <div><span>Transferred</span><strong>{{ transferredLabel(selectedTransfer) }}</strong></div>
          </div>
          <button v-if="failedTaskCount(selectedTransfer)" class="admin-button ghost compact" @click="showFailedTasksOnly = !showFailedTasksOnly">
            {{ showFailedTasksOnly ? 'Show all tasks' : 'View failed tasks' }}
          </button>
        </section>

        <section class="detail-section">
          <h3>{{ showFailedTasksOnly ? 'Failed Tasks' : 'All Tasks' }}</h3>
          <div class="task-table-scroll">
            <div class="transfer-task-row transfer-task-header">
              <span>Task ID</span>
              <span>State</span>
              <span>Worker</span>
              <span>Source Path</span>
              <span>Target Path</span>
              <span>Retry</span>
              <span>Message</span>
              <span>Updated At</span>
            </div>
            <div v-for="task in selectedTaskList" :key="task.task_id || task.id" class="transfer-task-row" :class="{ failed: isFailedTask(task) }">
              <span class="mono" :title="task.task_id || task.id">{{ shortTaskId(task.task_id || task.id) }}</span>
              <span :class="['status-pill', transferTaskClass(task)]">{{ stateLabel(task) }}</span>
              <span :title="task.worker || '-'">{{ task.worker || '-' }}</span>
              <span :title="task.source_path || '-'">{{ task.source_path || '-' }}</span>
              <span :title="task.target_path || '-'">{{ task.target_path || '-' }}</span>
              <span>{{ retryCount(task) }}</span>
              <span :title="taskMessage(task, selectedTransfer)">{{ taskMessage(task, selectedTransfer) }}</span>
              <span :title="formatTime(task.update_time_ms)">{{ relativeTime(task.update_time_ms) }}</span>
            </div>
          </div>
          <div v-if="selectedTaskList.length === 0" class="empty-state">{{ selectedTaskEmptyMessage }}</div>
          <div v-if="canLoadMoreTasks" class="task-list-footer">
            <button class="admin-button ghost compact" :disabled="taskLoadingMore" @click="loadMoreTasks">
              {{ taskLoadingMore ? 'Loading...' : `Load more tasks (showing ${selectedTransfer.tasks ? selectedTransfer.tasks.length : 0})` }}
            </button>
          </div>
          <div v-else-if="taskListTruncated" class="task-list-footer task-list-hint">
            Showing the first {{ taskPageSize }} tasks. Use <code>curvine-cli transfer status &lt;job&gt; --all</code> for the full list.
          </div>
        </section>
      </aside>
    </section>

    <div v-if="showSubmitForm" class="admin-modal-backdrop">
      <div class="admin-panel form-panel transfer-form admin-modal-panel">
        <div class="panel-heading">
          <div>
            <h2>Submit Transfer</h2>
            <p>Submit a load or export to the transfer service</p>
          </div>
          <button class="admin-button ghost compact" @click="closeSubmitForm">Close</button>
        </div>
        <div class="form-row">
          <label>Transfer Type
            <select v-model="form.kind">
              <option value="load">Load (UFS -> Curvine)</option>
              <option value="export">Export (Curvine -> UFS)</option>
            </select>
          </label>
          <label>Overwrite
            <select v-model="form.overwrite">
              <option :value="true">true</option>
              <option :value="false">false</option>
            </select>
          </label>
        </div>
        <label>Source Path<input v-model="form.source_path" :placeholder="sourcePlaceholder"></label>
        <label>Target Path (optional)<input v-model="form.target_path" :placeholder="targetPlaceholder"></label>
        <p class="form-hint">Leave target blank to derive it from the mount mapping. Load requires a UFS source and Curvine target; export is the reverse.</p>
        <div class="form-actions">
          <button class="admin-button ghost" @click="resetForm">Reset</button>
          <button class="admin-button primary" @click="submitTransferJob">Submit Transfer</button>
          <button class="admin-button ghost" @click="closeSubmitForm">Cancel</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import MetricCard from '@/components/admin/MetricCard.vue'
import { cancelTransfer, fetchTransferStatus, fetchTransfersData, retryTransfer, submitTransfer } from '@/api/client'
import { formatBytes, formatTime } from '@/utils/format'
import { copyToClipboard } from '@/utils/clipboard'
import { buildJobDisplayName, jobListSubtitle, jobListTitle, jobListTooltip, shortJobId } from '@/utils/jobDisplay'
import eventBus from '@/utils/eventBus'

const emptyForm = () => ({
  kind: 'load',
  source_path: '',
  target_path: '',
  overwrite: true
})

// Task pages are requested from offset 0 with a growing page size instead of
// following page tokens, so each poll refresh replaces the task list as a whole
// and never has to merge paged state. Jobs beyond TASK_PAGE_MAX are inspected
// with `curvine-cli transfer status --all`.
const TASK_PAGE_DEFAULT = 100
const TASK_PAGE_STEP = 100
const TASK_PAGE_MAX = 1000

export default {
  name: 'TransfersPage',
  components: { MetricCard },
  data() {
    return {
      loading: false,
      detailLoading: false,
      transfers: [],
      form: emptyForm(),
      statusFilter: 'all',
      query: '',
      kindFilter: 'all',
      timeRange: 'all',
      failedOnly: false,
      selectedTransferId: '',
      showFailedTasksOnly: false,
      showSubmitForm: false,
      polling: true,
      pollTimer: null,
      busyTransfers: {},
      notice: '',
      taskPageSize: TASK_PAGE_DEFAULT,
      taskNextPageToken: '',
      taskLoadingMore: false
    }
  },
  computed: {
    activeCount() { return this.transfers.filter((transfer) => this.jobBucket(transfer) === 'active').length },
    completedCount() { return this.transfers.filter((transfer) => this.jobBucket(transfer) === 'completed').length },
    failedCount() { return this.transfers.filter((transfer) => this.jobBucket(transfer) === 'failed').length },
    canceledCount() { return this.transfers.filter((transfer) => this.jobBucket(transfer) === 'canceled').length },
    selectedTransfer() {
      if (!this.selectedTransferId) return null
      return this.transfers.find((transfer) => this.jobId(transfer) === this.selectedTransferId) || null
    },
    selectedTaskList() {
      if (!this.selectedTransfer) return []
      const tasks = this.taskDetails(this.selectedTransfer)
      return this.showFailedTasksOnly ? tasks.filter((task) => this.isFailedTask(task)) : tasks
    },
    selectedTaskEmptyMessage() {
      if (!this.selectedTransfer) return 'No task details available for this view.'
      if (this.showFailedTasksOnly) return 'No failed tasks in this transfer.'
      return 'No task details available for this transfer.'
    },
    canLoadMoreTasks() {
      if (!this.selectedTransfer) return false
      if (this.taskPageSize >= TASK_PAGE_MAX) return false
      return Boolean(this.taskNextPageToken)
    },
    taskListTruncated() {
      return this.taskPageSize >= TASK_PAGE_MAX && Boolean(this.taskNextPageToken)
    },
    emptyMessage() {
      if (!this.transfers.length) return 'No transfer jobs tracked yet. Submit a transfer to get started.'
      const hints = []
      if (this.timeRange !== 'all') hints.push(`time range: ${this.timeRange}`)
      if (this.kindFilter !== 'all') hints.push(`kind: ${this.kindFilter}`)
      if (this.failedOnly) hints.push('failed only')
      const suffix = hints.length ? ` Active filters (${hints.join(', ')}).` : ''
      return `No transfers matched the current filters.${suffix}`
    },
    filteredTransfers() {
      const q = this.query.trim().toLowerCase()
      return this.transfers.filter((transfer) => {
        const bucket = this.jobBucket(transfer)
        if (this.statusFilter !== 'all' && bucket !== this.statusFilter) return false
        if (this.failedOnly && bucket !== 'failed') return false
        if (this.kindFilter !== 'all' && this.kindValue(transfer) !== this.kindFilter) return false
        if (!this.isWithinRange(transfer.update_time_ms, this.timeRange)) return false
        if (!q) return true
        const haystack = `${this.jobId(transfer)} ${this.jobDisplayName(transfer)} ${this.jobListSubtitle(transfer)} ${this.sourcePath(transfer)} ${this.targetPath(transfer)} ${transfer.submitter || ''} ${transfer.tenant || ''} ${transfer.kind || ''} ${transfer.message || ''}`.toLowerCase()
        return haystack.includes(q)
      })
    },
    sourcePlaceholder() {
      return this.form.kind === 'load' ? 'file:///data/path or mounted path' : '/curvine/path or cv://path'
    },
    targetPlaceholder() {
      return this.form.kind === 'load' ? '/curvine/path (derived from mount if blank)' : 'file:///data/path (derived from mount if blank)'
    }
  },
  created() { this.refreshTransfers() },
  mounted() {
    eventBus.on('admin-refresh', this.refreshTransfers)
    this.togglePolling()
  },
  beforeUnmount() {
    eventBus.off('admin-refresh', this.refreshTransfers)
    this.stopPolling()
  },
  methods: {
    formatBytes,
    formatTime,
    jobId(transfer) { return transfer && (transfer.job_id || transfer.id) },
    jobDisplayName(transfer) { return buildJobDisplayName(transfer) },
    jobListTitle(transfer) { return jobListTitle(transfer) },
    jobListSubtitle(transfer) { return jobListSubtitle(transfer, this.jobId(transfer)) },
    jobListTooltip(transfer) { return jobListTooltip(transfer, this.jobId(transfer)) },
    shortJobId(id) { return shortJobId(id) },
    shortTaskId(id) {
      if (!id) return '-'
      const value = String(id)
      return value.length > 22 ? `${value.slice(0, 12)}...${value.slice(-7)}` : value
    },
    sourcePath(transfer) { return transfer && (transfer.path || transfer.source_path || '-') },
    targetPath(transfer) { return transfer && (transfer.target_path || '-') },
    kindValue(transfer) { return String(transfer && (transfer.kind || 'load')).toLowerCase() },
    kindLabel(transfer) {
      return this.kindValue(transfer) === 'export' ? 'Export' : 'Load'
    },
    kindClass(transfer) {
      return this.kindValue(transfer) === 'export' ? 'export' : 'load'
    },
    stateValue(transfer) {
      const raw = transfer && (transfer.state || transfer.status)
      if (!raw) return ''
      return String(raw).toLowerCase()
    },
    stateLabel(transfer) { return transfer && (transfer.state || transfer.status || 'Unknown') },
    isFinal(transfer) {
      if (transfer && transfer.done === true) return true
      return ['completed', 'failed', 'canceled', 'partialsuccess'].includes(this.stateValue(transfer))
    },
    // Align with transfer service retry_transfer: terminal jobs only
    // (Completed / Failed / Canceled / PartialSuccess).
    canRetry(transfer) {
      return this.isFinal(transfer)
    },
    jobBucket(transfer) {
      const value = this.stateValue(transfer)
      if (['pending', 'planning', 'dispatching', 'running', 'canceling'].includes(value)) return 'active'
      if (value === 'completed') return 'completed'
      if (value === 'failed' || value === 'partialsuccess') return 'failed'
      if (value === 'canceled') return 'canceled'
      return 'active'
    },
    transferClass(transfer) {
      const value = this.stateValue(transfer)
      if (value === 'partialsuccess' || value.includes('fail') || value.includes('cancel')) return 'failed'
      if (value.includes('pend') || value.includes('wait') || value.includes('plan') || value.includes('dispatch')) return 'waiting'
      if (value.includes('run')) return 'running'
      if (value.includes('complete')) return 'healthy'
      return 'warning'
    },
    transferTaskClass(task) { return this.transferClass(task) },
    progress(transfer) { return Math.max(0, Math.min(100, Math.round(Number(transfer && transfer.progress || 0)))) },
    formatFiles(transfer) {
      const total = Number(transfer && transfer.total_files || 0)
      if (!total) return '-'
      return `${Number(transfer && transfer.completed_files || 0)} / ${total}`
    },
    sizeLabel(transfer) {
      const loaded = Number(transfer && transfer.loaded_size || 0)
      const total = Number(transfer && transfer.total_size || 0)
      if (!loaded && !total) return '-'
      return `${this.formatBytes(loaded)} / ${this.formatBytes(total)}`
    },
    taskDetails(transfer) {
      return Array.isArray(transfer && transfer.tasks) ? transfer.tasks : []
    },
    displayTaskTotal(transfer) {
      if (!transfer) return '-'
      const tasks = this.taskDetails(transfer).length
      if (transfer.total_files === null || transfer.total_files === undefined) {
        return tasks || '-'
      }
      const total = Number(transfer.total_files || 0)
      return total > 0 ? total : (tasks || '-')
    },
    taskCountLabel(value) {
      if (value === null || value === undefined) return '-'
      return Number(value) || 0
    },
    transferredLabel(transfer) {
      const value = transfer && transfer.completed_size
      if (value === null || value === undefined) return '-'
      return this.formatBytes(Number(value) || 0)
    },
    failedTaskCount(transfer) {
      const tasks = this.taskDetails(transfer)
      if (tasks.length) return tasks.filter((task) => this.isFailedTask(task)).length
      const value = Number(transfer && (transfer.failed_files || transfer.failed_tasks || 0))
      return value || 0
    },
    runningTaskCount(transfer) {
      if (!['pending', 'planning', 'dispatching', 'running', 'canceling'].includes(this.stateValue(transfer))) return 0
      const tasks = this.taskDetails(transfer)
      if (tasks.length) return tasks.filter((task) => ['pending', 'running'].includes(this.stateValue(task))).length
      return Number(transfer && (transfer.running_files || transfer.loading_files || transfer.pending_files || 0))
    },
    isFailedTask(task) { return this.stateValue(task) === 'failed' },
    // Scheduling reasons ("waiting for scheduling", capacity backpressure)
    // surface here too; only render them as errors for failed jobs.
    detailMessageClass(transfer) {
      const value = this.stateValue(transfer)
      const isError = value === 'failed' || value === 'partialsuccess' || value === 'canceled'
      return isError ? 'detail-error' : 'detail-hint'
    },
    taskMessage(task, transfer = null) {
      const message = task && (task.message || (task.progress && task.progress.message))
      return message || (transfer && transfer.message) || '-'
    },
    retryCount(task) {
      const value = task && (task.retry_count ?? task.retries ?? task.attempts)
      return value === undefined || value === null ? '-' : value
    },
    isWithinRange(timestamp, range) {
      if (!range || range === 'all') return true
      const value = Number(timestamp || 0)
      if (!value) return true
      const span = { '1h': 60 * 60 * 1000, '24h': 24 * 60 * 60 * 1000, '7d': 7 * 24 * 60 * 60 * 1000 }[range]
      return !span || Date.now() - value <= span
    },
    relativeTime(timestamp) {
      const value = Number(timestamp || 0)
      if (!value) return '-'
      const delta = Math.max(0, Date.now() - value)
      if (delta < 60 * 1000) return 'just now'
      if (delta < 60 * 60 * 1000) return `${Math.floor(delta / 60000)} min ago`
      if (delta < 24 * 60 * 60 * 1000) return `${Math.floor(delta / 3600000)} h ago`
      return `${Math.floor(delta / 86400000)} d ago`
    },
    mergeTransferList(nextTransfers) {
      const previousById = new Map(this.transfers.map((transfer) => [this.jobId(transfer), transfer]))
      const byId = new Map()
      for (const transfer of nextTransfers) {
        const id = this.jobId(transfer)
        if (!id) continue
        const previous = previousById.get(id)
        if (previous) {
          const next = { ...previous, ...transfer }
          if (!Array.isArray(next.tasks) || next.tasks.length === 0) next.tasks = previous.tasks || []
          byId.set(id, next)
        } else {
          byId.set(id, transfer)
        }
      }
      return Array.from(byId.values()).sort((left, right) => {
        const leftTime = Number(left.update_time_ms || 0)
        const rightTime = Number(right.update_time_ms || 0)
        return rightTime - leftTime
      })
    },
    upsertTransfer(transfer) {
      const id = this.jobId(transfer)
      if (!id) return
      const index = this.transfers.findIndex((item) => this.jobId(item) === id)
      if (index >= 0) {
        const previous = this.transfers[index]
        const next = { ...previous, ...transfer }
        if (!Array.isArray(next.tasks) || next.tasks.length === 0) next.tasks = previous.tasks || []
        this.transfers.splice(index, 1, next)
      } else {
        this.transfers.unshift(transfer)
      }
    },
    setNotice(message) { this.notice = message },
    setTransferBusy(id, action) {
      if (!id) return
      if (action) this.busyTransfers = { ...this.busyTransfers, [id]: action }
      else {
        const next = { ...this.busyTransfers }
        delete next[id]
        this.busyTransfers = next
      }
    },
    isTransferBusy(transfer) { return Boolean(this.busyTransfers[this.jobId(transfer)]) },
    busyLabel(transfer, action, fallback) { return this.busyTransfers[this.jobId(transfer)] === action ? 'Working...' : fallback },
    async refreshTransfers() {
      this.loading = true
      try {
        const response = await fetchTransfersData({ page_size: 1000 })
        const items = Array.isArray(response) ? response : (response.items || [])
        this.transfers = this.mergeTransferList(items)
      } catch (error) {
        window.alert(`Transfer refresh failed: ${error}`)
      } finally { this.loading = false }
    },
    async submitTransferJob() {
      const source_path = String(this.form.source_path || '').trim()
      if (!source_path) {
        window.alert('Source Path is required')
        return
      }
      this.loading = true
      try {
        const transfer = await submitTransfer({
          kind: this.form.kind,
          source_path,
          target_path: String(this.form.target_path || '').trim() || null,
          overwrite: this.form.overwrite
        })
        this.upsertTransfer(transfer)
        window.alert(`Transfer submitted: ${this.jobDisplayName(transfer)}`)
        this.closeSubmitForm()
        this.togglePolling()
      } catch (error) {
        window.alert(`Submit transfer failed: ${error}`)
      } finally { this.loading = false }
    },
    async openDetails(transfer) {
      const id = this.jobId(transfer)
      if (!id) return
      this.selectedTransferId = id
      this.showFailedTasksOnly = false
      this.taskPageSize = TASK_PAGE_DEFAULT
      await this.refreshTransfer(transfer)
    },
    async refreshTransfer(transfer) {
      const id = this.jobId(transfer)
      if (!id) return
      this.detailLoading = true
      try {
        const next = await fetchTransferStatus(id, { page_size: this.taskPageSize })
        this.taskNextPageToken = next && next.next_page_token ? next.next_page_token : ''
        this.upsertTransfer(next)
        this.setNotice(`Status refreshed: ${id} is ${this.stateLabel(next)} (${this.progress(next)}%)`)
      } catch (error) {
        this.setNotice(`Status failed: ${error}`)
      } finally { this.detailLoading = false }
    },
    async loadMoreTasks() {
      if (!this.selectedTransfer || this.taskLoadingMore) return
      if (!this.canLoadMoreTasks) return
      this.taskLoadingMore = true
      this.taskPageSize = Math.min(this.taskPageSize + TASK_PAGE_STEP, TASK_PAGE_MAX)
      try {
        await this.refreshTransfer(this.selectedTransfer)
      } finally { this.taskLoadingMore = false }
    },
    closeDetails() {
      this.selectedTransferId = ''
      this.showFailedTasksOnly = false
      this.taskNextPageToken = ''
      this.taskPageSize = TASK_PAGE_DEFAULT
    },
    async cancelTransferJob(transfer) {
      const id = this.jobId(transfer)
      if (!id) return
      if (this.isFinal(transfer)) {
        this.setNotice(`Cancel skipped: ${id} is already ${this.stateLabel(transfer)}`)
        return
      }
      if (!window.confirm(`Cancel transfer job ${id}?`)) return
      this.setTransferBusy(id, 'cancel')
      try {
        const next = await cancelTransfer(id)
        this.upsertTransfer(next)
        this.setNotice(`Cancel requested: ${id} is ${this.stateLabel(next)}`)
      } catch (error) {
        this.setNotice(`Cancel failed: ${error}`)
        window.alert(`Cancel failed: ${error}`)
      } finally { this.setTransferBusy(id, null) }
    },
    async retryTransferJob(transfer) {
      const id = this.jobId(transfer)
      if (!id) return
      if (!this.canRetry(transfer)) {
        this.setNotice(`Retry skipped: ${id} is ${this.stateLabel(transfer)} (only terminal jobs can retry)`)
        return
      }
      if (!window.confirm(`Retry transfer job ${id}? This submits a new run with a fresh request id.`)) return
      this.setTransferBusy(id, 'retry')
      try {
        const next = await retryTransfer(id)
        this.upsertTransfer(next)
        const nextId = this.jobId(next)
        this.selectedTransferId = nextId
        this.showFailedTasksOnly = false
        this.setNotice(`Retry submitted: ${nextId} (from ${id})`)
        if (nextId) {
          try {
            this.upsertTransfer(await fetchTransferStatus(nextId))
          } catch (_) {}
        }
      } catch (error) {
        this.setNotice(`Retry failed: ${error}`)
        window.alert(`Retry failed: ${error}`)
      } finally { this.setTransferBusy(id, null) }
    },
    async copyText(text, message = 'Copied') {
      if (!text || text === '-') {
        this.setNotice('Nothing to copy')
        return
      }
      try {
        await copyToClipboard(String(text))
        this.setNotice(message)
      } catch (error) {
        this.setNotice(`Copy failed: ${error}`)
      }
    },
    openSubmitForm() { this.resetForm(); this.showSubmitForm = true },
    closeSubmitForm() { this.showSubmitForm = false; this.resetForm() },
    resetForm() { this.form = emptyForm() },
    togglePolling() {
      this.stopPolling()
      if (!this.polling) return
      this.pollTimer = window.setTimeout(this.pollActiveTransfers, 3000)
    },
    stopPolling() {
      if (this.pollTimer) window.clearTimeout(this.pollTimer)
      this.pollTimer = null
    },
    async pollActiveTransfers() {
      this.stopPolling()
      if (!this.polling) return
      const activeTransfers = this.transfers.filter((transfer) => !this.isFinal(transfer))
      for (const transfer of activeTransfers) {
        const id = this.jobId(transfer)
        if (!id) continue
        // Summary only: the list needs counters, not every active job's task list.
        // Loaded task rows are preserved by upsertTransfer's merge.
        try { this.upsertTransfer(await fetchTransferStatus(id, { page_size: 0 })) }
        catch (_) {}
      }
      this.togglePolling()
    }
  }
}
</script>

<style scoped>
.transfers-grid {
  grid-template-columns: 1fr;
  align-items: start;
}

.transfer-metric-grid {
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
}

.transfers-grid,
.transfer-form,
.transfer-form label,
.transfer-form input,
.transfer-form select,
.toolbar,
.admin-notice {
  min-width: 0;
}

.transfer-filter-bar {
  margin-top: 4px;
  padding-top: 0;
}

.transfers-toolbar {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
}

.transfers-toolbar .admin-input {
  flex: 1 1 220px;
  min-width: 160px;
}

.transfers-toolbar .transfer-search-input {
  flex: 2 1 360px;
}

.transfers-toolbar .compact-select {
  flex: 0 1 150px;
  min-width: 130px;
  height: 34px;
}

.transfer-list {
  display: grid;
  gap: 10px;
  overflow-x: auto;
  padding-bottom: 2px;
}

.transfer-header {
  padding: 0 12px;
  min-height: 24px;
  border: 0;
  background: transparent;
  color: var(--admin-muted);
  font-size: 12px;
  font-weight: 760;
}

.transfer-row {
  display: grid;
  grid-template-columns: minmax(148px, 0.58fr) 84px minmax(260px, 1.45fr) 110px minmax(112px, .75fr) 84px 80px 96px;
  gap: 8px;
  align-items: center;
  min-width: 1180px;
  padding: 9px 12px;
  border: 1px solid var(--admin-border);
  border-radius: var(--admin-radius);
  background: var(--admin-surface-soft);
}

.transfer-row.selected {
  border-color: rgba(74, 122, 54, .35);
  box-shadow: 0 0 0 2px rgba(74, 122, 54, .08);
}

.transfer-main,
.transfer-id-cell,
.transfer-kind-cell,
.transfer-meta,
.transfer-progress {
  min-width: 0;
}

.transfer-main strong,
.transfer-id-cell strong,
.transfer-meta strong {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 13px;
}

.transfer-id-cell .transfer-list-title {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  line-height: 1.2;
  font-size: 11px;
}

.transfer-id-cell .transfer-display-name {
  color: #7d8b73;
  display: block;
  line-height: 1.25;
  font-size: 11px;
  word-break: break-all;
}

.transfer-id-cell small {
  color: #7d8b73;
}

.transfer-id-cell {
  cursor: copy;
  overflow: hidden;
}

.transfer-main small,
.transfer-id-cell small,
.transfer-kind-cell small,
.transfer-meta span,
.transfer-progress small,
.transfer-progress .usage-line span {
  display: block;
  color: var(--admin-muted);
  font-size: 11px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.transfer-main {
  min-width: 0;
  overflow: hidden;
}

.transfer-kind-cell {
  display: flex;
  align-items: center;
}

.transfer-kind-badge {
  display: inline-flex;
  width: fit-content;
  max-width: 100%;
  padding: 3px 7px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 800;
  line-height: 1.1;
  white-space: nowrap;
}

.transfer-kind-badge.export {
  color: #265d89;
  background: #eaf4ff;
}

.transfer-kind-badge.load {
  color: #48612b;
  background: #eff7e8;
}

.transfer-hint {
  color: #7a8f70 !important;
}

.transfer-progress .usage-line {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  align-items: center;
  margin-bottom: 4px;
}

.transfer-progress .usage-line strong {
  flex: 0 0 auto;
  font-size: 13px;
}

.transfer-progress .inline-meter {
  width: 100%;
}

.transfer-meta-files strong,
.transfer-meta-updated strong {
  font-size: 12px;
  white-space: nowrap;
  overflow: visible;
  text-overflow: clip;
}

.transfer-actions {
  flex-direction: column;
  align-items: stretch;
  gap: 2px;
}

.transfer-actions .link-button {
  width: 100%;
  height: 24px;
  padding: 0 6px;
  text-align: left;
  background: #fff;
  border-color: transparent;
  white-space: nowrap;
}

.failed-only-toggle {
  border-color: rgba(190, 80, 80, 0.18);
  background: #fff8f8;
}

.transfer-detail-panel {
  position: fixed;
  top: 84px;
  right: 24px;
  bottom: 24px;
  z-index: 60;
  display: flex;
  flex-direction: column;
  gap: 12px;
  width: min(760px, calc(100vw - 48px));
  max-height: calc(100vh - 108px);
  overflow-y: auto;
  overflow-x: hidden;
  box-shadow: 0 24px 70px rgba(31, 48, 26, .22);
}

.detail-panel-heading,
.detail-badges {
  display: flex;
  gap: 10px;
  align-items: flex-start;
  justify-content: space-between;
}

.detail-panel-heading h2 {
  margin: 0;
}

.detail-panel-heading p {
  margin: 3px 0 0;
  color: var(--admin-muted);
  font-size: 12px;
}

.detail-badges {
  justify-content: flex-start;
  flex-wrap: wrap;
  align-items: center;
}

.detail-section {
  display: flex;
  flex: 0 0 auto;
  flex-direction: column;
  gap: 10px;
  min-width: 0;
  padding-top: 12px;
  border-top: 1px solid rgba(91, 103, 122, 0.14);
}

.detail-section h3 {
  margin: 0;
  font-size: 13px;
  line-height: 1.3;
}

.detail-grid,
.counter-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
  align-items: stretch;
}

.detail-grid div,
.counter-grid div {
  display: flex;
  flex-direction: column;
  gap: 4px;
  min-width: 0;
  min-height: 58px;
  padding: 8px;
  border: 1px solid rgba(91, 103, 122, 0.12);
  border-radius: 8px;
  background: rgba(255, 255, 255, .72);
}

.detail-grid span,
.counter-grid span,
.path-detail span {
  display: block;
  margin: 0;
  color: var(--admin-muted);
  font-size: 11px;
  line-height: 1.25;
}

.detail-grid strong,
.counter-grid strong,
.path-detail strong {
  display: block;
  min-width: 0;
  overflow-wrap: anywhere;
  font-size: 12px;
  line-height: 1.35;
}

.path-detail {
  display: grid;
  gap: 8px;
}

.path-detail div {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 8px;
  align-items: center;
  min-height: 64px;
  padding: 8px;
  border: 1px solid rgba(91, 103, 122, 0.12);
  border-radius: 8px;
  background: rgba(255, 255, 255, .72);
}

.path-detail span {
  grid-column: 1 / -1;
}

.detail-error {
  margin: 0;
  padding: 8px 10px;
  border: 1px solid rgba(190, 80, 80, 0.18);
  border-radius: 8px;
  color: #8a2f2f;
  background: #fff8f8;
  overflow-wrap: anywhere;
  font-size: 12px;
}

.detail-hint {
  margin: 0;
  padding: 8px 10px;
  border: 1px solid rgba(91, 103, 122, 0.14);
  border-radius: 8px;
  color: #5b677a;
  background: rgba(245, 247, 250, 0.85);
  overflow-wrap: anywhere;
  font-size: 12px;
}

.bad-count {
  color: #a03939;
}

.task-table-scroll {
  overflow-x: auto;
  border: 1px solid rgba(91, 103, 122, 0.14);
  border-radius: 8px;
  background: rgba(255, 255, 255, .76);
}

.task-list-footer {
  margin-top: 8px;
}

.task-list-hint {
  color: #6b7a90;
  font-size: 11px;
}

.transfer-task-row {
  display: grid;
  grid-template-columns: 150px 96px 170px 260px 260px 72px 280px 120px;
  min-width: 1504px;
  gap: 8px;
  align-items: center;
  padding: 7px 8px;
  color: #44546a;
  font-size: 11px;
}

.transfer-task-row + .transfer-task-row {
  border-top: 1px solid rgba(91, 103, 122, 0.1);
}

.transfer-task-row.failed {
  background: #fff8f8;
}

.transfer-task-header {
  color: var(--admin-muted);
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: .02em;
  background: #f8fbf5;
}

.transfer-task-row > span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.transfer-id-subtitle {
  margin-top: 4px;
  font-size: 12px;
  color: var(--admin-text-muted, #6b7280);
  word-break: break-all;
}

.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
}

.admin-notice {
  border: 1px solid #d9e4d2;
  border-radius: 8px;
  color: #315322;
  background: #f7fbf3;
  font-size: 13px;
  margin: 10px 0;
  padding: 10px 12px;
}

.link-button:disabled,
.admin-button:disabled {
  color: #9cae92;
  cursor: not-allowed;
}

.transfer-form label {
  display: grid;
  grid-template-columns: 1fr;
  gap: 6px;
}

.transfer-form input:not([type="checkbox"]),
.transfer-form select {
  width: 100%;
  max-width: 100%;
  border: 1px solid #d9e4d2;
  border-radius: 8px;
  padding: 9px 10px;
  font: inherit;
  background: #fff;
  color: var(--admin-text);
}

.transfer-form .form-row {
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}

.admin-modal-backdrop {
  position: fixed;
  inset: 0;
  z-index: 1000;
  background: rgba(18, 32, 20, .28);
}

.admin-modal-panel {
  position: fixed;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  width: min(720px, calc(100vw - 48px));
  max-height: calc(100vh - 48px);
  overflow: auto;
  box-shadow: 0 24px 70px rgba(31, 48, 26, .24);
  z-index: 1001;
}

.form-hint {
  margin: 0 0 12px;
  color: #5b677a;
  font-size: 12px;
  line-height: 1.45;
}

@media (max-width: 820px) {
  .transfer-row {
    min-width: 0;
    grid-template-columns: minmax(0, 1fr) 100px;
    align-items: start;
  }

  .transfer-header {
    display: none;
  }

  .transfer-main { grid-column: 1 / -1; }
  .transfer-state { justify-self: end; }
  .transfer-progress {
    grid-column: 1 / -1;
  }
  .transfer-actions {
    grid-column: 1 / -1;
    flex-direction: row;
    flex-wrap: wrap;
  }

  .transfer-actions .link-button {
    width: auto;
    flex: 1 1 auto;
  }

  .transfer-form .form-row { grid-template-columns: 1fr; }
}

@media (max-width: 760px) {
  .transfer-row {
    grid-template-columns: 1fr;
  }

  .transfer-state {
    justify-self: start;
  }

  .transfer-task-header {
    display: none;
  }

  .transfer-task-row {
    grid-template-columns: 1fr;
    gap: 4px;
    padding: 6px 0;
    border-top: 1px solid rgba(91, 103, 122, 0.12);
  }
}
</style>
