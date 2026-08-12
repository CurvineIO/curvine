<template>
  <div class="admin-page" v-loading="loading">
    <section class="metric-grid job-metric-grid">
      <MetricCard label="Recent Jobs" :value="jobs.length" meta="manual load and fs_mode export" delta="recent" tone="neutral" />
      <MetricCard label="Auto Export Jobs" :value="autoExportCount" meta="fs_mode generated jobs" delta="fs_mode" tone="neutral" />
      <MetricCard label="Failed Jobs 24h" :value="failedJobs24h" meta="needs investigation" :tone="failedJobs24h ? 'bad' : 'good'" :delta="failedJobs24h ? 'review' : 'none'" />
      <MetricCard label="Failed Tasks" :value="failedTaskCountAll" meta="failed file/task count" :tone="failedTaskCountAll ? 'bad' : 'good'" delta="tasks" />
      <MetricCard label="Running Tasks" :value="runningTaskCountAll" meta="pending or loading tasks" delta="active" tone="good" />
    </section>

    <section class="content-grid jobs-grid">
      <div class="admin-panel table-panel">
        <div class="panel-heading table-heading">
          <div>
            <h2>Load / Export Jobs (Master)</h2>
            <p>Inspect master load and fs_mode auto-export jobs</p>
          </div>
          <div class="row-actions">
            <button class="admin-button ghost compact" @click="refreshJobs">Refresh</button>
            <button class="admin-button primary compact" @click="openLoadForm">Submit Load</button>
          </div>
        </div>

        <div class="panel-heading table-heading job-filter-bar">
          <div class="segmented" role="tablist" aria-label="Job status filter">
            <button :class="{ active: statusFilter === 'all' }" @click="statusFilter = 'all'">All {{ jobs.length }}</button>
            <button :class="{ active: statusFilter === 'running' }" @click="statusFilter = 'running'">Active {{ activeCount }}</button>
            <button :class="{ active: statusFilter === 'completed' }" @click="statusFilter = 'completed'">Completed {{ completedCount }}</button>
            <button :class="{ active: statusFilter === 'failed' }" @click="statusFilter = 'failed'">Failed {{ failedCount }}</button>
            <button :class="{ active: statusFilter === 'canceled' }" @click="statusFilter = 'canceled'">Canceled {{ canceledCount }}</button>
          </div>
        </div>

        <div class="toolbar jobs-toolbar">
          <input
            v-model="query"
            class="admin-input job-search-input"
            placeholder="Search by name, path, mount... or paste full job_id"
            @keydown.enter.prevent="onJobSearchEnter"
          >
          <select v-model="typeFilter" class="admin-input compact-select">
            <option value="all">All types</option>
            <option value="manual">Manual Load</option>
            <option value="auto">fs_mode Auto Export</option>
          </select>
          <select v-model="mountFilter" class="admin-input compact-select">
            <option value="">All mounts</option>
            <option v-for="mount in mountOptions" :key="mount" :value="mount">{{ mount }}</option>
          </select>
          <select v-model="workerFilter" class="admin-input compact-select">
            <option value="">All workers</option>
            <option v-for="worker in workerOptions" :key="worker" :value="worker">{{ worker }}</option>
          </select>
          <select v-model="taskFilter" class="admin-input compact-select">
            <option value="all">All tasks</option>
            <option value="failed">Has failed task</option>
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

        <div class="job-list">
          <div class="job-row job-header">
            <span>Job</span>
            <span>Type</span>
            <span>Path</span>
            <span>Status</span>
            <span>Progress</span>
            <span>Files</span>
            <span>Updated</span>
            <span>Actions</span>
          </div>
          <article v-for="job in filteredJobs" :key="jobId(job)" class="job-row" :class="{ selected: selectedJobId === jobId(job) }">
            <div class="job-id-cell" :title="jobListTooltip(job)" @click.stop="copyText(jobId(job), 'Job id copied')">
              <strong class="mono job-list-title">{{ jobListTitle(job) }}</strong>
              <small class="job-display-name">{{ jobListSubtitle(job) }}</small>
            </div>
            <div class="job-type-cell">
              <span :class="['job-type-badge', jobTypeClass(job)]">{{ jobTypeLabel(job) }}</span>
            </div>
            <div class="job-main" :title="`${sourcePath(job)}\n${targetPath(job)}`">
              <strong :title="sourcePath(job)">{{ sourcePath(job) }}</strong>
              <small :title="targetPath(job)">→ {{ targetPath(job) }}</small>
              <small v-if="job.message" class="job-hint" :title="job.message">{{ job.message }}</small>
            </div>
            <div class="job-state">
              <span :class="['status-pill', jobClass(job)]">{{ stateLabel(job) }}</span>
            </div>
            <div class="job-progress">
              <div class="usage-line"><strong>{{ progress(job) }}%</strong></div>
              <div class="inline-meter"><span :style="{ width: progress(job) + '%' }"></span></div>
            </div>
            <div class="job-meta job-meta-files"><span>Files</span><strong :title="formatFiles(job)">{{ formatFiles(job) }}</strong></div>
            <div class="job-meta job-meta-updated"><span>Updated</span><strong :title="formatTime(job.update_time_ms)">{{ relativeTime(job.update_time_ms) }}</strong></div>
            <div class="row-actions job-actions">
              <button class="link-button" @click="openDetails(job)">Details</button>
              <button v-if="!isFinal(job)" class="link-button danger-link" :disabled="isJobBusy(job)" @click="cancelJob(job)">{{ busyLabel(job, 'cancel', 'Cancel') }}</button>
            </div>
          </article>
        </div>
        <div v-if="filteredJobs.length === 0" class="empty-state">{{ emptyJobsMessage }}</div>
      </div>

      <aside v-if="selectedJob" class="admin-panel job-detail-panel" v-loading="detailLoading">
        <div class="detail-panel-heading">
          <div>
            <h2>Job Diagnostics</h2>
            <p class="mono">{{ jobListTitle(selectedJob) }}</p>
            <p class="mono job-id-subtitle">{{ jobId(selectedJob) }}</p>
          </div>
          <button class="admin-button ghost compact" @click="closeDetails">Close</button>
        </div>

        <div class="detail-badges">
          <span :class="['status-pill', jobClass(selectedJob)]">{{ stateLabel(selectedJob) }}</span>
          <span :class="['job-type-badge', jobTypeClass(selectedJob)]">{{ jobTypeLabel(selectedJob) }}</span>
          <button class="link-button" @click="copyText(jobId(selectedJob), 'Job id copied')">Copy Job ID</button>
          <button class="link-button" @click="copyCli(selectedJob)">Copy CLI</button>
        </div>

        <section class="detail-section">
          <h3>Job Overview</h3>
          <div class="detail-grid">
            <div><span>Source Type</span><strong>{{ jobTypeLabel(selectedJob) }}</strong></div>
            <div><span>Trigger Event</span><strong>{{ jobTriggerLabel(selectedJob) }}</strong></div>
            <div><span>Created By</span><strong>{{ jobCreatedBy(selectedJob) }}</strong></div>
            <div><span>Mount Path</span><strong>{{ mountPath(selectedJob) }}</strong></div>
            <div><span>UFS URI</span><strong>{{ ufsUri(selectedJob) }}</strong></div>
            <div><span>Updated At</span><strong>{{ formatTime(selectedJob.update_time_ms) }}</strong></div>
          </div>
          <div class="path-detail">
            <div><span>Source Path</span><strong :title="sourcePath(selectedJob)">{{ sourcePath(selectedJob) }}</strong><button class="link-button" @click="copyText(sourcePath(selectedJob), 'Source path copied')">Copy</button></div>
            <div><span>Target Path</span><strong :title="targetPath(selectedJob)">{{ targetPath(selectedJob) }}</strong><button class="link-button" @click="copyText(targetPath(selectedJob), 'Target path copied')">Copy</button></div>
          </div>
          <p v-if="selectedJob.message" class="detail-error">{{ selectedJob.message }}</p>
        </section>

        <section class="detail-section">
          <h3>Task Counters</h3>
          <div class="counter-grid">
            <div><span>Total</span><strong>{{ displayTaskTotal(selectedJob) }}</strong></div>
            <div><span>Completed</span><strong>{{ Number(selectedJob.completed_files || 0) }}</strong></div>
            <div><span>Failed</span><strong class="bad-count">{{ failedTaskCount(selectedJob) }}</strong></div>
            <div><span>Running</span><strong>{{ runningTaskCount(selectedJob) }}</strong></div>
            <div><span>Loaded</span><strong>{{ sizeLabel(selectedJob) }}</strong></div>
          </div>
          <button v-if="failedTaskCount(selectedJob)" class="admin-button ghost compact" @click="showFailedTasksOnly = !showFailedTasksOnly">
            {{ showFailedTasksOnly ? 'Show all tasks' : 'View failed tasks' }}
          </button>
        </section>

        <section class="detail-section">
          <h3>{{ showFailedTasksOnly ? 'Failed Tasks' : 'All Tasks' }}</h3>
          <div class="task-table-scroll">
            <div class="diagnostic-task-row diagnostic-task-header">
              <span>Task ID</span>
              <span>State</span>
              <span>Worker</span>
              <span>Source Path</span>
              <span>Target Path</span>
              <span>Error Code</span>
              <span>Error Message</span>
              <span>Retry</span>
              <span>Updated At</span>
            </div>
            <div v-for="task in selectedTaskList" :key="task.task_id || task.id" class="diagnostic-task-row" :class="{ failed: isFailedTask(task) }">
              <span class="mono" :title="task.task_id || task.id">{{ shortTaskId(task.task_id || task.id) }}</span>
              <span :class="['status-pill', taskClass(task)]">{{ stateLabel(task) }}</span>
              <span :title="taskWorker(task)">{{ taskWorker(task) }}</span>
              <span :title="task.source_path || '-'">{{ task.source_path || '-' }}</span>
              <span :title="task.target_path || '-'">{{ task.target_path || '-' }}</span>
              <span>{{ taskErrorCode(task, selectedJob) }}</span>
              <span :title="taskMessage(task, selectedJob)">{{ taskMessage(task, selectedJob) }}</span>
              <span>{{ retryCount(task) }}</span>
              <span :title="formatTime(task.update_time_ms)">{{ relativeTime(task.update_time_ms) }}</span>
            </div>
          </div>
          <div v-if="selectedTaskList.length === 0" class="empty-state">{{ selectedTaskEmptyMessage }}</div>
        </section>

        <section class="detail-section">
          <h3>CLI Commands</h3>
          <div class="path-detail">
            <div v-for="item in cliCommands(selectedJob)" :key="item.key">
              <span>{{ item.label }}</span>
              <div class="cli-row-body">
                <code class="cli-snippet">{{ item.command }}</code>
                <button type="button" class="copy-path-button" @click="copyCliCommand(item.key, item.command)">
                  {{ copiedCliKey === item.key ? 'Copied' : 'Copy' }}
                </button>
              </div>
            </div>
          </div>
        </section>
      </aside>

    </section>

    <div v-if="showLoadForm" class="admin-modal-backdrop">
      <div class="admin-panel form-panel load-form admin-modal-panel" :style="modalStyle">
        <div class="panel-heading drag-handle" @mousedown.prevent="startDrag">
          <div>
            <h2>Submit Load</h2>
            <p>Load data from a mounted UFS path into Curvine</p>
          </div>
          <button class="admin-button ghost compact" @mousedown.stop @click="closeLoadForm">Close</button>
        </div>

        <div class="form-row">
          <label>Source Mode
            <select v-model="form.source_mode">
              <option value="manual">Manual path</option>
              <option value="mount">From mount</option>
            </select>
          </label>
          <label v-if="form.source_mode === 'mount'">Mount Prefix
            <select v-model="form.mount_cv_path">
              <option value="">Select mount</option>
              <option v-for="mount in mounts" :key="mount.cv_path" :value="mount.cv_path">{{ mount.cv_path }} -> {{ mount.ufs_path }}</option>
            </select>
          </label>
        </div>
        <label v-if="form.source_mode === 'manual'">Source Path<input v-model="form.path" placeholder="file:///data/file or mounted path"></label>
        <label v-else>Relative Path<input v-model="form.relative_path" placeholder="sample.txt or dir/file.txt"></label>
        <div v-if="form.source_mode === 'mount'" class="load-preview">
          <div><span>Source</span><strong :title="generatedSourcePath">{{ generatedSourcePath || '-' }}</strong></div>
          <div><span>Auto Target</span><strong :title="generatedTargetPath">{{ generatedTargetPath || '-' }}</strong></div>
        </div>
        <label>Target Path (optional)<input v-model="form.target_path" :placeholder="generatedTargetPath || 'leave blank to use mount mapping'"></label>
        <p class="form-hint">Leave blank to derive target from mount mapping. Specify an explicit Curvine path to override.</p>
        <div class="form-row">
          <label>TTL<input v-model="form.ttl" placeholder="7d"></label>
          <label>TTL Action
            <select v-model="form.ttl_action">
              <option value="">default</option>
              <option value="delete">delete</option>
              <option value="free">free</option>
              <option value="none">none</option>
            </select>
          </label>
        </div>
        <div class="form-row">
          <label>Replicas<input v-model.number="form.replicas" type="number" min="1" placeholder="default"></label>
          <label>Block Size<input v-model="form.block_size" placeholder="128MB"></label>
        </div>
        <div class="form-row">
          <label>Storage
            <select v-model="form.storage_type">
              <option value="">default</option>
              <option value="disk">disk</option>
              <option value="mem">mem</option>
              <option value="ssd">ssd</option>
              <option value="hdd">hdd</option>
              <option value="ufs">ufs</option>
            </select>
          </label>
          <label>Overwrite
            <select v-model="form.overwrite">
              <option :value="true">true</option>
              <option :value="false">false</option>
            </select>
          </label>
        </div>
        <label class="check-line"><input v-model="form.recursive" type="checkbox"> Recursive</label>
        <label>Configs<textarea v-model="configsText" rows="5" placeholder="key=value"></textarea></label>

        <div class="form-actions">
          <button class="admin-button ghost" @click="resetForm">Reset</button>
          <button class="admin-button primary" @click="submitLoad">Submit Load</button>
          <button class="admin-button ghost" @click="closeLoadForm">Cancel</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import MetricCard from '@/components/admin/MetricCard.vue'
import { cancelLoadJob, fetchJobStatus, fetchJobsData, fetchMountsData, submitLoadJob, validateMount } from '@/api/client'
import { formatBytes, formatTime } from '@/utils/format'
import { copyToClipboard } from '@/utils/clipboard'
import { buildJobDisplayName, isCompleteJobId, jobListSubtitle as formatJobListSubtitle, jobListTitle as formatJobListTitle, jobListTooltip as formatJobListTooltip, normalizeJobId, shortJobId } from '@/utils/jobDisplay'
import eventBus from '@/utils/eventBus'

const emptyForm = () => ({
  source_mode: 'manual',
  mount_cv_path: '',
  relative_path: '',
  path: '',
  target_path: '',
  ttl: '7d',
  ttl_action: '',
  recursive: false,
  replicas: null,
  block_size: '',
  storage_type: '',
  overwrite: true
})

export default {
  name: 'JobsPage',
  components: { MetricCard },
  data() {
    return {
      loading: false,
      detailLoading: false,
      jobs: [],
      mounts: [],
      form: emptyForm(),
      configsText: '',
      statusFilter: 'all',
      query: '',
      typeFilter: 'all',
      mountFilter: '',
      workerFilter: '',
      taskFilter: 'all',
      timeRange: 'all',
      failedOnly: false,
      selectedJobId: '',
      showFailedTasksOnly: false,
      showLoadForm: false,
      polling: true,
      pollTimer: null,
      busyJobs: {},
      notice: '',
      copiedCliKey: '',
      dragOffset: { x: 0, y: 0 },
      isDragging: false,
      dragStart: null,
      lookupTimer: null,
      lastLookupQuery: '',
      lookupInFlight: ''
    }
  },
  watch: {
    query(value) {
      this.scheduleJobIdLookup(value)
    }
  },
  computed: {
    activeCount() { return this.jobs.filter((job) => this.jobBucket(job) === 'running').length },
    completedCount() { return this.jobs.filter((job) => this.stateValue(job) === 'completed').length },
    failedCount() { return this.jobs.filter((job) => this.stateValue(job) === 'failed').length },
    canceledCount() { return this.jobs.filter((job) => this.stateValue(job) === 'canceled').length },
    autoExportCount() { return this.jobs.filter((job) => this.isAutoExportJob(job)).length },
    failedJobs24h() { return this.jobs.filter((job) => this.stateValue(job) === 'failed' && this.isWithinRange(job.update_time_ms, '24h')).length },
    failedTaskCountAll() { return this.jobs.reduce((sum, job) => sum + this.failedTaskCount(job), 0) },
    runningTaskCountAll() { return this.jobs.reduce((sum, job) => sum + this.runningTaskCount(job), 0) },
    mountOptions() {
      const values = new Set(this.mounts.map((mount) => mount.cv_path).filter(Boolean))
      for (const job of this.jobs) {
        const mount = this.mountPath(job)
        if (mount && mount !== '-') values.add(mount)
      }
      return Array.from(values).sort()
    },
    workerOptions() {
      const values = new Set()
      for (const job of this.jobs) {
        const tasks = this.taskDetails(job)
        if (!tasks.length) continue
        for (const task of tasks) {
          const worker = this.taskWorker(task)
          if (worker && worker !== '-') values.add(worker)
        }
      }
      return Array.from(values).sort()
    },
    selectedJob() {
      if (!this.selectedJobId) return null
      return this.jobs.find((job) => this.jobId(job) === this.selectedJobId) || null
    },
    selectedTaskList() {
      if (!this.selectedJob) return []
      const tasks = this.taskDetails(this.selectedJob)
      const allTasks = tasks.length ? tasks : this.jobLevelFailureTasks(this.selectedJob)
      return this.showFailedTasksOnly ? allTasks.filter((task) => this.isFailedTask(task)) : allTasks
    },
    filteredJobs() {
      const q = this.query.trim()
      const qLower = q.toLowerCase()
      const exactJobId = isCompleteJobId(q)
      return this.jobs.filter((job) => {
        if (exactJobId && normalizeJobId(this.jobId(job)) === qLower) return true
        const bucket = this.jobBucket(job)
        if (this.statusFilter !== 'all' && bucket !== this.statusFilter) return false
        if (this.failedOnly && !this.hasFailedWork(job)) return false
        if (this.typeFilter === 'manual' && this.isAutoExportJob(job)) return false
        if (this.typeFilter === 'auto' && !this.isAutoExportJob(job)) return false
        if (this.mountFilter && this.mountPath(job) !== this.mountFilter) return false
        const tasks = this.taskDetails(job)
        if (this.workerFilter && tasks.length && !tasks.some((task) => this.taskWorker(task) === this.workerFilter)) return false
        if (this.taskFilter === 'failed' && !this.hasFailedWork(job)) return false
        if (!this.isWithinRange(job.update_time_ms || job.updated_at, this.timeRange)) return false
        if (!qLower) return true
        const taskText = tasks.length
          ? tasks.map((task) => `${task.task_id || ''} ${task.source_path || ''} ${task.target_path || ''} ${this.taskWorker(task)} ${this.taskMessage(task, job)}`).join(' ')
          : ''
        const haystack = `${this.jobListTitle(job)} ${this.jobListSubtitle(job)} ${this.jobDisplayName(job)} ${this.jobId(job)} ${job.submitter || ''} ${this.sourcePath(job)} ${this.targetPath(job)} ${this.mountPath(job)} ${this.ufsUri(job)} ${this.jobTypeLabel(job)} ${job.message || ''} ${taskText}`.toLowerCase()
        return haystack.includes(qLower)
      })
    },
    selectedMount() { return this.mounts.find((mount) => mount.cv_path === this.form.mount_cv_path) || null },
    selectedTaskEmptyMessage() {
      if (!this.selectedJob) return 'No task details available for this view.'
      if (this.isJobLevelFailure(this.selectedJob)) {
        return 'This job failed before tasks were created. See the error message in Job Overview.'
      }
      if (this.showFailedTasksOnly) return 'No failed tasks in this job.'
      return 'No task details available for this view.'
    },
    emptyJobsMessage() {
      if (!this.jobs.length) return 'No load jobs tracked yet. Submit a path or look up a job id.'
      const hints = []
      if (this.timeRange !== 'all') hints.push(`time range: ${this.timeRange}`)
      if (this.statusFilter !== 'all') hints.push(`status: ${this.statusFilter}`)
      if (this.typeFilter !== 'all') hints.push(`type: ${this.typeFilter}`)
      if (this.failedOnly) hints.push('failed only')
      const suffix = hints.length ? ` Active filters (${hints.join(', ')}).` : ''
      return `No jobs matched the current filters.${suffix}`
    },
    generatedSourcePath() {
      if (this.form.source_mode === 'mount' && this.selectedMount) {
        return this.joinPath(this.selectedMount.ufs_path, this.form.relative_path)
      }
      return String(this.form.path || '').trim()
    },
    generatedTargetPath() {
      if (this.form.source_mode !== 'mount' || !this.selectedMount) return ''
      return this.joinPath(this.selectedMount.cv_path, this.form.relative_path)
    },
    modalStyle() {
      return this.dragOffset.x || this.dragOffset.y
        ? { transform: `translate(calc(-50% + ${this.dragOffset.x}px), calc(-50% + ${this.dragOffset.y}px))` }
        : {}
    }
  },
  created() { this.refreshJobs(); this.fetchMounts() },
  mounted() {
    eventBus.on('admin-refresh', this.refreshJobs)
    this.togglePolling()
  },
  beforeUnmount() {
    eventBus.off('admin-refresh', this.refreshJobs)
    this.stopPolling()
    this.endDrag()
    this.clearJobIdLookupTimer()
  },
  methods: {
    formatBytes,
    formatTime,
    async fetchMounts() {
      try {
        const data = await fetchMountsData()
        this.mounts = Array.isArray(data) ? data : (data.items || [])
      } catch (_) {
        this.mounts = []
      }
    },
    trimSlashes(value) { return String(value || '').replace(/^\/+|\/+$/g, '') },
    joinPath(prefix, suffix) {
      const base = String(prefix || '').replace(/\/+$/g, '')
      const rest = this.trimSlashes(suffix)
      return rest ? `${base}/${rest}` : base
    },
    parseConfigs() {
      const configs = {}
      for (const line of this.configsText.split('\n')) {
        const value = line.trim()
        if (!value) continue
        const index = value.indexOf('=')
        if (index <= 0) throw new Error(`Invalid config: ${value}`)
        configs[value.slice(0, index).trim()] = value.slice(index + 1).trim()
      }
      return configs
    },
    payload() {
      if (this.form.source_mode === 'mount' && !this.selectedMount) {
        throw new Error('Mount Prefix is required')
      }
      const path = this.generatedSourcePath
      if (!path) throw new Error('Source Path is required')
      const target_path = String(this.form.target_path || '').trim() || null
      return {
        path,
        target_path,
        ttl: String(this.form.ttl || '').trim() || null,
        ttl_action: this.form.ttl_action || null,
        recursive: this.form.recursive,
        replicas: this.form.replicas || null,
        block_size: this.form.block_size || null,
        storage_type: this.form.storage_type || null,
        overwrite: this.form.overwrite,
        configs: this.parseConfigs()
      }
    },
    async validateMountedSource(path) {
      if (this.form.source_mode !== 'mount') return
      try {
        await validateMount({
          ufs_path: path,
          provider: this.selectedMount.provider || null,
          properties: this.selectedMount.properties || {}
        })
      } catch (error) {
        throw new Error(`Mounted UFS source is unavailable now: ${path}. The mount record can still exist when its backing path is missing in the running containers. ${error}`)
      }
    },
    jobId(job) { return job && (job.job_id || job.id) },
    jobDisplayName(job) { return buildJobDisplayName(job) },
    jobListTitle(job) { return formatJobListTitle(job) },
    jobListSubtitle(job) { return formatJobListSubtitle(job, this.jobId(job)) },
    jobListTooltip(job) { return formatJobListTooltip(job, this.jobId(job)) },
    shortJobId(id) { return shortJobId(id) },
    sourcePath(job) { return job.path || job.source_path || '-' },
    targetPath(job) { return job.target_path || '-' },
    isAutoExportJob(job) {
      if (!job) return false
      const sourceType = String(job.source_type || job.type || '').toLowerCase()
      if (sourceType.includes('auto') || sourceType.includes('fs_mode')) return true
      if (sourceType.includes('manual')) return false
      return false
    },
    jobTypeLabel(job) {
      return this.isAutoExportJob(job) ? 'fs_mode Auto Export' : 'Manual Load'
    },
    jobTypeClass(job) {
      return this.isAutoExportJob(job) ? 'auto' : 'manual'
    },
    jobTriggerLabel(job) {
      if (!this.isAutoExportJob(job)) return 'Manual'
      return job.trigger || job.trigger_event || 'CompleteFile / Rename'
    },
    jobCreatedBy(job) {
      const submitter = String(job.submitter || '').trim()
      if (submitter) return submitter
      return job.created_by || (this.isAutoExportJob(job) ? 'UfsLoader' : 'User')
    },
    mountPath(job) {
      if (!job) return '-'
      if (job.mount_path) return job.mount_path
      const paths = [this.sourcePath(job), this.targetPath(job)]
      const mount = this.mounts.find((item) => paths.some((path) => this.pathMatchesMount(path, item)))
      return mount ? mount.cv_path : '-'
    },
    ufsUri(job) {
      if (!job) return '-'
      if (job.ufs_path) return job.ufs_path
      const paths = [this.sourcePath(job), this.targetPath(job)]
      const direct = paths.find((path) => this.isUfsPath(path))
      if (direct) return direct
      const mount = this.mounts.find((item) => paths.some((path) => this.pathMatchesMount(path, item)))
      return mount && mount.ufs_path ? mount.ufs_path : '-'
    },
    isUfsPath(path) {
      const value = String(path || '')
      return value.includes('://') && !value.startsWith('cv://')
    },
    pathStartsWith(path, prefix) {
      const value = String(path || '').replace(/\/+$/g, '')
      const base = String(prefix || '').replace(/\/+$/g, '')
      if (!value || !base) return false
      return value === base || value.startsWith(`${base}/`)
    },
    pathMatchesMount(path, mount) {
      return this.pathStartsWith(path, mount && mount.cv_path) || this.pathStartsWith(path, mount && mount.ufs_path)
    },
    jobBucket(job) {
      const value = this.stateValue(job)
      if (['pending', 'loading', 'running'].includes(value)) return 'running'
      if (value === 'completed') return 'completed'
      if (value === 'failed') return 'failed'
      if (value === 'canceled') return 'canceled'
      if (!value || value === 'unknown') return 'running'
      return 'failed'
    },
    sizeLabel(job) {
      const loaded = Number(job.loaded_size || 0)
      const total = Number(job.total_size || 0)
      if (!loaded && !total) return '-'
      return `${this.formatBytes(loaded)} / ${this.formatBytes(total)}`
    },
    upsertJob(job) {
      const id = this.jobId(job)
      if (!id) return
      const index = this.jobs.findIndex((item) => this.jobId(item) === id)
      if (index >= 0) {
        const previous = this.jobs[index]
        const next = { ...previous, ...job }
        if (!Array.isArray(next.tasks) || next.tasks.length === 0) next.tasks = previous.tasks || []
        this.jobs.splice(index, 1, next)
      }
      else this.jobs.unshift(job)
    },
    setNotice(message) { this.notice = message },
    setJobBusy(id, action) {
      if (!id) return
      if (action) this.busyJobs = { ...this.busyJobs, [id]: action }
      else {
        const next = { ...this.busyJobs }
        delete next[id]
        this.busyJobs = next
      }
    },
    isJobBusy(job) { return Boolean(this.busyJobs[this.jobId(job)]) },
    busyLabel(job, action, fallback) { return this.busyJobs[this.jobId(job)] === action ? 'Working...' : fallback },
    mergeJobList(nextJobs) {
      const previousById = new Map(this.jobs.map((job) => [this.jobId(job), job]))
      const byId = new Map()
      for (const job of nextJobs) {
        const id = this.jobId(job)
        if (!id) continue
        const previous = previousById.get(id)
        if (previous) {
          const next = { ...previous, ...job }
          if (!Array.isArray(next.tasks) || next.tasks.length === 0) next.tasks = previous.tasks || []
          byId.set(id, next)
        } else {
          byId.set(id, job)
        }
      }
      return Array.from(byId.values()).sort((left, right) => {
        const leftTime = Number(left.update_time_ms || left.updated_at || 0)
        const rightTime = Number(right.update_time_ms || right.updated_at || 0)
        return rightTime - leftTime
      })
    },
    async refreshJobs() {
      this.loading = true
      try {
        const response = await fetchJobsData({ page_size: 500 })
        const legacyJobs = Array.isArray(response) ? response : (response.items || [])
        this.jobs = this.mergeJobList(legacyJobs)
      } catch (error) {
        window.alert(`Load jobs refresh failed: ${error}`)
      } finally { this.loading = false }
    },
    async submitLoad() {
      this.loading = true
      try {
        const payload = this.payload()
        await this.validateMountedSource(payload.path)
        const job = await submitLoadJob(payload)
        this.upsertJob(job)
        window.alert(`Load job submitted: ${this.jobDisplayName(job)}`)
        this.closeLoadForm()
        this.togglePolling()
      } catch (error) {
        window.alert(`Submit load failed: ${error}`)
      } finally { this.loading = false }
    },
    clearJobIdLookupTimer() {
      if (!this.lookupTimer) return
      clearTimeout(this.lookupTimer)
      this.lookupTimer = null
    },
    scheduleJobIdLookup(value) {
      this.clearJobIdLookupTimer()
      const trimmed = String(value || '').trim()
      if (!isCompleteJobId(trimmed)) {
        this.lastLookupQuery = ''
        return
      }
      const normalized = normalizeJobId(trimmed)
      if (normalized === this.lastLookupQuery) return
      this.lookupTimer = setTimeout(() => {
        this.lookupTimer = null
        this.performJobIdLookup(trimmed)
      }, 300)
    },
    onJobSearchEnter() {
      const trimmed = this.query.trim()
      if (!isCompleteJobId(trimmed)) return
      this.clearJobIdLookupTimer()
      this.performJobIdLookup(trimmed)
    },
    async performJobIdLookup(jobId) {
      const normalized = normalizeJobId(jobId)
      if (!normalized) return
      const existing = this.jobs.find((job) => normalizeJobId(this.jobId(job)) === normalized)
      if (existing) {
        this.lastLookupQuery = normalized
        this.setNotice(`Job found: ${normalized}`)
        return
      }
      await this.lookupJob(jobId)
    },
    async lookupJob(id) {
      const jobId = String(id || '').trim()
      if (!jobId || !isCompleteJobId(jobId)) return null
      const normalized = normalizeJobId(jobId)
      if (this.lookupInFlight === normalized) return null
      this.lookupInFlight = normalized
      this.loading = true
      try {
        const job = await fetchJobStatus(jobId)
        this.upsertJob(job)
        this.lastLookupQuery = normalized
        this.setNotice(`Job loaded: ${normalized}`)
        return job
      } catch (error) {
        this.lastLookupQuery = ''
        window.alert(`Lookup failed: ${error}`)
        return null
      } finally {
        this.lookupInFlight = ''
        this.loading = false
      }
    },
    async refreshJob(job) {
      const id = this.jobId(job)
      if (!id) return
      this.setJobBusy(id, 'status')
      try {
        const next = await fetchJobStatus(id)
        this.upsertJob(next)
        this.setNotice(`Status refreshed: ${id} is ${this.stateLabel(next)} (${this.progress(next)}%)`)
      } catch (error) {
        this.setNotice(`Status failed: ${error}`)
        window.alert(`Status failed: ${error}`)
      } finally { this.setJobBusy(id, null) }
    },
    async cancelJob(job) {
      const id = this.jobId(job)
      if (!id) return
      if (this.isFinal(job)) {
        this.setNotice(`Cancel skipped: ${id} is already ${this.stateLabel(job)}`)
        return
      }
      if (!window.confirm(`Cancel load job ${id}?`)) return
      this.setJobBusy(id, 'cancel')
      try {
        const next = await cancelLoadJob(id)
        this.upsertJob(next)
        this.setNotice(`Cancel requested: ${id} is ${this.stateLabel(next)}`)
      } catch (error) {
        this.setNotice(`Cancel failed: ${error}`)
        window.alert(`Cancel failed: ${error}`)
      } finally { this.setJobBusy(id, null) }
    },
    stateValue(job) {
      const raw = job && (job.state || job.status)
      if (!raw) return ''
      return String(raw).toLowerCase()
    },
    stateLabel(job) { return job.state || job.status || 'Unknown' },
    isFinal(job) {
      if (job && job.done === true) return true
      const value = this.stateValue(job)
      return ['completed', 'failed', 'canceled'].includes(value)
    },
    jobClass(job) {
      const value = this.stateValue(job)
      if (value.includes('fail') || value.includes('cancel')) return 'failed'
      if (value.includes('pend') || value.includes('wait')) return 'waiting'
      if (value.includes('load') || value.includes('run')) return 'running'
      if (value.includes('complete')) return 'healthy'
      return 'warning'
    },
    progress(job) { return Math.max(0, Math.min(100, Math.round(Number(job.progress || 0)))) },
    formatFiles(job) {
      const total = Number(job.total_files || 0)
      if (!total) return '-'
      return `${Number(job.completed_files || 0)} / ${total}`
    },
    taskDetails(job) {
      return Array.isArray(job && job.tasks) ? job.tasks : []
    },
    displayTaskTotal(job) {
      if (!job) return 0
      const total = Number(job.total_files || 0)
      if (total > 0) return total
      if (this.isJobLevelFailure(job)) return 1
      return this.taskDetails(job).length || 0
    },
    async loadJobTasks(job) {
      const id = this.jobId(job)
      if (!id) return
      if (this.taskDetails(job).length || this.isJobLevelFailure(job)) return
      this.detailLoading = true
      try {
        const next = await fetchJobStatus(id)
        this.upsertJob(next)
      } catch (error) {
        this.setNotice(`Status failed: ${error}`)
      } finally {
        this.detailLoading = false
      }
    },
    async openDetails(job) {
      const id = this.jobId(job)
      if (!id) return
      this.selectedJobId = id
      this.showFailedTasksOnly = false
      await this.loadJobTasks(job)
    },
    async openFailedTasks(job) {
      const id = this.jobId(job)
      if (!id || !this.hasFailedWork(job)) return
      this.selectedJobId = id
      this.showFailedTasksOnly = true
      await this.loadJobTasks(job)
    },
    closeDetails() {
      this.selectedJobId = ''
      this.showFailedTasksOnly = false
    },
    shortTaskId(id) {
      if (!id) return '-'
      const value = String(id)
      return value.length > 22 ? `${value.slice(0, 12)}...${value.slice(-7)}` : value
    },
    taskClass(task) {
      return this.jobClass(task)
    },
    taskWorker(task) {
      const value = String((task && task.worker) || '')
      if (!value) return '-'
      const match = value.match(/addr = ([^,\s]+)/)
      return match ? match[1] : value
    },
    taskPaths(task) {
      const source = task && task.source_path ? this.fileName(task.source_path) : '-'
      const target = task && task.target_path ? this.fileName(task.target_path) : '-'
      return `${source} → ${target}`
    },
    taskMessage(task, job = null) {
      const message = task && (task.message || (task.progress && task.progress.message))
      return message || (job && job.message) || '-'
    },
    isFailedTask(task) {
      return this.stateValue(task) === 'failed'
    },
    isJobLevelFailure(job) {
      if (!job || this.stateValue(job) !== 'failed') return false
      if (this.taskDetails(job).length > 0) return false
      return Number(job.total_files || 0) === 0
    },
    jobLevelFailureTasks(job) {
      if (!this.isJobLevelFailure(job)) return []
      return [{
        task_id: '(job-level)',
        state: 'failed',
        source_path: this.sourcePath(job),
        target_path: this.targetPath(job),
        message: job.message || 'Job failed before tasks were created',
        worker: '-',
        update_time_ms: job.update_time_ms
      }]
    },
    hasFailedWork(job) {
      return this.stateValue(job) === 'failed' || this.failedTaskCount(job) > 0
    },
    failedTaskCount(job) {
      const tasks = this.taskDetails(job)
      if (tasks.length) return tasks.filter((task) => this.isFailedTask(task)).length
      const failedFiles = Number(job && (job.failed_files || job.failed_tasks || 0))
      if (failedFiles > 0) return failedFiles
      if (this.isJobLevelFailure(job)) return 1
      return 0
    },
    runningTaskCount(job) {
      if (!['pending', 'loading', 'running'].includes(this.stateValue(job))) return 0
      const tasks = this.taskDetails(job)
      if (tasks.length) return tasks.filter((task) => ['pending', 'loading', 'running'].includes(this.stateValue(task))).length
      return Number(job && (job.running_files || job.loading_files || job.pending_files || 0))
    },
    taskErrorCode(task, job = null) {
      const message = String(this.taskMessage(task, job) || '').toLowerCase()
      if (!message || message === '-') return '-'
      if (message.includes('no such') || message.includes('not found')) return 'ENOENT'
      if (message.includes('permission') || message.includes('access denied')) return 'EACCES'
      if (message.includes('timeout') || message.includes('timed out')) return 'ETIMEDOUT'
      if (message.includes('lookup address') || message.includes('name or service')) return 'DNS'
      if (message.includes('io') || message.includes('failed')) return 'EIO'
      return '-'
    },
    retryCount(task) {
      const value = task && (task.retry_count ?? task.retries ?? task.attempts)
      return value === undefined || value === null ? '-' : value
    },
    taskTitle(task, job = null) {
      if (!task) return ''
      const message = this.taskMessage(task, job)
      return [
        task.task_id && `Task: ${task.task_id}`,
        task.worker && `Worker: ${task.worker}`,
        task.source_path && `Source: ${task.source_path}`,
        task.target_path && `Target: ${task.target_path}`,
        message !== '-' && `Message: ${message}`
      ].filter(Boolean).join('\n')
    },
    fileName(path) {
      const parts = String(path || '').split('/').filter(Boolean)
      return parts.pop() || '/'
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
    loadStatusCli(job) {
      return `curvine load-status ${this.jobId(job)} --verbose`
    },
    pathStatusCli(job) {
      return `curvine load-status --path ${this.sourcePath(job)} --verbose`
    },
    mountListCli(job) {
      return `curvine load-list --mount ${this.mountPath(job)} --state failed --limit 50 --verbose`
    },
    cliCommands(job) {
      if (!job) return []
      const items = [
        { key: 'job-status', label: 'Job status', command: this.loadStatusCli(job) },
        { key: 'path-status', label: 'Path status', command: this.pathStatusCli(job) }
      ]
      if (this.mountPath(job) !== '-') {
        items.push({ key: 'mount-list', label: 'Failed on mount', command: this.mountListCli(job) })
      }
      return items
    },
    async copyCliCommand(key, text) {
      const copied = await this.copyText(text, 'Command copied')
      if (!copied) return
      this.copiedCliKey = key
      window.setTimeout(() => {
        if (this.copiedCliKey === key) this.copiedCliKey = ''
      }, 1500)
    },
    async copyCli(job) {
      await this.copyText(this.loadStatusCli(job), 'CLI command copied')
    },
    async copyText(text, message = 'Copied') {
      if (!text || text === '-') {
        this.setNotice('Nothing to copy')
        return false
      }
      try {
        await copyToClipboard(String(text))
        this.setNotice(message)
        return true
      } catch (error) {
        this.setNotice(`Copy failed: ${error}`)
        return false
      }
    },
    openLoadForm() { this.dragOffset = { x: 0, y: 0 }; this.resetForm(); this.fetchMounts(); this.showLoadForm = true },
    closeLoadForm() { this.endDrag(); this.dragOffset = { x: 0, y: 0 }; this.showLoadForm = false; this.resetForm() },
    resetForm() { this.form = emptyForm(); this.configsText = '' },
    startDrag(e) {
      if (e.button !== 0) return
      this.isDragging = true
      this.dragStart = { mouseX: e.clientX, mouseY: e.clientY, baseX: this.dragOffset.x, baseY: this.dragOffset.y }
      window.addEventListener('mousemove', this.onDrag)
      window.addEventListener('mouseup', this.endDrag)
    },
    onDrag(e) {
      if (!this.isDragging || !this.dragStart) return
      this.dragOffset = {
        x: this.dragStart.baseX + e.clientX - this.dragStart.mouseX,
        y: this.dragStart.baseY + e.clientY - this.dragStart.mouseY
      }
    },
    endDrag() {
      if (!this.isDragging) return
      this.isDragging = false
      this.dragStart = null
      window.removeEventListener('mousemove', this.onDrag)
      window.removeEventListener('mouseup', this.endDrag)
    },
    togglePolling() {
      this.stopPolling()
      if (!this.polling) return
      this.pollTimer = window.setTimeout(this.pollActiveJobs, 3000)
    },
    stopPolling() {
      if (this.pollTimer) window.clearTimeout(this.pollTimer)
      this.pollTimer = null
    },
    async pollActiveJobs() {
      this.stopPolling()
      if (!this.polling) return
      const activeJobs = this.jobs.filter((job) => !this.isFinal(job))
      for (const job of activeJobs) {
        const id = job.job_id || job.id
        if (!id) continue
        try { this.upsertJob(await fetchJobStatus(id)) }
        catch (_) {}
      }
      this.togglePolling()
    }
  }
}
</script>


<style scoped>
.jobs-grid {
  grid-template-columns: 1fr;
  align-items: start;
}

.job-metric-grid {
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
}

.jobs-grid,
.load-form,
.load-form label,
.load-form input,
.load-form select,
.load-form textarea,
.toolbar,
.admin-notice {
  min-width: 0;
}

.job-filter-bar {
  margin-top: 4px;
  padding-top: 0;
}

.jobs-toolbar {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
}

.jobs-toolbar .admin-input {
  flex: 1 1 220px;
  min-width: 160px;
}

.jobs-toolbar .job-search-input {
  flex: 2 1 360px;
}

.jobs-toolbar .compact-select {
  flex: 0 1 150px;
  min-width: 130px;
  height: 34px;
}

.job-list {
  display: grid;
  gap: 10px;
  overflow-x: auto;
  padding-bottom: 2px;
}

.job-header {
  padding: 0 12px;
  min-height: 24px;
  border: 0;
  background: transparent;
  color: var(--admin-muted);
  font-size: 12px;
  font-weight: 760;
}

.job-row {
  display: grid;
  grid-template-columns: minmax(148px, 0.58fr) 124px minmax(260px, 1.45fr) 100px minmax(112px, .75fr) 84px 80px 82px;
  gap: 8px;
  align-items: center;
  min-width: 1180px;
  padding: 9px 12px;
  border: 1px solid var(--admin-border);
  border-radius: var(--admin-radius);
  background: var(--admin-surface-soft);
}

.job-row.selected {
  border-color: rgba(74, 122, 54, .35);
  box-shadow: 0 0 0 2px rgba(74, 122, 54, .08);
}

.job-main,
.job-id-cell,
.job-type-cell,
.job-meta,
.job-progress {
  min-width: 0;
}

.job-main strong,
.job-id-cell strong,
.job-meta strong {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 13px;
}

.job-id-cell strong {
  font-size: 13px;
  letter-spacing: -.01em;
}

.job-id-cell .job-list-title {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  line-height: 1.2;
  font-size: 11px;
  letter-spacing: -0.01em;
}

.job-id-cell .job-display-name {
  color: #7d8b73;
  display: block;
  line-height: 1.25;
  font-size: 11px;
  word-break: break-all;
}

.job-id-cell small {
  color: #7d8b73;
}

.job-id-cell {
  cursor: copy;
  overflow: hidden;
}

.job-main small,
.job-id-cell small,
.job-type-cell small,
.job-meta span,
.job-progress small,
.job-progress .usage-line span {
  display: block;
  color: var(--admin-muted);
  font-size: 11px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.job-main {
  min-width: 0;
  overflow: hidden;
}

.job-type-cell {
  display: flex;
  align-items: center;
}

.job-type-badge {
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

.job-type-badge.auto {
  color: #265d89;
  background: #eaf4ff;
}

.job-type-badge.manual {
  color: #48612b;
  background: #eff7e8;
}

.job-hint {
  color: #7a8f70 !important;
}

.job-progress .usage-line {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  align-items: center;
  margin-bottom: 4px;
}

.job-progress .usage-line strong {
  flex: 0 0 auto;
  font-size: 13px;
}

.job-progress .inline-meter {
  width: 100%;
}

.job-row .job-meta span {
  display: none;
}

.job-meta-files strong {
  font-size: 12px;
  white-space: nowrap;
  overflow: visible;
  text-overflow: clip;
}

.job-meta-updated strong {
  font-size: 12px;
  white-space: nowrap;
  overflow: visible;
  text-overflow: clip;
}

.job-actions {
  flex-direction: column;
  align-items: stretch;
  gap: 2px;
}

.job-actions .link-button {
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

.job-detail-panel {
  position: fixed;
  top: 84px;
  right: 24px;
  bottom: 24px;
  z-index: 60;
  display: flex;
  flex-direction: column;
  grid-template-columns: none;
  grid-template-rows: none;
  grid-auto-flow: row;
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

.bad-count {
  color: #a03939;
}

.task-table-scroll {
  overflow-x: auto;
  border: 1px solid rgba(91, 103, 122, 0.14);
  border-radius: 8px;
  background: rgba(255, 255, 255, .76);
}

.diagnostic-task-row {
  display: grid;
  grid-template-columns: 150px 96px 170px 260px 260px 96px 280px 72px 120px;
  min-width: 1504px;
  gap: 8px;
  align-items: center;
  padding: 7px 8px;
  color: #44546a;
  font-size: 11px;
}

.diagnostic-task-row + .diagnostic-task-row {
  border-top: 1px solid rgba(91, 103, 122, 0.1);
}

.diagnostic-task-row.failed {
  background: #fff8f8;
}

.diagnostic-task-header {
  color: var(--admin-muted);
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: .02em;
  background: #f8fbf5;
}

.diagnostic-task-row > span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.path-detail .cli-row-body {
  grid-column: 1 / -1;
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 8px;
  align-items: start;
}

.cli-snippet {
  display: block;
  min-width: 0;
  margin: 0;
  padding: 7px 9px;
  border: 1px solid rgba(91, 103, 122, 0.1);
  border-radius: 6px;
  background: #f8faf6;
  color: #314736;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
  font-size: 11.5px;
  line-height: 1.45;
  white-space: pre-wrap;
  word-break: break-all;
}

.job-task-details {
  grid-column: 1 / -1;
  display: grid;
  gap: 4px;
  padding: 8px;
  border: 1px solid rgba(91, 103, 122, 0.16);
  border-radius: 8px;
  background: rgba(255, 255, 255, 0.72);
}

.job-task-summary {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
  padding-bottom: 4px;
  color: var(--admin-text);
  font-size: 12px;
}

.job-task-summary span {
  color: var(--admin-muted);
}

.job-task-header,
.job-task-row {
  display: grid;
  grid-template-columns: minmax(120px, 0.9fr) 96px minmax(130px, 0.9fr) minmax(180px, 1.2fr) minmax(160px, 1fr);
  gap: 8px;
  align-items: center;
}

.job-task-header {
  color: var(--admin-muted);
  font-size: 11px;
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: .02em;
}

.job-task-row {
  color: #44546a;
  font-size: 11px;
}

.job-task-row > span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.job-id-subtitle {
  margin-top: 4px;
  font-size: 12px;
  color: var(--admin-text-muted, #6b7280);
  word-break: break-all;
}

.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
}

.job-row .job-state .status-pill {
  max-width: 100%;
  white-space: nowrap;
}

.load-preview {
  display: grid;
  gap: 8px;
  padding: 10px 12px;
  border: 1px solid #edf4e9;
  border-radius: 8px;
  background: var(--admin-surface-soft);
}

.load-preview div {
  display: grid;
  grid-template-columns: 56px minmax(0, 1fr);
  gap: 10px;
  align-items: center;
}

.load-preview span {
  color: var(--admin-muted);
  font-size: 12px;
}

.load-preview strong {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 13px;
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

.load-form label {
  display: grid;
  grid-template-columns: 1fr;
  gap: 6px;
}

.load-form input:not([type="checkbox"]),
.load-form select,
.load-form textarea {
  width: 100%;
  max-width: 100%;
  border: 1px solid #d9e4d2;
  border-radius: 8px;
  padding: 9px 10px;
  font: inherit;
  background: #fff;
  color: var(--admin-text);
}

.load-form .form-row {
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}

.load-form .check-line {
  display: inline-flex !important;
  grid-template-columns: none !important;
  align-items: center;
  justify-content: flex-start;
  gap: 8px !important;
  width: fit-content;
  line-height: 1.2;
}

.load-form .check-line input[type="checkbox"] {
  flex: 0 0 auto;
  width: 16px;
  height: 16px;
  margin: 0;
  padding: 0;
  accent-color: var(--admin-primary);
}

@media (max-width: 820px) {
  .job-row {
    min-width: 0;
    grid-template-columns: minmax(0, 1fr) 100px;
    align-items: start;
  }

  .job-header {
    display: none;
  }

  .job-main { grid-column: 1 / -1; }
  .job-state { justify-self: end; }
  .job-progress,
  .job-task-details {
    grid-column: 1 / -1;
  }
  .job-actions {
    grid-column: 1 / -1;
    flex-direction: row;
    flex-wrap: wrap;
  }

  .job-actions .link-button {
    width: auto;
    flex: 1 1 auto;
  }

  .load-form .form-row { grid-template-columns: 1fr; }
}

@media (max-width: 760px) {
  .job-row {
    grid-template-columns: 1fr;
  }

  .job-state {
    justify-self: start;
  }

  .job-task-header {
    display: none;
  }

  .job-task-row {
    grid-template-columns: 1fr;
    gap: 4px;
    padding: 6px 0;
    border-top: 1px solid rgba(91, 103, 122, 0.12);
  }
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

.drag-handle {
  cursor: grab;
  user-select: none;
}

.drag-handle:active {
  cursor: grabbing;
}

.form-hint {
  margin: 0 0 12px;
  color: #5b677a;
  font-size: 12px;
  line-height: 1.45;
}

</style>
