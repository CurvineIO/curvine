<template>
  <div class="admin-page" v-loading="loading">
    <section class="admin-panel table-panel">
      <div class="panel-heading table-heading"><div><h2>{{ $t('browse.title') }}</h2><p>{{ $t('browse.subtitle') }}</p></div><div class="segmented"><button class="active">{{ $t('browse.view.list') }}</button><button @click="goBlocks">{{ $t('browse.view.blocks') }}</button></div></div>
      <div class="path-toolbar"><button class="admin-button ghost compact" @click="setRoot">{{ $t('common.root') }}</button><button class="admin-button ghost compact" :disabled="!canGoUp" :title="$t('browse.goUpTitle')" @click="goUp">{{ $t('common.up') }}</button><input v-model="browsePath" class="admin-input path-input" @keyup.enter="navigate" :placeholder="$t('browse.pathPlaceholder')"><button class="admin-button primary compact" @click="navigate">{{ $t('common.go') }}</button><label class="cache-toggle"><input type="checkbox" v-model="cacheOnly" @change="fetchData"> {{ $t('browse.cacheOnly') }}</label><button class="admin-button ghost compact" @click="mkdirDialog">{{ $t('browse.mkdir') }}</button><button class="admin-button ghost compact" @click="uploadFileDialog">{{ $t('browse.uploadFile') }}</button><button class="admin-button ghost compact" @click="uploadFolderDialog">{{ $t('browse.uploadFolder') }}</button><button class="admin-button ghost compact" :disabled="!canFreeCurrentPath" :title="currentFreeTitle" @click="freeCurrentPath">{{ $t('browse.free') }}</button><input ref="fileInput" class="hidden-file-input" type="file" @change="onFilePicked"><input ref="folderInput" class="hidden-file-input" type="file" webkitdirectory directory multiple @change="onFolderPicked"></div>
      <div v-if="uploadNotice" class="upload-notice">{{ uploadNotice }}</div>
      <div v-if="uploadBatchList.length" class="sync-panel">
        <div class="sync-panel-title">
          <span>{{ $t('browse.upload.progress') }}</span>
          <div class="sync-title-actions">
            <button v-if="hasCompletedUploadBatches" class="link-button" @click="clearCompletedUploadBatches">{{ $t('browse.upload.clearCompleted') }}</button>
            <button class="link-button" @click="clearUploadBatches">{{ $t('browse.upload.clear') }}</button>
          </div>
        </div>
        <div v-for="batch in uploadBatchList" :key="batch.id" class="sync-job batch-sync">
          <strong :title="batch.label">{{ batch.label }}</strong>
          <span :class="['sync-status', syncStatusClass(batch)]">{{ syncStateLabel(batch.state) }}</span>
          <span>{{ $t('browse.upload.uploadedCount', { uploaded: batch.uploadedFiles, total: batch.totalFiles }) }}</span>
          <span>{{ formatBytes(batch.uploadedSize) }}</span>
          <div class="sync-progress-cell"><div class="meter sync-meter"><span :style="{ width: batch.progress + '%' }"></span></div><strong>{{ batch.progress }}%</strong></div>
          <div v-if="batch.message" class="sync-message">{{ batch.message }}</div>
        </div>
      </div>
      <div v-if="ufsSyncJobCount" class="sync-panel ufs-sync-panel" :class="{ 'is-collapsed': !ufsSyncPanelExpanded }">
        <div class="sync-panel-header">
          <button
            type="button"
            class="sync-collapse-btn"
            :aria-expanded="ufsSyncPanelExpanded"
            :aria-label="ufsSyncPanelExpanded ? $t('browse.ufsSync.collapseAria') : $t('browse.ufsSync.expandAria')"
            :title="ufsSyncPanelExpanded ? $t('browse.ufsSync.collapse') : $t('browse.ufsSync.expand')"
            @click="toggleUfsSyncPanel"
          >
            <svg class="sync-collapse-icon" :class="{ expanded: ufsSyncPanelExpanded }" viewBox="0 0 16 16" aria-hidden="true">
              <path d="M4 6l4 4 4-4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
            </svg>
          </button>
          <div class="sync-panel-heading-main" role="button" tabindex="0" @click="toggleUfsSyncPanel" @keydown.enter.prevent="toggleUfsSyncPanel" @keydown.space.prevent="toggleUfsSyncPanel">
            <span class="sync-panel-toggle-label">{{ $t('browse.ufsSync.title') }}</span>
            <span class="sync-panel-summary">{{ ufsSyncSummaryText }}</span>
          </div>
          <div class="sync-title-actions">
            <button class="link-button" @click.stop="$router.push('/jobs')">{{ $t('browse.ufsSync.viewJobs') }}</button>
            <button v-if="hasCompletedUfsSyncJobs" class="link-button" @click.stop="clearCompletedUfsSyncJobs">{{ $t('browse.upload.clearCompleted') }}</button>
          </div>
        </div>
        <div v-show="ufsSyncPanelExpanded" class="sync-panel-body">
        <div v-for="batch in ufsSyncBatchList" :key="batch.id" class="sync-job batch-sync">
          <div class="sync-target" :title="batch.title || batch.label">
            <strong>{{ batch.label }}</strong>
            <small v-if="batch.subtitle">{{ batch.subtitle }}</small>
          </div>
          <span :class="['sync-status', syncStatusClass(batch)]">{{ syncStateLabel(batch.state) }}</span>
          <span>{{ $t('browse.ufsSync.ufsSynced', { synced: batch.syncedFiles, total: batch.totalFiles }) }}</span>
          <span>{{ formatBytes(batch.syncedSize) }} / {{ formatBytes(batch.totalSize) }}</span>
          <div class="sync-progress-cell"><div class="meter sync-meter"><span :style="{ width: batch.progress + '%' }"></span></div><strong>{{ batch.progress }}%</strong></div>
          <div v-if="batch.message" class="sync-message">{{ batch.message }}</div>
        </div>
        <div v-for="job in ufsSyncList" :key="job.path" class="sync-job">
          <div class="sync-target" :title="syncJobTitle(job)">
            <strong>{{ syncDisplayPath(job) }}</strong>
            <small>{{ syncJobSubtitle(job) }}</small>
          </div>
          <span :class="['sync-status', syncStatusClass(job)]">{{ syncStateLabel(syncState(job)) }}</span>
          <span>{{ syncFiles(job) }}</span>
          <span>{{ syncBytes(job) }}</span>
          <div class="sync-progress-cell"><div class="meter sync-meter"><span :style="{ width: syncProgress(job) + '%' }"></span></div><strong>{{ syncProgress(job) }}%</strong></div>
          <div v-if="job.message" class="sync-message">{{ job.message }}</div>
        </div>
        </div>
      </div>
      <table class="admin-table file-browser-table">
        <colgroup><col class="name-col"><col class="type-col"><col class="size-col"><col class="modified-col"><col class="actions-col"></colgroup>
        <thead><tr><th>{{ $t('browse.table.name') }}</th><th>{{ $t('browse.table.type') }}</th><th>{{ $t('browse.table.size') }}</th><th>{{ $t('browse.table.modified') }}</th><th></th></tr></thead>
        <tbody><tr v-for="item in items" :key="item.path"><td class="file-name-cell" :title="item.path" @click="enterItem(item)"><div class="compact-name-cell"><strong>{{ fileName(item.path) }}</strong><button class="copy-path-button" :title="$t('browse.copyPath')" @click.stop="copyPath(item.path)">{{ copiedPath === item.path ? $t('common.copied') : $t('common.copy') }}</button></div></td><td>{{ item.is_dir ? $t('browse.itemType.directory') : $t('browse.itemType.file') }}</td><td>{{ itemSizeText(item) }}</td><td>{{ formatTime(item.mtime) }}</td><td class="row-actions"><button class="link-button" @click="item.is_dir ? openDir(item.path) : openBlocks(item.path)">{{ item.is_dir ? $t('common.open') : $t('common.blocks') }}</button><button class="link-button" @click="downloadItem(item)">{{ $t('common.download') }}</button><button class="link-button" :disabled="!canFreeItem(item)" :title="freeItemTitle(item)" @click="freeItem(item)">{{ $t('browse.free') }}</button><button class="link-button danger-link" :disabled="isMountRoot(item.path)" :title="deleteItemTitle(item)" @click="deleteItem(item)">{{ $t('common.delete') }}</button></td></tr></tbody>
      </table>
      <div v-if="items.length === 0 && !loading" class="empty-state">{{ cacheOnly ? $t('browse.empty.cacheOnly') : $t('browse.empty.directory') }}</div>
    </section>
  </div>
</template>

<script>
import { createDirectory, deletePath, fetchBrowseData, fetchMountsData, fetchUfsSyncJobs, fetchUfsSyncStatus, freePath, uploadFile } from '@/api/client'
import { copyToClipboard } from '@/utils/clipboard'
import { translateSyncState, syncStateMessage } from '@/utils/browseSyncState'
import { formatBytes, formatTime } from '@/utils/format'
import { buildJobDisplayName, shortJobId } from '@/utils/jobDisplay'
import eventBus from '@/utils/eventBus'

export default {
  name: 'FileSystemPage',
  data() { return { loading: false, browsePath: this.$route.query.path || '/', cacheOnly: false, items: [], copiedPath: '', uploadNotice: '', downloadNoticeActive: false, mounts: [], mountsLoaded: false, summaryLoadToken: 0, uploadBatches: {}, uploadBatchStorageRefreshTimer: null, ufsSyncJobs: {}, ufsSyncTimers: {}, ufsSyncBatchTimers: {}, ufsSyncSaveTimer: null, ufsSyncStorageRefreshTimer: null, ufsSyncDiscoverTimer: null, ufsSyncDiscoverToken: 0, ufsSyncDismissedAt: {}, ufsSyncPanelExpanded: true } },
  computed: {
    uploadBatchList() { return Object.values(this.uploadBatches) },
    hasCompletedUploadBatches() {
      return Object.values(this.uploadBatches).some((batch) => batch && batch.done)
    },
    ufsSyncList() { return Object.values(this.ufsSyncJobs).filter((job) => !job.batch_id) },
    hasCompletedUfsSyncJobs() {
      return Object.values(this.ufsSyncJobs).some((job) => job && job.done)
    },
    ufsSyncJobCount() {
      return this.ufsSyncList.length + this.ufsSyncBatchList.length
    },
    ufsSyncActiveCount() {
      return Object.values(this.ufsSyncJobs).filter((job) => {
        if (!job) return false
        return !job.done && !this.isTerminalSyncState(this.syncState(job))
      }).length
    },
    ufsSyncSummaryText() {
      const total = this.ufsSyncJobCount
      const active = this.ufsSyncActiveCount
      if (!total) return ''
      if (active > 0) return this.$t('browse.ufsSync.activeTotal', { active, total })
      return this.$t('browse.ufsSync.tracked', { total })
    },
    ufsSyncBatchList() {
      const batches = {}
      for (const job of Object.values(this.ufsSyncJobs)) {
        if (!job.batch_id) continue
        const firstPath = job.path || job.source_path || ''
        const batch = batches[job.batch_id] || { id: job.batch_id, label: job.batch_label || this.syncDisplayPath(job), subtitle: firstPath, title: firstPath, totalFiles: 0, syncedFiles: 0, terminalFiles: 0, uploadFiles: 0, waitingFiles: 0, syncingFiles: 0, failedFiles: 0, syncedSize: 0, totalSize: 0, progressSize: 0, state: 'Queued', message: '' }
        const state = this.syncState(job)
        const progress = this.syncProgress(job)
        const totalSize = Number(job.total_size || 0)
        const totalFiles = Math.max(1, Number(job.total_files || 0))
        const completedFiles = state === 'Completed' ? totalFiles : Number(job.completed_files || 0)
        batch.totalFiles += totalFiles
        batch.syncedFiles += Math.max(0, Math.min(totalFiles, completedFiles))
        batch.totalSize += totalSize
        batch.progressSize += totalSize > 0 ? totalSize * progress / 100 : 0
        if (state === 'Completed') {
          batch.syncedSize += totalSize || Number(job.loaded_size || 0)
        }
        if (['Uploading to Curvine', 'Queued'].includes(state)) batch.uploadFiles += 1
        else if (['Waiting for UFS sync', 'Checking UFS sync', 'NotStarted'].includes(state)) batch.waitingFiles += 1
        else if (['Pending', 'Loading'].includes(state)) batch.syncingFiles += 1
        if (Boolean(job.done) || this.isTerminalSyncState(state)) batch.terminalFiles += 1
        if (['Failed', 'Canceled', 'Unknown', 'Timed out'].includes(state)) batch.failedFiles += 1
        if (!batch.message && job.message) batch.message = job.message
        batches[job.batch_id] = batch
      }
      return Object.values(batches).map((batch) => {
        const progress = batch.totalSize > 0
          ? Math.max(0, Math.min(100, Math.round(batch.progressSize / batch.totalSize * 100)))
          : (batch.syncedFiles === batch.totalFiles ? 100 : 0)
        if (batch.syncedFiles >= batch.totalFiles) return { ...batch, state: 'Completed', progress: 100 }
        if (batch.failedFiles > 0) return { ...batch, state: 'Needs attention', progress }
        if (batch.syncingFiles > 0) return { ...batch, state: 'Syncing to UFS', progress }
        if (batch.waitingFiles > 0) return { ...batch, state: 'Waiting for UFS sync', progress }
        if (batch.uploadFiles > 0) return { ...batch, state: 'Uploading to Curvine', progress }
        if (batch.terminalFiles >= batch.totalFiles) return { ...batch, state: 'Finished with skipped files', progress }
        return { ...batch, progress }
      })
    },
    canGoUp() { return !this.isRootPath },
    isRootPath() { return this.normalizePath(this.browsePath) === '/' },
    canFreeCurrentPath() {
      return this.isMountedPath(this.normalizePath(this.browsePath))
    },
    currentFreeTitle() {
      if (!this.isMountedPath(this.normalizePath(this.browsePath))) {
        return this.$t('browse.freeAction.onlyMount')
      }
      return this.$t('browse.freeAction.currentTitle')
    }
  },
  watch: {
    '$route.query.path'(path) { this.browsePath = path || '/'; this.restoreBrowseSnapshot(); this.fetchData() },
    ufsSyncActiveCount(newCount, oldCount) {
      if (newCount > oldCount && newCount > 0 && !this.ufsSyncPanelExpanded) {
        this.ufsSyncPanelExpanded = true
        this.saveUfsSyncPanelExpanded()
      }
    }
  },
  created() {
    this.restoreUploadBatches()
    this.loadUfsSyncDismissed()
    this.loadUfsSyncPanelExpanded()
    this.restoreUfsSyncJobs()
    this.restoreBrowseSnapshot()
    this.fetchData()
  },
  mounted() {
    eventBus.on('admin-refresh', this.fetchData)
    this.startUploadBatchStorageRefresh()
    this.startUfsSyncStorageRefresh()
    this.startUfsSyncDiscover()
  },
  beforeUnmount() {
    eventBus.off('admin-refresh', this.fetchData)
    Object.values(this.ufsSyncTimers).forEach((timer) => window.clearTimeout(timer))
    Object.values(this.ufsSyncBatchTimers).forEach((timer) => window.clearTimeout(timer))
    if (this.uploadBatchStorageRefreshTimer) window.clearTimeout(this.uploadBatchStorageRefreshTimer)
    if (this.ufsSyncStorageRefreshTimer) window.clearTimeout(this.ufsSyncStorageRefreshTimer)
    if (this.ufsSyncSaveTimer) window.clearTimeout(this.ufsSyncSaveTimer)
    this.stopUfsSyncDiscover()
  },
  methods: {
    formatBytes, formatTime,
    syncStateLabel(state) {
      return translateSyncState(this.$t, state)
    },
    itemSizeText(item) {
      if (!item) return '-'
      if (item.is_dir) return '-'
      return this.formatBytes(item.len)
    },
    async fetchData() {
      const token = this.summaryLoadToken + 1
      this.summaryLoadToken = token
      this.restoreBrowseSnapshot()
      this.loading = true
      try {
        await this.ensureMounts()
        const data = await fetchBrowseData({
          path: this.normalizePath(this.browsePath),
          cache_only: this.cacheOnly
        })
        this.items = Array.isArray(data) ? data : (data.items || [])
        this.saveBrowseSnapshot()
        this.fetchVisibleUfsSyncJobs(token, { includeFinished: false })
        this.fetchVisiblePathUfsSyncStatuses(token)
      } finally { this.loading = false }
    },
    browseSnapshotStorageKey() {
      return `curvine.web.fs.browseSnapshot.v1:${this.cacheOnly ? 'cache' : 'all'}:${this.normalizePath(this.browsePath)}`
    },
    saveBrowseSnapshot() {
      try {
        window.localStorage.setItem(this.browseSnapshotStorageKey(), JSON.stringify({
          items: this.items,
          saved_at: Date.now()
        }))
      } catch (_) {}
    },
    restoreBrowseSnapshot() {
      try {
        const raw = window.localStorage.getItem(this.browseSnapshotStorageKey())
        if (!raw) return
        const snapshot = JSON.parse(raw)
        if (!snapshot || !Array.isArray(snapshot.items)) return
        if (Date.now() - Number(snapshot.saved_at || 0) > 10 * 60 * 1000) return
        this.items = snapshot.items
      } catch (_) {}
    },
    async fetchVisibleUfsSyncJobs(token, options = {}) {
      const pathPrefix = options.pathPrefix || this.normalizePath(this.browsePath)
      const limit = options.limit || 100
      const includeFinished = options.includeFinished !== undefined ? options.includeFinished : true
      const includeTasks = options.includeTasks !== undefined ? options.includeTasks : false
      try {
        const data = await fetchUfsSyncJobs(pathPrefix, limit, includeFinished, { includeTasks })
        if (token !== this.summaryLoadToken && token !== this.ufsSyncDiscoverToken) return
        const jobs = Array.isArray(data) ? data : (data.items || [])
        for (const job of jobs) {
          if (!job || !job.path) continue
          this.mergeUfsSyncJobFromServer(this.normalizePath(job.path), job)
        }
      } catch (_) {}
    },
    async fetchVisiblePathUfsSyncStatuses(token) {
      if (Object.keys(this.ufsSyncBatchTimers).length > 0 || this.ufsSyncActiveCount > 20) return
      const visibleFiles = this.items
        .filter((item) => item && !item.is_dir && this.isFsModePath(item.path))
        .slice(0, 20)
      for (const item of visibleFiles) {
        if (token !== this.summaryLoadToken && token !== this.ufsSyncDiscoverToken) return
        try {
          const job = await fetchUfsSyncStatus(item.path)
          if (token !== this.summaryLoadToken && token !== this.ufsSyncDiscoverToken) return
          if (!job || !job.path) continue
          const normalizedPath = this.normalizePath(job.path)
          const state = this.syncState(job)
          const hasExisting = Boolean(this.ufsSyncJobs[normalizedPath])
          if (!hasExisting && ['NotStarted', 'NotMounted', 'Unsupported'].includes(state)) continue
          this.mergeUfsSyncJobFromServer(normalizedPath, job)
        } catch (_) {}
      }
    },
    ufsSyncPanelStorageKey() {
      return 'curvine.web.fs.ufsSyncPanelExpanded.v1'
    },
    loadUfsSyncPanelExpanded() {
      try {
        const raw = window.localStorage.getItem(this.ufsSyncPanelStorageKey())
        this.ufsSyncPanelExpanded = raw === null ? true : raw === '1'
      } catch (_) {
        this.ufsSyncPanelExpanded = true
      }
    },
    saveUfsSyncPanelExpanded() {
      try {
        window.localStorage.setItem(this.ufsSyncPanelStorageKey(), this.ufsSyncPanelExpanded ? '1' : '0')
      } catch (_) {}
    },
    toggleUfsSyncPanel() {
      this.ufsSyncPanelExpanded = !this.ufsSyncPanelExpanded
      this.saveUfsSyncPanelExpanded()
    },
    ufsSyncDismissedStorageKey() {
      return 'curvine.web.fs.ufsSyncDismissed.v1'
    },
    loadUfsSyncDismissed() {
      try {
        const raw = window.localStorage.getItem(this.ufsSyncDismissedStorageKey())
        this.ufsSyncDismissedAt = raw ? (JSON.parse(raw) || {}) : {}
      } catch (_) {
        this.ufsSyncDismissedAt = {}
      }
    },
    saveUfsSyncDismissed() {
      try {
        window.localStorage.setItem(this.ufsSyncDismissedStorageKey(), JSON.stringify(this.ufsSyncDismissedAt))
      } catch (_) {}
    },
    isUfsSyncDismissed(path, job) {
      const normalizedPath = this.normalizePath(path)
      const dismissedAt = Number(this.ufsSyncDismissedAt[normalizedPath] || 0)
      if (!dismissedAt) return false
      if (!job) return true
      const state = job.state || job.status || ''
      const done = Boolean(job.done) || this.isTerminalSyncState(state)
      if (!done) return false
      const serverUpdated = Number(job.update_time_ms || job.updated_at || 0)
      return !serverUpdated || serverUpdated <= dismissedAt
    },
    markUfsSyncDismissed(path, job) {
      const normalizedPath = this.normalizePath(path)
      const now = Date.now()
      const stamp = Math.max(
        Number(this.ufsSyncDismissedAt[normalizedPath] || 0),
        Number(job && (job.update_time_ms || job.updated_at) || 0),
        now
      )
      this.ufsSyncDismissedAt = { ...this.ufsSyncDismissedAt, [normalizedPath]: stamp }
      this.saveUfsSyncDismissed()
    },
    mergeUfsSyncJobFromServer(path, job) {
      if (this.isUfsSyncDismissed(path, job)) return
      const existing = this.ufsSyncJobs[path] || {}
      const state = job.state || job.status || ''
      const done = Boolean(job.done) || this.isTerminalSyncState(state)
      const localState = this.syncState(existing)
      if (localState === 'Uploading to Curvine' && !done) {
        this.setUfsSyncJob(path, { ...existing, ...job, path, done: Boolean(existing.done) })
        return
      }
      const serverUpdated = Number(job.update_time_ms || job.updated_at || 0)
      const localUpdated = Number(existing.updated_at || 0)
      if (serverUpdated && localUpdated && serverUpdated < localUpdated && !done) return
      this.setUfsSyncJob(path, { ...job, path, done })
    },
    ufsSyncDiscoverIntervalMs() {
      const active = this.ufsSyncActiveCount
      if (active > 100) return 12000
      if (active > 20) return 8000
      return 4000
    },
    startUfsSyncDiscover() {
      this.stopUfsSyncDiscover()
      const tick = async () => {
        const token = this.ufsSyncDiscoverToken
        await this.fetchVisibleUfsSyncJobs(token, { pathPrefix: '/', limit: 50, includeFinished: false })
        await this.fetchVisiblePathUfsSyncStatuses(token)
        if (token === this.ufsSyncDiscoverToken) {
          this.ufsSyncDiscoverTimer = window.setTimeout(tick, this.ufsSyncDiscoverIntervalMs())
        }
      }
      this.ufsSyncDiscoverTimer = window.setTimeout(tick, 500)
    },
    stopUfsSyncDiscover() {
      if (this.ufsSyncDiscoverTimer) {
        window.clearTimeout(this.ufsSyncDiscoverTimer)
        this.ufsSyncDiscoverTimer = null
      }
    },
    async mkdirDialog() {
      const path = window.prompt(this.$t('browse.mkdirAction.prompt'), this.joinPath(this.browsePath, this.$t('browse.mkdirAction.defaultName')))
      if (!path) return
      this.loading = true
      try {
        await createDirectory(path, true)
        window.alert(this.$t('browse.mkdirAction.success', { path }))
        await this.fetchData()
      } catch (error) {
        window.alert(this.$t('browse.mkdirAction.failed', { error }))
      } finally { this.loading = false }
    },
    async deleteItem(item) {
      if (this.isMountRoot(item.path)) {
        window.alert(this.deleteItemTitle(item))
        return
      }
      const recursive = Boolean(item.is_dir)
      const mounted = this.isMountedPath(item.path)
      const message = recursive
        ? (mounted ? this.$t('browse.delete.confirmDirMounted', { path: item.path }) : this.$t('browse.delete.confirmDir', { path: item.path }))
        : (mounted ? this.$t('browse.delete.confirmFileMounted', { path: item.path }) : this.$t('browse.delete.confirmFile', { path: item.path }))
      if (!window.confirm(message)) return
      this.loading = true
      try {
        await deletePath(item.path, recursive)
        window.alert(this.$t('browse.delete.success', { path: item.path }))
        await this.fetchData()
      } catch (error) {
        window.alert(this.$t('browse.delete.failed', { error }))
      } finally { this.loading = false }
    },
    uploadFileDialog() {
      this.$refs.fileInput.value = ''
      this.$refs.fileInput.click()
    },
    uploadFolderDialog() {
      this.$refs.folderInput.value = ''
      this.$refs.folderInput.click()
    },
    async onFilePicked(event) {
      const file = event.target.files && event.target.files[0]
      if (!file) return
      const remotePath = this.joinPath(this.browsePath, file.name)
      this.uploadNotice = ''
      this.setUfsSyncJob(remotePath, {
        path: remotePath,
        state: 'Uploading to Curvine',
        status: 'Uploading to Curvine',
        progress: 0,
        total_files: 1,
        completed_files: 0,
        loaded_size: 0,
        total_size: Number(file.size || 0),
        done: false,
        attempts: 0
      })
      try {
        const result = await uploadFile(remotePath, file, true, (progressEvent) => {
          const total = Number(progressEvent.total || file.size || 0)
          const loaded = Number(progressEvent.loaded || 0)
          this.setUfsSyncJob(remotePath, {
            state: 'Uploading to Curvine',
            status: 'Uploading to Curvine',
            loaded_size: loaded,
            total_size: total,
            total_files: 1,
            completed_files: 0,
            done: false
          })
        })
        if (result && result.ufs_sync) {
          this.setUfsSyncJob(remotePath, {
            ...(result.ufs_sync || {}),
            state: (result.ufs_sync && (result.ufs_sync.state || result.ufs_sync.status)) || 'Waiting for UFS sync',
            status: (result.ufs_sync && (result.ufs_sync.status || result.ufs_sync.state)) || 'Waiting for UFS sync',
            loaded_size: Number(file.size || 0),
            total_size: Number(file.size || 0),
            total_files: 1,
            completed_files: 0,
            done: false
          })
          this.trackUfsSync(remotePath)
        } else {
          this.removeUfsSyncJob(remotePath)
        }
        this.uploadNotice = this.$t('browse.upload.uploaded', { path: remotePath })
        await this.fetchData()
      } catch (error) {
        window.alert(this.$t('browse.upload.failed', { error }))
        this.setUfsSyncJob(remotePath, { state: 'Upload failed', status: 'Upload failed', message: String(error), done: true })
      }
    },
    async onFolderPicked(event) {
      const files = Array.from((event.target && event.target.files) || [])
      if (files.length === 0) return
      const folderName = files[0].webkitRelativePath ? files[0].webkitRelativePath.split('/')[0] : this.$t('browse.upload.folderUpload')
      await this.uploadFolderBatch(files.map((file) => ({ file, relativePath: file.webkitRelativePath || file.name })), folderName)
    },
    async uploadFolderBatch(fileItems, folderName) {
      const batchId = `folder-${Date.now()}`
      const totalFiles = fileItems.length
      const totalSize = fileItems.reduce((sum, item) => sum + Number(item.file && item.file.size ? item.file.size : 0), 0)
      const trackSync = this.isFsModePath(this.normalizePath(this.browsePath))
      const folderPath = this.normalizePath(this.joinPath(this.browsePath, folderName))
      const folderLabel = folderName || this.$t('browse.upload.folderUpload')
      this.uploadNotice = this.$t('browse.upload.uploadingFiles', { count: totalFiles })
      await this.ensureMounts()
      this.setUploadBatch(batchId, {
        label: folderLabel,
        state: 'Uploading to Curvine',
        totalFiles,
        uploadedFiles: 0,
        uploadedSize: 0,
        totalSize,
        progress: 0,
        done: false,
        message: ''
      })

      let uploadedFiles = 0
      let uploadedSize = 0
      let lastUiUpdateMs = 0
      try {
        for (const { file, relativePath } of fileItems) {
          const remotePath = this.normalizePath(this.joinPath(this.browsePath, relativePath))
          // Folder uploads use batch-level UFS sync tracking to avoid per-file polling storms.
          await uploadFile(remotePath, file, true, null, 300000, false)
          uploadedFiles += 1
          uploadedSize += Number(file.size || 0)
          const now = Date.now()
          if (now - lastUiUpdateMs >= 500 || uploadedFiles === totalFiles) {
            lastUiUpdateMs = now
            this.setUploadBatch(batchId, {
              state: uploadedFiles === totalFiles ? 'Uploaded to Curvine' : 'Uploading to Curvine',
              uploadedFiles,
              uploadedSize,
              progress: totalFiles > 0 ? Math.max(0, Math.min(100, Math.round(uploadedFiles / totalFiles * 100))) : 100,
              done: uploadedFiles === totalFiles
            })
            await this.nextUiFrame()
          }
        }

        this.uploadNotice = this.$t('browse.upload.uploadedFolder', { count: uploadedFiles })
        if (trackSync) {
          this.trackBatchUfsSync(batchId, folderPath, folderLabel, totalFiles, totalSize)
        }
        await this.fetchData()
      } catch (error) {
        this.setUploadBatch(batchId, {
          state: 'Upload failed',
          uploadedFiles,
          uploadedSize,
          progress: totalFiles > 0 ? Math.max(0, Math.min(100, Math.round(uploadedFiles / totalFiles * 100))) : 0,
          done: true,
          message: String(error)
        })
        window.alert(this.$t('browse.upload.folderFailed', { error }))
      }
    },

    downloadItem(item) {
      const path = item.path
      const base = (import.meta.env.VITE_API_BASE || '/api').replace(/\/+$/, '')
      const url = `${base}/v1/fs/download?path=${encodeURIComponent(path)}`
      const frame = document.createElement('iframe')
      frame.style.display = 'none'
      frame.src = url
      document.body.appendChild(frame)
      frame.onload = () => {
        if (frame.parentNode) frame.parentNode.removeChild(frame)
      }
      // Avoid aborting large downloads by removing the iframe too early.
      window.setTimeout(() => {
        if (frame.parentNode) frame.parentNode.removeChild(frame)
      }, 30 * 60 * 1000)
      this.downloadNoticeActive = true
      this.uploadNotice = item.is_dir
        ? this.$t('browse.download.folder', { path })
        : this.$t('browse.download.file', { path })
      window.setTimeout(() => {
        if (this.downloadNoticeActive) {
          this.downloadNoticeActive = false
          this.uploadNotice = ''
        }
      }, 8000)
    },
    async freeItem(item) {
      if (!this.canFreeItem(item)) {
        window.alert(this.freeItemTitle(item))
        return
      }
      const recursive = Boolean(item.is_dir)
      const message = recursive
        ? this.$t('browse.freeAction.confirmDir', { path: item.path })
        : this.$t('browse.freeAction.confirmFile', { path: item.path })
      if (!window.confirm(message)) return
      await this.freePath(item.path, recursive)
    },
    async freeCurrentPath() {
      const path = this.normalizePath(this.browsePath)
      if (!this.canFreeCurrentPath) {
        window.alert(this.currentFreeTitle)
        return
      }
      if (!window.confirm(this.$t('browse.freeAction.confirmCurrent', { path }))) return
      await this.freePath(path, true)
    },
    async freePath(path, recursive) {
      this.loading = true
      try {
        const data = await freePath(path, recursive)
        const bytes = data.bytes || 0
        const inodes = data.inodes || 0
        window.alert(this.$t('browse.freeAction.result', { inodes, space: this.formatBytes(bytes) }))
        await this.fetchData()
      } catch (error) {
        window.alert(this.$t('browse.freeAction.failed', { error }))
      } finally { this.loading = false }
    },
    setUploadBatch(batchId, patch) {
      const previous = this.uploadBatches[batchId] || {}
      this.uploadBatches = { ...this.uploadBatches, [batchId]: { ...previous, ...patch, id: batchId, updated_at: Date.now() } }
      this.saveUploadBatches()
    },
    clearCompletedUploadBatches() {
      const next = {}
      for (const [id, batch] of Object.entries(this.uploadBatches)) {
        if (!batch || batch.done) continue
        next[id] = batch
      }
      this.uploadBatches = next
      this.writeUploadBatches(next)
    },
    clearUploadBatches() {
      this.uploadBatches = {}
      this.writeUploadBatches({})
    },
    uploadBatchStaleMs() {
      return 10 * 60 * 1000
    },
    normalizeUploadBatch(batch, now = Date.now()) {
      if (!batch) return batch
      const updatedAt = Number(batch.updated_at || 0)
      if (batch.done || updatedAt <= 0 || now - updatedAt <= this.uploadBatchStaleMs()) return batch
      return {
        ...batch,
        state: 'Interrupted',
        done: true,
        message: syncStateMessage(this.$t, 'Interrupted')
      }
    },
    uploadBatchStorageKey() {
      return 'curvine.web.fs.uploadBatches.v1'
    },
    writeUploadBatches(batches) {
      try {
        window.localStorage.setItem(this.uploadBatchStorageKey(), JSON.stringify(batches))
      } catch (_) {}
    },
    readStoredUploadBatches() {
      try {
        const raw = window.localStorage.getItem(this.uploadBatchStorageKey())
        return raw ? (JSON.parse(raw) || {}) : {}
      } catch (_) {
        return {}
      }
    },
    saveUploadBatches() {
      try {
        const now = Date.now()
        const keepMs = 24 * 60 * 60 * 1000
        const stored = this.readStoredUploadBatches()
        const batches = { ...stored }
        for (const [id, batch] of Object.entries(this.uploadBatches)) {
          const normalized = this.normalizeUploadBatch(batch, now)
          const currentUpdatedAt = Number(normalized.updated_at || 0)
          const storedUpdatedAt = Number(batches[id]?.updated_at || 0)
          if (now - Number(normalized.updated_at || now) <= keepMs && currentUpdatedAt >= storedUpdatedAt) batches[id] = normalized
        }
        for (const [id, batch] of Object.entries(batches)) {
          const normalized = this.normalizeUploadBatch(batch, now)
          if (!normalized || now - Number(normalized.updated_at || now) > keepMs) delete batches[id]
          else batches[id] = normalized
        }
        this.writeUploadBatches(batches)
      } catch (_) {}
    },
    restoreUploadBatches() {
      try {
        const now = Date.now()
        const keepMs = 24 * 60 * 60 * 1000
        const restored = this.readStoredUploadBatches()
        const batches = {}
        for (const [id, batch] of Object.entries(restored)) {
          if (!batch || now - Number(batch.updated_at || now) > keepMs) continue
          batches[id] = this.normalizeUploadBatch(batch, now)
        }
        this.uploadBatches = batches
        this.writeUploadBatches(batches)
      } catch (_) {
        this.uploadBatches = {}
      }
    },
    startUploadBatchStorageRefresh() {
      if (this.uploadBatchStorageRefreshTimer) window.clearTimeout(this.uploadBatchStorageRefreshTimer)
      const refresh = () => {
        this.refreshUploadBatchesFromStorage()
        this.uploadBatchStorageRefreshTimer = window.setTimeout(refresh, 1000)
      }
      this.uploadBatchStorageRefreshTimer = window.setTimeout(refresh, 1000)
    },
    refreshUploadBatchesFromStorage() {
      const stored = this.readStoredUploadBatches()
      const next = { ...this.uploadBatches }
      let changed = false
      const now = Date.now()
      for (const [id, batch] of Object.entries(stored)) {
        if (!batch) continue
        const normalized = this.normalizeUploadBatch(batch, now)
        const currentUpdatedAt = Number(next[id]?.updated_at || 0)
        const storedUpdatedAt = Number(normalized.updated_at || 0)
        if (!next[id] || storedUpdatedAt > currentUpdatedAt || next[id].state !== normalized.state || next[id].done !== normalized.done) {
          next[id] = normalized
          changed = true
        }
      }
      if (!changed) return
      this.uploadBatches = next
      this.writeUploadBatches(next)
    },
    nextUiFrame() {
      return new Promise((resolve) => window.requestAnimationFrame(() => resolve()))
    },
    trackUfsSync(path, batchId = '', batchLabel = '') {
      if (batchId) return
      const normalizedPath = this.normalizePath(path)
      const previous = this.ufsSyncJobs[normalizedPath] || {}
      this.setUfsSyncJob(normalizedPath, {
        path: normalizedPath,
        batch_id: batchId,
        batch_label: batchLabel,
        state: 'Checking UFS sync',
        status: 'Checking UFS sync',
        progress: Number(previous.progress || 0),
        total_files: Number(previous.total_files || 0),
        completed_files: Number(previous.completed_files || 0),
        loaded_size: Number(previous.loaded_size || 0),
        total_size: Number(previous.total_size || 0),
        done: false,
        attempts: 0
      })
      this.pollUfsSync(normalizedPath)
    },
    trackBatchUfsSync(batchId, pathPrefix, batchLabel, totalFiles, totalSize) {
      const normalizedPath = this.normalizePath(pathPrefix)
      this.setUfsSyncJob(normalizedPath, {
        path: normalizedPath,
        batch_id: batchId,
        batch_label: batchLabel,
        batch_aggregate: true,
        state: 'Waiting for UFS sync',
        status: 'Waiting for UFS sync',
        progress: 0,
        total_files: totalFiles,
        completed_files: 0,
        loaded_size: 0,
        total_size: totalSize,
        done: false,
        attempts: 0
      })
      this.pollBatchUfsSync(batchId, normalizedPath)
    },
    ufsSyncPollIntervalMs() {
      const active = Object.keys(this.ufsSyncTimers).length
      if (active > 20) return 5000
      if (active > 5) return 3000
      return 2000
    },
    batchUfsSyncPollIntervalMs(jobCount) {
      if (jobCount > 200) return 8000
      if (jobCount > 50) return 5000
      return 3000
    },
    setUfsSyncJob(path, job) {
      const normalizedPath = this.normalizePath(path)
      const nextJob = { ...(this.ufsSyncJobs[normalizedPath] || {}), ...job, path: normalizedPath, updated_at: Date.now() }
      if (this.isUfsSyncDismissed(normalizedPath, nextJob)) return
      this.ufsSyncJobs = { ...this.ufsSyncJobs, [normalizedPath]: nextJob }
      this.saveUfsSyncJobs()
    },
    ufsSyncStorageKey() {
      return 'curvine.web.fs.ufsSyncJobs.v3'
    },
    saveUfsSyncJobs() {
      if (this.ufsSyncSaveTimer) window.clearTimeout(this.ufsSyncSaveTimer)
      this.ufsSyncSaveTimer = window.setTimeout(() => this.flushUfsSyncJobsToStorage(), 400)
    },
    flushUfsSyncJobsToStorage() {
      this.ufsSyncSaveTimer = null
      try {
        const now = Date.now()
        const keepMs = 30 * 60 * 1000
        const stored = this.readStoredUfsSyncJobs()
        const jobs = { ...stored }
        for (const [path, job] of Object.entries(this.ufsSyncJobs)) {
          const state = this.syncState(job)
          if (Boolean(job.done) || this.isTerminalSyncState(state)) {
            delete jobs[path]
            continue
          }
          const currentUpdatedAt = Number(job.updated_at || 0)
          const storedUpdatedAt = Number(jobs[path]?.updated_at || 0)
          if (now - Number(job.updated_at || now) <= keepMs && currentUpdatedAt >= storedUpdatedAt) jobs[path] = job
        }
        for (const [path, job] of Object.entries(jobs)) {
          const state = this.syncState(job)
          if (!job || Boolean(job.done) || this.isTerminalSyncState(state) || now - Number(job.updated_at || now) > keepMs || this.isUfsSyncDismissed(path, job)) delete jobs[path]
        }
        window.localStorage.setItem(this.ufsSyncStorageKey(), JSON.stringify(jobs))
      } catch (_) {}
    },
    collapseStoredUfsSyncJobs(restored) {
      const jobs = {}
      const batchGroups = {}
      for (const [path, job] of Object.entries(restored || {})) {
        if (!job) continue
        if (job.batch_aggregate) {
          jobs[path] = job
          continue
        }
        if (job.batch_id) {
          const group = batchGroups[job.batch_id] || { paths: [], job }
          group.paths.push(path)
          group.job = job
          batchGroups[job.batch_id] = group
          continue
        }
        jobs[path] = job
      }
      for (const group of Object.values(batchGroups)) {
        if (group.paths.length <= 1) {
          jobs[group.paths[0]] = group.job
          continue
        }
        const prefix = group.paths.sort((a, b) => a.length - b.length)[0]
        const parent = prefix.split('/').slice(0, -1).join('/') || prefix
        jobs[parent] = {
          ...group.job,
          path: parent,
          batch_aggregate: true,
          total_files: group.paths.length,
          completed_files: group.paths.filter((p) => {
            const state = this.syncState(restored[p] || {})
            return state === 'Completed'
          }).length
        }
      }
      return jobs
    },
    restoreUfsSyncJobs() {
      try {
        const raw = window.localStorage.getItem(this.ufsSyncStorageKey())
        if (!raw) return
        const now = Date.now()
        const keepMs = 24 * 60 * 60 * 1000
        const restored = JSON.parse(raw) || {}
        const collapsed = this.collapseStoredUfsSyncJobs(restored)
        const jobs = {}
        for (const [path, job] of Object.entries(collapsed)) {
          if (!job || now - Number(job.updated_at || now) > keepMs) continue
          if (this.isUfsSyncDismissed(path, job)) continue
          const state = this.syncState(job)
          if (Boolean(job.done) || this.isTerminalSyncState(state)) continue
          jobs[path] = job
        }
        this.ufsSyncJobs = jobs
        this.flushUfsSyncJobsToStorage()
        for (const [path, job] of Object.entries(jobs)) {
          const state = this.syncState(job)
          if (job.done || state === 'Queued') continue
          if (job.batch_aggregate && job.batch_id) {
            this.pollBatchUfsSync(job.batch_id, path)
          } else if (!job.batch_id) {
            this.pollUfsSync(path)
          }
        }
      } catch (_) {
        this.ufsSyncJobs = {}
      }
    },
    readStoredUfsSyncJobs() {
      try {
        const raw = window.localStorage.getItem(this.ufsSyncStorageKey())
        return raw ? (JSON.parse(raw) || {}) : {}
      } catch (_) {
        return {}
      }
    },
    startUfsSyncStorageRefresh() {
      if (this.ufsSyncStorageRefreshTimer) window.clearTimeout(this.ufsSyncStorageRefreshTimer)
      const refresh = () => {
        this.refreshUfsSyncJobsFromStorage()
        this.ufsSyncStorageRefreshTimer = window.setTimeout(refresh, 1000)
      }
      this.ufsSyncStorageRefreshTimer = window.setTimeout(refresh, 1000)
    },
    refreshUfsSyncJobsFromStorage() {
      const stored = this.readStoredUfsSyncJobs()
      const next = { ...this.ufsSyncJobs }
      let changed = false
      for (const [path, job] of Object.entries(stored)) {
        if (!job) continue
        if (this.isUfsSyncDismissed(path, job)) continue
        const state = this.syncState(job)
        if (Boolean(job.done) || this.isTerminalSyncState(state)) continue
        const currentUpdatedAt = Number(next[path]?.updated_at || 0)
        const storedUpdatedAt = Number(job.updated_at || 0)
        if (!next[path] || storedUpdatedAt > currentUpdatedAt) {
          next[path] = job
          changed = true
        }
      }
      if (!changed) return
      this.ufsSyncJobs = next
      for (const [path, job] of Object.entries(next)) {
        const state = this.syncState(job)
        if (job.done || state === 'Queued') continue
        if (job.batch_aggregate && job.batch_id) {
          if (!this.ufsSyncBatchTimers[job.batch_id]) this.pollBatchUfsSync(job.batch_id, path)
        } else if (!job.batch_id && !this.ufsSyncTimers[path] && Object.keys(this.ufsSyncTimers).length < 8) {
          this.pollUfsSync(path)
        }
      }
    },
    aggregateBatchUfsSyncJobs(serverJobs, previous, pathPrefix) {
      let completedFiles = 0
      let failedFiles = 0
      let syncingFiles = 0
      let waitingFiles = 0
      let loadedSize = 0
      let message = ''
      for (const job of serverJobs) {
        const state = this.syncState(job)
        const totalFiles = Math.max(1, Number(job.total_files || 1))
        const completed = state === 'Completed' ? totalFiles : Number(job.completed_files || 0)
        completedFiles += Math.max(0, Math.min(totalFiles, completed))
        loadedSize += Number(job.loaded_size || 0)
        if (['Failed', 'Canceled', 'Unknown', 'Timed out'].includes(state)) {
          failedFiles += 1
          if (!message && job.message) message = job.message
        } else if (['Pending', 'Loading'].includes(state)) {
          syncingFiles += 1
        } else if (!this.isTerminalSyncState(state)) {
          waitingFiles += 1
        }
      }
      const totalFiles = Math.max(Number(previous.total_files || 0), serverJobs.length)
      let activeState = 'Waiting for UFS sync'
      if (completedFiles >= totalFiles) activeState = 'Completed'
      else if (failedFiles > 0) activeState = 'Needs attention'
      else if (syncingFiles > 0) activeState = 'Syncing to UFS'
      else if (waitingFiles > 0) activeState = 'Waiting for UFS sync'
      const done = activeState === 'Completed' || (failedFiles > 0 && completedFiles + failedFiles >= totalFiles)
      return {
        state: activeState,
        status: activeState,
        total_files: totalFiles,
        completed_files: completedFiles,
        loaded_size: loadedSize,
        total_size: Number(previous.total_size || 0),
        done,
        message
      }
    },
    pollBatchUfsSync(batchId, pathPrefix) {
      if (this.ufsSyncBatchTimers[batchId]) {
        window.clearTimeout(this.ufsSyncBatchTimers[batchId])
        delete this.ufsSyncBatchTimers[batchId]
      }
      const tick = async () => {
        const previous = this.ufsSyncJobs[pathPrefix] || { attempts: 0 }
        const attempts = Number(previous.attempts || 0) + 1
        try {
          const data = await fetchUfsSyncJobs(pathPrefix, 500, true)
          const serverJobs = Array.isArray(data) ? data : (data.items || [])
          const aggregate = this.aggregateBatchUfsSyncJobs(serverJobs, previous, pathPrefix)
          this.setUfsSyncJob(pathPrefix, {
            ...previous,
            ...aggregate,
            path: pathPrefix,
            batch_id: batchId,
            batch_aggregate: true,
            attempts
          })
          if (!aggregate.done && attempts < 240) {
            this.ufsSyncBatchTimers[batchId] = window.setTimeout(
              tick,
              this.batchUfsSyncPollIntervalMs(serverJobs.length)
            )
          } else if (!aggregate.done) {
            this.setUfsSyncJob(pathPrefix, {
              state: 'Timed out',
              status: 'Timed out',
              message: 'UFS sync status did not finish after 12 minutes. Check Sync Jobs for details.',
              done: true
            })
            delete this.ufsSyncBatchTimers[batchId]
          } else {
            delete this.ufsSyncBatchTimers[batchId]
          }
        } catch (error) {
          const message = String(error)
          if (message.includes('Request URI Not Found') || message.includes('Expected JSON API response')) {
            this.setUfsSyncJob(pathPrefix, {
              state: 'Unsupported',
              status: 'Unsupported',
              message: 'The running web API does not expose /v1/fs/ufs-sync/jobs. Rebuild or restart the web service.',
              done: true
            })
            delete this.ufsSyncBatchTimers[batchId]
            return
          }
          if (attempts < 240) {
            this.ufsSyncBatchTimers[batchId] = window.setTimeout(tick, this.batchUfsSyncPollIntervalMs(0))
          } else {
            this.setUfsSyncJob(pathPrefix, {
              state: 'Timed out',
              status: 'Timed out',
              message: 'UFS sync status did not recover after repeated errors.',
              done: true
            })
            delete this.ufsSyncBatchTimers[batchId]
          }
        }
      }
      this.ufsSyncBatchTimers[batchId] = window.setTimeout(tick, 500)
    },
    async pollUfsSync(path) {
      if (this.ufsSyncTimers[path]) {
        window.clearTimeout(this.ufsSyncTimers[path])
        delete this.ufsSyncTimers[path]
      }
      const previous = this.ufsSyncJobs[path] || { attempts: 0 }
      try {
        const data = await fetchUfsSyncStatus(path)
        const attempts = (previous.attempts || 0) + 1
        const done = Boolean(data && data.done) || this.isTerminalSyncState(data && (data.state || data.status))
        this.setUfsSyncJob(path, { ...(data || {}), attempts, done })
        if (!done && attempts < 240) {
          this.ufsSyncTimers[path] = window.setTimeout(() => this.pollUfsSync(path), this.ufsSyncPollIntervalMs())
        } else if (!done) {
          this.setUfsSyncJob(path, { state: 'Timed out', status: 'Timed out', message: this.$t('browse.ufsSync.messages.timedOut'), done: true })
        }
      } catch (error) {
        const message = String(error)
        if (message.includes('Request URI Not Found') || message.includes('Expected JSON API response')) {
          this.setUfsSyncJob(path, { state: 'Unsupported', status: 'Unsupported', message: this.$t('browse.ufsSync.messages.unsupported'), done: true })
          return
        }
        const attempts = (previous.attempts || 0) + 1
        const keepState = previous.state || previous.status || 'Checking UFS sync'
        this.setUfsSyncJob(path, {
          ...previous,
          path,
          state: keepState,
          status: keepState,
          message: this.$t('browse.ufsSync.messages.statusCheckFailed', { attempt: attempts, message }),
          attempts,
          done: false
        })
        if (attempts < 240) {
          this.ufsSyncTimers[path] = window.setTimeout(() => this.pollUfsSync(path), this.ufsSyncPollIntervalMs())
        } else {
          this.setUfsSyncJob(path, {
            state: 'Timed out',
            status: 'Timed out',
            message: this.$t('browse.ufsSync.messages.timedOutRecovery'),
            done: true
          })
        }
      }
    },
    syncState(job) { return job.state || job.status || 'Unknown' },
    syncDisplayPath(job) {
      const path = this.normalizePath(job.path || job.source_path || '')
      const current = this.normalizePath(this.browsePath)
      if (!path) return '-'
      if (path === current) return path
      if (current !== '/' && path.startsWith(`${current}/`)) return path.slice(current.length + 1)
      return path
    },
    syncJobTitle(job) {
      const source = job.source_path || job.path || ''
      const target = job.target_path || job.ufs_path || ''
      const id = job.job_id || job.id || ''
      return [
        source && this.$t('browse.ufsSync.tooltip.cv', { path: source }),
        target && this.$t('browse.ufsSync.tooltip.ufs', { path: target }),
        id && this.$t('browse.ufsSync.tooltip.job', { id })
      ].filter(Boolean).join('\n')
    },
    syncJobSubtitle(job) {
      const displayName = buildJobDisplayName(job)
      const target = job.target_path || job.ufs_path || ''
      if (target && displayName) return `${displayName} · ${target}`
      if (displayName) return displayName
      if (target) return target
      return job.mount_path ? this.$t('browse.ufsSync.tooltip.mount', { path: job.mount_path }) : (job.source_path || job.path || '')
    },
    shortJobId(id) {
      return shortJobId(id)
    },
    isTerminalSyncState(state) {
      return ['Completed', 'Failed', 'Canceled', 'Unsupported', 'NotMounted', 'Unknown', 'Timed out', 'Upload failed'].includes(String(state || ''))
    },
    syncProgress(job) {
      const state = this.syncState(job)
      if (state === 'Completed') return 100
      if (['Queued', 'NotStarted', 'Waiting for UFS sync', 'Checking UFS sync'].includes(state)) return 0
      const value = Number(job.progress || 0)
      if (value > 0 || ['Pending', 'Loading'].includes(state)) return Math.max(0, Math.min(100, Math.round(value)))
      const total = Number(job.total_size || 0)
      if (total <= 0) return 0
      return Math.max(0, Math.min(100, Math.round(Number(job.loaded_size || 0) / total * 100)))
    },
    syncBytes(job) {
      const state = this.syncState(job)
      const total = Number(job.total_size || 0)
      const loaded = state === 'Completed' && total > 0 ? total : Number(job.loaded_size || 0)
      return `${this.formatBytes(loaded)} / ${this.formatBytes(total)}`
    },
    syncFiles(job) {
      const total = Number(job.total_files || 0)
      const state = this.syncState(job)
      if (!total) {
        if (state === 'NotStarted') return this.$t('browse.ufsSync.messages.waitingForJob')
        return this.$t('browse.ufsSync.messages.filesPending')
      }
      return this.$t('browse.ufsSync.messages.filesCount', { completed: Number(job.completed_files || 0), total })
    },
    syncStatusClass(job) {
      const state = typeof job === 'string' ? job : this.syncState(job)
      if (['Completed'].includes(state)) return 'completed'
      if (['Failed', 'Canceled', 'Unknown', 'Timed out', 'Needs attention', 'Upload failed', 'Interrupted'].includes(state)) return 'failed'
      if (['Unsupported', 'NotMounted', 'Finished with skipped files'].includes(state)) return 'warning'
      if (['Uploading to Curvine', 'Pending', 'Loading', 'Syncing to UFS'].includes(state)) return 'running'
      return 'waiting'
    },
    clearCompletedUfsSyncJobs() {
      const next = {}
      for (const [path, job] of Object.entries(this.ufsSyncJobs)) {
        if (!job) continue
        const state = this.syncState(job)
        const terminal = Boolean(job.done) || this.isTerminalSyncState(state)
        if (terminal) {
          this.markUfsSyncDismissed(path, job)
          if (this.ufsSyncTimers[path]) {
            window.clearTimeout(this.ufsSyncTimers[path])
            delete this.ufsSyncTimers[path]
          }
          if (job.batch_id && this.ufsSyncBatchTimers[job.batch_id]) {
            window.clearTimeout(this.ufsSyncBatchTimers[job.batch_id])
            delete this.ufsSyncBatchTimers[job.batch_id]
          }
          continue
        }
        next[path] = job
      }
      this.ufsSyncJobs = next
      try {
        window.localStorage.setItem(this.ufsSyncStorageKey(), JSON.stringify(next))
      } catch (_) {}
    },
    removeUfsSyncJob(path) {
      const normalizedPath = this.normalizePath(path)
      const next = { ...this.ufsSyncJobs }
      delete next[normalizedPath]
      this.ufsSyncJobs = next
      try {
        const stored = this.readStoredUfsSyncJobs()
        delete stored[normalizedPath]
        window.localStorage.setItem(this.ufsSyncStorageKey(), JSON.stringify(stored))
      } catch (_) {}
    },
    async copyPath(path) {
      try {
        await copyToClipboard(path)
        this.copiedPath = path
        window.setTimeout(() => { if (this.copiedPath === path) this.copiedPath = '' }, 1600)
      } catch (error) {
        window.alert(this.$t('browse.copy.failed', { error }))
      }
    },
    fileName(path) { const parts = String(path || '').split('/').filter(Boolean); return parts.pop() || '/' },
    joinPath(parent, name) { return `${String(parent || '/').replace(/\/$/, '')}/${name}`.replace(/^\/\//, '/') },
    async ensureMounts() {
      if (this.mountsLoaded) return
      try {
        const data = await fetchMountsData()
        this.mounts = Array.isArray(data) ? data : (data.items || [])
      } catch (_) {
        this.mounts = []
      } finally {
        this.mountsLoaded = true
      }
    },
    isPathInside(path, base) {
      const normalizedPath = this.normalizePath(path)
      const normalizedBase = this.normalizePath(base)
      return normalizedPath === normalizedBase || normalizedPath.startsWith(`${normalizedBase}/`)
    },
    writeTypeLabel(value) {
      if (value === 1 || value === 'FsMode') return 'fs_mode'
      if (value === 0 || value === 'CacheMode') return 'cache_mode'
      return String(value || 'fs_mode').toLowerCase()
    },
    isMountedPath(path) {
      return this.mounts.some((mount) => mount && mount.cv_path && this.isPathInside(path, mount.cv_path))
    },
    isFsModePath(path) {
      return this.mounts.some((mount) => mount && mount.cv_path && this.isPathInside(path, mount.cv_path) && this.writeTypeLabel(mount.write_type) === 'fs_mode')
    },
    isMountRoot(path) {
      const normalizedPath = this.normalizePath(path)
      return this.mounts.some((mount) => mount && mount.cv_path && normalizedPath === this.normalizePath(mount.cv_path))
    },
    deleteItemTitle(item) {
      if (item && this.isMountRoot(item.path)) return this.$t('browse.delete.mountRoot')
      if (item && this.isMountedPath(item.path)) return this.$t('browse.delete.mounted')
      return item && item.is_dir ? this.$t('browse.delete.directory') : this.$t('browse.delete.file')
    },
    canFreeItem(item) {
      return Boolean(item && this.isMountedPath(item.path))
    },
    freeItemTitle(item) {
      if (!item || !this.isMountedPath(item.path)) {
        return this.$t('browse.freeAction.onlyMount')
      }
      return item.is_dir ? this.$t('browse.freeAction.itemDir') : this.$t('browse.freeAction.itemFile')
    },
    normalizePath(path) {
      const value = String(path || '/').trim()
      if (!value || value === '/') return '/'
      return `/${value.replace(/^\/+|\/+$/g, '')}`.replace(/\/+/g, '/')
    },
    parentPath(path) {
      const parts = this.normalizePath(path).split('/').filter(Boolean)
      parts.pop()
      return parts.length ? `/${parts.join('/')}` : '/'
    },
    navigateTo(path) {
      const normalizedPath = this.normalizePath(path)
      this.browsePath = normalizedPath
      if (this.$route.path === '/browse' && (this.$route.query.path || '/') === normalizedPath) {
        this.fetchData()
        return
      }
      this.$router.push({ path: '/browse', query: { path: normalizedPath } })
    },
    navigate() { this.navigateTo(this.browsePath) },
    setRoot() { this.navigateTo('/') },
    goUp() { if (this.canGoUp) this.navigateTo(this.parentPath(this.browsePath)) },
    enterItem(item) { item.is_dir ? this.openDir(item.path) : this.openBlocks(item.path) },
    openDir(path) { this.navigateTo(path) },
    openBlocks(path) { this.$router.push({ path: '/blocks', query: { path } }) },
    goBlocks() { this.$router.push({ path: '/blocks', query: { path: this.browsePath } }) }
  }
}
</script>

<style scoped>
.cache-toggle {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  color: #44546a;
  font-size: 13px;
  white-space: nowrap;
}

.hidden-file-input {
  display: none;
}

.upload-notice {
  margin: 10px 0 0;
  color: #2f5d1f;
  font-size: 12px;
  font-weight: 700;
}

.sync-panel {
  display: grid;
  gap: 8px;
  margin: 10px 0 12px;
  padding: 10px 12px;
  border: 1px solid #dbe8d2;
  background: #f7fbf4;
  border-radius: 8px;
}

.sync-panel.ufs-sync-panel {
  display: flex;
  flex-direction: column;
  gap: 0;
}

.sync-panel.ufs-sync-panel.is-collapsed {
  margin-bottom: 8px;
}

.sync-panel-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  color: #24351f;
  font-size: 13px;
  font-weight: 800;
}

.sync-panel-header {
  display: flex;
  align-items: center;
  gap: 10px;
  min-width: 0;
  padding-bottom: 2px;
}

.sync-panel.ufs-sync-panel:not(.is-collapsed) .sync-panel-header {
  margin-bottom: 8px;
  padding-bottom: 8px;
  border-bottom: 1px solid #e3eedc;
}

.sync-collapse-btn {
  flex-shrink: 0;
  width: 32px;
  height: 32px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 0;
  margin: 0;
  border: 1px solid #b8d4ae;
  border-radius: 8px;
  background: #fff;
  color: #2f5d1f;
  cursor: pointer;
  outline: none;
  box-shadow: 0 1px 2px rgba(36, 53, 31, 0.08);
  transition: background 0.15s ease, border-color 0.15s ease, box-shadow 0.15s ease;
  -webkit-tap-highlight-color: transparent;
}

.sync-collapse-btn:hover {
  background: #eef8e8;
  border-color: #7fb070;
}

.sync-collapse-btn:active {
  background: #e3f2dc;
  box-shadow: inset 0 1px 2px rgba(36, 53, 31, 0.08);
}

.sync-collapse-btn:focus {
  outline: none;
}

.sync-collapse-btn:focus-visible {
  outline: 2px solid #6aae5c;
  outline-offset: 2px;
}

.sync-collapse-icon {
  width: 14px;
  height: 14px;
  display: block;
  transition: transform 0.2s ease;
  transform: rotate(-90deg);
}

.sync-collapse-icon.expanded {
  transform: rotate(0deg);
}

.sync-panel-heading-main {
  flex: 1;
  min-width: 0;
  display: flex;
  align-items: baseline;
  flex-wrap: wrap;
  gap: 8px 12px;
  cursor: pointer;
  border-radius: 6px;
  padding: 6px 8px;
  margin: -6px -8px;
  color: #24351f;
  outline: none;
  -webkit-tap-highlight-color: transparent;
}

.sync-panel-heading-main:hover {
  background: rgba(255, 255, 255, 0.72);
}

.sync-panel-heading-main:focus {
  outline: none;
}

.sync-panel-heading-main:focus-visible {
  box-shadow: 0 0 0 2px rgba(106, 174, 92, 0.45);
}

.sync-panel-toggle-label {
  font-size: 13px;
  font-weight: 800;
  line-height: 1.3;
}

.sync-panel-summary {
  color: #5b677a;
  font-size: 12px;
  font-weight: 600;
  line-height: 1.3;
}

.sync-panel-body {
  display: grid;
  gap: 8px;
  max-height: min(280px, 40vh);
  overflow-y: auto;
}

.sync-title-actions {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  flex-shrink: 0;
}

.sync-job {
  display: grid;
  grid-template-columns: minmax(120px, 1fr) 118px 126px 150px minmax(120px, .65fr);
  gap: 12px;
  align-items: center;
  color: #44546a;
  font-size: 12px;
}

.sync-target {
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.sync-target > strong,
.sync-target > small {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.sync-target > strong {
  color: #24351f;
}

.sync-target > small {
  color: #6b7687;
  font-size: 11px;
  font-weight: 500;
}

.batch-sync {
  color: #24351f;
  font-weight: 700;
}

.sync-status {
  justify-self: start;
  border-radius: 999px;
  padding: 3px 8px;
  background: #eef2f7;
  color: #44546a;
  font-weight: 700;
  white-space: nowrap;
}

.sync-status.completed {
  background: #e8f7ee;
  color: #16794a;
}

.sync-status.running {
  background: #e8f1ff;
  color: #2356a3;
}

.sync-status.waiting {
  background: #fff4df;
  color: #97610a;
}

.sync-status.warning {
  background: #f2e8ff;
  color: #6d3db5;
}

.sync-status.failed {
  background: #f8e8e8;
  color: #a13d3d;
}

.sync-progress-cell {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}

.sync-progress-cell > strong {
  flex: 0 0 38px;
  text-align: right;
  color: #24351f;
}

.sync-meter {
  flex: 1 1 auto;
  min-width: 72px;
  margin: 0;
}

.sync-message {
  grid-column: 1 / -1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: #7a4f09;
  font-weight: 500;
}

@media (max-width: 1100px) {
  .sync-job {
    grid-template-columns: minmax(0, 1fr) 118px;
  }

  .sync-progress-cell,
  .sync-message {
    grid-column: 1 / -1;
  }
}
</style>
