<template>
  <div class="admin-page" v-loading="loading">
    <section class="metric-grid">
      <MetricCard :label="$t('mounts.metrics.mountPoints')" :value="mounts.length" :meta="$t('mounts.metrics.mountPointsMeta')" :delta="$t('delta.current')" tone="good" />
      <MetricCard :label="$t('mounts.metrics.fsMode')" :value="fsModeCount" :meta="$t('mounts.metrics.fsModeMeta')" :delta="$t('delta.resync')" tone="neutral" />
      <MetricCard :label="$t('mounts.metrics.cacheMode')" :value="cacheModeCount" :meta="$t('mounts.metrics.cacheModeMeta')" :delta="$t('delta.cache')" tone="neutral" />
      <MetricCard :label="$t('mounts.metrics.providers')" :value="providerCount" :meta="$t('mounts.metrics.providersMeta')" :delta="$t('delta.auto')" tone="neutral" />
    </section>

    <section class="content-grid mounts-grid">
      <div class="admin-panel table-panel">
        <div class="panel-heading">
          <div>
            <h2>{{ $t('mounts.table.title') }}</h2>
            <p>{{ $t('mounts.table.subtitle') }}</p>
          </div>
          <button class="admin-button primary compact" @click="openAddMount">{{ $t('mounts.table.addMount') }}</button>
        </div>

        <div class="mode-tabs" role="tablist" :aria-label="$t('mounts.filters.ariaLabel')">
          <button :class="['mode-tab', { active: modeFilter === 'all' }]" @click="modeFilter = 'all'">{{ $t('mounts.filters.all') }} {{ mounts.length }}</button>
          <button :class="['mode-tab', { active: modeFilter === 'fs_mode' }]" @click="modeFilter = 'fs_mode'">{{ $t('mounts.filters.fsMode') }} {{ fsModeCount }}</button>
          <button :class="['mode-tab', { active: modeFilter === 'cache_mode' }]" @click="modeFilter = 'cache_mode'">{{ $t('mounts.filters.cacheMode') }} {{ cacheModeCount }}</button>
        </div>

        <div class="mount-table-scroll">
          <table class="admin-table mount-table">
          <thead>
            <tr><th>{{ $t('mounts.table.curvinePath') }}</th><th>{{ $t('mounts.table.ufsUri') }}</th><th>{{ $t('mounts.table.mode') }}</th><th>{{ $t('mounts.table.ttl') }}</th><th>{{ $t('mounts.table.provider') }}</th><th>{{ $t('mounts.table.resync') }}</th><th></th></tr>
          </thead>
          <tbody>
            <tr v-for="mount in filteredMounts" :key="mount.mount_id || mount.cv_path">
              <td><strong>{{ mount.cv_path }}</strong></td>
              <td class="ufs-cell">{{ mount.ufs_path }}</td>
              <td><span :class="['mode-pill', writeTypeLabel(mount.write_type)]">{{ writeTypeLabel(mount.write_type) }}</span></td>
              <td>{{ formatTtl(mount.ttl_ms) }}</td>
              <td>{{ providerLabel(mount.provider) || schemeOf(mount.ufs_path) || 'auto' }}</td>
              <td class="resync-cell">
                <button v-if="isFsMode(mount)" class="link-button" :disabled="isResyncRunning(mount)" @click="resyncMount(mount)">{{ $t('mounts.table.resync') }}</button>
                <span v-else class="muted-text">{{ $t('mounts.table.cacheModeLabel') }}</span>
                <span v-if="resyncTasks[mount.cv_path]" class="resync-progress" :title="resyncLabel(resyncTasks[mount.cv_path])">{{ resyncLabel(resyncTasks[mount.cv_path]) }}</span>
              </td>
              <td>
                <div class="row-actions mount-actions">
                  <button class="link-button" @click="editMount(mount)">{{ $t('common.edit') }}</button>
                  <button class="link-button" :disabled="rowValidating === mount.cv_path" @click="validateExisting(mount)">{{ $t('common.validate') }}</button>
                  <button class="link-button danger-link" @click="removeMount(mount)">{{ $t('common.delete') }}</button>
                </div>
              </td>
            </tr>
          </tbody>
          </table>
        </div>
        <div v-if="filteredMounts.length === 0" class="empty-state">{{ $t('mounts.table.empty') }}</div>
      </div>
    </section>

    <div v-if="showMountForm" class="admin-modal-backdrop">
      <div
        ref="mountModal"
        class="admin-panel form-panel mount-form admin-modal-panel"
        :style="modalStyle"
      >
        <div class="panel-heading modal-drag-handle" @mousedown="startDrag">
          <div>
            <h2>{{ form.update ? $t('mounts.form.updateTitle') : $t('mounts.form.addTitle') }}</h2>
            <p>{{ $t('mounts.form.subtitle') }}</p>
          </div>
          <button class="admin-button ghost compact" type="button" @click="cancelForm">{{ $t('common.close') }}</button>
        </div>

        <label>{{ $t('mounts.form.curvinePath') }}<input v-model="form.cv_path" :placeholder="$t('mounts.form.curvinePathPlaceholder')"></label>
        <label>{{ $t('mounts.form.ufsUri') }}<input v-model="form.ufs_path" :placeholder="$t('mounts.form.ufsUriPlaceholder')"></label>
        <div class="form-row">
          <label>{{ $t('mounts.form.writeType') }}
            <select
              v-model="form.write_type"
              :disabled="form.update"
              :title="form.update ? $t('mounts.form.writeTypeLocked') : ''"
            >
              <option value="fs_mode">{{ $t('mounts.form.fs_mode') }}</option>
              <option value="cache_mode">{{ $t('mounts.form.cache_mode') }}</option>
            </select>
          </label>
          <label>{{ $t('mounts.form.ttl') }}<input v-model="form.ttl" :placeholder="$t('mounts.form.ttlPlaceholder')"></label>
        </div>
        <div class="form-row">
          <label>{{ $t('mounts.form.provider') }}
            <select v-model="form.provider">
              <option value="auto">{{ $t('mounts.form.auto') }}</option>
              <option value="opendal">{{ $t('mounts.form.opendal') }}</option>
              <option value="oss-hdfs">{{ $t('mounts.form.ossHdfs') }}</option>
            </select>
          </label>
          <label>{{ $t('mounts.form.replicas') }}<input v-model.number="form.replicas" type="number" min="1" :placeholder="$t('mounts.form.replicasPlaceholder')"></label>
        </div>
        <label>{{ $t('mounts.form.blockSize') }}<input v-model="form.block_size" :placeholder="$t('mounts.form.blockSizePlaceholder')"></label>

        <section class="options-section" aria-labelledby="mount-options-heading">
          <div class="options-section-header">
            <h3 id="mount-options-heading">Cache &amp; consistency</h3>
            <p>Cache behavior and read consistency for this mount.</p>
          </div>
          <div class="options-grid">
            <label class="option-card">
              <span class="option-head">
                <input v-model="form.auto_cache" type="checkbox">
                <span class="option-title">Auto cache</span>
              </span>
              <span class="option-hint">Automatically submit load jobs on cache miss or invalid cache.</span>
            </label>

            <label class="option-field" :class="{ 'is-disabled': form.write_type === 'fs_mode' }">
              <span class="option-title">Access mode</span>
              <select
                v-model="form.access_mode"
                :disabled="form.write_type === 'fs_mode'"
                :title="form.write_type === 'fs_mode' ? 'Access mode applies to cache_mode mounts only' : ''"
              >
                <option value="read_only">read_only</option>
                <option value="read_write">read_write</option>
              </select>
              <span class="option-hint">Writable UFS passthrough; only for cache_mode.</span>
            </label>

            <label class="option-card option-card-wide">
              <span class="option-head">
                <input v-model="form.read_verify_ufs" type="checkbox">
                <span class="option-title">{{ $t('mounts.form.readVerifyUfs') }}</span>
              </span>
              <span class="option-hint">Mount consistency: verify cache against UFS mtime and length on read.</span>
            </label>
          </div>
        </section>

        <div class="properties-section">
          <div class="properties-header">
            <div class="properties-title-row">
              <strong>{{ $t('mounts.form.properties') }}</strong>
              <button class="admin-button ghost compact" type="button" @click="addPropertyRow">{{ $t('mounts.form.addProperty') }}</button>
            </div>
            <p class="field-hint">{{ propertiesHint }}</p>
          </div>
          <textarea
            v-model="propertiesText"
            class="properties-textarea"
            rows="5"
            :placeholder="propertiesTextPlaceholder"
            @input="clearValidateFeedback"
            @blur="syncRowsFromText"
          ></textarea>
          <div v-if="propertyRows.length > 0" class="properties-rows">
            <div v-for="(row, index) in propertyRows" :key="row.id" class="property-row">
              <input v-model="row.key" class="property-key" :placeholder="propertyKeyPlaceholder" @input="onPropertyRowInput" @blur="syncTextFromRows">
              <input
                v-model="row.value"
                class="property-value"
                :type="isSecretKey(row.key) ? 'password' : 'text'"
                :placeholder="$t('mounts.form.valuePlaceholder')"
                @input="onPropertyRowInput"
                @blur="syncTextFromRows"
              >
              <button class="link-button danger-link property-remove" type="button" @click="removePropertyRow(index)">{{ $t('mounts.form.remove') }}</button>
            </div>
          </div>
        </div>

        <div v-if="validateFeedback" :class="['validate-feedback', validateFeedback.type]" role="status">
          <strong>{{ validateFeedback.title }}</strong>
          <span>{{ validateFeedback.message }}</span>
        </div>

        <div class="form-actions">
          <button class="admin-button ghost" type="button" :disabled="formLoading" @click="validateForm">{{ $t('common.validate') }}</button>
          <button class="admin-button primary" type="button" :disabled="formLoading" @click="submitForm">{{ form.update ? $t('mounts.form.update') : $t('mounts.form.mount') }}</button>
          <button class="admin-button ghost" type="button" @click="cancelForm">{{ $t('mounts.form.cancel') }}</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import MetricCard from '@/components/admin/MetricCard.vue'
import { deleteMount, fetchMountResync, fetchMountsData, saveMount, startMountResync, validateMount } from '@/api/client'
import eventBus from '@/utils/eventBus'

const emptyForm = () => ({
  cv_path: '',
  ufs_path: '',
  update: false,
  write_type: 'fs_mode',
  ttl: '7d',
  read_verify_ufs: false,
  auto_cache: true,
  access_mode: 'read_only',
  replicas: null,
  block_size: '',
  storage_type: '',
  provider: 'auto',
  properties: {}
})

let propertyRowSeed = 0

const nextPropertyRowId = () => {
  propertyRowSeed += 1
  return `property-${propertyRowSeed}`
}

const SECRET_KEY_PATTERN = /(secret|password|token|credential|accesskeysecret|credentials\.secret)/i

export default {
  name: 'MountsPage',
  components: { MetricCard },
  data() {
    return {
      loading: false,
      formLoading: false,
      mounts: [],
      modeFilter: 'all',
      form: emptyForm(),
      propertyRows: [],
      propertiesText: '',
      showMountForm: false,
      validateFeedback: null,
      rowValidating: '',
      resyncTasks: {},
      resyncTimers: {},
      modalPosition: { x: 0, y: 0 },
      dragging: false,
      dragOffset: { x: 0, y: 0 }
    }
  },
  computed: {
    fsModeCount() { return this.mounts.filter((m) => this.isFsMode(m)).length },
    cacheModeCount() { return this.mounts.filter((m) => this.writeTypeLabel(m.write_type) === 'cache_mode').length },
    providerCount() { return new Set(this.mounts.map((m) => this.providerLabel(m.provider) || this.schemeOf(m.ufs_path)).filter(Boolean)).size },
    filteredMounts() {
      if (this.modeFilter === 'all') return this.mounts
      return this.mounts.filter((m) => this.writeTypeLabel(m.write_type) === this.modeFilter)
    },
    modalStyle() {
      if (!this.modalPosition.x && !this.modalPosition.y) return {}
      return { transform: `translate(${this.modalPosition.x}px, ${this.modalPosition.y}px)` }
    },
    propertiesHint() {
      const provider = this.form.provider
      const scheme = this.schemeOf(this.form.ufs_path)
      if (provider === 'oss-hdfs' || scheme === 'oss') {
        return this.$t('mounts.propertiesHints.ossHdfs')
      }
      if (scheme === 'hdfs') {
        return this.$t('mounts.propertiesHints.hdfs')
      }
      if (scheme === 'webhdfs' || scheme === 'http' || scheme === 'https') {
        return this.$t('mounts.propertiesHints.webhdfs')
      }
      if (scheme === 'file' || scheme === '') {
        return this.$t('mounts.propertiesHints.file')
      }
      return this.$t('mounts.propertiesHints.s3')
    },
    propertyKeyPlaceholder() {
      const provider = this.form.provider
      const scheme = this.schemeOf(this.form.ufs_path)
      if (provider === 'oss-hdfs' || scheme === 'oss') return 'oss.endpoint'
      if (scheme === 'hdfs') return 'hdfs.namenode'
      if (scheme === 'webhdfs' || scheme === 'http' || scheme === 'https') return 'webhdfs.endpoint'
      return 's3.endpoint_url'
    },
    propertiesTextPlaceholder() {
      const key = this.propertyKeyPlaceholder
      if (key === 'oss.endpoint') {
        return `${key}=https://oss-cn-hangzhou.aliyuncs.com\noss.accessKeyId=\noss.accessKeySecret=`
      }
      if (key === 'hdfs.namenode') {
        return `${key}=hdfs://namenode:8020\nhdfs.user=`
      }
      if (key === 'webhdfs.endpoint') {
        return `${key}=http://namenode:9870\nwebhdfs.root=/`
      }
      return `${key}=https://s3.example.com\ns3.credentials.access=\ns3.credentials.secret=\ns3.region_name=`
    }
  },
  created() { this.fetchData() },
  mounted() {
    eventBus.on('admin-refresh', this.fetchData)
    window.addEventListener('mousemove', this.onDrag)
    window.addEventListener('mouseup', this.stopDrag)
  },
  beforeUnmount() {
    eventBus.off('admin-refresh', this.fetchData)
    window.removeEventListener('mousemove', this.onDrag)
    window.removeEventListener('mouseup', this.stopDrag)
    Object.values(this.resyncTimers).forEach((timer) => window.clearTimeout(timer))
  },
  methods: {
    async fetchData() {
      this.loading = true
      try {
        const data = await fetchMountsData()
        this.mounts = Array.isArray(data) ? data : (data.items || [])
      } finally { this.loading = false }
    },
    propertiesFromRows() {
      const props = {}
      for (const row of this.propertyRows) {
        const key = String(row.key || '').trim()
        if (!key) continue
        props[key] = String(row.value ?? '').trim()
      }
      return props
    },
    textFromProperties(properties) {
      return Object.entries(properties || {}).map(([key, value]) => `${key}=${value}`).join('\n')
    },
    parsePropertiesText(text) {
      const props = {}
      for (const line of String(text || '').split('\n')) {
        const value = line.trim()
        if (!value || value.startsWith('#')) continue
        const index = value.indexOf('=')
        if (index <= 0) throw new Error(this.$t('mounts.validation.invalidProperty', { line: value }))
        props[value.slice(0, index).trim()] = value.slice(index + 1).trim()
      }
      return props
    },
    buildProperties() {
      const props = this.parsePropertiesText(this.propertiesText)
      return { ...props, ...this.propertiesFromRows() }
    },
    syncRowsFromText() {
      const text = String(this.propertiesText || '').trim()
      if (!text) {
        if (this.propertyRows.length === 0) return
        this.propertyRows = []
        return
      }
      try {
        this.propertyRows = this.rowsFromProperties(this.parsePropertiesText(text))
      } catch {
        // Keep free-form text while the user is still editing invalid lines.
      }
    },
    syncTextFromRows() {
      if (this.propertyRows.length === 0) return
      this.propertiesText = this.textFromProperties(this.propertiesFromRows())
    },
    defaultPropertyKeys() {
      const provider = this.form.provider
      const scheme = this.schemeOf(this.form.ufs_path)
      if (provider === 'oss-hdfs' || scheme === 'oss') {
        return ['oss.endpoint', 'oss.accessKeyId', 'oss.accessKeySecret', 'oss.region']
      }
      if (scheme === 'hdfs') {
        return ['hdfs.namenode', 'hdfs.user', 'hdfs.root']
      }
      if (scheme === 'webhdfs' || scheme === 'http' || scheme === 'https') {
        return ['webhdfs.endpoint', 'webhdfs.root']
      }
      return ['s3.endpoint_url', 's3.credentials.access', 's3.credentials.secret', 's3.region_name']
    },
    usedPropertyKeys() {
      const used = new Set()
      try {
        Object.keys(this.parsePropertiesText(this.propertiesText)).forEach((key) => used.add(key))
      } catch {
        // Ignore invalid lines while the user is still editing free-form text.
      }
      this.propertyRows.forEach((row) => {
        const key = String(row.key || '').trim()
        if (key) used.add(key)
      })
      return used
    },
    nextPropertyKey() {
      const used = this.usedPropertyKeys()
      const next = this.defaultPropertyKeys().find((key) => !used.has(key))
      return next || this.propertyKeyPlaceholder
    },
    rowsFromProperties(properties) {
      return Object.entries(properties || {}).map(([key, value]) => ({
        id: nextPropertyRowId(),
        key,
        value: String(value ?? '')
      }))
    },
    addPropertyRow() {
      if (this.propertyRows.length === 0 && String(this.propertiesText || '').trim()) {
        this.syncRowsFromText()
      }
      this.propertyRows.push({
        id: nextPropertyRowId(),
        key: this.nextPropertyKey(),
        value: ''
      })
      this.clearValidateFeedback()
    },
    removePropertyRow(index) {
      this.propertyRows.splice(index, 1)
      this.syncTextFromRows()
      this.clearValidateFeedback()
    },
    onPropertyRowInput() {
      this.clearValidateFeedback()
    },
    isSecretKey(key) {
      return SECRET_KEY_PATTERN.test(String(key || ''))
    },
    clearValidateFeedback() {
      this.validateFeedback = null
    },
    ensureFormValid() {
      if (!String(this.form.cv_path || '').trim()) throw new Error(this.$t('mounts.validation.curvinePathRequired'))
      if (!String(this.form.ufs_path || '').trim()) throw new Error(this.$t('mounts.validation.ufsUriRequired'))
    },
    payload() {
      this.ensureFormValid()
      return {
        ...this.form,
        update: Boolean(this.form.update),
        cv_path: String(this.form.cv_path || '').trim(),
        ufs_path: this.normalizeUfsPath(this.form.ufs_path),
        replicas: this.form.replicas || null,
        block_size: this.form.block_size || null,
        storage_type: this.form.storage_type || null,
        provider: this.form.provider === 'auto' ? null : this.form.provider,
        properties: this.buildProperties()
      }
    },
    validateSuccessMessage(data) {
      const ufsPath = data?.ufs_path || this.normalizeUfsPath(this.form.ufs_path)
      if (data?.is_dir) {
        const count = Number(data?.entries || 0)
        return this.$t('mounts.validation.reachableDir', { path: ufsPath, count })
      }
      return this.$t('mounts.validation.reachableFile', { path: ufsPath })
    },
    validateErrorMessage(error) {
      const message = String(error?.message || error || this.$t('common.unknown'))
      return message.replace(/^Error:\s*/i, '')
    },
    async submitForm() {
      this.formLoading = true
      try {
        await saveMount(this.payload())
        window.alert(this.form.update ? this.$t('mounts.alerts.updated') : this.$t('mounts.alerts.created'))
        this.closeForm()
        await this.fetchData()
      } catch (error) {
        window.alert(this.$t('mounts.alerts.mountFailed', { error }))
      } finally { this.formLoading = false }
    },
    async validateForm() {
      this.formLoading = true
      this.validateFeedback = null
      try {
        this.ensureFormValid()
        const data = await validateMount({
          ufs_path: this.normalizeUfsPath(this.form.ufs_path),
          provider: this.form.provider || null,
          properties: this.buildProperties()
        })
        this.validateFeedback = {
          type: 'success',
          title: this.$t('mounts.validation.succeeded'),
          message: this.validateSuccessMessage(data)
        }
      } catch (error) {
        this.validateFeedback = {
          type: 'error',
          title: this.$t('mounts.validation.failed'),
          message: this.validateErrorMessage(error)
        }
      } finally { this.formLoading = false }
    },
    async validateExisting(mount) {
      this.rowValidating = mount.cv_path
      try {
        const data = await validateMount({
          ufs_path: mount.ufs_path,
          provider: mount.provider || null,
          properties: mount.properties || {}
        })
        const detail = data?.is_dir
          ? this.$t('mounts.validation.existingDir', { count: Number(data?.entries || 0) })
          : this.$t('mounts.validation.existingFile')
        window.alert(this.$t('mounts.validation.existingSuccess', { path: mount.ufs_path, detail }))
      } catch (error) {
        window.alert(this.$t('mounts.validation.existingFailed', { message: this.validateErrorMessage(error) }))
      } finally { this.rowValidating = '' }
    },
    async resyncMount(mount) {
      try {
        const task = await startMountResync(mount.cv_path)
        this.setResyncTask(mount.cv_path, task)
        this.pollResync(mount.cv_path, task.id)
      } catch (error) {
        window.alert(this.$t('mounts.alerts.resyncFailed', { error }))
      }
    },
    async pollResync(cvPath, taskId) {
      try {
        const task = await fetchMountResync(taskId)
        this.setResyncTask(cvPath, task)
        if (!task.done) {
          this.resyncTimers[cvPath] = window.setTimeout(() => this.pollResync(cvPath, taskId), 1000)
        } else {
          await this.fetchData()
        }
      } catch (error) {
        this.setResyncTask(cvPath, { id: taskId, status: 'failed', done: true, message: String(error), scanned: 0, recreated: 0, skipped: 0, failed: 1, pending_dirs: 0 })
      }
    },
    setResyncTask(cvPath, task) {
      this.resyncTasks = { ...this.resyncTasks, [cvPath]: task }
    },
    isResyncRunning(mount) {
      const task = this.resyncTasks[mount.cv_path]
      return Boolean(task && !task.done)
    },
    resyncLabel(task) {
      const status = task.status || 'running'
      return this.$t('mounts.resync.label', {
        status,
        scanned: task.scanned || 0,
        recreated: task.recreated || 0,
        skipped: task.skipped || 0,
        failed: task.failed || 0,
        pending: task.pending_dirs || 0
      })
    },
    async removeMount(mount) {
      if (!window.confirm(this.$t('mounts.alerts.deleteConfirm', { path: mount.cv_path }))) return
      this.loading = true
      try {
        await deleteMount(mount.cv_path)
        window.alert(this.$t('mounts.alerts.deleted', { path: mount.cv_path }))
        await this.fetchData()
      } catch (error) {
        window.alert(this.$t('mounts.alerts.deleteFailed', { error }))
      } finally { this.loading = false }
    },
    openAddMount() {
      this.resetForm()
      this.modalPosition = { x: 0, y: 0 }
      this.showMountForm = true
    },
    editMount(mount) {
      this.form = {
        cv_path: mount.cv_path,
        ufs_path: mount.ufs_path,
        update: true,
        write_type: this.writeTypeLabel(mount.write_type),
        ttl: this.formatTtl(mount.ttl_ms),
        read_verify_ufs: Boolean(mount.read_verify_ufs),
        auto_cache: mount.auto_cache !== false,
        access_mode: this.accessModeLabel(mount.access_mode),
        replicas: mount.replicas || null,
        block_size: mount.block_size ? String(mount.block_size) : '',
        storage_type: mount.storage_type || '',
        provider: this.providerLabel(mount.provider),
        properties: mount.properties || {}
      }
      this.propertyRows = this.rowsFromProperties(mount.properties || {})
      this.propertiesText = this.textFromProperties(mount.properties || {})
      this.validateFeedback = null
      this.modalPosition = { x: 0, y: 0 }
      this.showMountForm = true
    },
    resetForm() {
      this.form = emptyForm()
      this.propertyRows = []
      this.propertiesText = ''
      this.validateFeedback = null
    },
    closeForm() {
      this.showMountForm = false
      this.resetForm()
      this.modalPosition = { x: 0, y: 0 }
      this.stopDrag()
    },
    cancelForm() { this.closeForm() },
    startDrag(event) {
      if (event.button !== 0) return
      if (event.target.closest('button, input, select, textarea, a, label')) return
      const modal = this.$refs.mountModal
      if (!modal) return
      const rect = modal.getBoundingClientRect()
      this.dragging = true
      this.dragOffset = {
        x: event.clientX - rect.left,
        y: event.clientY - rect.top
      }
      event.preventDefault()
    },
    onDrag(event) {
      if (!this.dragging) return
      const modal = this.$refs.mountModal
      if (!modal) return
      const rect = modal.getBoundingClientRect()
      const width = rect.width
      const height = rect.height
      const padding = 16
      const minX = padding - ((window.innerWidth - width) / 2)
      const maxX = (window.innerWidth - width) / 2 - padding
      const minY = padding - ((window.innerHeight - height) / 2)
      const maxY = (window.innerHeight - height) / 2 - padding
      const nextX = event.clientX - this.dragOffset.x - ((window.innerWidth - width) / 2)
      const nextY = event.clientY - this.dragOffset.y - ((window.innerHeight - height) / 2)
      this.modalPosition = {
        x: Math.min(maxX, Math.max(minX, nextX)),
        y: Math.min(maxY, Math.max(minY, nextY))
      }
    },
    stopDrag() {
      this.dragging = false
    },
    normalizeUfsPath(path) {
      const value = String(path || '').trim()
      return value.startsWith('/') ? `file://${value}` : value
    },
    isFsMode(mount) { return this.writeTypeLabel(mount.write_type) === 'fs_mode' },
    writeTypeLabel(value) {
      if (value === 1 || value === 'FsMode') return 'fs_mode'
      if (value === 0 || value === 'CacheMode') return 'cache_mode'
      return String(value || 'fs_mode').toLowerCase()
    },
    accessModeLabel(value) {
      if (value === 0 || value === 'ReadOnly' || value === 'read_only') return 'read_only'
      if (value === 1 || value === 'ReadWrite' || value === 'read_write') return 'read_write'
      return String(value || 'read_only').toLowerCase()
    },
    providerLabel(value) {
      if (!value) return 'auto'
      if (value === 'OssHdfs') return 'oss-hdfs'
      if (value === 'Opendal') return 'opendal'
      if (value === 'Auto') return 'auto'
      return String(value).toLowerCase()
    },
    schemeOf(path) { return String(path || '').split(':')[0] },
    formatTtl(ms) {
      const value = Number(ms || 0)
      if (!value) return '0ms'
      if (value % 86400000 === 0) return `${value / 86400000}d`
      if (value % 3600000 === 0) return `${value / 3600000}h`
      if (value % 60000 === 0) return `${value / 60000}m`
      if (value % 1000 === 0) return `${value / 1000}s`
      return `${value}ms`
    }
  }
}
</script>

<style scoped>
.mounts-grid {
  grid-template-columns: 1fr;
  align-items: start;
}

.mount-table-scroll {
  width: 100%;
  overflow: visible;
}

.mount-table {
  min-width: 0;
  table-layout: fixed;
}

.mount-table th,
.mount-table td {
  padding-left: clamp(4px, .55vw, 8px);
  padding-right: clamp(4px, .55vw, 8px);
  font-size: clamp(11px, .78vw, 13px);
}

.mount-table th:nth-child(1) { width: 15%; }
.mount-table th:nth-child(2) { width: 24%; }
.mount-table th:nth-child(3) { width: 14%; }
.mount-table th:nth-child(4) { width: 7%; }
.mount-table th:nth-child(5) { width: 8%; }
.mount-table th:nth-child(6) { width: 14%; }
.mount-table th:nth-child(7) { width: 18%; }

.mode-tabs {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 16px;
}

.mode-tab {
  border: 1px solid #cfe0c7;
  background: #fff;
  color: #243525;
  border-radius: 8px;
  padding: 8px clamp(8px, .75vw, 12px);
  font-size: clamp(12px, .9vw, 14px);
  font-weight: 700;
  cursor: pointer;
}

.mode-tab.active {
  background: #5ca63a;
  border-color: #5ca63a;
  color: #fff;
}

.mount-table td strong,
.mount-table td span,
.ufs-cell {
  white-space: normal;
  overflow-wrap: anywhere;
}

.ufs-cell {
  max-width: none;
  word-break: break-word;
}

.mode-pill {
  display: inline-flex;
  align-items: center;
  max-width: 100%;
  min-height: 22px;
  border-radius: 6px;
  padding: 2px 6px;
  font-size: clamp(10px, .72vw, 12px);
  font-weight: 800;
  background: #eef6ea;
  color: #2c6c1f;
  overflow-wrap: anywhere;
}

.mode-pill.cache_mode {
  background: #eef3f8;
  color: #315f7f;
}

.resync-cell {
  min-width: 0;
  max-width: none;
  white-space: normal;
}

.resync-progress {
  display: block;
  color: #51604c;
  font-size: clamp(10px, .72vw, 12px);
  line-height: 1.35;
  margin-top: 4px;
  max-width: 100%;
  white-space: normal;
  overflow: visible;
  text-overflow: clip;
  word-break: break-word;
  cursor: help;
}

.muted-text {
  color: #88967f;
  font-size: 12px;
}

.link-button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.mount-form,
.mount-form label,
.mount-form input,
.mount-form select,
.mount-form textarea {
  min-width: 0;
}

.mount-form label {
  display: grid;
  grid-template-columns: 1fr;
  gap: 6px;
}

.mount-form input:not([type="checkbox"]),
.mount-form select,
.mount-form textarea {
  width: 100%;
  max-width: 100%;
  border: 1px solid #d9e4d2;
  border-radius: 8px;
  padding: 9px 10px;
  font: inherit;
  background: #fff;
}

.form-row {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}

.options-section {
  margin-top: 4px;
  padding: 14px;
  border: 1px solid #dce8d6;
  border-radius: 10px;
  background: #f8fbf6;
}

.options-section-header {
  margin-bottom: 12px;
}

.options-section-header h3 {
  margin: 0;
  font-size: 14px;
  font-weight: 800;
  color: #243525;
}

.options-section-header p {
  margin: 4px 0 0;
  font-size: 12px;
  color: #6a7a64;
  line-height: 1.4;
}

.options-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}

.option-card,
.option-field {
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-width: 0;
  margin: 0;
  padding: 12px;
  border: 1px solid #d9e4d2;
  border-radius: 8px;
  background: #fff;
}

.option-card-wide {
  grid-column: 1 / -1;
}

.option-head {
  display: flex;
  align-items: center;
  gap: 8px;
}

.option-title {
  font-size: 13px;
  font-weight: 700;
  color: #243525;
  line-height: 1.3;
}

.option-hint {
  font-size: 12px;
  color: #6a7a64;
  line-height: 1.45;
}

.option-field select {
  width: 100%;
}

.option-field.is-disabled {
  opacity: 0.72;
  background: #f3f6f1;
}

.option-card input[type="checkbox"],
.option-head input[type="checkbox"] {
  flex: 0 0 auto;
  width: 16px;
  height: 16px;
  margin: 0;
  accent-color: var(--admin-primary);
}

.properties-section {
  display: grid;
  gap: 10px;
  padding: 12px;
  border: 1px solid #d9e4d2;
  border-radius: 10px;
  background: #fbfdf8;
}

.properties-header {
  display: grid;
  gap: 6px;
}

.properties-title-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  min-width: 0;
}

.properties-title-row strong {
  font-size: 14px;
  line-height: 1.2;
}

.properties-title-row .admin-button {
  flex: 0 0 auto;
  white-space: nowrap;
}

.field-hint {
  margin: 4px 0 0;
  color: #6a7865;
  font-size: 12px;
  line-height: 1.4;
}

.properties-textarea {
  width: 100%;
  min-height: 110px;
  border: 1px solid #d9e4d2;
  border-radius: 8px;
  padding: 9px 10px;
  font: inherit;
  line-height: 1.45;
  background: #fff;
  resize: vertical;
}

.properties-rows {
  display: grid;
  gap: 8px;
  padding-top: 4px;
  border-top: 1px dashed #dbe6d4;
}

.property-row {
  display: grid;
  grid-template-columns: minmax(120px, .9fr) minmax(160px, 1.4fr) auto;
  gap: 8px;
  align-items: center;
}

.property-key,
.property-value {
  width: 100%;
  border: 1px solid #d9e4d2;
  border-radius: 8px;
  padding: 9px 10px;
  font: inherit;
  background: #fff;
}

.property-remove {
  justify-self: end;
  white-space: nowrap;
}

.validate-feedback {
  display: grid;
  gap: 4px;
  padding: 10px 12px;
  border-radius: 8px;
  font-size: 13px;
  line-height: 1.45;
}

.validate-feedback strong {
  font-size: 14px;
}

.validate-feedback.success {
  border: 1px solid #b8d7a4;
  background: #eef8e8;
  color: #2f5f22;
}

.validate-feedback.error {
  border: 1px solid #efb2a8;
  background: #fff1ee;
  color: #8f3428;
}

.form-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.mount-table .mount-actions {
  align-items: center;
  gap: 3px 6px;
}

.mount-table .link-button {
  height: 26px;
  padding: 0 2px;
  font-size: clamp(10px, .72vw, 12px);
}

@media (max-width: 1180px) {
  .mounts-grid {
    grid-template-columns: minmax(0, 1.35fr) minmax(280px, .65fr);
    gap: 12px;
  }

  .admin-panel {
    padding: 14px;
  }

  .form-row { grid-template-columns: 1fr; }
  .property-row { grid-template-columns: 1fr; }
  .options-grid { grid-template-columns: 1fr; }
  .option-card-wide { grid-column: auto; }
}

@media (max-width: 860px) {
  .mounts-grid {
    grid-template-columns: 1fr;
  }
}


.admin-modal-backdrop {
  position: fixed;
  inset: 0;
  z-index: 1000;
  display: grid;
  place-items: center;
  padding: 24px;
  background: rgba(18, 32, 20, .28);
}

.admin-modal-panel {
  width: min(720px, calc(100vw - 48px));
  max-height: calc(100vh - 48px);
  overflow: auto;
  box-shadow: 0 24px 70px rgba(31, 48, 26, .24);
}

.modal-drag-handle {
  cursor: move;
  user-select: none;
}

.modal-drag-handle button {
  cursor: pointer;
}

</style>
