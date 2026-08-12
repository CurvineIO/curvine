<template>
  <div class="preview-shell">
    <aside class="preview-sidebar">
      <div class="brand-block">
        <img src="@/assets/logo.svg" alt="Curvine" class="brand-mark">
        <div>
          <div class="brand-name">Curvine</div>
          <div class="brand-subtitle">Admin Console</div>
        </div>
      </div>
      <nav class="side-nav" aria-label="Preview navigation">
        <button v-for="item in navItems" :key="item.key" class="side-link" :class="{ active: activeNav === item.key }" type="button" @click="activeNav = item.key">
          <span class="side-icon">{{ item.icon }}</span><span>{{ item.label }}</span>
        </button>
      </nav>
      <div class="side-status"><span class="status-dot"></span><div><strong>Master Active</strong><span>Last refresh 12s ago</span></div></div>
    </aside>

    <main class="preview-main">
      <header class="topbar">
        <div><h1>{{ activeTitle }}</h1><p>{{ activeSubtitle }}</p></div>
        <div class="top-actions">
          <div class="search-box"><span>Search</span><input v-model="query" type="search" :placeholder="searchPlaceholder"></div>
          <button class="ghost-button" type="button">Refresh</button>
          <button class="primary-button" type="button">{{ primaryAction }}</button>
        </div>
      </header>

      <section class="metric-grid">
        <article v-for="metric in activeMetrics" :key="metric.label" class="metric-card">
          <span>{{ metric.label }}</span><strong>{{ metric.value }}</strong>
          <div class="metric-meta"><span>{{ metric.meta }}</span><em :class="metric.tone">{{ metric.delta }}</em></div>
        </article>
      </section>

      <template v-if="activeNav === 'overview'">
        <section class="content-grid">
          <div class="panel capacity-panel">
            <div class="panel-heading"><div><h2>Cluster Capacity</h2><p>Storage, cache and file metadata</p></div><button class="icon-button" type="button" aria-label="More actions">...</button></div>
            <div class="capacity-layout">
              <div class="donut" :style="capacityStyle"><div><strong>72%</strong><span>Used</span></div></div>
              <div class="capacity-list">
                <div v-for="item in capacity" :key="item.label" class="capacity-row"><div><span>{{ item.label }}</span><strong>{{ item.value }}</strong></div><div class="bar"><span :style="{ width: item.percent + '%' }"></span></div></div>
              </div>
            </div>
          </div>
          <div class="panel">
            <div class="panel-heading"><div><h2>Recent Sync Jobs</h2><p>Load job progress and failures</p></div><button class="ghost-button compact" type="button">Submit</button></div>
            <div class="job-list"><article v-for="job in jobs" :key="job.id" class="job-item"><div class="job-topline"><strong>{{ job.path }}</strong><span :class="['pill', job.status.toLowerCase()]">{{ job.status }}</span></div><div class="job-progress"><span :style="{ width: job.progress + '%' }"></span></div><div class="job-meta"><span>{{ job.id }}</span><span>{{ job.progress }}%</span></div></article></div>
          </div>
        </section>
        <section class="lower-grid">
          <div class="panel"><div class="panel-heading"><div><h2>Workers</h2><p>Live state and capacity pressure</p></div></div><PreviewWorkers :workers="workers" /></div>
          <div class="panel"><div class="panel-heading"><div><h2>Mounts</h2><p>UFS mappings and validation</p></div></div><PreviewMounts :mounts="mounts" /></div>
        </section>
      </template>

      <section v-else-if="activeNav === 'filesystem'" class="panel table-panel">
        <div class="panel-heading table-heading"><div><h2>File Browser</h2><p>Current path /warehouse/orders</p></div><div class="segmented"><button class="active" type="button">List</button><button type="button">Blocks</button><button type="button">Stat</button></div></div>
        <div class="path-toolbar"><button class="ghost-button compact" type="button">Mkdir</button><button class="ghost-button compact" type="button">Upload</button><button class="ghost-button compact danger" type="button">Delete</button><span>/warehouse/orders</span></div>
        <PreviewFiles :files="files" />
      </section>

      <section v-else-if="activeNav === 'workers'" class="panel table-panel">
        <div class="panel-heading table-heading"><div><h2>Worker Nodes</h2><p>Heartbeat, blocks and decommission workflow</p></div><div class="segmented"><button class="active" type="button">All</button><button type="button">Live</button><button type="button">Lost</button></div></div>
        <PreviewWorkers :workers="workers" />
      </section>

      <section v-else-if="activeNav === 'mounts'" class="content-grid">
        <div class="panel table-panel"><div class="panel-heading"><div><h2>Mount Table</h2><p>UFS mappings and health state</p></div><button class="primary-button compact" type="button">Add Mount</button></div><PreviewMounts :mounts="mounts" /></div>
        <div class="panel form-panel"><div class="panel-heading"><div><h2>Mount Form</h2><p>Preview of create/update dialog fields</p></div></div><label>Mount Path<input value="/lake/raw"></label><label>UFS URI<input value="s3://prod-data-lake/raw"></label><label>Options<input value="readonly=false, cache=true"></label></div>
      </section>

      <section v-else-if="activeNav === 'jobs'" class="content-grid">
        <div class="panel"><div class="panel-heading"><div><h2>Load Jobs</h2><p>Status polling and cancellation</p></div><button class="primary-button compact" type="button">Submit Load</button></div><div class="job-list"><article v-for="job in jobs" :key="job.id" class="job-item"><div class="job-topline"><strong>{{ job.path }}</strong><span :class="['pill', job.status.toLowerCase()]">{{ job.status }}</span></div><div class="job-progress"><span :style="{ width: job.progress + '%' }"></span></div><div class="job-meta"><span>{{ job.id }}</span><span>{{ job.progress }}%</span></div></article></div></div>
        <div class="panel form-panel"><div class="panel-heading"><div><h2>Submit Load</h2><p>Source path, target policy and priority</p></div></div><label>Path<input value="/warehouse/orders/dt=2026-05-08"></label><label>Replicas<input value="3"></label><label>Priority<input value="Normal"></label></div>
      </section>

      <section v-else class="panel table-panel">
        <div class="panel-heading table-heading"><div><h2>Configuration</h2><p>Readonly cluster settings with search and grouping</p></div><div class="segmented"><button class="active" type="button">Master</button><button type="button">Worker</button><button type="button">Web</button></div></div>
        <div class="config-list"><label v-for="item in configs" :key="item.key"><span>{{ item.key }}</span><input :value="item.value" readonly></label></div>
      </section>
    </main>
  </div>
</template>

<script>
const PreviewWorkers = {
  props: { workers: { type: Array, required: true } },
  template: '<table class="data-table"><thead><tr><th>Worker</th><th>Status</th><th>Capacity</th><th>Heartbeat</th><th>Blocks</th><th></th></tr></thead><tbody><tr v-for="worker in workers" :key="worker.name"><td><strong>{{ worker.name }}</strong><span>{{ worker.host }}</span></td><td><span :class="[\'pill\', worker.status.toLowerCase()]">{{ worker.status }}</span></td><td><div class="inline-meter"><span :style="{ width: worker.used + \'%\' }"></span></div><small>{{ worker.used }}% used</small></td><td>{{ worker.heartbeat }}</td><td>{{ worker.blocks }}</td><td><button class="link-button" type="button">Detail</button></td></tr></tbody></table>'
}

const PreviewFiles = {
  props: { files: { type: Array, required: true } },
  template: '<table class="data-table"><thead><tr><th>Name</th><th>Type</th><th>Size</th><th>Replicas</th><th>Modified</th><th></th></tr></thead><tbody><tr v-for="file in files" :key="file.name"><td><strong>{{ file.name }}</strong><span>{{ file.path }}</span></td><td>{{ file.type }}</td><td>{{ file.size }}</td><td>{{ file.replicas }}</td><td>{{ file.modified }}</td><td><button class="link-button" type="button">Blocks</button></td></tr></tbody></table>'
}

const PreviewMounts = {
  props: { mounts: { type: Array, required: true } },
  template: '<div class="mount-list"><div v-for="mount in mounts" :key="mount.path" class="mount-row"><div><strong>{{ mount.path }}</strong><span>{{ mount.target }}</span></div><span :class="[\'pill\', mount.state.toLowerCase()]">{{ mount.state }}</span></div></div>'
}

export default {
  name: 'CurvinePreview',
  components: { PreviewWorkers, PreviewFiles, PreviewMounts },
  data() {
    return {
      activeNav: 'overview', query: '',
      navItems: [
        { key: 'overview', label: 'Overview', icon: 'O' }, { key: 'filesystem', label: 'File System', icon: 'F' }, { key: 'workers', label: 'Workers', icon: 'W' },
        { key: 'mounts', label: 'Mounts', icon: 'M' }, { key: 'jobs', label: 'Sync Jobs', icon: 'J' }, { key: 'config', label: 'Config', icon: 'C' }
      ],
      metricSets: {
        overview: [{ label: 'Workers', value: '18', meta: '17 live, 1 lost', delta: '+2', tone: 'good' }, { label: 'Used Capacity', value: '126.8 TB', meta: '49.4 TB free', delta: '72%', tone: 'warn' }, { label: 'Files', value: '4.82 M', meta: '321 K directories', delta: '+8.6%', tone: 'good' }, { label: 'Active Jobs', value: '7', meta: '2 waiting, 5 running', delta: '2 failed', tone: 'bad' }],
        filesystem: [{ label: 'Current Path', value: '3 items', meta: '/warehouse/orders', delta: '2 files', tone: 'good' }, { label: 'Logical Size', value: '21.2 GB', meta: 'replica factor avg 2.5', delta: '+1.8 GB', tone: 'warn' }, { label: 'Cached', value: '84%', meta: 'hot data coverage', delta: '+6%', tone: 'good' }, { label: 'Blocks', value: '4,492', meta: 'spread on 12 workers', delta: 'stable', tone: 'good' }],
        workers: [{ label: 'Live', value: '17', meta: 'healthy heartbeat', delta: '+2', tone: 'good' }, { label: 'Lost', value: '1', meta: 'worker-07', delta: '16m', tone: 'bad' }, { label: 'Avg Usage', value: '68%', meta: 'cluster worker disks', delta: '+4%', tone: 'warn' }, { label: 'Blocks', value: '4.1 M', meta: 'registered blocks', delta: '+82 K', tone: 'good' }],
        mounts: [{ label: 'Mount Points', value: '3', meta: '2 healthy, 1 warning', delta: '+1', tone: 'good' }, { label: 'Providers', value: 'S3/HDFS/OSS', meta: 'active UFS types', delta: '3', tone: 'good' }, { label: 'Validated', value: '2', meta: 'latest probe passed', delta: '1 warn', tone: 'warn' }, { label: 'Resync Queue', value: '5', meta: 'pending mount scans', delta: '+2', tone: 'warn' }],
        jobs: [{ label: 'Running', value: '5', meta: 'load jobs', delta: '+2', tone: 'good' }, { label: 'Waiting', value: '2', meta: 'queued by priority', delta: 'normal', tone: 'warn' }, { label: 'Failed', value: '1', meta: 'retry available', delta: '1', tone: 'bad' }, { label: 'Throughput', value: '8.4 GB/s', meta: 'last 5 minutes', delta: '+12%', tone: 'good' }],
        config: [{ label: 'Groups', value: '6', meta: 'master, worker, web', delta: 'readonly', tone: 'good' }, { label: 'Entries', value: '184', meta: 'loaded from cluster conf', delta: '+9', tone: 'good' }, { label: 'Overrides', value: '17', meta: 'non-default values', delta: 'audit', tone: 'warn' }, { label: 'API', value: '/api/v1', meta: 'versioned endpoints', delta: 'new', tone: 'good' }]
      },
      capacity: [{ label: 'SSD Cache', value: '48.5 / 64 TB', percent: 76 }, { label: 'HDD Storage', value: '78.3 / 112 TB', percent: 70 }, { label: 'Metadata', value: '4.82 M files', percent: 54 }],
      jobs: [{ id: 'load-8921', path: '/warehouse/orders', status: 'Running', progress: 68 }, { id: 'load-8918', path: '/lake/events/2026', status: 'Waiting', progress: 12 }, { id: 'load-8904', path: '/models/recommend', status: 'Failed', progress: 43 }],
      workers: [{ name: 'worker-01', host: '10.31.8.21:19998', status: 'Live', used: 64, heartbeat: '3s ago', blocks: '1,248,091' }, { name: 'worker-02', host: '10.31.8.22:19998', status: 'Live', used: 71, heartbeat: '4s ago', blocks: '1,105,442' }, { name: 'worker-07', host: '10.31.8.29:19998', status: 'Lost', used: 88, heartbeat: '16m ago', blocks: '982,114' }, { name: 'worker-12', host: '10.31.8.43:19998', status: 'Live', used: 52, heartbeat: '2s ago', blocks: '804,650' }],
      files: [{ name: 'events', path: '/lake/events', type: 'Directory', size: '-', replicas: '-', modified: '2026-05-08 09:42' }, { name: 'part-00042.parquet', path: '/warehouse/orders', type: 'File', size: '2.8 GB', replicas: '3', modified: '2026-05-08 08:17' }, { name: 'model.bin', path: '/models/recommend/v4', type: 'File', size: '18.4 GB', replicas: '2', modified: '2026-05-07 23:06' }],
      mounts: [{ path: '/lake', target: 's3://prod-data-lake', state: 'Healthy' }, { path: '/warehouse', target: 'hdfs://nameservice1/warehouse', state: 'Healthy' }, { path: '/archive', target: 'oss://cold-archive', state: 'Warning' }],
      configs: [{ key: 'curvine.master.hostname', value: 'master-01' }, { key: 'curvine.worker.memory.size', value: '256GB' }, { key: 'curvine.web.api.prefix', value: '/api/v1' }, { key: 'curvine.master.worker.timeout', value: '30s' }, { key: 'curvine.fs.block.size', value: '128MB' }]
    }
  },
  computed: {
    activeTitle() { const current = this.navItems.find((item) => item.key === this.activeNav); return current ? current.label : 'Overview' },
    activeSubtitle() { return { overview: 'Cluster health, capacity and running operations', filesystem: 'Browse paths, inspect blocks and manage files', workers: 'Worker status, capacity and decommission workflow', mounts: 'Mount table, UFS targets and validation state', jobs: 'Load jobs, progress polling and cancellation', config: 'Readonly cluster configuration grouped for search' }[this.activeNav] },
    activeMetrics() { return this.metricSets[this.activeNav] || this.metricSets.overview },
    capacityStyle() { return { background: 'conic-gradient(#6fae3f 0 72%, #e9f0e4 72% 100%)' } },
    primaryAction() { return { overview: 'New Mount', filesystem: 'Upload', workers: 'Decommission', mounts: 'Add Mount', jobs: 'Submit Load', config: 'Export' }[this.activeNav] },
    searchPlaceholder() { return { overview: 'Path, worker, config', filesystem: 'Search current path', workers: 'Worker host or id', mounts: 'Mount path or UFS URI', jobs: 'Job id or path', config: 'Config key' }[this.activeNav] }
  }
}
</script>

<style lang="scss" scoped>
.preview-shell{min-height:100vh;display:flex;background:#f6f8f3;color:#1b261f;font-family:Inter,"Segoe UI",Arial,sans-serif}.preview-sidebar{width:248px;flex:0 0 248px;background:#fff;border-right:1px solid #d8e2d4;padding:22px 16px;display:flex;flex-direction:column;gap:22px}.brand-block{display:flex;align-items:center;gap:12px;padding:0 8px 14px;border-bottom:1px solid #edf4e9}.brand-mark{width:34px;height:34px}.brand-name{font-weight:760;font-size:17px;line-height:1.1}.brand-subtitle,.side-status span,.panel-heading p,.metric-card span,.data-table td span,.data-table small,.mount-row span,.job-meta,.config-list span,.topbar p,.path-toolbar span{color:#6a7865}.brand-subtitle{font-size:12px;margin-top:3px}.side-nav{display:flex;flex-direction:column;gap:4px}.side-link{width:100%;min-height:42px;border:0;background:transparent;color:#465445;border-radius:8px;display:flex;align-items:center;gap:10px;padding:0 12px;font-weight:650;text-align:left}.side-link.active,.side-link:hover{background:#e8f5dc;color:#3f7a2f}.side-icon{width:24px;height:24px;border-radius:6px;background:#eef5e8;display:inline-flex;align-items:center;justify-content:center;font-size:12px}.side-link.active .side-icon{background:#6fae3f;color:#fff}.side-status{margin-top:auto;display:flex;gap:10px;align-items:flex-start;padding:12px;background:#f8faf4;border:1px solid #dfe8db;border-radius:8px}.side-status strong,.side-status span{display:block;font-size:12px}.status-dot{width:9px;height:9px;margin-top:4px;border-radius:999px;background:#5fa83a}.preview-main{flex:1;min-width:0;padding:24px;overflow-x:hidden}.topbar{display:flex;justify-content:space-between;gap:18px;align-items:flex-start;margin-bottom:18px}h1,h2,p{margin:0}h1{font-size:26px;line-height:1.2;font-weight:760}h2{font-size:16px;line-height:1.3;font-weight:760}.topbar p,.panel-heading p{margin-top:5px;font-size:13px}.top-actions{display:flex;align-items:center;gap:10px;flex-wrap:wrap;justify-content:flex-end}.search-box{height:38px;min-width:280px;display:flex;align-items:center;gap:8px;background:#fff;border:1px solid #d8e2d4;border-radius:8px;padding:0 11px;color:#6a7865;font-size:12px}.search-box input{border:0;outline:0;min-width:0;flex:1;color:#1b261f}.ghost-button,.primary-button,.icon-button,.link-button,.segmented button{height:38px;border-radius:8px;font-weight:680;border:1px solid #cfdcc9;background:#fff;color:#263426;padding:0 14px}.primary-button{background:#6fae3f;border-color:#6fae3f;color:#fff}.compact{height:32px}.danger{color:#b84635}.icon-button{width:34px;padding:0}.link-button{height:30px;color:#6fae3f;border-color:transparent;padding:0 4px}.metric-grid,.content-grid,.lower-grid{display:grid;gap:16px}.metric-grid{grid-template-columns:repeat(4,minmax(0,1fr));margin-bottom:16px}.metric-card,.panel{background:#fff;border:1px solid #dfe8d9;border-radius:8px}.metric-card{padding:16px}.metric-card strong{display:block;margin-top:8px;font-size:26px;line-height:1}.metric-meta{margin-top:13px;display:flex;justify-content:space-between;align-items:center;gap:8px;font-size:12px}.metric-meta em{font-style:normal;font-weight:760}.good{color:#4f8f34}.warn{color:#a8791f}.bad{color:#b84635}.content-grid{grid-template-columns:minmax(0,1.25fr) minmax(340px,.75fr);margin-bottom:16px}.panel{padding:16px}.panel-heading{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:16px}.capacity-layout{display:grid;grid-template-columns:180px minmax(0,1fr);gap:22px;align-items:center}.donut{width:172px;aspect-ratio:1;border-radius:999px;display:grid;place-items:center;position:relative}.donut:after{content:"";position:absolute;inset:18px;border-radius:999px;background:#fff}.donut div{position:relative;z-index:1;text-align:center}.donut strong{display:block;font-size:26px}.donut span{color:#6a7865;font-size:12px}.capacity-list{display:flex;flex-direction:column;gap:15px}.capacity-row{display:grid;gap:7px}.capacity-row div:first-child{display:flex;justify-content:space-between;gap:12px;font-size:13px}.bar,.job-progress,.inline-meter{height:8px;background:#edf4e9;border-radius:999px;overflow:hidden}.bar span,.job-progress span,.inline-meter span{display:block;height:100%;background:#6fae3f;border-radius:inherit}.job-list{display:grid;gap:12px}.job-item{padding:12px;background:#f8faf4;border:1px solid #e3eadf;border-radius:8px}.job-topline,.job-meta,.mount-row{display:flex;align-items:center;justify-content:space-between;gap:12px}.job-topline strong{font-size:13px}.job-progress{margin:12px 0 8px}.pill{display:inline-flex;align-items:center;justify-content:center;min-height:24px;padding:0 9px;border-radius:999px;font-size:12px;font-weight:760;color:#36506c;background:#eef5e8}.pill.live,.pill.healthy,.pill.running{color:#3f7a2f;background:#e8f6df}.pill.lost,.pill.failed{color:#a63c2f;background:#fff0ea}.pill.warning,.pill.waiting{color:#8a6416;background:#fff5d9}.table-panel,.lower-grid{margin-bottom:16px}.table-heading{align-items:center}.segmented{display:inline-flex;padding:3px;border:1px solid #d8e2d4;border-radius:8px;background:#f8faf4}.segmented button{height:30px;border:0;background:transparent;padding:0 12px}.segmented button.active{background:#fff;color:#6fae3f;box-shadow:0 1px 2px rgba(22,32,51,.12)}.data-table{width:100%;border-collapse:collapse;table-layout:fixed}.data-table th{color:#66765f;font-size:12px;text-align:left;font-weight:760;border-bottom:1px solid #e2eadc;padding:10px 8px}.data-table td{border-bottom:1px solid #edf4e9;padding:13px 8px;vertical-align:middle;font-size:13px}.data-table tr:last-child td{border-bottom:0}.data-table td strong,.data-table td span{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.inline-meter{width:min(160px,100%);margin-bottom:5px}.lower-grid{grid-template-columns:minmax(0,1fr) minmax(340px,.85fr)}.mount-list,.config-list,.form-panel{display:grid;gap:10px}.mount-row{padding:12px;border:1px solid #e2eadc;border-radius:8px}.mount-row strong,.mount-row span{display:block}.mount-row span{font-size:12px;margin-top:3px}.config-list label,.form-panel label{display:grid;grid-template-columns:1fr minmax(140px,260px);gap:10px;align-items:center}.config-list input,.form-panel input{height:34px;border:1px solid #d8e2d4;border-radius:8px;padding:0 10px;background:#f8faf4;color:#263426}.path-toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:14px;padding:10px;background:#f8faf4;border:1px solid #e2eadc;border-radius:8px}@media (max-width:1100px){.metric-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.content-grid,.lower-grid{grid-template-columns:1fr}.topbar{flex-direction:column}.top-actions{justify-content:flex-start}}@media (max-width:760px){.preview-shell{display:block}.preview-sidebar{width:auto;flex:none;border-right:0;border-bottom:1px solid #d8e2d4}.side-nav{display:grid;grid-template-columns:repeat(2,minmax(0,1fr))}.preview-main{padding:16px}.metric-grid,.capacity-layout{grid-template-columns:1fr}.search-box{min-width:100%}.data-table{min-width:720px}.table-panel{overflow-x:auto}.config-list label,.form-panel label{grid-template-columns:1fr}}
</style>

