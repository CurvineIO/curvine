<!--
  - Copyright 2025 OPPO.
  -
  - Licensed under the Apache License, Version 2.0 (the "License");
  - you may not use this file except in compliance with the License.
  - You may obtain a copy of the License at
  -
  -     http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing, software
  - distributed under the License is distributed on an "AS IS" BASIS,
  - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  - See the License for the specific language governing permissions and
  - limitations under the License.
  -->

<template>
  <div class="transfer-detail" v-loading="loading">
    <div class="page-header">
      <div>
        <router-link to="/transfers" class="back-link">← Transfers</router-link>
        <h1>
          {{ job ? job.job_id : jobId }}
        </h1>
        <p class="subtitle" v-if="job">
          <span class="kind">{{ job.kind }}</span>
          <TransferStateBadge :state="job.state" />
          <span class="run">run {{ job.run_id }}</span>
        </p>
      </div>
      <div class="header-actions" v-if="job">
        <button v-if="canCancel(job.state)" type="button" class="btn btn-outline-warning" @click="cancel">Cancel</button>
        <button v-if="canRetry(job.state)" type="button" class="btn btn-outline-info" @click="retry">Retry</button>
        <button type="button" class="btn btn-primary" @click="load(true)">Refresh</button>
      </div>
    </div>

    <div v-if="notice" class="notice">{{ notice }}</div>

    <template v-if="job">
      <div class="tech-card">
        <div class="progress-header">
          <span>Progress</span>
          <span>{{ formatBytes(job.progress.loaded_size) }} / {{ formatBytes(job.progress.total_size) }}
            ({{ progressPercent(job.progress) }}%)</span>
        </div>
        <div class="progress">
          <div class="progress-bar" :style="{ width: progressPercent(job.progress) + '%' }"></div>
        </div>
        <p v-if="job.progress.message" class="progress-message">{{ job.progress.message }}</p>
        <div class="task-counts">
          <span v-for="(value, key) in taskSummary" :key="key" class="count-item" :class="key">
            {{ key }}: {{ value }}
          </span>
        </div>
      </div>

      <div class="tech-card">
        <h2>Details</h2>
        <dl class="meta-grid">
          <div><dt>Source</dt><dd class="path">{{ job.source_path }}</dd></div>
          <div><dt>Target</dt><dd class="path">{{ job.target_path }}</dd></div>
          <div><dt>Submitter</dt><dd>{{ job.submitter || '-' }}</dd></div>
          <div><dt>Tenant</dt><dd>{{ job.tenant || '-' }}</dd></div>
          <div><dt>Owner</dt><dd>{{ job.owner || '-' }}</dd></div>
          <div><dt>Lease Epoch</dt><dd>{{ job.lease_epoch === null ? '-' : job.lease_epoch }}</dd></div>
          <div><dt>Lease Expires</dt><dd>{{ job.lease_expire_at ? formatTimestamp(job.lease_expire_at) : '-' }}</dd></div>
          <div><dt>Metadata Epoch</dt><dd>{{ job.cv_metadata_epoch === null ? '-' : job.cv_metadata_epoch }}</dd></div>
          <div><dt>Created</dt><dd>{{ formatTimestamp(job.created_at) }}</dd></div>
          <div><dt>Updated</dt><dd>{{ formatTimestamp(job.updated_at) }}</dd></div>
        </dl>
      </div>

      <div class="tech-card">
        <div class="tasks-header">
          <h2>Tasks</h2>
          <span class="hint" v-if="tasks.length">Page {{ taskPageIndex + 1 }}</span>
        </div>
        <table class="table align-middle">
          <thead>
            <tr>
              <th>Task</th>
              <th>State</th>
              <th>Worker</th>
              <th>Attempt</th>
              <th>Progress</th>
              <th>Updated</th>
              <th>Message</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="!tasks.length">
              <td colspan="7" class="empty">No tasks in this page</td>
            </tr>
            <tr v-for="task in tasks" :key="task.task_id">
              <td class="task-id" :title="task.task_id">{{ shortId(task.task_id) }}</td>
              <td><TransferStateBadge :state="task.state" /></td>
              <td>
                <div>{{ task.worker_id || '-' }}</div>
                <div class="worker-session">{{ task.worker_session_id || '' }}</div>
              </td>
              <td>{{ task.attempt_id }}<span v-if="task.retry_count"> ({{ task.retry_count }} retries)</span></td>
              <td class="progress-cell">
                <div class="progress">
                  <div class="progress-bar" :style="{ width: progressPercent(task.progress) + '%' }"></div>
                </div>
                <span class="progress-text">
                  {{ formatBytes(task.progress.loaded_size) }} / {{ formatBytes(task.progress.total_size) }}
                </span>
              </td>
              <td class="time">{{ formatTimestamp(task.updated_at) }}</td>
              <td class="message">{{ task.progress.message || '-' }}</td>
            </tr>
          </tbody>
        </table>
        <div class="pager">
          <button type="button" class="btn btn-sm btn-outline-secondary" :disabled="taskPageIndex === 0"
            @click="prevTaskPage">Previous</button>
          <button type="button" class="btn btn-sm btn-outline-secondary" :disabled="!nextTaskPageToken"
            @click="nextTaskPage">Next</button>
        </div>
      </div>
    </template>
  </div>
</template>

<script>
/* eslint-disable vue/multi-word-component-names */
import eventBus from '@/utils/eventBus'
import TransferStateBadge from '@/components/TransferStateBadge.vue'
import { cancelTransferJob, fetchTransferJob, retryTransferJob } from '@/api/transfer'
import { formatBytes, formatTimestamp } from '@/utils/utils'
import { canCancel, canRetry, isTerminalState, progressPercent } from '@/utils/transfer'

export default {
  name: 'TransferDetail',
  components: { TransferStateBadge },
  props: {
    jobId: {
      type: String,
      required: true
    }
  },
  setup() {
    return { formatBytes, formatTimestamp, canCancel, canRetry, progressPercent }
  },
  data() {
    return {
      loading: false,
      job: null,
      tasks: [],
      taskPageTokens: [null],
      taskPageIndex: 0,
      nextTaskPageToken: null,
      notice: ''
    }
  },
  computed: {
    taskSummary() {
      if (!this.job) return {}
      return this.job.task_summary || {}
    }
  },
  mounted() {
    this.load()
    eventBus.on('auto-refresh-trigger', this.handleAutoRefresh)
  },
  beforeUnmount() {
    eventBus.off('auto-refresh-trigger', this.handleAutoRefresh)
  },
  methods: {
    handleAutoRefresh() {
      if (this.job && isTerminalState(this.job.state)) return
      this.load(true)
    },
    async load(silent = false) {
      if (!silent) {
        this.loading = true
      }
      try {
        const data = await fetchTransferJob(this.jobId, {
          limit: 50,
          page_token: this.taskPageTokens[this.taskPageIndex]
        })
        this.job = data.job
        this.tasks = data.tasks || []
        this.nextTaskPageToken = data.next_page_token || null
      } catch (error) {
        // The request interceptor surfaces the message banner.
      } finally {
        this.loading = false
      }
    },
    nextTaskPage() {
      if (!this.nextTaskPageToken) return
      this.taskPageTokens = this.taskPageTokens
        .slice(0, this.taskPageIndex + 1)
        .concat([this.nextTaskPageToken])
      this.taskPageIndex = this.taskPageTokens.length - 1
      this.load()
    },
    prevTaskPage() {
      if (this.taskPageIndex === 0) return
      this.taskPageIndex -= 1
      this.load()
    },
    async cancel() {
      if (!window.confirm(`Cancel transfer ${this.jobId}?`)) return
      try {
        const result = await cancelTransferJob(this.jobId)
        this.notice = `Cancel requested (${result.state})`
        await this.load(true)
      } catch (error) {
        // The request interceptor surfaces the message banner.
      }
    },
    async retry() {
      if (!window.confirm('Retry this transfer? A new job will be created.')) return
      try {
        const created = await retryTransferJob(this.jobId)
        this.$router.push(`/transfers/${encodeURIComponent(created.job_id)}`)
      } catch (error) {
        // The request interceptor surfaces the message banner.
      }
    },
    shortId(taskId) {
      return taskId ? taskId.slice(0, 12) : '-'
    }
  }
}
</script>

<style lang="scss" scoped>
.transfer-detail {
  padding: 2rem;
}

.page-header {
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
  gap: 1rem;
  margin-bottom: 1.5rem;

  h1 {
    margin: 0.4rem 0 0;
    font-size: 1.6rem;
    font-weight: 700;
    font-family: 'JetBrains Mono', 'Fira Code', monospace;
    word-break: break-all;
  }

  .subtitle {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin: 0.5rem 0 0;
    color: var(--text-secondary);
    font-size: 0.85rem;
  }

  .kind {
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }

  .header-actions {
    display: flex;
    gap: 0.75rem;
    white-space: nowrap;
  }
}

.back-link {
  color: var(--text-secondary);
  text-decoration: none;
  font-size: 0.85rem;

  &:hover {
    color: var(--accent-blue);
  }
}

.notice {
  margin-bottom: 1rem;
  padding: 0.65rem 1rem;
  border-radius: 8px;
  border: 1px solid var(--accent-blue);
  background: rgba(0, 212, 255, 0.1);
  color: var(--accent-blue);
  font-size: 0.9rem;
}

.tech-card h2 {
  margin: 0 0 1rem;
  font-size: 1rem;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--accent-blue);
}

.progress-header {
  display: flex;
  justify-content: space-between;
  margin-bottom: 0.5rem;
  font-size: 0.85rem;
  color: var(--text-secondary);
}

.progress-message {
  margin: 0.75rem 0 0;
  color: var(--text-secondary);
  font-size: 0.85rem;
}

.task-counts {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
  margin-top: 1rem;

  .count-item {
    padding: 0.2rem 0.6rem;
    border-radius: 6px;
    background: var(--secondary-bg);
    border: 1px solid var(--border-color);
    font-size: 0.75rem;
    color: var(--text-secondary);

    &.running {
      color: var(--accent-blue);
      border-color: var(--accent-blue);
    }

    &.failed {
      color: #ff6b6b;
      border-color: #ff6b6b;
    }

    &.completed {
      color: var(--accent-green);
      border-color: var(--accent-green);
    }
  }
}

.meta-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
  gap: 1rem;
  margin: 0;

  dt {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    color: var(--text-secondary);
    margin-bottom: 0.2rem;
  }

  dd {
    margin: 0;
    font-size: 0.9rem;
    word-break: break-all;
  }

  .path {
    font-family: 'JetBrains Mono', 'Fira Code', monospace;
  }
}

.tasks-header {
  display: flex;
  align-items: baseline;
  justify-content: space-between;

  .hint {
    font-size: 0.8rem;
    color: var(--text-secondary);
  }
}

.task-id {
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  font-size: 0.85rem;
}

.worker-session {
  font-size: 0.7rem;
  color: var(--text-secondary);
}

.progress-cell {
  min-width: 160px;

  .progress-text {
    display: block;
    margin-top: 0.25rem;
    font-size: 0.7rem;
    color: var(--text-secondary);
  }
}

.time {
  white-space: nowrap;
  font-size: 0.8rem;
  color: var(--text-secondary);
}

.message {
  font-size: 0.85rem;
  color: var(--text-secondary);
  max-width: 320px;
  word-break: break-word;
}

.empty {
  text-align: center;
  padding: 2rem 0;
  color: var(--text-secondary);
}

.pager {
  display: flex;
  justify-content: flex-end;
  gap: 0.75rem;
  margin-top: 1rem;
}
</style>
