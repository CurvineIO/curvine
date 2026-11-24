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
  <div class="transfers-page" v-loading="loading">
    <div class="page-header">
      <div>
        <h1>Transfers</h1>
        <p class="subtitle">Load and export jobs executed by the transfer service</p>
      </div>
      <div class="header-actions">
        <router-link to="/transfer-tenants" class="btn btn-outline-secondary">Tenant Summary</router-link>
        <button type="button" class="btn btn-primary" @click="refresh">Refresh</button>
      </div>
    </div>

    <div class="tech-card filters">
      <div class="filter">
        <label>Kind</label>
        <select v-model="filters.kind" class="form-select" @change="applyFilters">
          <option value="">All</option>
          <option value="load">Load</option>
          <option value="export">Export</option>
        </select>
      </div>
      <div class="filter">
        <label>State</label>
        <select v-model="filters.state" class="form-select" @change="applyFilters">
          <option value="">All</option>
          <option v-for="state in states" :key="state" :value="state">{{ displayState(state) }}</option>
        </select>
      </div>
      <div class="filter">
        <label>Submitter</label>
        <input v-model.trim="filters.submitter" class="form-control" placeholder="any" @keyup.enter="applyFilters" />
      </div>
      <div class="filter">
        <label>Tenant</label>
        <input v-model.trim="filters.tenant" class="form-control" placeholder="any" @keyup.enter="applyFilters" />
      </div>
      <div class="filter">
        <label>Page size</label>
        <select v-model.number="filters.limit" class="form-select" @change="applyFilters">
          <option :value="20">20</option>
          <option :value="50">50</option>
          <option :value="100">100</option>
        </select>
      </div>
      <button type="button" class="btn btn-primary apply" @click="applyFilters">Apply</button>
    </div>

    <div v-if="notice" class="notice">{{ notice }}</div>

    <div class="tech-card">
      <table class="table align-middle">
        <thead>
          <tr>
            <th>Job</th>
            <th>Kind</th>
            <th>State</th>
            <th>Progress</th>
            <th>Submitter / Tenant</th>
            <th>Updated</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="!jobs.length">
            <td colspan="7" class="empty">No transfer jobs</td>
          </tr>
          <tr v-for="job in jobs" :key="job.job_id">
            <td>
              <router-link class="job-link" :to="`/transfers/${encodeURIComponent(job.job_id)}`" :title="job.job_id">
                {{ shortId(job.job_id) }}
              </router-link>
            </td>
            <td class="kind">{{ job.kind }}</td>
            <td><TransferStateBadge :state="job.state" /></td>
            <td class="progress-cell">
              <div class="progress">
                <div class="progress-bar" :style="{ width: progressPercent(job.progress) + '%' }"></div>
              </div>
              <span class="progress-text">
                {{ formatBytes(job.progress.loaded_size) }} / {{ formatBytes(job.progress.total_size) }}
                ({{ progressPercent(job.progress) }}%)
              </span>
            </td>
            <td>
              <div>{{ job.submitter || '-' }}</div>
              <div class="tenant">{{ job.tenant || '-' }}</div>
            </td>
            <td class="time">{{ formatTimestamp(job.updated_at) }}</td>
            <td class="actions">
              <button v-if="canCancel(job.state)" type="button" class="btn btn-sm btn-outline-warning"
                @click="cancel(job)">Cancel</button>
              <button v-if="canRetry(job.state)" type="button" class="btn btn-sm btn-outline-info"
                @click="retry(job)">Retry</button>
            </td>
          </tr>
        </tbody>
      </table>

      <div class="pager">
        <button type="button" class="btn btn-sm btn-outline-secondary" :disabled="pageIndex === 0"
          @click="prevPage">Previous</button>
        <span class="page-number">Page {{ pageIndex + 1 }}</span>
        <button type="button" class="btn btn-sm btn-outline-secondary" :disabled="!nextPageToken"
          @click="nextPage">Next</button>
      </div>
    </div>
  </div>
</template>

<script>
/* eslint-disable vue/multi-word-component-names */
import eventBus from '@/utils/eventBus'
import TransferStateBadge from '@/components/TransferStateBadge.vue'
import { cancelTransferJob, fetchTransferJobs, retryTransferJob } from '@/api/transfer'
import { formatBytes, formatTimestamp } from '@/utils/utils'
import { canCancel, canRetry, progressPercent } from '@/utils/transfer'

export default {
  name: 'Transfers',
  components: { TransferStateBadge },
  setup() {
    return { formatBytes, formatTimestamp, canCancel, canRetry, progressPercent }
  },
  data() {
    return {
      loading: false,
      jobs: [],
      notice: '',
      states: [
        'pending',
        'planning',
        'dispatching',
        'running',
        'canceling',
        'completed',
        'failed',
        'canceled',
        'partial_success'
      ],
      filters: {
        kind: '',
        state: '',
        submitter: '',
        tenant: '',
        limit: 20
      },
      pageTokens: [null],
      pageIndex: 0,
      nextPageToken: null
    }
  },
  mounted() {
    this.refresh()
    eventBus.on('auto-refresh-trigger', this.handleAutoRefresh)
  },
  beforeUnmount() {
    eventBus.off('auto-refresh-trigger', this.handleAutoRefresh)
  },
  methods: {
    handleAutoRefresh() {
      this.loadPage(this.pageIndex, true)
    },
    refresh() {
      this.pageTokens = [null]
      this.pageIndex = 0
      this.loadPage(0)
    },
    applyFilters() {
      this.notice = ''
      this.refresh()
    },
    async loadPage(index, silent = false) {
      if (!silent) {
        this.loading = true
      }
      try {
        const params = {
          limit: this.filters.limit,
          page_token: this.pageTokens[index]
        }
        if (this.filters.kind) params.kind = this.filters.kind
        if (this.filters.state) params.state = this.filters.state
        if (this.filters.submitter) params.submitter = this.filters.submitter
        if (this.filters.tenant) params.tenant = this.filters.tenant

        const data = await fetchTransferJobs(params)
        this.jobs = data.jobs || []
        this.nextPageToken = data.next_page_token || null
      } catch (error) {
        // The request interceptor surfaces the message banner.
      } finally {
        this.loading = false
      }
    },
    nextPage() {
      if (!this.nextPageToken) return
      this.pageTokens = this.pageTokens.slice(0, this.pageIndex + 1).concat([this.nextPageToken])
      this.pageIndex = this.pageTokens.length - 1
      this.loadPage(this.pageIndex)
    },
    prevPage() {
      if (this.pageIndex === 0) return
      this.pageIndex -= 1
      this.loadPage(this.pageIndex)
    },
    async cancel(job) {
      if (!window.confirm(`Cancel transfer ${job.job_id}?`)) return
      try {
        const result = await cancelTransferJob(job.job_id)
        this.notice = `Cancel requested for ${result.job_id} (${result.state})`
        await this.loadPage(this.pageIndex, true)
      } catch (error) {
        // The request interceptor surfaces the message banner.
      }
    },
    async retry(job) {
      if (!window.confirm(`Retry transfer ${job.job_id}? A new job will be created.`)) return
      try {
        const created = await retryTransferJob(job.job_id)
        this.notice = `Retry submitted as ${created.job_id}`
        await this.loadPage(this.pageIndex, true)
      } catch (error) {
        // The request interceptor surfaces the message banner.
      }
    },
    shortId(jobId) {
      return jobId ? jobId.slice(0, 8) : '-'
    },
    displayState(state) {
      return state.replace(/_/g, ' ')
    }
  }
}
</script>

<style lang="scss" scoped>
.transfers-page {
  padding: 2rem;
}

.page-header {
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
  gap: 1rem;
  margin-bottom: 1.5rem;

  h1 {
    margin: 0;
    font-size: 2rem;
    font-weight: 700;
  }

  .subtitle {
    margin: 0.4rem 0 0;
    color: var(--text-secondary);
  }

  .header-actions {
    display: flex;
    gap: 0.75rem;
  }
}

.filters {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 1rem;
  align-items: end;

  .filter {
    display: flex;
    flex-direction: column;
    gap: 0.35rem;

    label {
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      color: var(--text-secondary);
    }
  }

  .apply {
    height: fit-content;
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

.job-link {
  color: var(--accent-blue);
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  text-decoration: none;

  &:hover {
    text-decoration: underline;
  }
}

.kind {
  text-transform: uppercase;
  font-size: 0.8rem;
  letter-spacing: 0.5px;
  color: var(--text-secondary);
}

.progress-cell {
  min-width: 220px;

  .progress-text {
    display: block;
    margin-top: 0.3rem;
    font-size: 0.75rem;
    color: var(--text-secondary);
  }
}

.tenant {
  font-size: 0.75rem;
  color: var(--text-secondary);
}

.time {
  white-space: nowrap;
  font-size: 0.85rem;
  color: var(--text-secondary);
}

.actions {
  white-space: nowrap;
  text-align: right;

  .btn {
    margin-left: 0.35rem;
  }
}

.empty {
  text-align: center;
  padding: 2rem 0;
  color: var(--text-secondary);
}

.pager {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 0.75rem;
  margin-top: 1rem;

  .page-number {
    font-size: 0.85rem;
    color: var(--text-secondary);
  }
}
</style>
