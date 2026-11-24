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
  <div class="tenants-page" v-loading="loading">
    <div class="page-header">
      <div>
        <router-link to="/transfers" class="back-link">← Transfers</router-link>
        <h1>Tenant Summary</h1>
        <p class="subtitle">Transfer jobs grouped by tenant</p>
      </div>
      <div class="header-actions">
        <button type="button" class="btn btn-primary" @click="load()">Refresh</button>
      </div>
    </div>

    <div class="tech-card">
      <table class="table align-middle">
        <thead>
          <tr>
            <th>Tenant</th>
            <th>Total</th>
            <th>Pending</th>
            <th>Executing</th>
            <th>Completed</th>
            <th>Failed</th>
            <th>Canceled</th>
            <th>Partial Success</th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="!tenants.length">
            <td colspan="8" class="empty">No tenants</td>
          </tr>
          <tr v-for="tenant in tenants" :key="tenant.tenant">
            <td class="tenant-name">{{ tenant.tenant }}</td>
            <td>{{ tenant.total }}</td>
            <td>{{ tenant.pending }}</td>
            <td>{{ tenant.executing }}</td>
            <td class="completed">{{ tenant.completed }}</td>
            <td class="failed">{{ tenant.failed }}</td>
            <td>{{ tenant.canceled }}</td>
            <td>{{ tenant.partial_success }}</td>
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
import { fetchTransferTenants } from '@/api/transfer'

export default {
  name: 'TransferTenants',
  data() {
    return {
      loading: false,
      tenants: [],
      pageTokens: [null],
      pageIndex: 0,
      nextPageToken: null
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
      this.load(true)
    },
    async load(silent = false) {
      if (!silent) {
        this.loading = true
      }
      try {
        const data = await fetchTransferTenants({
          limit: 50,
          page_token: this.pageTokens[this.pageIndex]
        })
        this.tenants = data.tenants || []
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
      this.load()
    },
    prevPage() {
      if (this.pageIndex === 0) return
      this.pageIndex -= 1
      this.load()
    }
  }
}
</script>

<style lang="scss" scoped>
.tenants-page {
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
    font-size: 2rem;
    font-weight: 700;
  }

  .subtitle {
    margin: 0.4rem 0 0;
    color: var(--text-secondary);
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

.tenant-name {
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  color: var(--accent-blue);
}

.completed {
  color: var(--accent-green);
}

.failed {
  color: #ff6b6b;
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
