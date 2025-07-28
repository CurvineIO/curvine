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
  <div v-loading="loading" class="workers">
    <!-- Live Workers Section -->
    <div class="workers-section">
      <div class="section-header">
        <div class="section-title">
          <div class="title-icon">🟢</div>
          <h2>Live Workers</h2>
          <div class="worker-count">{{ filteredLiveWorkers.length }}</div>
        </div>
        <div class="section-stats">
          <div class="stat-item">
            <span class="stat-label">Total Capacity</span>
            <span class="stat-value">{{ getTotalCapacity(data.live_workers) }}GB</span>
          </div>
          <div class="stat-item">
            <span class="stat-label">Available</span>
            <span class="stat-value available">{{ getTotalAvailable(data.live_workers) }}GB</span>
          </div>
        </div>
      </div>
      <!-- Search Section -->
      <div class="search-section">
        <div class="search-container">
          <div class="search-box">
            <i class="search-icon">🔍</i>
            <input
                v-model="searchQuery"
                type="text"
                placeholder="Search workers by ID, hostname, or IP address..."
                class="search-input"
            >
            <div v-if="searchQuery" class="clear-search" @click="clearSearch">✕</div>
          </div>
          <div class="search-stats">
          <span class="search-result-count">{{
              filteredLiveWorkers.length + filteredLostWorkers.length
            }} workers found</span>
          </div>
        </div>
      </div>
      <div class="workers-table" v-if="filteredLiveWorkers.length > 0">
        <div class="table-header">
          <div class="table-row header-row">
            <div class="table-cell status-cell sortable" @click="sortBy('status')">
              Status
              <span class="sort-icon" v-if="sortField === 'status'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell worker-id-cell sortable" @click="sortBy('worker_id')">
              Worker ID
              <span class="sort-icon" v-if="sortField === 'worker_id'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell hostname-cell sortable" @click="sortBy('hostname')">
              Hostname
              <span class="sort-icon" v-if="sortField === 'hostname'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell ip-cell sortable" @click="sortBy('ip_addr')">
              IP Address
              <span class="sort-icon" v-if="sortField === 'ip_addr'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell heartbeat-cell sortable" @click="sortBy('heartbeat')">
              Last Heartbeat
              <span class="sort-icon" v-if="sortField === 'heartbeat'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell capacity-cell sortable" @click="sortBy('capacity')">
              Capacity
              <span class="sort-icon" v-if="sortField === 'capacity'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell available-cell sortable" @click="sortBy('available')">
              Available
              <span class="sort-icon" v-if="sortField === 'available'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell used-cell sortable" @click="sortBy('used')">
              Used
              <span class="sort-icon" v-if="sortField === 'used'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell usage-cell sortable" @click="sortBy('usage')">
              Usage
              <span class="sort-icon" v-if="sortField === 'usage'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
          </div>
        </div>
        <div class="table-body">
          <div v-for="worker in filteredLiveWorkers" :key="worker.address.worker_id" class="table-row worker-row live">
            <div class="table-cell status-cell">
              <div class="status-indicator online">
                <div class="pulse-dot"></div>
              </div>
            </div>
            <div class="table-cell worker-id-cell">
              <span class="worker-id">{{ worker.address.worker_id }}</span>
            </div>
            <div class="table-cell hostname-cell">
              <span class="hostname">{{ worker.address.hostname }}</span>
              <span class="port">:{{ worker.address.rpc_port }}</span>
            </div>
            <div class="table-cell ip-cell">
              <span class="ip-address">{{ worker.address.ip_addr }}</span>
            </div>
            <div class="table-cell heartbeat-cell">
              <span class="heartbeat">{{ formatTimestamp(worker.last_update) }}</span>
            </div>
            <div class="table-cell capacity-cell">
              <span class="capacity">{{ toGB(worker.capacity) }}GB</span>
            </div>
            <div class="table-cell available-cell">
              <span class="available">{{ toGB(worker.available) }}GB</span>
            </div>
            <div class="table-cell used-cell">
              <span class="used">{{ toGB(worker.fs_used) }}GB</span>
            </div>
            <div class="table-cell usage-cell">
              <div class="usage-bar">
                <div class="usage-track">
                  <div class="usage-fill" :style="{ width: getUsagePercentage(worker) + '%' }"></div>
                </div>
                <span class="usage-text">{{ getUsagePercentage(worker) }}%</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div v-else class="empty-state">
        <div class="empty-icon">📡</div>
        <div class="empty-text">No live workers found</div>
      </div>
    </div>

    <!-- Lost Workers Section -->
    <div class="workers-section lost-section">
      <div class="section-header">
        <div class="section-title">
          <div class="title-icon">🔴</div>
          <h2>Lost Workers</h2>
          <div class="worker-count lost">{{ filteredLostWorkers.length }}</div>
        </div>
        <div class="section-stats" v-if="data.lost_workers && data.lost_workers.length > 0">
          <div class="stat-item">
            <span class="stat-label">Lost Capacity</span>
            <span class="stat-value lost">{{ getTotalCapacity(data.lost_workers) }}GB</span>
          </div>
        </div>
      </div>

      <div class="workers-table" v-if="filteredLostWorkers.length > 0">
        <div class="table-header">
          <div class="table-row header-row">
            <div class="table-cell status-cell sortable" @click="sortBy('status')">
              Status
              <span class="sort-icon" v-if="sortField === 'status'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell worker-id-cell sortable" @click="sortBy('worker_id')">
              Worker ID
              <span class="sort-icon" v-if="sortField === 'worker_id'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell hostname-cell sortable" @click="sortBy('hostname')">
              Hostname
              <span class="sort-icon" v-if="sortField === 'hostname'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell ip-cell sortable" @click="sortBy('ip_addr')">
              IP Address
              <span class="sort-icon" v-if="sortField === 'ip_addr'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell heartbeat-cell sortable" @click="sortBy('heartbeat')">
              Last Heartbeat
              <span class="sort-icon" v-if="sortField === 'heartbeat'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell capacity-cell sortable" @click="sortBy('capacity')">
              Capacity
              <span class="sort-icon" v-if="sortField === 'capacity'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell available-cell sortable" @click="sortBy('available')">
              Available
              <span class="sort-icon" v-if="sortField === 'available'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell used-cell sortable" @click="sortBy('used')">
              Used
              <span class="sort-icon" v-if="sortField === 'used'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
            <div class="table-cell usage-cell sortable" @click="sortBy('usage')">
              Usage
              <span class="sort-icon" v-if="sortField === 'usage'">
                {{ sortDirection === 'asc' ? '↑' : '↓' }}
              </span>
            </div>
          </div>
        </div>
        <div class="table-body">
          <div v-for="worker in filteredLostWorkers" :key="worker.address.worker_id" class="table-row worker-row lost">
            <div class="table-cell status-cell">
              <div class="status-indicator offline"></div>
            </div>
            <div class="table-cell worker-id-cell">
              <span class="worker-id">{{ worker.address.worker_id }}</span>
            </div>
            <div class="table-cell hostname-cell">
              <span class="hostname">{{ worker.address.hostname }}</span>
              <span class="port">:{{ worker.address.rpc_port }}</span>
            </div>
            <div class="table-cell ip-cell">
              <span class="ip-address">{{ worker.address.ip_addr }}</span>
            </div>
            <div class="table-cell heartbeat-cell">
              <span class="heartbeat">{{ formatTimestamp(worker.last_update) }}</span>
            </div>
            <div class="table-cell capacity-cell">
              <span class="capacity">{{ toGB(worker.capacity) }}GB</span>
            </div>
            <div class="table-cell available-cell">
              <span class="available">{{ toGB(worker.available) }}GB</span>
            </div>
            <div class="table-cell used-cell">
              <span class="used">{{ toGB(worker.fs_used) }}GB</span>
            </div>
            <div class="table-cell usage-cell">
              <div class="usage-bar">
                <div class="usage-track">
                  <div class="usage-fill" :style="{ width: getUsagePercentage(worker) + '%' }"></div>
                </div>
                <span class="usage-text">{{ getUsagePercentage(worker) }}%</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div v-else class="empty-state">
        <div class="empty-icon">✅</div>
        <div class="empty-text">No lost workers - All systems operational</div>
      </div>
    </div>
  </div>
</template>

<script>
import {fetchWorkersData} from '@/api/client'
import {toGB, formatTimestamp} from '@/utils/utils'
import eventBus from '@/utils/eventBus'

/* eslint-disable vue/multi-word-component-names */
export default {
  name: 'Workers',
  setup() {
    return {
      toGB,
      formatTimestamp
    }
  },
  data() {
    return {
      loading: false,
      data: {},
      live_workers: [],
      lost_workers: [],
      searchQuery: '',
      sortField: '',
      sortDirection: 'asc' // 'asc' or 'desc'
    }
  },
  computed: {
    /**
     * Filter and sort live workers based on search query and sort settings
     */
    filteredLiveWorkers() {
      if (!this.data.live_workers) return []
      
      let filtered = this.data.live_workers
      
      // Apply search filter
      if (this.searchQuery.trim()) {
        const query = this.searchQuery.toLowerCase().trim()
        filtered = filtered.filter(worker =>
            String(worker.address.worker_id).toLowerCase().includes(query) ||
            String(worker.address.hostname).toLowerCase().includes(query) ||
            String(worker.address.ip_addr).toLowerCase().includes(query)
        )
      }
      
      // Apply sorting
      return this.sortWorkers(filtered)
    },

    /**
     * Filter and sort lost workers based on search query and sort settings
     */
    filteredLostWorkers() {
      if (!this.data.lost_workers) return []
      
      let filtered = this.data.lost_workers
      
      // Apply search filter
      if (this.searchQuery.trim()) {
        const query = this.searchQuery.toLowerCase().trim()
        filtered = filtered.filter(worker =>
            String(worker.address.worker_id).toLowerCase().includes(query) ||
            String(worker.address.hostname).toLowerCase().includes(query) ||
            String(worker.address.ip_addr).toLowerCase().includes(query)
        )
      }
      
      // Apply sorting
      return this.sortWorkers(filtered)
    }
  },
  created() {
    this.loading = true
    this.fetchWorkersData()
    this.loading = false
  },
  mounted() {
    // Listen for auto refresh events
    eventBus.on('auto-refresh-trigger', this.handleAutoRefresh)
  },
  beforeUnmount() {
    // Clean up event listeners
    eventBus.off('auto-refresh-trigger', this.handleAutoRefresh)
  },
  methods: {
    /**
     * Handle auto refresh trigger from header component
     */
    handleAutoRefresh() {
      this.fetchWorkersData()
    },

    /**
     * Clear search query
     */
    clearSearch() {
      this.searchQuery = ''
    },

    /**
     * Handle column sorting
     * @param {string} field - The field to sort by
     */
    sortBy(field) {
      if (this.sortField === field) {
        // Toggle direction if same field
        this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc'
      } else {
        // Set new field and default to ascending
        this.sortField = field
        this.sortDirection = 'asc'
      }
    },

    /**
     * Get sort value for a worker based on field
     * @param {object} worker - Worker object
     * @param {string} field - Field to get value for
     * @returns {any} - Sort value
     */
    getSortValue(worker, field) {
      switch (field) {
        case 'status':
          return worker.status || ''
        case 'worker_id':
          return worker.address.worker_id
        case 'hostname':
          return worker.address.hostname || ''
        case 'ip_addr':
          return worker.address.ip_addr || ''
        case 'heartbeat':
          // Convert timestamp to Date object for proper sorting
          return worker.last_update ? new Date(worker.last_update).getTime() : 0
        case 'capacity':
          return worker.capacity || 0
        case 'available':
          return worker.available || 0
        case 'used':
          return worker.fs_used || 0
        case 'usage':
          return this.getUsagePercentage(worker)
        default:
          return ''
      }
    },

    /**
     * Sort workers array
     * @param {array} workers - Array of workers to sort
     * @returns {array} - Sorted workers array
     */
    sortWorkers(workers) {
      if (!this.sortField || !workers) return workers

      return [...workers].sort((a, b) => {
        const aValue = this.getSortValue(a, this.sortField)
        const bValue = this.getSortValue(b, this.sortField)

        let comparison = 0
        if (typeof aValue === 'string' && typeof bValue === 'string') {
          comparison = aValue.localeCompare(bValue)
        } else {
          comparison = aValue - bValue
        }

        return this.sortDirection === 'asc' ? comparison : -comparison
      })
    },

    /**
     * Fetch workers data from API
     */
    fetchWorkersData() {
      fetchWorkersData().then(res => {
        this.data = res.data
      }).catch(err => {
        console.error("fetch browse data error: " + err)
      })
    },
    getTotalCapacity(workers) {
      if (!workers || workers.length === 0) return 0
      return this.toGB(workers.reduce((total, worker) => total + worker.capacity, 0))
    },
    getTotalAvailable(workers) {
      if (!workers || workers.length === 0) return 0
      return this.toGB(workers.reduce((total, worker) => total + worker.available, 0))
    },
    getUsagePercentage(worker) {
      if (!worker.capacity || worker.capacity === 0) return 0
      return Math.round((worker.fs_used / worker.capacity) * 100)
    }
  }
}
</script>

<style lang="scss" scoped>
.workers {
  padding: 2rem;
  background: var(--primary-bg);
  min-height: 100vh;

  .search-section {
    margin-bottom: 1rem;

    .search-container {
      background: linear-gradient(135deg, var(--secondary-bg) 0%, var(--card-bg) 100%);
      border-radius: 16px;
      border: 1px solid var(--border-color);
      padding: 1rem;
      box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);

      .search-box {
        position: relative;
        display: flex;
        align-items: center;
        background: var(--secondary-bg);
        border: 2px solid var(--border-color);
        border-radius: 12px;
        padding: 0.75rem 1rem;
        transition: all 0.3s ease;

        &:focus-within {
          border-color: var(--accent-blue);
          box-shadow: 0 0 20px rgba(0, 212, 255, 0.3);
        }

        .search-icon {
          font-size: 1.2rem;
          color: var(--text-secondary);
          margin-right: 0.75rem;
          filter: grayscale(1);
          transition: filter 0.3s ease;
        }

        .search-input {
          flex: 1;
          background: transparent;
          border: none;
          outline: none;
          color: var(--text-primary);
          font-size: 1rem;
          font-family: 'JetBrains Mono', monospace;

          &::placeholder {
            color: var(--text-secondary);
            opacity: 0.7;
          }
        }

        .clear-search {
          color: var(--text-secondary);
          cursor: pointer;
          padding: 0.25rem;
          border-radius: 50%;
          transition: all 0.3s ease;

          &:hover {
            color: var(--accent-blue);
            background: rgba(0, 212, 255, 0.1);
          }
        }

        &:focus-within .search-icon {
          filter: grayscale(0);
          color: var(--accent-blue);
        }
      }

      .search-stats {
        margin-top: 1rem;
        text-align: center;

        .search-result-count {
          color: var(--text-secondary);
          font-size: 0.9rem;
          font-family: 'JetBrains Mono', monospace;
          text-transform: uppercase;
          letter-spacing: 0.5px;
        }
      }
    }
  }

  .workers-section {
    margin-bottom: 3rem;

    &.lost-section {
      .section-header {
        border-left: 4px solid #ff6b6b;
      }
    }

    .section-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 2rem;
      padding: 1.5rem;
      background: linear-gradient(135deg, var(--secondary-bg) 0%, var(--card-bg) 100%);
      border-radius: 16px;
      border: 1px solid var(--border-color);
      border-left: 4px solid var(--accent-green);
      box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);

      .section-title {
        display: flex;
        align-items: center;
        gap: 1rem;

        .title-icon {
          font-size: 1.5rem;
          filter: drop-shadow(0 0 8px currentColor);
        }

        h2 {
          margin: 0;
          font-size: 1.8rem;
          font-weight: 700;
          color: var(--text-primary);
          font-family: 'JetBrains Mono', monospace;
          letter-spacing: 1px;
          text-transform: uppercase;
        }

        .worker-count {
          background: var(--accent-blue);
          color: white;
          padding: 0.5rem 1rem;
          border-radius: 20px;
          font-weight: 700;
          font-family: 'JetBrains Mono', monospace;
          box-shadow: 0 0 15px rgba(0, 212, 255, 0.5);

          &.lost {
            background: #ff6b6b;
            box-shadow: 0 0 15px rgba(255, 107, 107, 0.5);
          }
        }
      }
    }

    .section-stats {
        display: flex;
        gap: 2rem;

        .stat-item {
          text-align: right;

          .stat-label {
            display: block;
            font-size: 0.8rem;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 0.5rem;
          }

          .stat-value {
            display: block;
            font-size: 1.5rem;
            font-weight: 700;
            color: var(--accent-blue);
            font-family: 'JetBrains Mono', monospace;

            &.available {
              color: var(--accent-green);
            }

            &.lost {
              color: #ff6b6b;
            }
          }
        }
      }
    }

    .workers-table {
      background: var(--card-bg);
      border: 1px solid var(--border-color);
      border-radius: 16px;
      overflow: hidden;
      box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);

      .table-header {
        background: linear-gradient(135deg, var(--secondary-bg) 0%, var(--card-bg) 100%);
        border-bottom: 2px solid var(--border-color);

        .header-row {
          .table-cell {
            font-weight: 700;
            color: var(--text-primary);
            text-transform: uppercase;
            letter-spacing: 1px;
            font-size: 0.8rem;
            padding: 1.25rem 1rem;
            border-right: 1px solid var(--border-color);
            position: relative;

            &:last-child {
              border-right: none;
            }

            &.sortable {
              cursor: pointer;
              user-select: none;
              transition: all 0.2s ease;

              &:hover {
                background: rgba(0, 212, 255, 0.1);
                color: var(--accent-blue);
              }

              .sort-icon {
                margin-left: 0.5rem;
                font-size: 0.9rem;
                color: var(--accent-blue);
                opacity: 0.8;
              }
            }
          }
        }
      }

      .table-body {
        .worker-row {
          border-bottom: 1px solid var(--border-color);
          transition: all 0.3s ease;
          position: relative;

          &::before {
            content: '';
            position: absolute;
            left: 0;
            top: 0;
            bottom: 0;
            width: 4px;
            background: var(--accent-green);
            transition: all 0.3s ease;
          }

          &.lost::before {
            background: #ff6b6b;
          }

          &:hover {
            background: rgba(0, 212, 255, 0.05);

            &.live {
              box-shadow: inset 0 0 20px rgba(0, 255, 127, 0.1);
            }

            &.lost {
              box-shadow: inset 0 0 20px rgba(255, 107, 107, 0.1);
            }
          }

          &:last-child {
            border-bottom: none;
          }
        }
      }

      .table-row {
        display: grid;
        grid-template-columns: minmax(60px, 80px) minmax(100px, 1fr) minmax(120px, 1.5fr) minmax(120px, 1fr) minmax(140px, 1fr) minmax(80px, 0.8fr) minmax(80px, 0.8fr) minmax(80px, 0.8fr) minmax(100px, 1fr);
        align-items: center;
        gap: 0;

        .table-cell {
          padding: 1rem;
          border-right: 1px solid var(--border-color);
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;

          &:last-child {
            border-right: none;
          }
        }
      }

      // Table cell specific styles

      .status-cell {
        display: flex;
        justify-content: center;

        .status-indicator {
          position: relative;
          width: 16px;
          height: 16px;
          border-radius: 50%;

          &.online {
            background: var(--accent-green);
            box-shadow: 0 0 15px var(--accent-green);

            .pulse-dot {
              position: absolute;
              top: 50%;
              left: 50%;
              transform: translate(-50%, -50%);
              width: 100%;
              height: 100%;
              border-radius: 50%;
              background: var(--accent-green);
              animation: pulse 2s infinite;
            }
          }

          &.offline {
            background: #ff6b6b;
            box-shadow: 0 0 15px #ff6b6b;
          }
        }
      }

      .worker-id-cell {
        .worker-id {
          font-weight: 700;
          color: var(--text-primary);
          font-family: 'JetBrains Mono', monospace;
          font-size: 0.95rem;
        }
      }

      .hostname-cell {
        .hostname {
          font-weight: 600;
          color: var(--accent-blue);
          font-family: 'JetBrains Mono', monospace;
          display: block;
        }

        .port {
          font-size: 0.8rem;
          color: var(--text-secondary);
          font-family: 'JetBrains Mono', monospace;
        }
      }

      .ip-cell {
        .ip-address {
          font-family: 'JetBrains Mono', monospace;
          color: var(--text-primary);
          font-size: 0.9rem;
        }
      }

      .heartbeat-cell {
        .heartbeat {
          font-family: 'JetBrains Mono', monospace;
          color: var(--text-secondary);
          font-size: 0.8rem;
        }
      }

      .capacity-cell, .available-cell, .used-cell {
        text-align: center;

        .capacity, .available, .used {
          font-weight: 600;
          font-family: 'JetBrains Mono', monospace;
          font-size: 0.9rem;
        }

        .capacity {
          color: var(--text-primary);
        }

        .available {
          color: var(--accent-green);
        }

        .used {
          color: #ff9500;
        }
      }

      .usage-cell {
        white-space: normal;
        
        .usage-bar {
          display: flex;
          align-items: center;
          gap: 0.5rem;

          .usage-track {
            flex: 1;
            height: 6px;
            background: var(--secondary-bg);
            border-radius: 3px;
            overflow: hidden;
            border: 1px solid var(--border-color);
            min-width: 40px;

            .usage-fill {
              height: 100%;
              background: linear-gradient(90deg, var(--accent-green) 0%, #ff9500 70%, #ff6b6b 100%);
              border-radius: 3px;
              transition: width 0.3s ease;
            }
          }

          .usage-text {
            font-size: 0.75rem;
            font-weight: 600;
            color: var(--text-secondary);
            font-family: 'JetBrains Mono', monospace;
            min-width: 35px;
            flex-shrink: 0;
          }
        }
        }
        
        // Responsive design for smaller screens
        @media (max-width: 1200px) {
            .table-row {
                grid-template-columns: minmax(50px, 60px) minmax(80px, 1fr) minmax(100px, 1.2fr) minmax(100px, 0.8fr) minmax(120px, 0.8fr) minmax(70px, 0.6fr) minmax(70px, 0.6fr) minmax(70px, 0.6fr) minmax(80px, 0.8fr);
            }
        }
        
        @media (max-width: 768px) {
            .table-row {
                grid-template-columns: 40px 1fr 1fr 1fr 1fr 60px 60px 60px 80px;
                font-size: 0.85rem;
                
                .table-cell {
                    padding: 0.75rem 0.5rem;
                }
            }
        }
        
        .empty-state {
      text-align: center;
      padding: 4rem 2rem;
      background: var(--card-bg);
      border: 1px solid var(--border-color);
      border-radius: 16px;
      box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);

      .empty-icon {
        font-size: 4rem;
        margin-bottom: 1rem;
        filter: grayscale(0.7);
      }

      .empty-text {
        font-size: 1.2rem;
        color: var(--text-secondary);
        font-weight: 500;
      }
    }
  }
}

@keyframes pulse {
  0% {
    transform: translate(-50%, -50%) scale(1);
    opacity: 1;
  }
  100% {
    transform: translate(-50%, -50%) scale(2);
    opacity: 0;
  }
}
</style>