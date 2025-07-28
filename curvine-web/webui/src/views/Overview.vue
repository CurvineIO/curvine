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
    <div class="overview">
        <div class="container-fluid">
            <!-- System Status Header -->
            <div class="status-header">
                <div class="status-card">
                    <div class="status-icon">
                        <div class="pulse-ring"></div>
                        <div class="status-dot" :class="{ 'active': data.master_state === 'Active' }"></div>
                    </div>
                    <div class="status-info">
<!--                        <h2 class="cluster-name">CURVINE CLUSTER</h2>-->
                        <p class="cluster-status">{{ data.master_state || 'Unknown' }}</p>
                        <p class="cluster-id">ID: {{ data.cluster_id || 'N/A' }}</p>
                    </div>
                </div>
                <div class="uptime-card">
                    <div class="uptime-label">UPTIME</div>
                    <div class="uptime-value">{{ formatUptime(data.start_time) }}</div>
                </div>
            </div>

            <!-- Main Dashboard Grid -->
            <div class="dashboard-grid">
                <!-- System Information -->
                <div class="tech-card system-info">
                    <div class="card-header">
                        <h3 class="card-title">
                            <i class="title-icon">🖥️</i>
                            SYSTEM INFORMATION
                        </h3>
                    </div>
                    <div class="card-content">
                        <div class="info-grid">
                            <div class="info-item">
                                <span class="info-label">Master Address</span>
                                <span class="info-value">{{ data.master_addr || 'N/A' }}</span>
                            </div>
                            <div class="info-item">
                                <span class="info-label">Version</span>
                                <span class="info-value version-badge">v1.0.0</span>
                            </div>
                            <div class="info-item">
                                <span class="info-label">Started</span>
                                <span class="info-value">{{ formatDate(data.start_time) }}</span>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Workers Status -->
                <div class="tech-card workers-status">
                    <div class="card-header">
                        <h3 class="card-title">
                            <i class="title-icon">⚡</i>
                            WORKERS STATUS
                        </h3>
                    </div>
                    <div class="card-content">
                        <div class="workers-grid">
                            <div class="worker-stat">
                                <div class="stat-number running">{{ data.live_workers || 0 }}</div>
                                <div class="stat-label">Running</div>
                                <div class="stat-bar">
                                    <div class="stat-fill running" :style="{ width: getWorkerPercentage('running') + '%' }"></div>
                                </div>
                            </div>
                            <div class="worker-stat">
                                <div class="stat-number lost">{{ data.lost_workers || 0 }}</div>
                                <div class="stat-label">Lost</div>
                                <div class="stat-bar">
                                    <div class="stat-fill lost" :style="{ width: getWorkerPercentage('lost') + '%' }"></div>
                                </div>
                            </div>
                            <div class="worker-stat">
                                <div class="stat-number capacity">{{ toGB(data.capacity) || 0 }}GB</div>
                                <div class="stat-label">Total Capacity</div>
                                <div class="stat-bar">
                                    <div class="stat-fill capacity" style="width: 100%"></div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Storage Usage -->
                <div class="tech-card storage-usage">
                    <div class="card-header">
                        <h3 class="card-title">
                            <i class="title-icon">💾</i>
                            STORAGE METRICS
                        </h3>
                    </div>
                    <div class="card-content">
                        <div class="storage-grid">
                            <div class="storage-item">
                                <div class="storage-icon">📁</div>
                                <div class="storage-info">
                                    <div class="storage-value">{{ formatNumber(data.files_total) }}</div>
                                    <div class="storage-label">Total Files</div>
                                </div>
                            </div>
                            <div class="storage-item">
                                <div class="storage-icon">📂</div>
                                <div class="storage-info">
                                    <div class="storage-value">{{ formatNumber(data.dir_total) }}</div>
                                    <div class="storage-label">Directories</div>
                                </div>
                            </div>
                            <div class="storage-item">
                                <div class="storage-icon">🧊</div>
                                <div class="storage-info">
                                    <div class="storage-value">0</div>
                                    <div class="storage-label">Unique Blocks</div>
                                </div>
                            </div>
                            <div class="storage-item">
                                <div class="storage-icon">🔄</div>
                                <div class="storage-info">
                                    <div class="storage-value">0</div>
                                    <div class="storage-label">Replica Blocks</div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Capacity Usage -->
                <div class="tech-card capacity-usage">
                    <div class="card-header">
                        <h3 class="card-title">
                            <i class="title-icon">📊</i>
                            CAPACITY UTILIZATION
                        </h3>
                    </div>
                    <div class="card-content">
                        <div class="capacity-chart">
                            <div class="capacity-ring">
                                <svg viewBox="0 0 100 100" class="capacity-svg">
                                    <circle cx="50" cy="50" r="45" class="capacity-bg"></circle>
                                    <circle cx="50" cy="50" r="45" class="capacity-fill" 
                                            :stroke-dasharray="getCapacityDashArray()" 
                                            :stroke-dashoffset="getCapacityDashOffset()"></circle>
                                </svg>
                                <div class="capacity-center">
                                    <div class="capacity-percentage">{{ getCapacityPercentage() }}%</div>
                                    <div class="capacity-label">Used</div>
                                </div>
                            </div>
                            <div class="capacity-details">
                                <div class="capacity-detail">
                                    <span class="detail-label">Available</span>
                                    <span class="detail-value available">{{ toGB(data.available) }}GB</span>
                                </div>
                                <div class="capacity-detail">
                                    <span class="detail-label">Used</span>
                                    <span class="detail-value used">{{ toGB(data.fs_used) }}GB</span>
                                </div>
                                <div class="capacity-detail">
                                    <span class="detail-label">Total</span>
                                    <span class="detail-value total">{{ toGB(data.capacity) }}GB</span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
import { fetchOverviewData } from '@/api/client'
import eventBus from '@/utils/eventBus'

export default {
    name: 'OverviewPage',
  data() {
    return {
      data: {
        master_addr: '',
        cluster_id: '',
        start_time: '',
        master_state: '',
        live_workers: 0,
        lost_workers: 0,
        capacity: 0,
        available: 0,
        fs_used: 0,
        files_total: 0,
        dir_total: 0
      },
      loading: false
    }
  },
  created() {
    this.fetchData()
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
      this.fetchData()
    },
    
    /**
     * Fetch overview data from API
     */
    fetchData() {
      this.loading = true
      fetchOverviewData().then(response => {
        this.data = response.data
        this.loading = false
      }).catch(() => {
        this.loading = false
      })
    },
    toGB(bytes) {
      if (!bytes) return 0
      return (bytes / 1024 / 1024 / 1024).toFixed(2)
    },
    formatNumber(num) {
      if (!num) return '0'
      return num.toLocaleString()
    },
    formatBytes(bytes) {
      if (!bytes) return '0 B'
      const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
      const i = Math.floor(Math.log(bytes) / Math.log(1024))
      return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i]
    },
    formatDate(dateString) {
      if (!dateString) return 'N/A'
      return new Date(dateString).toLocaleString()
    },
    formatUptime(startTime) {
      if (!startTime) return 'N/A'
      const start = new Date(startTime)
      const now = new Date()
      const diff = now - start
      const days = Math.floor(diff / (1000 * 60 * 60 * 24))
      const hours = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60))
      const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60))
      return `${days}d ${hours}h ${minutes}m`
    },
    getWorkerPercentage(type) {
      const total = (this.data.live_workers || 0) + (this.data.lost_workers || 0)
      if (total === 0) return 0
      
      if (type === 'running') {
        return Math.round((this.data.live_workers / total) * 100)
      } else if (type === 'lost') {
        return Math.round((this.data.lost_workers / total) * 100)
      }
      return 100
    },
    getCapacityPercentage() {
      if (!this.data.capacity || this.data.capacity === 0) return 0
      return Math.round((this.data.fs_used / this.data.capacity) * 100)
    },
    getCapacityDashArray() {
      const circumference = 2 * Math.PI * 45
      return `${circumference} ${circumference}`
    },
    getCapacityDashOffset() {
      const circumference = 2 * Math.PI * 45
      const percentage = this.getCapacityPercentage()
      return circumference - (percentage / 100) * circumference
    }
  }
}
</script>

<style lang="scss" scoped>
.overview {
    padding: 2rem;
    background: var(--primary-bg);
    min-height: 100vh;
    
    .status-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 2rem;
        padding: 1.5rem;
        background: linear-gradient(135deg, var(--secondary-bg) 0%, var(--card-bg) 100%);
        border-radius: 16px;
        border: 1px solid var(--border-color);
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        
        .status-card {
            display: flex;
            align-items: center;
            gap: 1.5rem;
            
            .status-icon {
                position: relative;
                
                .pulse-ring {
                    position: absolute;
                    top: 50%;
                    left: 50%;
                    transform: translate(-50%, -50%);
                    width: 60px;
                    height: 60px;
                    border: 2px solid var(--accent-blue);
                    border-radius: 50%;
                    animation: pulse-ring 2s infinite;
                }
                
                .status-dot {
                    width: 20px;
                    height: 20px;
                    border-radius: 50%;
                    background: var(--text-secondary);
                    transition: all 0.3s ease;
                    
                    &.active {
                        background: var(--accent-green);
                        box-shadow: 0 0 20px var(--accent-green);
                    }
                }
            }
            
            .status-info {
                .cluster-name {
                    font-size: 2rem;
                    font-weight: 700;
                    color: var(--text-primary);
                    margin: 0;
                    font-family: 'JetBrains Mono', monospace;
                    letter-spacing: 2px;
                    text-shadow: 0 0 10px var(--accent-blue);
                }
                
                .cluster-status {
                    font-size: 1.1rem;
                    color: var(--accent-green);
                    margin: 0.5rem 0;
                    font-weight: 600;
                    text-transform: uppercase;
                }
                
                .cluster-id {
                    font-size: 0.9rem;
                    color: var(--text-secondary);
                    margin: 0;
                    font-family: 'JetBrains Mono', monospace;
                }
            }
        }
        
        .uptime-card {
            text-align: right;
            
            .uptime-label {
                font-size: 0.8rem;
                color: var(--text-secondary);
                text-transform: uppercase;
                letter-spacing: 1px;
                margin-bottom: 0.5rem;
            }
            
            .uptime-value {
                font-size: 1.5rem;
                font-weight: 700;
                color: var(--accent-blue);
                font-family: 'JetBrains Mono', monospace;
            }
        }
    }
    
    .dashboard-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
        gap: 2rem;
        
        .tech-card {
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 0;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
            transition: all 0.3s ease;
            overflow: hidden;
            
            &:hover {
                transform: translateY(-4px);
                box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4), 0 0 20px rgba(0, 212, 255, 0.2);
            }
            
            .card-header {
                background: linear-gradient(135deg, var(--secondary-bg) 0%, var(--border-color) 100%);
                padding: 1.5rem;
                border-bottom: 1px solid var(--border-color);
                
                .card-title {
                    display: flex;
                    align-items: center;
                    gap: 0.75rem;
                    margin: 0;
                    font-size: 1.1rem;
                    font-weight: 600;
                    color: var(--accent-blue);
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    
                    .title-icon {
                        font-size: 1.3rem;
                        filter: drop-shadow(0 0 5px var(--accent-blue));
                    }
                }
            }
            
            .card-content {
                padding: 1.5rem;
            }
        }
        
        .system-info {
            .info-grid {
                display: flex;
                flex-direction: column;
                gap: 1rem;
                
                .info-item {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    padding: 1rem;
                    background: var(--secondary-bg);
                    border-radius: 8px;
                    border: 1px solid var(--border-color);
                    
                    .info-label {
                        color: var(--text-secondary);
                        font-weight: 500;
                        text-transform: uppercase;
                        font-size: 0.85rem;
                        letter-spacing: 0.5px;
                    }
                    
                    .info-value {
                        color: var(--text-primary);
                        font-weight: 600;
                        font-family: 'JetBrains Mono', monospace;
                        
                        &.version-badge {
                            background: var(--gradient-primary);
                            padding: 0.25rem 0.75rem;
                            border-radius: 20px;
                            font-size: 0.8rem;
                            color: white;
                        }
                    }
                }
            }
        }
        
        .workers-status {
            .workers-grid {
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 1rem;
                
                .worker-stat {
                    text-align: center;
                    padding: 1rem;
                    background: var(--secondary-bg);
                    border-radius: 12px;
                    border: 1px solid var(--border-color);
                    
                    .stat-number {
                        font-size: 2rem;
                        font-weight: 700;
                        font-family: 'JetBrains Mono', monospace;
                        margin-bottom: 0.5rem;
                        
                        &.running {
                            color: var(--accent-green);
                            text-shadow: 0 0 10px var(--accent-green);
                        }
                        
                        &.lost {
                            color: #ff6b6b;
                            text-shadow: 0 0 10px #ff6b6b;
                        }
                        
                        &.capacity {
                            color: var(--accent-blue);
                            text-shadow: 0 0 10px var(--accent-blue);
                        }
                    }
                    
                    .stat-label {
                        color: var(--text-secondary);
                        font-size: 0.8rem;
                        text-transform: uppercase;
                        letter-spacing: 0.5px;
                        margin-bottom: 1rem;
                    }
                    
                    .stat-bar {
                        height: 4px;
                        background: var(--border-color);
                        border-radius: 2px;
                        overflow: hidden;
                        
                        .stat-fill {
                            height: 100%;
                            transition: width 0.3s ease;
                            
                            &.running {
                                background: var(--accent-green);
                                box-shadow: 0 0 10px var(--accent-green);
                            }
                            
                            &.lost {
                                background: #ff6b6b;
                                box-shadow: 0 0 10px #ff6b6b;
                            }
                            
                            &.capacity {
                                background: var(--accent-blue);
                                box-shadow: 0 0 10px var(--accent-blue);
                            }
                        }
                    }
                }
            }
        }
        
        .storage-usage {
            .storage-grid {
                display: grid;
                grid-template-columns: repeat(2, 1fr);
                gap: 1rem;
                
                .storage-item {
                    display: flex;
                    align-items: center;
                    gap: 1rem;
                    padding: 1rem;
                    background: var(--secondary-bg);
                    border-radius: 12px;
                    border: 1px solid var(--border-color);
                    transition: all 0.3s ease;
                    
                    &:hover {
                        background: rgba(0, 212, 255, 0.05);
                        border-color: var(--accent-blue);
                    }
                    
                    .storage-icon {
                        font-size: 2rem;
                        filter: grayscale(0.7);
                        transition: filter 0.3s ease;
                    }
                    
                    .storage-info {
                        .storage-value {
                            font-size: 1.5rem;
                            font-weight: 700;
                            color: var(--text-primary);
                            font-family: 'JetBrains Mono', monospace;
                        }
                        
                        .storage-label {
                            color: var(--text-secondary);
                            font-size: 0.8rem;
                            text-transform: uppercase;
                            letter-spacing: 0.5px;
                        }
                    }
                    
                    &:hover .storage-icon {
                        filter: grayscale(0);
                    }
                }
            }
        }
        
        .capacity-usage {
            .capacity-chart {
                display: flex;
                align-items: center;
                gap: 2rem;
                
                .capacity-ring {
                    position: relative;
                    width: 150px;
                    height: 150px;
                    
                    .capacity-svg {
                        width: 100%;
                        height: 100%;
                        transform: rotate(-90deg);
                        
                        .capacity-bg {
                            fill: none;
                            stroke: var(--border-color);
                            stroke-width: 8;
                        }
                        
                        .capacity-fill {
                            fill: none;
                            stroke: url(#capacityGradient);
                            stroke-width: 8;
                            stroke-linecap: round;
                            transition: stroke-dashoffset 0.5s ease;
                        }
                    }
                    
                    .capacity-center {
                        position: absolute;
                        top: 50%;
                        left: 50%;
                        transform: translate(-50%, -50%);
                        text-align: center;
                        
                        .capacity-percentage {
                            font-size: 1.8rem;
                            font-weight: 700;
                            color: var(--accent-blue);
                            font-family: 'JetBrains Mono', monospace;
                        }
                        
                        .capacity-label {
                            font-size: 0.8rem;
                            color: var(--text-secondary);
                            text-transform: uppercase;
                        }
                    }
                }
                
                .capacity-details {
                    flex: 1;
                    
                    .capacity-detail {
                        display: flex;
                        justify-content: space-between;
                        align-items: center;
                        padding: 0.75rem 0;
                        border-bottom: 1px solid var(--border-color);
                        
                        &:last-child {
                            border-bottom: none;
                        }
                        
                        .detail-label {
                            color: var(--text-secondary);
                            font-weight: 500;
                            text-transform: uppercase;
                            font-size: 0.85rem;
                        }
                        
                        .detail-value {
                            font-weight: 600;
                            font-family: 'JetBrains Mono', monospace;
                            
                            &.available {
                                color: var(--accent-green);
                            }
                            
                            &.used {
                                color: #ff6b6b;
                            }
                            
                            &.total {
                                color: var(--accent-blue);
                            }
                        }
                    }
                }
            }
        }
    }
}

@keyframes pulse-ring {
    0% {
        transform: translate(-50%, -50%) scale(0.8);
        opacity: 1;
    }
    100% {
        transform: translate(-50%, -50%) scale(1.2);
        opacity: 0;
    }
}

// Add gradient definition for capacity ring
.capacity-svg {
    defs {
        linearGradient#capacityGradient {
            stop:nth-child(1) {
                stop-color: var(--accent-blue);
            }
            stop:nth-child(2) {
                stop-color: var(--accent-purple);
            }
        }
    }
}
</style>