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
    <div v-loading="loading" class="browse-page">
        <!-- Path Navigation -->
        <div class="path-navigation">
            <div class="nav-header">
                <div class="nav-title">
                    <div class="title-icon">📁</div>
                    <h2>File System Browser</h2>
                </div>
                <div class="nav-controls">
                    <a class="nav-btn root-btn" href="/browse?path=/">
                        <span class="btn-icon">🏠</span>
                        <span class="btn-text">Root</span>
                    </a>
                </div>
            </div>
            
            <div class="path-input-section">
                <div class="input-group">
                    <div class="input-label">
                        <span class="label-icon">📍</span>
                        <span class="label-text">Current Path</span>
                    </div>
                    <div class="input-wrapper">
                        <input 
                            id="browsePath" 
                            v-model="browsePath" 
                            type="text" 
                            class="path-input"
                            placeholder="Enter file system path..."
                        >
                        <a class="go-btn" :href="pathLink('/browse', browsePath)">
                            <span class="go-icon">🚀</span>
                            <span class="go-text">Navigate</span>
                        </a>
                    </div>
                </div>
            </div>
        </div>
        
        <!-- File Browser Grid -->
        <div class="file-browser">
            <div class="browser-header">
                <div class="header-info">
                    <span class="item-count">{{ data.length }} items</span>
                    <span class="current-path">{{ browsePath }}</span>
                </div>
                <div class="view-options">
                    <div class="view-toggle active">
                        <span class="toggle-icon">⊞</span>
                        <span class="toggle-text">Grid View</span>
                    </div>
                </div>
            </div>
            
            <div class="files-grid" v-if="data && data.length > 0">
                <div 
                    v-for="item in data" 
                    :key="item.id" 
                    class="file-item"
                    :class="{ 'is-directory': item.is_dir, 'is-file': !item.is_dir }"
                >
                    <div class="file-header">
                        <div class="file-icon">
                            <div v-if="item.is_dir" class="icon-wrapper directory">
                                <svg class="icon" viewBox="0 0 512 512">
                                    <path fill="currentColor" d="M464 128H272l-54.63-54.63c-6-6-14.14-9.37-22.63-9.37H48C21.49 64 0 85.49 0 112v288c0 26.51 21.49 48 48 48h416c26.51 0 48-21.49 48-48V176c0-26.51-21.49-48-48-48zm0 272H48V112h140.12l54.63 54.63c6 6 14.14 9.37 22.63 9.37H464v224z"/>
                                </svg>
                                <div class="icon-glow"></div>
                            </div>
                            <div v-else class="icon-wrapper file">
                                <svg class="icon" viewBox="0 0 384 512">
                                    <path fill="currentColor" d="M369.9 97.9L286 14C277 5 264.8-.1 252.1-.1H48C21.5 0 0 21.5 0 48v416c0 26.5 21.5 48 48 48h288c26.5 0 48-21.5 48-48V131.9c0-12.7-5.1-25-14.1-34zM332.1 128H256V51.9l76.1 76.1zM48 464V48h160v104c0 13.3 10.7 24 24 24h104v288H48z"/>
                                </svg>
                                <div class="icon-glow"></div>
                            </div>
                        </div>
                        
                        <div class="file-type-badge">
                            <span v-if="item.is_dir" class="badge directory">DIR</span>
                            <span v-else class="badge file">FILE</span>
                        </div>
                    </div>
                    
                    <div class="file-name">
                        <a 
                            v-if="item.is_dir" 
                            :href="pathLink('/browse', item.path.replace('//', '/'))"
                            class="file-link directory"
                        >
                            {{ getFileName(item.path) }}
                        </a>
                        <a 
                            v-else 
                            :href="pathLink('/blocks', item.path.replace('//', '/'))"
                            class="file-link file"
                        >
                            {{ getFileName(item.path) }}
                        </a>
                    </div>
                    
                    <div class="file-details">
                        <div class="detail-row" v-if="!item.is_dir">
                            <span class="detail-label">Size</span>
                            <span class="detail-value">{{ formatBytes(item.len) }}</span>
                        </div>
                        <div class="detail-row" v-if="!item.is_dir">
                            <span class="detail-label">Block Size</span>
                            <span class="detail-value">{{ formatBytes(item.block_size) }}</span>
                        </div>
                        <div class="detail-row" v-if="item.is_dir">
                            <span class="detail-label">Children</span>
                            <span class="detail-value">{{ item.children_num || 0 }}</span>
                        </div>
                        <div class="detail-row" v-if="!item.is_dir">
                            <span class="detail-label">Replicas</span>
                            <span class="detail-value">{{ item.replicas || 0 }}</span>
                        </div>
                    </div>
                    
                    <div class="file-metadata">
                        <div class="metadata-row">
                            <span class="metadata-label">Modified</span>
                            <span class="metadata-value">{{ formatTimestamp(item.mtime) }}</span>
                        </div>
                        <div class="metadata-row" v-if="!item.is_dir">
                            <span class="metadata-label">Complete</span>
                            <span class="metadata-value" :class="{ 'complete': item.is_complete, 'incomplete': !item.is_complete }">
                                {{ item.is_complete ? '✓' : '✗' }}
                            </span>
                        </div>
                        <div class="metadata-row" v-if="!item.is_dir && item.storage_policy">
                            <span class="metadata-label">Storage</span>
                            <span class="metadata-value storage-type">{{ item.storage_policy.storage_type || 'N/A' }}</span>
                        </div>
                    </div>
                </div>
            </div>
            
            <div v-else class="empty-state">
                <div class="empty-icon">📂</div>
                <div class="empty-text">Directory is empty</div>
                <div class="empty-subtext">No files or folders found in this location</div>
            </div>
        </div>
    </div>
</template>

<script>
import { fetchBrowseData } from '@/api/client'
import { formatBytes, formatTimestamp, pathLink } from '@/utils/utils'
import eventBus from '@/utils/eventBus'

/* eslint-disable vue/multi-word-component-names */
export default {
    name: 'Browse',
    setup() {
        return {
            formatBytes,
            formatTimestamp,
            pathLink
        }
    },
    data() {
        return {
            loading: false,
            data: [],
            browsePath: this.$route.query.path || '/'
        }
    },
    created() {
        this.loading = true
        this.fetchBrowseData()
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
            this.fetchBrowseData()
        },
        
        /**
         * Fetch browse data from API
         */
        fetchBrowseData() {
            fetchBrowseData({ path: this.browsePath }).then(res => {
                this.data = res.data
            }).catch(err => {
                console.error("fetch browse data error: " + err)
            })
        },
        getFileName(path) {
            if (!path) return ''
            const parts = path.split('/')
            return parts[parts.length - 1] || parts[parts.length - 2] || path
        }
    },
}
</script>

<style lang="scss" scoped>
.browse-page {
    padding: 2rem;
    background: var(--primary-bg);
    min-height: 100vh;
    
    .path-navigation {
        margin-bottom: 2rem;
        
        .nav-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.5rem;
            padding: 1.5rem;
            background: linear-gradient(135deg, var(--secondary-bg) 0%, var(--card-bg) 100%);
            border-radius: 16px;
            border: 1px solid var(--border-color);
            border-left: 4px solid var(--accent-blue);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
            
            .nav-title {
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
            }
            
            .nav-controls {
                .root-btn {
                    display: flex;
                    align-items: center;
                    gap: 0.5rem;
                    padding: 0.75rem 1.5rem;
                    background: var(--accent-green);
                    color: white;
                    text-decoration: none;
                    border-radius: 12px;
                    font-weight: 600;
                    font-family: 'JetBrains Mono', monospace;
                    transition: all 0.3s ease;
                    box-shadow: 0 0 15px rgba(0, 255, 127, 0.3);
                    
                    &:hover {
                        transform: translateY(-2px);
                        box-shadow: 0 4px 20px rgba(0, 255, 127, 0.5);
                        color: white;
                        text-decoration: none;
                    }
                    
                    .btn-icon {
                        font-size: 1.1rem;
                    }
                }
            }
        }
        
        .path-input-section {
            .input-group {
                background: var(--card-bg);
                border: 1px solid var(--border-color);
                border-radius: 16px;
                padding: 1.5rem;
                box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
                
                .input-label {
                    display: flex;
                    align-items: center;
                    gap: 0.5rem;
                    margin-bottom: 1rem;
                    
                    .label-icon {
                        font-size: 1.2rem;
                    }
                    
                    .label-text {
                        font-weight: 600;
                        color: var(--text-secondary);
                        text-transform: uppercase;
                        font-size: 0.9rem;
                        letter-spacing: 0.5px;
                    }
                }
                
                .input-wrapper {
                    display: flex;
                    gap: 1rem;
                    align-items: center;
                    
                    .path-input {
                        flex: 1;
                        padding: 1rem 1.5rem;
                        background: var(--secondary-bg);
                        border: 1px solid var(--border-color);
                        border-radius: 12px;
                        color: var(--text-primary);
                        font-family: 'JetBrains Mono', monospace;
                        font-size: 1rem;
                        transition: all 0.3s ease;
                        
                        &:focus {
                            outline: none;
                            border-color: var(--accent-blue);
                            box-shadow: 0 0 0 3px rgba(0, 212, 255, 0.2);
                        }
                        
                        &::placeholder {
                            color: var(--text-secondary);
                            opacity: 0.7;
                        }
                    }
                    
                    .go-btn {
                        display: flex;
                        align-items: center;
                        gap: 0.5rem;
                        padding: 1rem 1.5rem;
                        background: var(--accent-blue);
                        color: white;
                        text-decoration: none;
                        border-radius: 12px;
                        font-weight: 600;
                        font-family: 'JetBrains Mono', monospace;
                        transition: all 0.3s ease;
                        box-shadow: 0 0 15px rgba(0, 212, 255, 0.3);
                        
                        &:hover {
                            transform: translateY(-2px);
                            box-shadow: 0 4px 20px rgba(0, 212, 255, 0.5);
                            color: white;
                            text-decoration: none;
                        }
                        
                        .go-icon {
                            font-size: 1.1rem;
                        }
                    }
                }
            }
        }
    }
    
    .file-browser {
        .browser-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.5rem;
            padding: 1rem 1.5rem;
            background: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
            
            .header-info {
                display: flex;
                align-items: center;
                gap: 2rem;
                
                .item-count {
                    font-weight: 600;
                    color: var(--accent-blue);
                    font-family: 'JetBrains Mono', monospace;
                }
                
                .current-path {
                    font-family: 'JetBrains Mono', monospace;
                    color: var(--text-secondary);
                    font-size: 0.9rem;
                }
            }
            
            .view-options {
                .view-toggle {
                    display: flex;
                    align-items: center;
                    gap: 0.5rem;
                    padding: 0.5rem 1rem;
                    background: var(--accent-green);
                    color: white;
                    border-radius: 8px;
                    font-size: 0.9rem;
                    font-weight: 600;
                    
                    .toggle-icon {
                        font-size: 1rem;
                    }
                }
            }
        }
        
        .files-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
            gap: 1.5rem;
            
            .file-item {
                background: var(--card-bg);
                border: 1px solid var(--border-color);
                border-radius: 16px;
                padding: 1.5rem;
                box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
                transition: all 0.3s ease;
                position: relative;
                overflow: hidden;
                
                &::before {
                    content: '';
                    position: absolute;
                    top: 0;
                    left: 0;
                    right: 0;
                    height: 4px;
                    background: var(--accent-blue);
                    transition: all 0.3s ease;
                }
                
                &.is-directory::before {
                    background: var(--accent-green);
                }
                
                &:hover {
                    transform: translateY(-4px);
                    box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4);
                    
                    &.is-directory {
                        box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4), 0 0 20px rgba(0, 255, 127, 0.3);
                    }
                    
                    &.is-file {
                        box-shadow: 0 12px 40px rgba(0, 0, 0, 0.4), 0 0 20px rgba(0, 212, 255, 0.3);
                    }
                }
                
                .file-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 1rem;
                    
                    .file-icon {
                        .icon-wrapper {
                            position: relative;
                            width: 48px;
                            height: 48px;
                            display: flex;
                            align-items: center;
                            justify-content: center;
                            border-radius: 12px;
                            
                            &.directory {
                                background: rgba(0, 255, 127, 0.1);
                                border: 1px solid rgba(0, 255, 127, 0.3);
                                
                                .icon {
                                    width: 24px;
                                    height: 24px;
                                    color: var(--accent-green);
                                }
                                
                                .icon-glow {
                                    position: absolute;
                                    inset: 0;
                                    border-radius: 12px;
                                    background: radial-gradient(circle, rgba(0, 255, 127, 0.2) 0%, transparent 70%);
                                }
                            }
                            
                            &.file {
                                background: rgba(0, 212, 255, 0.1);
                                border: 1px solid rgba(0, 212, 255, 0.3);
                                
                                .icon {
                                    width: 24px;
                                    height: 24px;
                                    color: var(--accent-blue);
                                }
                                
                                .icon-glow {
                                    position: absolute;
                                    inset: 0;
                                    border-radius: 12px;
                                    background: radial-gradient(circle, rgba(0, 212, 255, 0.2) 0%, transparent 70%);
                                }
                            }
                        }
                    }
                    
                    .file-type-badge {
                        .badge {
                            padding: 0.25rem 0.75rem;
                            border-radius: 20px;
                            font-size: 0.7rem;
                            font-weight: 700;
                            letter-spacing: 0.5px;
                            
                            &.directory {
                                background: var(--accent-green);
                                color: white;
                                box-shadow: 0 0 10px rgba(0, 255, 127, 0.3);
                            }
                            
                            &.file {
                                background: var(--accent-blue);
                                color: white;
                                box-shadow: 0 0 10px rgba(0, 212, 255, 0.3);
                            }
                        }
                    }
                }
                
                .file-name {
                    margin-bottom: 1.5rem;
                    
                    .file-link {
                        display: block;
                        font-size: 1.1rem;
                        font-weight: 600;
                        text-decoration: none;
                        font-family: 'JetBrains Mono', monospace;
                        word-break: break-all;
                        transition: all 0.3s ease;
                        
                        &.directory {
                            color: var(--accent-green);
                            
                            &:hover {
                                color: var(--accent-green);
                                text-shadow: 0 0 8px rgba(0, 255, 127, 0.5);
                                text-decoration: none;
                            }
                        }
                        
                        &.file {
                            color: var(--accent-blue);
                            
                            &:hover {
                                color: var(--accent-blue);
                                text-shadow: 0 0 8px rgba(0, 212, 255, 0.5);
                                text-decoration: none;
                            }
                        }
                    }
                }
                
                .file-details {
                    margin-bottom: 1rem;
                    
                    .detail-row {
                        display: flex;
                        justify-content: space-between;
                        align-items: center;
                        padding: 0.5rem 0;
                        border-bottom: 1px solid var(--border-color);
                        
                        &:last-child {
                            border-bottom: none;
                        }
                        
                        .detail-label {
                            color: var(--text-secondary);
                            font-weight: 500;
                            font-size: 0.8rem;
                            text-transform: uppercase;
                            letter-spacing: 0.5px;
                        }
                        
                        .detail-value {
                            color: var(--text-primary);
                            font-weight: 600;
                            font-family: 'JetBrains Mono', monospace;
                            font-size: 0.9rem;
                        }
                    }
                }
                
                .file-metadata {
                    .metadata-row {
                        display: flex;
                        justify-content: space-between;
                        align-items: center;
                        padding: 0.25rem 0;
                        
                        .metadata-label {
                            color: var(--text-secondary);
                            font-size: 0.7rem;
                            text-transform: uppercase;
                            letter-spacing: 0.5px;
                        }
                        
                        .metadata-value {
                            font-family: 'JetBrains Mono', monospace;
                            font-size: 0.8rem;
                            color: var(--text-primary);
                            
                            &.complete {
                                color: var(--accent-green);
                                font-weight: 700;
                            }
                            
                            &.incomplete {
                                color: #ff6b6b;
                                font-weight: 700;
                            }
                            
                            &.storage-type {
                                background: var(--secondary-bg);
                                padding: 0.2rem 0.5rem;
                                border-radius: 4px;
                                font-size: 0.7rem;
                                text-transform: uppercase;
                            }
                        }
                    }
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
                color: var(--text-primary);
                font-weight: 600;
                margin-bottom: 0.5rem;
            }
            
            .empty-subtext {
                font-size: 1rem;
                color: var(--text-secondary);
            }
        }
    }
}
</style>