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
    <div class="header">
        <nav class="navbar navbar-expand-lg tech-navbar">
            <div class="container-fluid">
                <div class="brand">
                    <a class="navbar-brand tech-brand" href="#">
                        <div class="logo-container">
                            <img src="@/assets/logo.svg" alt="Curvine" class="logo-icon">
                            <div class="logo-glow"></div>
                        </div>
                        <img src="@/assets/curvine_font_white.svg" alt="CURVINE Distributed Cache" class="brand-logo">
                    </a>
                </div>
                <div class="tabs">
                    <ul class="nav tech-nav">
                        <li class="nav-item">
                            <router-link class="nav-link tech-nav-link" :class="{ active: $route.name === 'Overview' }"
                                to="/overview">
                                <i class="nav-icon">📊</i>
                                <span>Overview</span>
                            </router-link>
                        </li>
                        <li class="nav-item">
                            <router-link class="nav-link tech-nav-link" :class="{ active: $route.name === 'Browse' }"
                                to="/browse">
                                <i class="nav-icon">📁</i>
                                <span>Browse</span>
                            </router-link>
                        </li>
                        <li class="nav-item">
                            <router-link class="nav-link tech-nav-link" :class="{ active: $route.name === 'Workers' }"
                                to="/workers">
                                <i class="nav-icon">⚡</i>
                                <span>Workers</span>
                            </router-link>
                        </li>
                        <li class="nav-item">
                            <router-link class="nav-link tech-nav-link" :class="{ active: $route.name === 'Config' }"
                                to="/config">
                                <i class="nav-icon">⚙️</i>
                                <span>Configuration</span>
                            </router-link>
                        </li>
                    </ul>
                </div>
                <div class="auto-refresh">
                    <button type="button" class="btn tech-btn" :class="{ 'active': autoRefresh }" @click="toggleAutoRefresh">
                        <div class="btn-content">
                            <div class="status-indicator" :class="{ 'active': autoRefresh }"></div>
                            <span class="btn-text">Auto Refresh</span>
                            <span class="btn-status">{{ autoRefresh ? 'ON' : 'OFF' }}</span>
                        </div>
                    </button>
                </div>
            </div>
        </nav>
    </div>
</template>

<script>
/* eslint-disable vue/multi-word-component-names */
import eventBus from '@/utils/eventBus'

export default {
    name: "Header",
    data() {
        return {
            autoRefresh: false,
            refreshInterval: null,
            refreshIntervalMs: 5000 // 5 seconds default
        }
    },
    methods: {
        /**
         * Toggle auto refresh functionality on/off
         */
        toggleAutoRefresh() {
            this.autoRefresh = !this.autoRefresh;
            
            if (this.autoRefresh) {
                this.startAutoRefresh();
            } else {
                this.stopAutoRefresh();
            }
            
            // Emit event to parent components or global event bus
            this.$emit('auto-refresh-changed', this.autoRefresh);
            
            // Store preference in localStorage
            localStorage.setItem('curvine-auto-refresh', this.autoRefresh.toString());
        },
        
        /**
         * Start the auto refresh timer
         */
        startAutoRefresh() {
            if (this.refreshInterval) {
                clearInterval(this.refreshInterval);
            }
            
            this.refreshInterval = setInterval(() => {
                this.triggerRefresh();
            }, this.refreshIntervalMs);
        },
        
        /**
         * Stop the auto refresh timer
         */
        stopAutoRefresh() {
            if (this.refreshInterval) {
                clearInterval(this.refreshInterval);
                this.refreshInterval = null;
            }
        },
        
        /**
         * Trigger refresh for current page
         */
        triggerRefresh() {
            // Emit global refresh event using eventBus
            eventBus.emit('auto-refresh-trigger');
            
            // Also emit to parent
            this.$emit('refresh-triggered');
        },
        
        /**
         * Load auto refresh preference from localStorage
         */
        loadAutoRefreshPreference() {
            const saved = localStorage.getItem('curvine-auto-refresh');
            if (saved !== null) {
                this.autoRefresh = saved === 'true';
                if (this.autoRefresh) {
                    this.startAutoRefresh();
                }
            }
        }
    },
    
    mounted() {
        // Load saved preference on component mount
        this.loadAutoRefreshPreference();
    },
    
    beforeUnmount() {
        // Clean up interval when component is destroyed
        this.stopAutoRefresh();
    }
}
</script>

<style lang="scss" scoped>
.header {
    .tech-navbar {
        background: linear-gradient(135deg, var(--secondary-bg) 0%, var(--card-bg) 100%);
        border-bottom: 2px solid var(--border-color);
        backdrop-filter: blur(10px);
        padding: 0.75rem 0;
        box-shadow: 0 2px 20px rgba(0, 0, 0, 0.5);
        
        .container-fluid {
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        
        .brand {
            .tech-brand {
                display: flex;
                align-items: center;
                text-decoration: none;
                color: var(--text-primary);
                
                .logo-container {
                    position: relative;
                    margin-right: 1rem;
                    
                    .logo-icon {
                        width: 40px;
                        height: 40px;
                        filter: drop-shadow(0 0 10px var(--accent-blue));
                        animation: logoGlow 3s ease-in-out infinite alternate;
                    }
                    
                    .logo-glow {
                        position: absolute;
                        top: 50%;
                        left: 50%;
                        transform: translate(-50%, -50%);
                        width: 60px;
                        height: 60px;
                        background: radial-gradient(circle, var(--accent-blue) 0%, transparent 70%);
                        opacity: 0.3;
                        animation: pulseGlow 2s ease-in-out infinite;
                    }
                }
                
                .brand-logo {
                    height: 2.2rem;
                    width: auto;
                    filter: drop-shadow(0 0 8px rgba(0, 212, 255, 0.3));
                    transition: all 0.3s ease;
                    
                    &:hover {
                        filter: drop-shadow(0 0 12px rgba(0, 212, 255, 0.5));
                        transform: scale(1.02);
                    }
                }
            }
        }
        
        .tabs {
            .tech-nav {
                display: flex;
                gap: 0.5rem;
                border: none;
                
                .nav-item {
                    .tech-nav-link {
                        display: flex;
                        align-items: center;
                        gap: 0.5rem;
                        padding: 0.75rem 1.25rem;
                        color: var(--text-secondary);
                        text-decoration: none;
                        border-radius: 8px;
                        transition: all 0.3s ease;
                        font-weight: 500;
                        text-transform: uppercase;
                        letter-spacing: 0.5px;
                        font-size: 0.85rem;
                        border: 1px solid transparent;
                        
                        .nav-icon {
                            font-size: 1.1rem;
                            filter: grayscale(1);
                            transition: filter 0.3s ease;
                        }
                        
                        &:hover {
                            color: var(--accent-blue);
                            background: rgba(0, 212, 255, 0.1);
                            border-color: var(--accent-blue);
                            transform: translateY(-1px);
                            
                            .nav-icon {
                                filter: grayscale(0);
                            }
                        }
                        
                        &.active {
                            color: var(--accent-blue);
                            background: linear-gradient(135deg, rgba(0, 212, 255, 0.2) 0%, rgba(139, 92, 246, 0.2) 100%);
                            border-color: var(--accent-blue);
                            box-shadow: 0 0 15px rgba(0, 212, 255, 0.3);
                            
                            .nav-icon {
                                filter: grayscale(0);
                            }
                        }
                    }
                }
            }
        }
        
        .auto-refresh {
            .tech-btn {
                background: var(--secondary-bg);
                border: 1px solid var(--border-color);
                color: var(--text-secondary);
                border-radius: 8px;
                padding: 0.5rem 1rem;
                transition: all 0.3s ease;
                
                .btn-content {
                    display: flex;
                    align-items: center;
                    gap: 0.5rem;
                    
                    .status-indicator {
                        width: 8px;
                        height: 8px;
                        border-radius: 50%;
                        background: var(--text-secondary);
                        transition: all 0.3s ease;
                        
                        &.active {
                            background: var(--accent-green);
                            box-shadow: 0 0 10px var(--accent-green);
                            animation: pulse 2s infinite;
                        }
                    }
                    
                    .btn-text {
                        font-weight: 500;
                        text-transform: uppercase;
                        letter-spacing: 0.5px;
                        font-size: 0.8rem;
                    }
                    
                    .btn-status {
                        font-weight: 700;
                        font-size: 0.75rem;
                    }
                }
                
                &:hover {
                    border-color: var(--accent-blue);
                    color: var(--accent-blue);
                    transform: translateY(-1px);
                }
                
                &.active {
                    background: linear-gradient(135deg, rgba(0, 255, 136, 0.2) 0%, rgba(0, 212, 255, 0.2) 100%);
                    border-color: var(--accent-green);
                    color: var(--accent-green);
                    box-shadow: 0 0 15px rgba(0, 255, 136, 0.3);
                }
            }
        }
    }
}

@keyframes logoGlow {
    0% {
        filter: drop-shadow(0 0 5px var(--accent-blue));
    }
    100% {
        filter: drop-shadow(0 0 20px var(--accent-blue));
    }
}

@keyframes pulseGlow {
    0%, 100% {
        opacity: 0.3;
        transform: translate(-50%, -50%) scale(1);
    }
    50% {
        opacity: 0.6;
        transform: translate(-50%, -50%) scale(1.1);
    }
}

@keyframes pulse {
    0%, 100% {
        opacity: 1;
    }
    50% {
        opacity: 0.5;
    }
}
</style>