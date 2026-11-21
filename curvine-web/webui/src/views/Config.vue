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
  <div v-loading="loading" class="configuration-page">
    <div class="container-fluid">
      <div class="row">
        <div class="col-6">
          <h5>Curvine Configuration</h5>
        </div>
        <div class=" search-container col-6">
          <input id="searchConfig" placeholder="Search by Property" type="text" class="form-control"
            v-model="searchQuery">
        </div>
        <div class="col-12">
          <table class="table table-hover">
            <thead>
              <tr>
                <th>Property</th>
                <th>Value</th>
              </tr>
            </thead>
            <template v-if="filteredData.length > 0">
              <tbody v-for="item in filteredData" :key="item.key">
                <tr>
                  <td>
                    <pre class="mb-0"><code>{{ item.key }}</code></pre>
                  </td>
                  <td>{{ item.value }}</td>
                </tr>
              </tbody>
            </template>
            <tbody v-else-if="searchQuery">
              <tr>
                <td> not found matched properties.</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { fetchConfigData } from '@/api/client'
import eventBus from '@/utils/eventBus'

/* eslint-disable vue/multi-word-component-names */
export default {
  name: 'Config',
  components: {
  },
  data() {
    return {
      loading: false,
      data: [],
      searchQuery: ''
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
  computed: {
    filteredData() {
      return this.data.filter(item => item.key.toLowerCase().includes(this.searchQuery.toLowerCase()))
    }
  },
  methods: {
    /**
     * Handle auto refresh trigger from header component
     */
    handleAutoRefresh() {
      this.fetchData()
    },
    
    /**
     * Fetch configuration data from API
     */
    fetchData() {
      this.loading = true
      this.data = [] // Clear existing data
      fetchConfigData().then(res => {
        this.flattenData(res.data)
        this.loading = false
      }).catch(err => {
        console.error("fetch config data error: " + err)
        this.loading = false
      })
    },
    
    /**
     * Flatten nested configuration object into key-value pairs
     */
    flattenData(data, parentKey = "") {
      for (let key in data) {
        if (Object.prototype.hasOwnProperty.call(data, key)) {
          let propName = parentKey ? `${parentKey}.${key}` : key;
          if (typeof data[key] === 'object' && !Array.isArray(data[key])) {
            this.flattenData(data[key], propName);
          } else {
            // this.data[propName] = data[key];
            this.data.push({ 'key': propName, 'value': data[key] })
          }
        }
      }
    }
  },
}
</script>

<style lang="scss">
// Clears the fixed header, like the padding of the other pages.
.configuration-page {
  padding: 2rem;
}

#searchConfig {
  width: 50%;
  margin-left: 50%;
}

.search-container {
  align-content: center;
  bottom: 5px;
}
</style>