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
  <div v-loading="loading" class="block-page">
    <div class="container-fluid">
      <div class="row">
        <div class="col-6">
          <h5>File Block Locations</h5>
        </div>
        <div class="col-12">
          <table class="table table-hover">
            <thead>
              <tr>
                <th>ID</th>
                <th>Block Size</th>
                <th>File Type</th>
                <th>Storage Type</th>
                <th>Locations</th>
              </tr>
            </thead>
            <tbody v-for="item in data" :key="item">
              <tr>
                <td>{{ item.block.id }}</td>
                <td>{{ item.block.block_size }}</td>
                <td>{{ item.block.file_type }}</td>
                <td>{{ item.block.storage_type }}</td>
                <td>{{ item.locs }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { fetchBlocksData } from '@/api/client'

/* eslint-disable vue/multi-word-component-names */
export default {
  name: 'Blocks',
  components: {
  },
  data() {
    return {
      loading: false,
      data: [],
      path: this.$route.query.path
    }
  },
  created() {
    this.loading = true
    fetchBlocksData({ path: this.path }).then(res => {
      let block_ids = res.data.block_ids
      for (const block_id of block_ids) {
        let block = res.data.block_locs[block_id].block
        let locations = res.data.block_locs[block_id].locs
        this.data.push({
          "block": block,
          "locs": locations.map(item => item.hostname + "_" + item.rpc_port).join(", ")
        })
      }
    }).catch(err => {
      console.error("fetch block data error: " + err)
    })
    this.loading = false
  },
}
</script>

<style lang="scss" scoped>
// Clears the fixed header, like the padding of the other pages.
.block-page {
  padding: 2rem;
}
</style>
