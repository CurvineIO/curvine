<template>
  <div class="admin-page" v-loading="loading">
    <section class="admin-panel table-panel">
      <div class="panel-heading table-heading">
        <div>
          <h2>{{ $t('blocks.title') }}</h2>
          <p>{{ $t('blocks.subtitle', { path }) }}</p>
        </div>
        <button class="admin-button ghost compact" @click="$router.push({ path: '/browse', query: { path } })">{{ $t('common.back') }}</button>
      </div>
      <div class="path-toolbar">
        <input v-model="path" class="admin-input path-input" :placeholder="$t('blocks.pathPlaceholder')" @keyup.enter="fetchData">
        <button class="admin-button primary compact" @click="fetchData">{{ $t('blocks.inspect') }}</button>
      </div>
      <pre class="json-view">{{ formattedBlocks }}</pre>
    </section>
  </div>
</template>

<script>
import { fetchBlocksData } from '@/api/client'
import eventBus from '@/utils/eventBus'

export default {
  name: 'BlocksPage',
  data() {
    return {
      loading: false,
      path: this.$route.query.path || '/',
      blocks: null
    }
  },
  computed: {
    formattedBlocks() { return JSON.stringify(this.blocks || {}, null, 2) }
  },
  created() { this.fetchData() },
  mounted() { eventBus.on('admin-refresh', this.fetchData) },
  beforeUnmount() { eventBus.off('admin-refresh', this.fetchData) },
  methods: {
    async fetchData() {
      if (!this.path) return
      this.loading = true
      try {
        this.blocks = await fetchBlocksData({ path: this.path })
      } finally { this.loading = false }
    }
  }
}
</script>
