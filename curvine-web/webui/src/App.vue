<template>
  <router-view v-if="$route.meta.public" />
  <AdminLayout v-else-if="authenticated" />
  <div v-else class="auth-loading"></div>
</template>

<script>
import AdminLayout from '@/layouts/AdminLayout.vue'
import { fetchAuthSession } from '@/api/client'

export default {
  name: 'App',
  components: { AdminLayout },
  data() {
    return {
      authenticated: false,
      checkingAuth: false
    }
  },
  watch: {
    '$route.fullPath': {
      immediate: true,
      async handler() {
        if (this.$route.meta.public) {
          this.authenticated = false
          return
        }
        await this.checkAuth()
      }
    }
  },
  methods: {
    async checkAuth() {
      if (this.checkingAuth) return
      this.checkingAuth = true
      try {
        const session = await fetchAuthSession()
        this.authenticated = Boolean(session && session.authenticated)
        if (!this.authenticated) {
          this.$router.replace({ path: '/login', query: { redirect: this.$route.fullPath } })
        }
      } catch (_) {
        this.authenticated = false
        this.$router.replace({ path: '/login', query: { redirect: this.$route.fullPath } })
      } finally {
        this.checkingAuth = false
      }
    }
  }
}
</script>

<style scoped>
.auth-loading {
  min-height: 100vh;
  background: var(--admin-bg, #f7faf4);
}
</style>
