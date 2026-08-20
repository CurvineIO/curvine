<template>
  <div class="admin-shell">
    <aside class="admin-sidebar">
      <router-link class="admin-brand" to="/overview">
        <img src="@/assets/logo.svg" alt="Curvine">
        <div><strong>Curvine</strong><span>{{ $t('layout.brandSubtitle') }}</span></div>
      </router-link>
      <nav class="admin-nav">
        <router-link v-for="item in navItems" :key="item.path" :to="item.path" class="admin-nav-link">
          <span>{{ item.icon }}</span>{{ $t(item.labelKey) }}
        </router-link>
      </nav>
      <div class="admin-sidebar-status"><span></span><div><strong>{{ $t('layout.masterReady') }}</strong><small>{{ masterReadyHintText }}</small></div></div>
    </aside>
    <main class="admin-main">
      <header class="admin-topbar">
        <div><h1>{{ currentTitle }}</h1><p>{{ currentSubtitle }}</p></div>
        <div class="admin-topbar-actions">
          <label class="admin-locale">
            <span>{{ $t('common.language') }}</span>
            <select :value="locale" class="admin-input" @change="onLocaleChange">
              <option v-for="item in localeOptions" :key="item.value" :value="item.value">{{ item.label }}</option>
            </select>
          </label>
          <button class="admin-button ghost" type="button" @click="refreshPage">{{ $t('common.refresh') }}</button>
          <label class="admin-toggle"><input v-model="autoRefresh" type="checkbox">{{ $t('common.autoRefresh') }}</label>
          <button class="admin-button ghost" type="button" @click="logoutPage">{{ $t('common.logout') }}</button>
        </div>
      </header>
      <router-view />
    </main>
  </div>
</template>

<script>
import eventBus from '@/utils/eventBus'
import { logout } from '@/api/client'
import { i18n, localeOptions, setLocale } from '@/i18n'
import { useAppStore } from '@/stores/app'
import { getUptimeParts } from '@/utils/format'

export default {
  name: 'AdminLayout',
  data() {
    return {
      autoRefresh: false,
      timer: null,
      uptimeTick: Date.now(),
      localeOptions,
      navItems: [
        { path: '/overview', labelKey: 'nav.overview', subtitleKey: 'nav.overviewSubtitle', icon: 'O' },
        { path: '/browse', labelKey: 'nav.browse', subtitleKey: 'nav.browseSubtitle', icon: 'F' },
        { path: '/workers', labelKey: 'nav.workers', subtitleKey: 'nav.workersSubtitle', icon: 'W' },
        { path: '/observability', labelKey: 'nav.observability', subtitleKey: 'nav.observabilitySubtitle', icon: 'V' },
        { path: '/operations', labelKey: 'nav.operations', subtitleKey: 'nav.operationsSubtitle', icon: 'H' },
        { path: '/mounts', labelKey: 'nav.mounts', subtitleKey: 'nav.mountsSubtitle', icon: 'M' },
        { path: '/jobs', labelKey: 'nav.jobs', subtitleKey: 'nav.jobsSubtitle', icon: 'J' },
        { path: '/transfers', labelKey: 'nav.transfers', subtitleKey: 'nav.transfersSubtitle', icon: 'T' },
        { path: '/config', labelKey: 'nav.config', subtitleKey: 'nav.configSubtitle', icon: 'C' }
      ]
    }
  },
  computed: {
    locale() {
      return i18n.global.locale.value
    },
    appStore() {
      return useAppStore()
    },
    currentItem() {
      return this.navItems.find((item) => this.$route.path.startsWith(item.path)) || this.navItems[0]
    },
    currentTitle() {
      return this.$t(this.currentItem.labelKey)
    },
    currentSubtitle() {
      return this.$t(this.currentItem.subtitleKey)
    },
    masterReadyHintText() {
      void this.uptimeTick
      const parts = getUptimeParts(
        this.appStore.masterStartTime,
        Date.now(),
        this.appStore.masterStartTimeMs
      )
      if (!parts) return this.$t('layout.masterReadyHint')
      let uptime = ''
      if (parts.kind === 'min') uptime = this.$t('common.uptime.min', { count: parts.count })
      else if (parts.kind === 'hours') uptime = this.$t('common.uptime.hours', { count: parts.count })
      else if (parts.kind === 'days') uptime = this.$t('common.uptime.days', { count: parts.count })
      else uptime = this.$t('common.uptime.daysHours', { days: parts.days, hours: parts.hours })
      return this.$t('layout.masterReadyHintRunning', { uptime })
    }
  },
  watch: {
    autoRefresh(value) {
      if (value) this.startAutoRefresh()
      else this.stopAutoRefresh()
    }
  },
  created() {
    this.appStore.refreshMasterRuntime()
  },
  mounted() {
    eventBus.on('admin-refresh', this.onAdminRefresh)
  },
  beforeUnmount() {
    this.stopAutoRefresh()
    eventBus.off('admin-refresh', this.onAdminRefresh)
  },
  methods: {
    onAdminRefresh() {
      this.uptimeTick = Date.now()
      if (this.$route.path.startsWith('/overview')) return
      this.appStore.refreshMasterRuntime()
    },
    onLocaleChange(event) {
      setLocale(event.target.value)
    },
    refreshPage() { eventBus.emit('admin-refresh') },
    async logoutPage() { await logout(); this.$router.replace('/login') },
    startAutoRefresh() { this.stopAutoRefresh(); this.timer = setInterval(this.refreshPage, 5000) },
    stopAutoRefresh() { if (this.timer) clearInterval(this.timer); this.timer = null }
  }
}
</script>

<style scoped>
.admin-locale {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  font-size: 0.85rem;
  color: var(--admin-muted, #5f6b57);
}

.admin-locale select {
  min-width: 112px;
}
</style>
