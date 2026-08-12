/*
 * Copyright 2025 OPPO.
 *
 * Licensed under the Apache License, Version 2.0.
 */

import { createRouter, createWebHistory, type RouteRecordRaw } from 'vue-router'
import Overview from '@/views/Overview.vue'
import Config from '@/views/Config.vue'
import Browse from '@/views/Browse.vue'
import Workers from '@/views/Workers.vue'
import WorkerDetail from '@/views/WorkerDetail.vue'
import Blocks from '@/views/Blocks.vue'
import Mounts from '@/views/Mounts.vue'
import Jobs from '@/views/Jobs.vue'
import Transfers from '@/views/Transfers.vue'
import Observability from '@/views/Observability.vue'
import Operations from '@/views/Operations.vue'
import Preview from '@/views/Preview.vue'
import Login from '@/views/Login.vue'
import { fetchAuthSession } from '@/api/client'

const routes: RouteRecordRaw[] = [
  { path: '/login', name: 'login', component: Login, meta: { public: true } },
  { path: '/', redirect: '/overview' },
  { path: '/overview', name: 'overview', component: Overview },
  { path: '/browse', name: 'browse', component: Browse },
  { path: '/workers', name: 'workers', component: Workers },
  { path: '/workers/:id', name: 'worker-detail', component: WorkerDetail },
  { path: '/mounts', name: 'mounts', component: Mounts },
  { path: '/jobs', name: 'jobs', component: Jobs },
  { path: '/transfers', name: 'transfers', component: Transfers },
  { path: '/observability', name: 'observability', component: Observability },
  { path: '/operations', name: 'operations', component: Operations },
  { path: '/config', name: 'config', component: Config },
  { path: '/blocks', name: 'blocks', component: Blocks },
  { path: '/preview', name: 'preview', component: Preview }
]

const router = createRouter({
  linkActiveClass: 'active',
  history: createWebHistory(import.meta.env.BASE_URL),
  routes
})

router.beforeEach(async (to) => {
  if (to.meta.public) return true
  try {
    const session = await fetchAuthSession()
    if (session && session.authenticated) return true
  } catch (_) {}
  return { path: '/login', query: { redirect: to.fullPath } }
})

export default router
