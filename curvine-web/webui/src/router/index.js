/*
 * Copyright 2025 OPPO.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { createRouter, createWebHistory } from 'vue-router'
import Overview from '@/views/Overview.vue'
import Config from '@/views/Config.vue'
import Browse from '@/views/Browse.vue'
import Workers from '@/views/Workers.vue'
import Blocks from '@/views/Blocks.vue'
import Transfers from '@/views/Transfers.vue'
import TransferDetail from '@/views/TransferDetail.vue'

const routes = [
  {
    path: '/',
    redirect: '/overview'
  },
  {
    path: '/overview',
    name: 'overview',
    component: Overview
  },
  {
    path: '/config',
    name: 'config',
    component: Config
  },
  {
    path: '/browse',
    name: 'browse',
    component: Browse
  },
  {
    path: '/workers',
    name: 'workers',
    component: Workers
  },
  {
    path: '/blocks',
    name: 'blocks',
    component: Blocks
  },
  {
    path: '/transfers',
    name: 'transfers',
    component: Transfers
  },
  {
    path: '/transfers/:jobId',
    name: 'transfer-detail',
    component: TransferDetail,
    props: true
  },
  {
    path: '/about',
    name: 'about',
    // route level code-splitting
    // this generates a separate chunk (about.[hash].js) for this route
    // which is lazy-loaded when the route is visited.
    component: () => import(/* webpackChunkName: "about" */ '../views/AboutView.vue')
  }
]

const router = createRouter({
  linkActiveClass: 'active',
  history: createWebHistory(process.env.BASE_URL),
  routes
})

export default router
