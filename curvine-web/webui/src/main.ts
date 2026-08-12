/*
 * Copyright 2025 OPPO.
 *
 * Licensed under the Apache License, Version 2.0.
 */

import { createApp } from 'vue'
import { createPinia } from 'pinia'
import App from './App.vue'
import router from './router'
import loadingDirective from '@/utils/loading'
import { applyDocumentLocale, detectLocale, i18n } from '@/i18n'
import '@/styles/index.scss'

const app = createApp(App)

app.use(createPinia())
app.use(i18n)
app.use(router)
app.directive('loading', loadingDirective)
applyDocumentLocale(detectLocale())
app.mount('#app')
