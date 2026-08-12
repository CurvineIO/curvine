/*
 * Copyright 2025 OPPO.
 *
 * Licensed under the Apache License, Version 2.0.
 */

import axios from 'axios'
import { useAppStore } from '@/stores/app'

const service = axios.create({
  baseURL: import.meta.env.VITE_API_BASE || '/api',
  timeout: 10000,
  withCredentials: true,
  headers: {
    Accept: 'application/json'
  }
})

service.interceptors.request.use(
  (config) => config,
  (error) => {
    useAppStore().setErrmsg(error)
    return Promise.reject(error)
  }
)

service.interceptors.response.use(
  (response) => response,
  (error) => {
    let errMsg = ''
    if (error && error.response) {
      const responseMessage = error.response.data?.error?.message || error.response.data?.message || error.response.data?.msg
      switch (error.response.status) {
        case 401:
          errMsg = responseMessage || 'UnAuthorized Request'
          if (window.location.pathname !== '/login') window.location.href = `/login?redirect=${encodeURIComponent(window.location.pathname + window.location.search)}`
          break
        case 403: errMsg = responseMessage || 'Request Reject'; break
        case 404: errMsg = responseMessage || 'Request URI Not Found'; break
        case 408: errMsg = responseMessage || 'Request Timeout'; break
        case 500: errMsg = responseMessage || 'Internal service error'; break
        case 501: errMsg = responseMessage || 'Not support service'; break
        case 502: errMsg = responseMessage || 'Gateway Error'; break
        case 503: errMsg = responseMessage || 'Service temporary unavailable'; break
        case 504: errMsg = responseMessage || 'Gateway Timeout'; break
        case 505: errMsg = responseMessage || 'HTTP version not support'; break
        default: errMsg = responseMessage || error.message; break
      }
    } else {
      errMsg = String(error)
    }
    useAppStore().setErrmsg(errMsg)
    return Promise.reject(errMsg)
  }
)

export default service
