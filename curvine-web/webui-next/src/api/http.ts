import axios, { type AxiosInstance, type AxiosRequestConfig } from 'axios'

import { ApiError } from './error'

export const API_BASE = import.meta.env.VITE_API_BASE || '/api'

/** Mirrors the status → message mapping the legacy Vue client used. */
const STATUS_MESSAGES: Record<number, string> = {
  400: 'Bad Request',
  401: 'UnAuthorized Request',
  403: 'Forbidden',
  404: 'Not Found',
  405: 'Method Not Allowed',
  408: 'Request Timeout',
  429: 'Too Many Requests',
  500: 'Internal Server Error',
  501: 'Not Implemented',
  502: 'Bad Gateway',
  503: 'Service Unavailable',
  504: 'Gateway Timeout',
  505: 'HTTP Version Not Supported'
}

function extractMessage(error: unknown, status: number): string {
  const response = (error as { response?: { data?: unknown } } | null)?.response
  const data = response?.data
  if (typeof data === 'string' && data.trim()) {
    return data
  }
  if (data && typeof data === 'object') {
    const nested = data as { message?: unknown; error?: { message?: unknown } }
    if (typeof nested.error?.message === 'string' && nested.error.message) return nested.error.message
    if (typeof nested.message === 'string' && nested.message) return nested.message
  }
  if (typeof STATUS_MESSAGES[status] === 'string') return STATUS_MESSAGES[status]
  const message = (error as { message?: unknown } | null)?.message
  return typeof message === 'string' && message ? message : 'Unknown error'
}

/** 401 is the single sign-out exit: the handler clears the session instead of hard-reloading. */
export interface HttpClientOptions {
  onUnauthorized?: () => void
}

export function createHttpClient({ onUnauthorized }: HttpClientOptions = {}): AxiosInstance {
  const client = axios.create({
    baseURL: API_BASE,
    timeout: 10000,
    withCredentials: true,
    headers: { Accept: 'application/json' }
  })

  client.interceptors.response.use(
    (response) => response,
    (error) => {
      const status = Number((error as { response?: { status?: number } }).response?.status) || 0
      if (status === 401) {
        onUnauthorized?.()
      }
      throw new ApiError(status, extractMessage(error, status))
    }
  )

  return client
}

/** Uploads and downloads need a much longer budget than the 10s API default. */
export function createDownloadClient(): AxiosInstance {
  return axios.create({
    baseURL: API_BASE,
    timeout: 120000,
    withCredentials: true,
    headers: { Accept: 'application/json' }
  })
}

export async function parseDownloadError(error: unknown): Promise<string> {
  const response = (error as { response?: { data?: unknown } } | null)?.response
  if (!response || response.data === undefined || response.data === null) {
    const message = (error as { message?: unknown } | null)?.message
    return typeof message === 'string' && message ? message : 'Unknown error'
  }
  const data = response.data
  if (typeof data === 'string') return data
  if (typeof Blob !== 'undefined' && data instanceof Blob) {
    try {
      const text = await data.text()
      const parsed = JSON.parse(text) as { message?: string; error?: { message?: string } }
      return parsed.message || parsed.error?.message || text
    } catch {
      const message = (error as { message?: unknown } | null)?.message
      return typeof message === 'string' && message ? message : 'Unknown error'
    }
  }
  const typed = data as { message?: string; error?: { message?: string } }
  return typed.message || typed.error?.message || 'Unknown error'
}

/**
 * Unwrap the `{ success, data }` envelope.
 *
 * Rejects an HTML body so a misrouted request surfaces an error instead of
 * being parsed as JSON, and keeps the legacy non-versioned `/api/*` routes
 * unused: every data endpoint here is an authenticated `/v1` call.
 */
export function unwrap<T>(response: { data?: unknown } | unknown): T {
  const payload =
    response && typeof response === 'object' && 'data' in (response as Record<string, unknown>)
      ? (response as { data?: unknown }).data
      : response

  if (typeof payload === 'string') {
    const trimmed = payload.trim().toLowerCase()
    if (trimmed.startsWith('<!doctype') || trimmed.startsWith('<html')) {
      throw new ApiError(0, 'Expected JSON API response, got HTML fallback')
    }
  }
  if (payload && typeof payload === 'object') {
    const envelope = payload as { success?: boolean; data?: unknown; error?: { message?: string } }
    if (envelope.success === true) return envelope.data as T
    if (envelope.success === false) {
      throw new ApiError(0, envelope.error?.message || 'API request failed')
    }
  }
  return payload as T
}

export async function requestJson<T>(
  client: AxiosInstance,
  config: AxiosRequestConfig
): Promise<T> {
  return unwrap<T>(await client.request(config))
}
