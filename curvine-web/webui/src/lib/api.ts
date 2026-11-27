import { clearApiError, setApiError } from '@/lib/api-error'
import type {
  ConfigMap,
  FileBlocks,
  FileStatus,
  Overview,
  WorkersResponse,
} from '@/lib/types'
import type {
  TransferJob,
  TransferJobDetailResponse,
  TransferJobState,
  TransferJobsQuery,
  TransferJobsResponse,
  TransferTasksResponse,
  TransferTenantsResponse,
} from '@/lib/transfer-types'

const API_BASE = import.meta.env.VITE_API_BASE || '/api'

const STATUS_TEXT: Record<number, string> = {
  401: 'UnAuthorized Request',
  403: 'Request Reject',
  404: 'Request URI Not Found',
  408: 'Request Timeout',
  500: 'Internal service error',
  501: 'Not support service',
  502: 'Gateway Error',
  503: 'Service temporary unavailable',
  504: 'Gateway Timeout',
  505: 'HTTP version not support',
}

export class ApiError extends Error {
  readonly status: number | undefined

  constructor(message: string, status?: number) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

type QueryValue = string | number | boolean | null | undefined

export interface RequestOptions {
  signal?: AbortSignal
  /** Leave the shared error banner alone, e.g. for the capability probe. */
  quiet?: boolean
}

interface PageQuery {
  limit?: number
  page_token?: string | null
}

interface InternalOptions extends RequestOptions {
  query?: object
  method?: 'GET' | 'POST'
  /** Serialized as a JSON request body; sets the Content-Type header. */
  body?: unknown
}

let requestSeq = 0
let errorSeq = 0

function queryString(query?: object): string {
  if (!query) {
    return ''
  }
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(query) as [string, QueryValue][]) {
    if (value === undefined || value === null || value === '') {
      continue
    }
    search.set(key, String(value))
  }
  const encoded = search.toString()
  return encoded ? `?${encoded}` : ''
}

async function toResponseError(response: Response): Promise<ApiError> {
  let message = ''
  try {
    const body: unknown = await response.json()
    if (body && typeof body === 'object' && typeof (body as { message?: unknown }).message === 'string') {
      message = (body as { message: string }).message
    }
  } catch {
    // Not JSON: keep the status text instead.
  }
  if (!message) {
    message = STATUS_TEXT[response.status] ?? `Request failed with status ${response.status}`
  }
  return new ApiError(message, response.status)
}

function errorText(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message
  }
  if (error instanceof Error) {
    return error.message
  }
  return String(error)
}

function isAbort(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'AbortError'
}

function failed(error: unknown, seq: number) {
  // A slow failure must not replace the message of a newer one.
  if (seq < errorSeq) {
    return
  }
  errorSeq = seq
  setApiError(errorText(error))
}

async function request<T>(path: string, options: InternalOptions = {}): Promise<T> {
  const seq = ++requestSeq
  const { query, method = 'GET', body: requestBody, signal, quiet = false } = options
  const hasBody = requestBody !== undefined
  try {
    const response = await fetch(`${API_BASE}${path}${queryString(query)}`, {
      method,
      headers: hasBody
        ? { Accept: 'application/json', 'Content-Type': 'application/json' }
        : { Accept: 'application/json' },
      body: hasBody ? JSON.stringify(requestBody) : undefined,
      signal,
    })
    if (!response.ok) {
      throw await toResponseError(response)
    }
    let body: T
    try {
      body = (await response.json()) as T
    } catch {
      throw new ApiError('The server returned a non-JSON response', response.status)
    }
    // An answer sent after a failure clears the banner, as the old request
    // interceptor did.
    if (!quiet && seq >= errorSeq) {
      clearApiError()
    }
    return body
  } catch (error) {
    if (!quiet && !isAbort(error)) {
      failed(error, seq)
    }
    throw error
  }
}

/** Master API. */
export const api = {
  overview: (options?: RequestOptions) => request<Overview>('/overview', options),
  config: (options?: RequestOptions) => request<ConfigMap>('/config', options),
  browse: (
    path: string,
    page?: { limit?: number; startAfter?: string },
    options?: RequestOptions,
  ) =>
    request<FileStatus[]>('/browse', {
      ...options,
      query: { path, limit: page?.limit, start_after: page?.startAfter },
    }),
  blockLocations: (path: string, options?: RequestOptions) =>
    request<FileBlocks>('/block_locations', { ...options, query: { path } }),
  workers: (options?: RequestOptions) => request<WorkersResponse>('/workers', options),

  /** Transfer API, served by a standalone transfer process. */
  transferJobs: (query: TransferJobsQuery = {}, options?: RequestOptions) =>
    request<TransferJobsResponse>('/transfer/jobs', { ...options, query }),
  transferJob: (jobId: string, query: PageQuery = {}, options?: RequestOptions) =>
    request<TransferJobDetailResponse>(`/transfer/jobs/${encodeURIComponent(jobId)}`, {
      ...options,
      query,
    }),
  transferTasks: (jobId: string, query: PageQuery = {}, options?: RequestOptions) =>
    request<TransferTasksResponse>(`/transfer/jobs/${encodeURIComponent(jobId)}/tasks`, {
      ...options,
      query,
    }),
  transferTenants: (query: PageQuery = {}, options?: RequestOptions) =>
    request<TransferTenantsResponse>('/transfer/tenants', { ...options, query }),
  cancelTransfer: (jobId: string, options?: RequestOptions) =>
    request<TransferJobState>(`/transfer/jobs/${encodeURIComponent(jobId)}/cancel`, {
      ...options,
      method: 'POST',
    }),
  retryTransfer: (jobId: string, options?: RequestOptions) =>
    request<TransferJob>(`/transfer/jobs/${encodeURIComponent(jobId)}/retry`, {
      ...options,
      method: 'POST',
    }),
}
