import type { AxiosInstance, AxiosRequestConfig, AxiosProgressEvent } from 'axios'

import {
  createDownloadClient,
  createHttpClient,
  parseDownloadError,
  requestJson,
  type HttpClientOptions
} from './http'
import type {
  AuthSession,
  BlockLocationData,
  BrowseData,
  ConfigData,
  DashboardQueryParams,
  DiagnosticsPayload,
  InstantMetric,
  JobInfo,
  LoginPayload,
  MasterHaData,
  MetricQueryParams,
  DirSpaceState,
  DirSpaceStatus,
  DirSpaceSummary,
  DirSpaceUpdate,
  MountInfo,
  ObservabilityDashboard,
  ObservabilityHealth,
  ObservabilitySettings,
  OperationsPayload,
  OverviewData,
  RangeSeries,
  TargetHealth,
  WorkerAction,
  WorkerDetailData,
  WorkersData
} from './types'

export interface UfsSyncJobQuery {
  includeTasks?: boolean
  failedOnly?: boolean
  state?: string
}

export interface JobQuery {
  [key: string]: string | number | boolean | undefined
}

export interface ApiClient {
  login(data: LoginPayload): Promise<unknown>
  fetchAuthSession(): Promise<AuthSession>
  logout(): Promise<unknown>

  fetchOverviewData(): Promise<OverviewData>
  fetchMasterHa(): Promise<MasterHaData>
  requestMasterFailover(targetMaster: string): Promise<unknown>
  fetchConfigData(): Promise<ConfigData>

  fetchBrowseData(query: Record<string, unknown>): Promise<BrowseData>
  fetchBlocksData(query: Record<string, unknown>): Promise<BlockLocationData>
  createDirectory(path: string, createParent?: boolean): Promise<unknown>
  deletePath(path: string, recursive?: boolean): Promise<unknown>
  freePath(path: string, recursive?: boolean): Promise<unknown>
  uploadFile(
    path: string,
    file: File,
    overwrite?: boolean,
    onUploadProgress?: ((event: AxiosProgressEvent) => void) | null,
    timeout?: number,
    syncUfs?: boolean
  ): Promise<unknown>
  /** The file's bytes. The endpoint has no Range support, so this is the whole file. */
  downloadFile(path: string): Promise<Blob>
  fetchUfsSyncStatus(path: string): Promise<unknown>
  fetchUfsSyncJobs(
    path: string,
    limit?: number,
    includeFinished?: boolean,
    options?: UfsSyncJobQuery
  ): Promise<unknown>

  fetchWorkersData(): Promise<WorkersData>
  fetchWorkerDetail(worker: string): Promise<WorkerDetailData>
  updateWorkerAction(worker: string, action: WorkerAction): Promise<unknown>

  fetchDirSpace(): Promise<DirSpaceState>
  fetchDirSpaceStatus(): Promise<DirSpaceStatus>
  updateDirSpace(data: DirSpaceUpdate): Promise<DirSpaceState>
  fetchDirSpaceSummaries(paths: string[]): Promise<{ items: DirSpaceSummary[] }>

  fetchMountsData(): Promise<unknown>
  saveMount(data: Record<string, unknown>): Promise<unknown>
  deleteMount(cvPath: string): Promise<unknown>
  validateMount(data: Record<string, unknown>): Promise<unknown>
  startMountResync(cvPath: string, dryRun?: boolean): Promise<unknown>
  fetchMountResync(taskId: string): Promise<unknown>

  fetchJobsData(params?: JobQuery): Promise<unknown>
  submitLoadJob(data: Record<string, unknown>): Promise<unknown>
  fetchJobStatus(jobId: string): Promise<JobInfo>
  cancelLoadJob(jobId: string): Promise<unknown>

  fetchTransfersData(params?: JobQuery): Promise<unknown>
  submitTransfer(data: Record<string, unknown>): Promise<unknown>
  fetchTransferStatus(jobId: string, params?: JobQuery): Promise<JobInfo>
  cancelTransfer(jobId: string): Promise<unknown>
  retryTransfer(jobId: string): Promise<unknown>

  fetchObservabilityHealth(): Promise<ObservabilityHealth>
  fetchObservabilitySettings(): Promise<ObservabilitySettings>
  updateObservabilitySettings(data: {
    scrape_interval_ms?: number
    retention_ms?: number
  }): Promise<ObservabilitySettings>
  updateObservabilityMode(mode: string): Promise<ObservabilityHealth>
  fetchObservabilityTargets(): Promise<TargetHealth[]>
  fetchObservabilityNames(): Promise<string[]>
  fetchInstantMetrics(params?: MetricQueryParams): Promise<InstantMetric[]>
  queryMetrics(params: MetricQueryParams): Promise<RangeSeries[]>
  fetchObservabilityDashboard(params?: DashboardQueryParams): Promise<ObservabilityDashboard>
  fetchObservabilityOperations(): Promise<OperationsPayload>
  fetchObservabilityDiagnostics(): Promise<DiagnosticsPayload>
}

/**
 * Factory rather than a module singleton, so the API client can be injected and
 * stays decoupled from how the session is stored. `onUnauthorized` is the only
 * sign-out exit.
 */
export function createApiClient(options: HttpClientOptions = {}): ApiClient {
  const request: AxiosInstance = createHttpClient(options)
  const downloadClient: AxiosInstance = createDownloadClient()

  function get<T>(url: string, config?: AxiosRequestConfig): Promise<T> {
    return requestJson<T>(request, { url, method: 'get', ...config })
  }

  function post<T>(url: string, data?: unknown, config?: AxiosRequestConfig): Promise<T> {
    return requestJson<T>(request, { url, method: 'post', data, ...config })
  }

  function put<T>(url: string, data?: unknown, config?: AxiosRequestConfig): Promise<T> {
    return requestJson<T>(request, { url, method: 'put', data, ...config })
  }

  return {
    login: (data) => post('/v1/auth/login', data),
    fetchAuthSession: () => get<AuthSession>('/v1/auth/session'),
    logout: () => post('/v1/auth/logout', {}),

    fetchOverviewData: () => get<OverviewData>('/v1/overview'),
    fetchMasterHa: () => get<MasterHaData>('/v1/master/ha'),
    requestMasterFailover: (targetMaster) =>
      requestJson(request, {
        url: '/v1/master/failover',
        method: 'post',
        data: { target_master: targetMaster },
        timeout: 180000
      }),
    fetchConfigData: () => get<ConfigData>('/v1/config'),

    fetchBrowseData: (query) => get<BrowseData>('/v1/fs/list', { params: query }),
    fetchBlocksData: (query) => get<BlockLocationData>('/v1/fs/blocks', { params: query }),
    createDirectory: (path, createParent = true) =>
      post('/v1/fs/mkdir', { path, create_parent: createParent }),
    deletePath: (path, recursive = false) => post('/v1/fs/delete', { path, recursive }),
    freePath: (path, recursive = false) => post('/v1/fs/free', { path, recursive }),
    uploadFile: (path, file, overwrite = true, onUploadProgress = null, timeout = 300000, syncUfs = true) =>
      requestJson(request, {
        url: `/v1/fs/upload?path=${encodeURIComponent(path)}&overwrite=${overwrite}&sync_ufs=${syncUfs}`,
        method: 'post',
        data: file,
        timeout,
        onUploadProgress: onUploadProgress || undefined
      }),
    downloadFile: async (path) => {
      try {
        const response = await downloadClient.get<Blob>('/v1/fs/download', {
          params: { path },
          responseType: 'blob'
        })
        // The blob itself, not the axios envelope: this used to hand back the response
        // object, which nothing consumed because nothing called it.
        return response.data
      } catch (error) {
        throw new Error(await parseDownloadError(error))
      }
    },
    fetchUfsSyncStatus: (path) => get('/v1/fs/ufs-sync', { params: { path } }),
    fetchUfsSyncJobs: (path, limit = 100, includeFinished = true, syncOptions = {}) =>
      get('/v1/fs/ufs-sync/jobs', {
        params: {
          path,
          limit,
          include_finished: includeFinished,
          include_tasks: syncOptions.includeTasks || false,
          failed_only: syncOptions.failedOnly || false,
          state: syncOptions.state || undefined
        }
      }),

    fetchWorkersData: () => get<WorkersData>('/v1/workers'),
    fetchWorkerDetail: (worker) => get<WorkerDetailData>(`/v1/workers/detail/${encodeURIComponent(worker)}`),
    updateWorkerAction: (worker, action) => post('/v1/workers/action', { worker, action }),

    fetchDirSpace: () => get<DirSpaceState>('/v1/fs/dir-space'),
    fetchDirSpaceStatus: () => get<DirSpaceStatus>('/v1/fs/dir-space/status'),
    updateDirSpace: (data) => post<DirSpaceState>('/v1/fs/dir-space', data),
    fetchDirSpaceSummaries: (paths) =>
      post<{ items: DirSpaceSummary[] }>('/v1/fs/dir-space/summaries', { paths }),

    fetchMountsData: () => get('/v1/mounts'),
    saveMount: (data) => post('/v1/mounts', data),
    deleteMount: (cvPath) => post('/v1/mounts/delete', { cv_path: cvPath }),
    validateMount: (data) => post('/v1/mounts/validate', data),
    startMountResync: (cvPath, dryRun = false) =>
      post('/v1/mounts/resync', { cv_path: cvPath, dry_run: dryRun }),
    fetchMountResync: (taskId) => get(`/v1/mounts/resync/${encodeURIComponent(taskId)}`),

    fetchJobsData: (params = {}) => get('/v1/jobs/load', { params }),
    submitLoadJob: (data) => post('/v1/jobs/load', data),
    fetchJobStatus: (jobId) => get<JobInfo>(`/v1/jobs/${encodeURIComponent(jobId)}`),
    cancelLoadJob: (jobId) =>
      requestJson(request, {
        url: `/v1/jobs/${encodeURIComponent(jobId)}`,
        method: 'delete',
        timeout: 30000
      }),

    fetchTransfersData: (params = {}) => get('/v1/transfers', { params }),
    submitTransfer: (data) => post('/v1/transfers', data),
    fetchTransferStatus: (jobId, params = {}) =>
      get<JobInfo>(`/v1/transfers/${encodeURIComponent(jobId)}`, { params }),
    cancelTransfer: (jobId) =>
      requestJson(request, {
        url: `/v1/transfers/${encodeURIComponent(jobId)}`,
        method: 'delete',
        timeout: 30000
      }),
    retryTransfer: (jobId) => post(`/v1/transfers/${encodeURIComponent(jobId)}/retry`, {}),

    fetchObservabilityHealth: () => get<ObservabilityHealth>('/v1/observability/health'),
    fetchObservabilitySettings: () => get<ObservabilitySettings>('/v1/observability/settings'),
    updateObservabilitySettings: (data) => put<ObservabilitySettings>('/v1/observability/settings', data),
    updateObservabilityMode: (mode) => post<ObservabilityHealth>('/v1/observability/mode', { mode }),
    fetchObservabilityTargets: async () => {
      const data = await get<{ targets?: TargetHealth[] }>('/v1/observability/targets')
      return Array.isArray(data?.targets) ? data.targets : []
    },
    fetchObservabilityNames: async () => {
      const data = await get<{ names?: string[] }>('/v1/observability/names')
      return Array.isArray(data?.names) ? data.names : []
    },
    fetchInstantMetrics: async (params = {}) => {
      const data = await get<{ series?: InstantMetric[] }>('/v1/observability/instant', { params })
      return Array.isArray(data?.series) ? data.series : []
    },
    queryMetrics: async (params) => {
      const data = await get<{ series?: RangeSeries[] }>('/v1/observability/query', { params })
      return Array.isArray(data?.series) ? data.series : []
    },
    fetchObservabilityDashboard: (params = {}) =>
      get<ObservabilityDashboard>('/v1/observability/dashboard', { params }),
    fetchObservabilityOperations: () => get<OperationsPayload>('/v1/observability/operations'),
    fetchObservabilityDiagnostics: async () => {
      const payload = await get<OperationsPayload>('/v1/observability/operations')
      return payload.diagnostics
    }
  }
}

export type { MountInfo }
