import type { AxiosInstance, AxiosRequestConfig, AxiosProgressEvent } from 'axios'

import {
  createDownloadClient,
  createHttpClient,
  parseDownloadError,
  requestJson,
  type HttpClientOptions
} from './http'
import { ApiError } from './error'
import type {
  AuthSession,
  BlockLocationData,
  BrowseData,
  ClusterProbe,
  ClusterSummary,
  ClusterValidation,
  ConfigData,
  ConfigUpdate,
  ConfigUpdateResult,
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
  /**
   * Push or clear one hot-tunable override.
   *
   * `value: null` clears the override and restores the file-backed value;
   * `undefined` would be dropped by JSON serialisation, so clearing needs the
   * explicit `null` this signature forces.
   */
  updateConfig(data: ConfigUpdate): Promise<ConfigUpdateResult>

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

  /** Every cluster this console can switch to, including the default one. */
  fetchClusters(): Promise<ClusterSummary[]>
  /** Parse and check a TOML payload without persisting it. */
  validateCluster(toml: string): Promise<ClusterValidation>
  createCluster(toml: string): Promise<ClusterSummary>
  updateCluster(id: string, toml: string): Promise<ClusterSummary>
  deleteCluster(id: string): Promise<void>
  /** Connectivity probe of one cluster's master. */
  probeCluster(id: string): Promise<ClusterProbe>
  /** Re-read the clusters directory from disk. */
  reloadClusters(): Promise<ClusterSummary[]>
  /**
   * One cluster's on-disk TOML, verbatim (comments included).
   *
   * `fetchClusters` only projects a summary, so the editor reads the document
   * back before letting the user change it. The default cluster answers 409 and
   * an unknown one 404; both carry the standard error envelope.
   */
  getClusterConfig(id: string): Promise<string>
}

/**
 * Tolerant list decode.
 *
 * The cluster endpoints answer with a JSON array, but accepting the wrapped
 * forms too keeps the console working if the backend decides to page the list
 * (`{ clusters: [...] }`) — a shape mismatch would otherwise read as "no
 * clusters at all" and silently disable the switcher.
 */
function asClusterList(data: unknown): ClusterSummary[] {
  if (Array.isArray(data)) return data as ClusterSummary[]
  if (data && typeof data === 'object') {
    const wrapped = data as { clusters?: unknown; items?: unknown }
    if (Array.isArray(wrapped.clusters)) return wrapped.clusters as ClusterSummary[]
    if (Array.isArray(wrapped.items)) return wrapped.items as ClusterSummary[]
  }
  return []
}

/**
 * Normalise a failed `getClusterConfig` call.
 *
 * The request asks for `text`, so an error body — the standard JSON envelope —
 * reaches the interceptor as a raw string: the interceptor then reports the
 * whole envelope as the message and cannot read the `code` out of it. Parsing it
 * back here keeps `not_removable` (409) and `unknown_cluster` (404) available to
 * the caller and turns the message into prose.
 */
function clusterConfigError(error: unknown): unknown {
  if (!(error instanceof ApiError)) return error
  try {
    const envelope = JSON.parse(error.message) as { error?: { code?: unknown; message?: unknown } }
    const code = typeof envelope.error?.code === 'string' ? envelope.error.code : undefined
    const message =
      typeof envelope.error?.message === 'string' && envelope.error.message
        ? envelope.error.message
        : error.message
    return new ApiError(error.status, message, code)
  } catch {
    return error
  }
}

/**
 * Factory rather than a module singleton, so the API client can be injected and
 * stays decoupled from how the session is stored. `onUnauthorized` is the only
 * sign-out exit.
 */
export function createApiClient(options: HttpClientOptions = {}): ApiClient {
  const request: AxiosInstance = createHttpClient(options)
  const downloadClient: AxiosInstance = createDownloadClient(options.clusterId)
  /** TOML is the payload of the cluster endpoints, never JSON. */
  const tomlConfig = (): AxiosRequestConfig => ({
    headers: { 'Content-Type': 'text/plain' }
  })

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
    updateConfig: (data) => post<ConfigUpdateResult>('/v1/config', data),

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
    },

    fetchClusters: async () => {
      const data = await get<unknown>('/v1/clusters')
      return asClusterList(data)
    },
    validateCluster: (toml) => post<ClusterValidation>('/v1/clusters/validate', toml, tomlConfig()),
    createCluster: (toml) => post<ClusterSummary>('/v1/clusters', toml, tomlConfig()),
    updateCluster: (id, toml) =>
      put<ClusterSummary>(`/v1/clusters/${encodeURIComponent(id)}`, toml, tomlConfig()),
    deleteCluster: async (id) => {
      await requestJson(request, {
        url: `/v1/clusters/${encodeURIComponent(id)}`,
        method: 'delete'
      })
    },
    probeCluster: (id) =>
      post<ClusterProbe>(`/v1/clusters/${encodeURIComponent(id)}/probe`, {}),
    reloadClusters: async () => {
      const data = await post<unknown>('/v1/clusters/reload', {})
      return asClusterList(data)
    },
    getClusterConfig: async (id) => {
      try {
        const response = await request.request<string>({
          url: `/v1/clusters/${encodeURIComponent(id)}/config`,
          method: 'get',
          // The body is the file itself: TOML with the author's comments, so no
          // JSON parsing step may run over it (responseType 'text' plus an
          // identity transform pins that down).
          responseType: 'text',
          transformResponse: [(data: unknown) => data],
          headers: { Accept: 'text/plain' }
        })
        return typeof response.data === 'string' ? response.data : String(response.data ?? '')
      } catch (error) {
        throw clusterConfigError(error)
      }
    }
  }
}

export type { MountInfo }
