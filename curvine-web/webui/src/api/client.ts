/*
 * Copyright 2025 OPPO.
 *
 * Licensed under the Apache License, Version 2.0.
 */

import request from '@/utils/request'
import axios from 'axios'

const downloadClient = axios.create({
  baseURL: import.meta.env.VITE_API_BASE || '/api',
  timeout: 120000,
  withCredentials: true,
  headers: { Accept: 'application/json' }
})

async function parseDownloadError(error) {
  const response = error && error.response
  if (!response || !response.data) {
    return (error && error.message) || 'Unknown error'
  }
  const data = response.data
  if (typeof data === 'string') return data
  if (data instanceof Blob) {
    try {
      const text = await data.text()
      const parsed = JSON.parse(text)
      return parsed.message || parsed.error?.message || text
    } catch (_) {
      return (error && error.message) || 'Unknown error'
    }
  }
  return data.message || data.error?.message || (error && error.message) || 'Unknown error'
}

function unwrap(response) {
  const payload = response && response.data !== undefined ? response.data : response
  if (typeof payload === 'string') {
    const trimmed = payload.trim().toLowerCase()
    if (trimmed.startsWith('<!doctype') || trimmed.startsWith('<html')) {
      throw new Error('Expected JSON API response, got HTML fallback')
    }
  }
  if (payload && payload.success === true) {
    return payload.data
  }
  if (payload && payload.success === false) {
    throw new Error(payload.error?.message || 'API request failed')
  }
  return payload
}

// All data endpoints are served by the authenticated /v1 API. The legacy
// non-versioned routes (/api/overview, /api/browse, ...) are unauthenticated,
// so we must NOT silently fall back to them: a rejected/failed v1 request must
// surface the error instead of leaking data through an anonymous route.
async function getV1(url, options = {}) {
  return unwrap(await request({ url, method: 'get', ...options }))
}

async function postV1(url, data) {
  const result = unwrap(await request({ url, method: 'post', data }))
  return result
}

async function putV1(url, data) {
  return unwrap(await request({ url, method: 'put', data }))
}


export function login(data) {
  return postV1('/v1/auth/login', data)
}

export function fetchAuthSession() {
  return getV1('/v1/auth/session')
}

export function logout() {
  return postV1('/v1/auth/logout', {})
}

export function fetchOverviewData() {
  return getV1('/v1/overview')
}
export function fetchMasterHa() {
  return getV1('/v1/master/ha')
}

export async function requestMasterFailover(targetMaster) {
  return unwrap(await request({ url: '/v1/master/failover', method: 'post', data: { target_master: targetMaster }, timeout: 180000 }))
}

export function fetchConfigData() {
  return getV1('/v1/config')
}

export function fetchBrowseData(query) {
  return getV1('/v1/fs/list', { params: query })
}

export function fetchWorkersData() {
  return getV1('/v1/workers')
}

export function fetchWorkerDetail(worker) {
  return getV1(`/v1/workers/detail/${encodeURIComponent(worker)}`)
}

export function updateWorkerAction(worker, action) {
  return postV1('/v1/workers/action', { worker, action })
}

export function fetchBlocksData(query) {
  return getV1('/v1/fs/blocks', { params: query })
}

export function fetchUfsSyncStatus(path) {
  return getV1('/v1/fs/ufs-sync', { params: { path } })
}

export function fetchUfsSyncJobs(path, limit = 100, includeFinished = true, options: { includeTasks?: boolean; failedOnly?: boolean; state?: string } = {}) {
  return getV1('/v1/fs/ufs-sync/jobs', {
    params: {
      path,
      limit,
      include_finished: includeFinished,
      include_tasks: options.includeTasks || false,
      failed_only: options.failedOnly || false,
      state: options.state || undefined
    }
  })
}

export function fetchMountsData() {
  return getV1('/v1/mounts')
}

export function saveMount(data) {
  return postV1('/v1/mounts', data)
}

export function deleteMount(cvPath) {
  return postV1('/v1/mounts/delete', { cv_path: cvPath })
}

export function validateMount(data) {
  return postV1('/v1/mounts/validate', data)
}

export function startMountResync(cvPath, dryRun = false) {
  return postV1('/v1/mounts/resync', { cv_path: cvPath, dry_run: dryRun })
}

export function fetchMountResync(taskId) {
  return getV1(`/v1/mounts/resync/${encodeURIComponent(taskId)}`)
}

export function fetchJobsData(params = {}) {
  return getV1('/v1/jobs/load', { params })
}

export function submitLoadJob(data) {
  return postV1('/v1/jobs/load', data)
}

export function fetchJobStatus(jobId) {
  return getV1(`/v1/jobs/${encodeURIComponent(jobId)}`)
}

export async function cancelLoadJob(jobId) {
  return unwrap(await request({ url: `/v1/jobs/${encodeURIComponent(jobId)}`, method: 'delete', timeout: 30000 }))
}

export function fetchTransfersData(params = {}) {
  return getV1('/v1/transfers', { params })
}

export function submitTransfer(data) {
  return postV1('/v1/transfers', data)
}

export function fetchTransferStatus(jobId, params = {}) {
  return getV1(`/v1/transfers/${encodeURIComponent(jobId)}`, { params })
}

export async function cancelTransfer(jobId) {
  return unwrap(await request({ url: `/v1/transfers/${encodeURIComponent(jobId)}`, method: 'delete', timeout: 30000 }))
}

export function retryTransfer(jobId) {
  return postV1(`/v1/transfers/${encodeURIComponent(jobId)}/retry`, {})
}


export function createDirectory(path, createParent = true) {
  return postV1('/v1/fs/mkdir', { path, create_parent: createParent })
}

export function deletePath(path, recursive = false) {
  return postV1('/v1/fs/delete', { path, recursive })
}

export function freePath(path, recursive = false) {
  return postV1('/v1/fs/free', { path, recursive })
}

export async function uploadFile(path, file, overwrite = true, onUploadProgress = null, timeout = 300000, syncUfs = true) {
  const result = unwrap(await request({
    url: `/v1/fs/upload?path=${encodeURIComponent(path)}&overwrite=${overwrite}&sync_ufs=${syncUfs}`,
    method: 'post',
    data: file,
    timeout,
    onUploadProgress: onUploadProgress || undefined
  }))
  return result
}

export async function downloadFile(path) {
  try {
    return await downloadClient.get('/v1/fs/download', {
      params: { path },
      responseType: 'blob'
    })
  } catch (error) {
    throw new Error(await parseDownloadError(error))
  }
}

export interface ObservabilityHealth {
  enabled: boolean
  mode: string
  active_provider: string
  prometheus_configured?: boolean
  provider_health?: string | null
  fallback_reason?: string | null
  prometheus_url?: string | null
  scrape_interval_ms: number
  retention_ms: number
  series_count: number
  sample_count: number
  last_discovery_ms?: number | null
  last_collect_ms?: number | null
}

export interface ObservabilitySettings {
  scrape_interval_ms: number
  retention_ms: number
  scrape_interval_options_ms: number[]
  retention_options_ms: number[]
  max_retention_ms: number
  max_samples_per_series_cap: number
  max_total_samples: number
  max_series: number
  estimated_peak_samples: number
}

export interface TargetHealth {
  id: string
  component: string
  instance: string
  url: string
  worker_id?: number | null
  worker_state?: string | null
  active_master?: string | null
  up: boolean
  last_scrape_ms?: number | null
  last_success_ms?: number | null
  last_scrape_duration_ms?: number | null
  last_error: string
  last_warning: string
  sample_count: number
  parse_error_count: number
}

export interface InstantMetric {
  metric: string
  labels: Record<string, string>
  timestamp_ms: number
  value: number
}

export interface RangeSeries {
  metric: string
  labels: Record<string, string>
  points: [number, number][]
}

export interface MetricQueryParams {
  metric?: string
  component?: string
  instance?: string
  range?: string
  step?: string
}

export interface DashboardQueryParams {
  range?: string
  step?: string
}

export interface SeriesSummary {
  current?: number | null
  min?: number | null
  max?: number | null
  avg?: number | null
}

export interface DashboardSeries {
  name: string
  labels: Record<string, string>
  points: [number, number][]
  summary?: SeriesSummary | null
}

export type PanelKind = 'timeseries' | 'table' | 'instant'

export interface DashboardTableRow {
  id: string
  component: string
  instance: string
  scrape_up: boolean
  worker_id?: number | null
  worker_state?: string | null
  status: string
  detail?: string
}

export interface DashboardPanel {
  id: string
  title: string
  subtitle: string
  kind: PanelKind
  series: DashboardSeries[]
  rows?: DashboardTableRow[]
  section?: string | null
  error?: string | null
}

export interface DashboardGroup {
  id: string
  title: string
  panels: DashboardPanel[]
}

export interface DashboardCard {
  id: string
  title: string
  value: string
  unit?: string | null
  tone?: string | null
  meta?: string | null
}

export interface DashboardWindow {
  range: string
  step: string
  start_ms: number
  end_ms: number
  last_collect_ms?: number | null
}

export interface ObservabilityDashboard {
  window: DashboardWindow
  cards: DashboardCard[]
  groups: DashboardGroup[]
}

export type DiagnosticStatus = 'pass' | 'warning' | 'fail' | 'skipped'

export interface DiagnosticCheck {
  id: string
  title: string
  status: DiagnosticStatus
  summary: string
  detail?: string
  suggested_fix?: string
}

export interface MissingTarget {
  id: string
  source: string
  component: string
  instance: string
  url: string
  reason: string
}

export interface DiagnosticsPayload {
  generated_ms: number
  active_provider: string
  overall_status: DiagnosticStatus
  checks: DiagnosticCheck[]
  discovered_master_targets: number
  discovered_live_worker_targets: number
  discovered_transfer_targets: number
  prometheus_covered_targets: number
  active_covered_targets: number
  expected_targets: number
  missing_targets: MissingTarget[]
}

export type AlertSeverity = 'critical' | 'warning'

export interface OperationsAlert {
  id: string
  severity: AlertSeverity
  title: string
  summary: string
  source: string
  target_id?: string | null
  suggested_fix: string
}

export interface OperationsPayload {
  generated_ms: number
  diagnostics: DiagnosticsPayload
  alerts: OperationsAlert[]
}

export function fetchObservabilityHealth() {
  return getV1('/v1/observability/health') as Promise<ObservabilityHealth>
}

export function fetchObservabilitySettings() {
  return getV1('/v1/observability/settings') as Promise<ObservabilitySettings>
}

export function updateObservabilitySettings(data: {
  scrape_interval_ms?: number
  retention_ms?: number
}) {
  return putV1('/v1/observability/settings', data) as Promise<ObservabilitySettings>
}

export function updateObservabilityMode(mode: string) {
  return postV1('/v1/observability/mode', { mode }) as Promise<ObservabilityHealth>
}

export async function fetchObservabilityTargets() {
  const data = await getV1('/v1/observability/targets') as { targets?: TargetHealth[] }
  return Array.isArray(data?.targets) ? data.targets : []
}

export async function fetchObservabilityNames() {
  const data = await getV1('/v1/observability/names') as { names?: string[] }
  return Array.isArray(data?.names) ? data.names : []
}

export async function fetchInstantMetrics(params: MetricQueryParams = {}) {
  const data = await getV1('/v1/observability/instant', { params }) as { series?: InstantMetric[] }
  return Array.isArray(data?.series) ? data.series : []
}

export async function queryMetrics(params: MetricQueryParams) {
  const data = await getV1('/v1/observability/query', { params }) as { series?: RangeSeries[] }
  return Array.isArray(data?.series) ? data.series : []
}

export function fetchObservabilityDashboard(params: DashboardQueryParams = {}) {
  return getV1('/v1/observability/dashboard', { params }) as Promise<ObservabilityDashboard>
}

export function fetchObservabilityOperations() {
  return getV1('/v1/observability/operations') as Promise<OperationsPayload>
}
