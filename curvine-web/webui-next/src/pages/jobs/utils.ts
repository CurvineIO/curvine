import type { JobInfo, JobTask, MountInfo } from '@/api/types'
import { formatBytes, formatTime } from '@/lib/format'
import {
  buildJobDisplayName,
  isCompleteJobId,
  jobListSubtitle,
  jobListTitle,
  jobListTooltip,
  normalizeJobId,
  shortJobId
} from '@/lib/jobDisplay'
import type { Translate } from '@/i18n'

export const TIME_RANGES: Record<string, number> = {
  '1h': 60 * 60 * 1000,
  '24h': 24 * 60 * 60 * 1000,
  '7d': 7 * 24 * 60 * 60 * 1000
}

export function jobId(job: JobInfo | null | undefined): string {
  return String(job?.job_id || job?.id || '')
}

export function stateValue(job: JobInfo | JobTask | null | undefined): string {
  const raw = job?.state || job?.status
  return raw ? String(raw).toLowerCase() : ''
}

export function stateLabel(job: JobInfo | JobTask | null | undefined): string {
  return String(job?.state || job?.status || 'Unknown')
}

export function isFinal(job: JobInfo | null | undefined): boolean {
  if (job?.done === true) return true
  return ['completed', 'failed', 'canceled'].includes(stateValue(job))
}

export function jobBucket(job: JobInfo): string {
  const value = stateValue(job)
  if (['pending', 'loading', 'running'].includes(value)) return 'running'
  if (value === 'completed') return 'completed'
  if (value === 'failed') return 'failed'
  if (value === 'canceled') return 'canceled'
  if (!value || value === 'unknown') return 'running'
  return 'failed'
}

export function jobClass(job: JobInfo | JobTask): string {
  const value = stateValue(job)
  if (value.includes('fail') || value.includes('cancel')) return 'failed'
  if (value.includes('pend') || value.includes('wait')) return 'waiting'
  if (value.includes('load') || value.includes('run')) return 'running'
  if (value.includes('complete')) return 'healthy'
  return 'warning'
}

export function jobProgress(job: JobInfo): number {
  return Math.max(0, Math.min(100, Math.round(Number(job.progress || 0))))
}

export function formatFiles(job: JobInfo): string {
  const total = Number(job.total_files || 0)
  if (!total) return '-'
  return `${Number(job.completed_files || 0)} / ${total}`
}

export function sizeLabel(job: JobInfo): string {
  const loaded = Number(job.loaded_size || 0)
  const total = Number(job.total_size || 0)
  if (!loaded && !total) return '-'
  return `${formatBytes(loaded)} / ${formatBytes(total)}`
}

export function sourcePath(job: JobInfo | null | undefined): string {
  return job?.path || job?.source_path || '-'
}

export function targetPath(job: JobInfo | null | undefined): string {
  return job?.target_path || '-'
}

export function isAutoExportJob(job: JobInfo | null | undefined): boolean {
  if (!job) return false
  const sourceType = String(job.source_type || job.type || '').toLowerCase()
  return sourceType.includes('auto') || sourceType.includes('fs_mode')
}

export function jobTypeLabel(t: Translate, job: JobInfo): string {
  return isAutoExportJob(job) ? t('jobs.types.autoExport') : t('jobs.types.manualLoad')
}

export function jobTriggerLabel(t: Translate, job: JobInfo): string {
  if (!isAutoExportJob(job)) return t('jobs.types.manual')
  return String(job.trigger || job.trigger_event || t('jobs.types.triggerDefault'))
}

export function jobCreatedBy(t: Translate, job: JobInfo): string {
  const submitter = String(job.submitter || '').trim()
  if (submitter) return submitter
  return String(
    job.created_by || (isAutoExportJob(job) ? t('jobs.createdBy.ufsLoader') : t('jobs.createdBy.user'))
  )
}

export function isUfsPath(path: unknown): boolean {
  const value = String(path || '')
  return value.includes('://') && !value.startsWith('cv://')
}

function pathStartsWith(path: unknown, prefix: unknown): boolean {
  const value = String(path || '').replace(/\/+$/g, '')
  const base = String(prefix || '').replace(/\/+$/g, '')
  if (!value || !base) return false
  return value === base || value.startsWith(`${base}/`)
}

function pathMatchesMount(path: unknown, mount: MountInfo): boolean {
  return pathStartsWith(path, mount.cv_path) || pathStartsWith(path, mount.ufs_path)
}

export function mountPath(job: JobInfo | null | undefined, mounts: MountInfo[]): string {
  if (!job) return '-'
  if (job.mount_path) return String(job.mount_path)
  const paths = [sourcePath(job), targetPath(job)]
  const mount = mounts.find((item) => paths.some((path) => pathMatchesMount(path, item)))
  return mount ? mount.cv_path : '-'
}

export function ufsUri(job: JobInfo | null | undefined, mounts: MountInfo[]): string {
  if (!job) return '-'
  if (job.ufs_path) return String(job.ufs_path)
  const paths = [sourcePath(job), targetPath(job)]
  const direct = paths.find((path) => isUfsPath(path))
  if (direct) return direct
  const mount = mounts.find((item) => paths.some((path) => pathMatchesMount(path, item)))
  return mount?.ufs_path ? mount.ufs_path : '-'
}

export function taskDetails(job: JobInfo | null | undefined): JobTask[] {
  return Array.isArray(job?.tasks) ? (job!.tasks as JobTask[]) : []
}

export function isJobLevelFailure(job: JobInfo | null | undefined): boolean {
  if (!job || stateValue(job) !== 'failed') return false
  if (taskDetails(job).length > 0) return false
  return Number(job.total_files || 0) === 0
}

export function jobLevelFailureTasks(job: JobInfo): JobTask[] {
  if (!isJobLevelFailure(job)) return []
  return [
    {
      task_id: '(job-level)',
      state: 'failed',
      source_path: sourcePath(job),
      target_path: targetPath(job),
      message: job.message || 'Job failed before tasks were created',
      worker: '-',
      update_time_ms: job.update_time_ms
    }
  ]
}

export function allTasksForJob(job: JobInfo): JobTask[] {
  const tasks = taskDetails(job)
  return tasks.length ? tasks : jobLevelFailureTasks(job)
}

export function isFailedTask(task: JobTask): boolean {
  return stateValue(task) === 'failed'
}

export function failedTaskCount(job: JobInfo): number {
  const tasks = taskDetails(job)
  if (tasks.length) return tasks.filter((task) => isFailedTask(task)).length
  const failedFiles = Number(job.failed_files || job.failed_tasks || 0)
  if (failedFiles > 0) return failedFiles
  if (isJobLevelFailure(job)) return 1
  return 0
}

export function runningTaskCount(job: JobInfo): number {
  if (!['pending', 'loading', 'running'].includes(stateValue(job))) return 0
  const tasks = taskDetails(job)
  if (tasks.length) {
    return tasks.filter((task) => ['pending', 'loading', 'running'].includes(stateValue(task))).length
  }
  return Number(job.running_files || job.loading_files || job.pending_files || 0)
}

export function hasFailedWork(job: JobInfo): boolean {
  return stateValue(job) === 'failed' || failedTaskCount(job) > 0
}

export function displayTaskTotal(job: JobInfo | null | undefined): number {
  if (!job) return 0
  const total = Number(job.total_files || 0)
  if (total > 0) return total
  if (isJobLevelFailure(job)) return 1
  return taskDetails(job).length || 0
}

export function taskWorker(task: JobTask): string {
  const value = String(task.worker || '')
  if (!value) return '-'
  const match = value.match(/addr = ([^,\s]+)/)
  return match ? match[1] : value
}

export function taskMessage(task: JobTask, job: JobInfo | null = null): string {
  const message = task.message || task.progress?.message
  return message || job?.message || '-'
}

export function taskErrorCode(task: JobTask, job: JobInfo | null = null): string {
  const message = String(taskMessage(task, job) || '').toLowerCase()
  if (!message || message === '-') return '-'
  if (message.includes('no such') || message.includes('not found')) return 'ENOENT'
  if (message.includes('permission') || message.includes('access denied')) return 'EACCES'
  if (message.includes('timeout') || message.includes('timed out')) return 'ETIMEDOUT'
  if (message.includes('lookup address') || message.includes('name or service')) return 'DNS'
  if (message.includes('io') || message.includes('failed')) return 'EIO'
  return '-'
}

export function retryCount(task: JobTask): string | number {
  const value = task.retry_count ?? task.retries ?? task.attempts
  return value === undefined || value === null ? '-' : value
}

export function shortTaskId(id: string | null | undefined): string {
  if (!id) return '-'
  const value = String(id)
  return value.length > 22 ? `${value.slice(0, 12)}...${value.slice(-7)}` : value
}

export function isWithinRange(timestamp: unknown, range: string): boolean {
  if (!range || range === 'all') return true
  const value = Number(timestamp || 0)
  if (!value) return true
  const span = TIME_RANGES[range]
  return !span || Date.now() - value <= span
}

export function relativeTime(timestamp: unknown): string {
  const value = Number(timestamp || 0)
  if (!value) return '-'
  const delta = Math.max(0, Date.now() - value)
  if (delta < 60 * 1000) return 'just now'
  if (delta < 60 * 60 * 1000) return `${Math.floor(delta / 60000)} min ago`
  if (delta < 24 * 60 * 60 * 1000) return `${Math.floor(delta / 3600000)} h ago`
  return `${Math.floor(delta / 86400000)} d ago`
}

export function cliCommands(
  t: Translate,
  job: JobInfo,
  mounts: MountInfo[]
): { key: string; label: string; command: string }[] {
  if (!job) return []
  const id = jobId(job)
  const items = [
    { key: 'job-status', label: t('jobs.detail.cliJobStatus'), command: `curvine load-status ${id} --verbose` },
    {
      key: 'path-status',
      label: t('jobs.detail.cliPathStatus'),
      command: `curvine load-status --path ${sourcePath(job)} --verbose`
    }
  ]
  const mount = mountPath(job, mounts)
  if (mount !== '-') {
    items.push({
      key: 'mount-list',
      label: t('jobs.detail.cliFailedOnMount'),
      command: `curvine load-list --mount ${mount} --state failed --limit 50 --verbose`
    })
  }
  return items
}

export function mergeJobList(previousJobs: JobInfo[], nextJobs: JobInfo[]): JobInfo[] {
  const previousById = new Map(previousJobs.map((job) => [jobId(job), job]))
  const byId = new Map<string, JobInfo>()
  for (const job of nextJobs) {
    const id = jobId(job)
    if (!id) continue
    const previous = previousById.get(id)
    if (previous) {
      const next: JobInfo = { ...previous, ...job }
      if (!Array.isArray(next.tasks) || next.tasks.length === 0) next.tasks = previous.tasks || []
      byId.set(id, next)
    } else {
      byId.set(id, job)
    }
  }
  return Array.from(byId.values()).sort((left, right) => {
    const leftTime = Number(left.update_time_ms || left.updated_at || 0)
    const rightTime = Number(right.update_time_ms || right.updated_at || 0)
    return rightTime - leftTime
  })
}

export function upsertJob(jobs: JobInfo[], job: JobInfo): JobInfo[] {
  const id = jobId(job)
  if (!id) return jobs
  const index = jobs.findIndex((item) => jobId(item) === id)
  if (index < 0) return [job, ...jobs]
  const previous = jobs[index]
  const next: JobInfo = { ...previous, ...job }
  if (!Array.isArray(next.tasks) || next.tasks.length === 0) next.tasks = previous.tasks || []
  return [...jobs.slice(0, index), next, ...jobs.slice(index + 1)]
}

export function trimSlashes(value: unknown): string {
  return String(value || '').replace(/^\/+|\/+$/g, '')
}

export function joinPath(prefix: unknown, suffix: unknown): string {
  const base = String(prefix || '').replace(/\/+$/g, '')
  const rest = trimSlashes(suffix)
  return rest ? `${base}/${rest}` : base
}

export function parseConfigs(
  t: Translate,
  text: string
): Record<string, string> {
  const configs: Record<string, string> = {}
  for (const line of text.split('\n')) {
    const value = line.trim()
    if (!value) continue
    const index = value.indexOf('=')
    if (index <= 0) throw new Error(t('jobs.alerts.invalidConfig', { line: value }))
    configs[value.slice(0, index).trim()] = value.slice(index + 1).trim()
  }
  return configs
}

export function fileName(path: unknown): string {
  const parts = String(path || '').split('/').filter(Boolean)
  return parts.pop() || '/'
}

export {
  buildJobDisplayName,
  formatTime,
  isCompleteJobId,
  jobListSubtitle,
  jobListTitle,
  jobListTooltip,
  normalizeJobId,
  shortJobId
}
