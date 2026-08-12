function sanitizeActor(name: string) {
  const lowered = String(name || '').trim().toLowerCase()
  const sanitized = lowered.replace(/[^a-z0-9_]+/g, '_').replace(/^_+|_+$/g, '')
  return sanitized.slice(0, 16)
}

function isAutoExportJob(job: Record<string, unknown>) {
  const sourceType = String(job.source_type || job.type || '').toLowerCase()
  return sourceType.includes('fs_mode') || sourceType.includes('auto')
}

export function jobActorLabel(job: Record<string, unknown> | null | undefined) {
  if (!job) return 'user'
  const submitter = String(job.submitter || '').trim()
  if (submitter) {
    return sanitizeActor(submitter) || submitter.toLowerCase()
  }
  return isAutoExportJob(job) ? 'ufsloader' : 'user'
}

function jobActor(job: Record<string, unknown>) {
  return jobActorLabel(job)
}

function formatJobTimestamp(ms: number) {
  const value = Number(ms || 0)
  if (!value) return '00000000-000000'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '00000000-000000'
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${date.getUTCFullYear()}${pad(date.getUTCMonth() + 1)}${pad(date.getUTCDate())}-${pad(date.getUTCHours())}${pad(date.getUTCMinutes())}${pad(date.getUTCSeconds())}`
}

export function formatJobListTime(ms: number) {
  const value = Number(ms || 0)
  if (!value) return '-- -- --:--'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '-- -- --:--'
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`
}

export function jobPathSlug(sourcePath: string) {
  const trimmed = String(sourcePath || '').trim()
  const withoutScheme = trimmed.split('://').pop() || trimmed
  const segments = withoutScheme.replace(/^\/+|\/+$/g, '').split('/').filter(Boolean)
  if (!segments.length) return 'root'
  const tail = segments.length >= 2
    ? `${segments[segments.length - 2]}-${segments[segments.length - 1]}`
    : segments[segments.length - 1]
  const slug = tail.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').replace(/-+/g, '-')
  if (!slug) return 'path'
  return slug.length > 24 ? slug.slice(0, 24).replace(/-+$/g, '') : slug
}

export function jobCreateTimeMs(job: Record<string, unknown>) {
  const direct = Number(job.create_time_ms || 0)
  if (direct > 0) return direct
  const tasks = Array.isArray(job.tasks) ? job.tasks : []
  const taskTimes = tasks
    .map((task) => Number(task && typeof task === 'object' ? (task as Record<string, unknown>).create_time_ms : 0))
    .filter((value) => value > 0)
  if (taskTimes.length) return Math.min(...taskTimes)
  const updated = Number(job.update_time_ms || job.updated_at || 0)
  if (updated > 0) return updated
  return 0
}

export function jobListTitle(job: Record<string, unknown> | null | undefined) {
  if (!job) return '-'
  const actor = jobActorLabel(job)
  const time = formatJobListTime(jobCreateTimeMs(job))
  return `${actor} · ${time}`
}

export function buildJobDisplayName(job: Record<string, unknown> | null | undefined) {
  if (!job) return '-'
  if (job.display_name) return String(job.display_name)
  const sourcePath = String(job.source_path || job.path || '')
  const actor = jobActor(job)
  const timestamp = formatJobTimestamp(jobCreateTimeMs(job))
  const slug = jobPathSlug(sourcePath)
  return `job_${actor}_${timestamp}_${slug}`
}

const COMPLETE_JOB_ID_RE = /^job_[a-f0-9]{32}$/i

export function isCompleteJobId(value: string | null | undefined) {
  return COMPLETE_JOB_ID_RE.test(String(value || '').trim())
}

export function normalizeJobId(value: string | null | undefined) {
  return String(value || '').trim().toLowerCase()
}

export function shortJobId(id: string | null | undefined) {
  if (!id) return '-'
  const value = String(id)
  if (value.length <= 20) return value
  if (value.startsWith('job_')) {
    return `${value.slice(0, 12)}...${value.slice(-5)}`
  }
  return `${value.slice(0, 10)}...${value.slice(-5)}`
}

export function jobListSubtitle(job: Record<string, unknown> | null | undefined, jobId?: string | null) {
  const id = String(jobId || job?.job_id || job?.id || '').trim()
  return id || '-'
}

export function jobListTooltip(job: Record<string, unknown> | null | undefined, jobId?: string | null) {
  const id = String(jobId || job?.job_id || job?.id || '')
  const displayName = buildJobDisplayName(job)
  if (!id && (!displayName || displayName === '-')) return ''
  if (!id || id === '-') return displayName
  if (!displayName || displayName === '-') return id
  return `${displayName}\n${id}`
}
