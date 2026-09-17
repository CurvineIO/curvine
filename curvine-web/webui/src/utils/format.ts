export function formatBytes(bytes: number | string | null | undefined): string {
  const value = Number(bytes ?? 0)
  if (!Number.isFinite(value) || value <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  const index = Math.min(
    Math.max(0, Math.floor(Math.log(value) / Math.log(1024))),
    units.length - 1
  )
  const scaled = value / Math.pow(1024, index)
  const decimals = index === 0
    ? (value < 0.1 ? 2 : value < 10 ? 1 : 0)
    : 2
  return `${scaled.toFixed(decimals)} ${units[index]}`
}

export function formatBytesPerSec(bytesPerSec: number | string | null | undefined): string {
  return `${formatBytes(bytesPerSec)}/s`
}

export function formatNumber(value) {
  return Number(value || 0).toLocaleString()
}

export function formatTime(value) {
  if (!value) return '-'
  if (typeof value === 'string') return value
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '-'
  return date.toLocaleString()
}

/**
 * Format POSIX permission bits as a nine character `rwx` string.
 *
 * Mirrors `format_permission` in curvine-cli/src/cmds/fs/common.rs, including
 * the setuid/setgid/sticky letters (`s`/`S`, `t`/`T`), so the web column matches
 * `curvine-cli fs stat` output. Non numeric input yields `-`; a mode of 0 is
 * still a valid `---------` (callers that treat 0 as "unset" check it first).
 */
export function formatPermission(mode: number | string | null | undefined): string {
  // Guard explicitly: `Number(null)` is 0, which would render `---------` for a
  // value the API never set.
  if (mode === null || mode === undefined || mode === '') return '-'
  const bits = Number(mode)
  if (!Number.isFinite(bits) || bits < 0) return '-'
  const value = Math.trunc(bits) & 0o7777

  const special = (set: boolean, plain: boolean, letter: string) => {
    if (set) return plain ? letter : letter.toUpperCase()
    return plain ? 'x' : '-'
  }

  return [
    value & 0o400 ? 'r' : '-',
    value & 0o200 ? 'w' : '-',
    special(Boolean(value & 0o4000), Boolean(value & 0o100), 's'),
    value & 0o040 ? 'r' : '-',
    value & 0o020 ? 'w' : '-',
    special(Boolean(value & 0o2000), Boolean(value & 0o010), 's'),
    value & 0o004 ? 'r' : '-',
    value & 0o002 ? 'w' : '-',
    special(Boolean(value & 0o1000), Boolean(value & 0o001), 't')
  ].join('')
}

const TTL_UNITS: Array<[number, string]> = [
  [86_400_000, 'd'],
  [3_600_000, 'h'],
  [60_000, 'm'],
  [1_000, 's']
]

/**
 * Format a millisecond TTL the way the CLI's `DurationUnit` Display does
 * (crates/core/runtime/src/common/duration_unit.rs). Unlike that Display, a
 * zero/absent TTL renders as `-` because it means "no TTL" for a file.
 */
export function formatTtl(ttlMs: number | string | null | undefined): string {
  const value = Number(ttlMs)
  if (!Number.isFinite(value) || value <= 0) return '-'
  for (const [span, suffix] of TTL_UNITS) {
    if (value % span === 0) return `${value / span}${suffix}`
  }
  return `${value}ms`
}

export function workerAddress(worker) {
  const address = worker.address || worker
  const host = address.hostname || worker.hostname || '-'
  const port = address.rpc_port || worker.rpc_port
  return port ? `${host}:${port}` : host
}

export function isLoopbackHost(host) {
  return /^(localhost|127\.0\.0\.1|::1|0\.0\.0\.0)$/i.test(String(host || '').trim())
}

export function displayMasterAddr(addr, preferredHostname = '') {
  if (!addr || addr === '-') return addr || '-'
  const match = String(addr).match(/^([^:]+):(\d+)$/)
  if (!match) return addr
  const [, host, port] = match
  if (!isLoopbackHost(host)) return addr
  const preferred = String(preferredHostname || '').trim()
  if (preferred && !isLoopbackHost(preferred)) return `${preferred}:${port}`
  return addr
}

export function workerHostname(worker) {
  const address = worker.address || worker
  return address.hostname || worker.hostname || '-'
}

export function workerId(worker) {
  const address = worker.address || worker
  return address.worker_id || worker.worker_id || '-'
}


export function normalizeCount(value) {
  const number = Number(value || 0)
  if (!Number.isFinite(number) || number < 0) return 0
  return number
}

export function formatCompactNumber(value) {
  const number = normalizeCount(value)
  return new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 }).format(number)
}

/** Parse Master/Web start_time strings such as "2026-08-18 11:00:00" or ISO-8601. */
export function parseDateTime(value: string | null | undefined): Date | null {
  if (!value || typeof value !== 'string') return null
  const trimmed = value.trim()
  if (!trimmed) return null
  const normalized = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(trimmed)
    ? trimmed.replace(' ', 'T')
    : trimmed
  const date = new Date(normalized)
  if (Number.isNaN(date.getTime())) return null
  return date
}

export type UptimeParts =
  | { kind: 'min'; count: number }
  | { kind: 'hours'; count: number }
  | { kind: 'days'; count: number }
  | { kind: 'daysHours'; days: number; hours: number }

/** Q12: <1h → min; <1d → hours; else days (optionally with hours). */
export function getUptimeParts(
  startTime: string | null | undefined,
  nowMs: number = Date.now(),
  startTimeMs?: number | null
): UptimeParts | null {
  let startMs: number | null = null
  if (startTimeMs != null && Number.isFinite(startTimeMs) && startTimeMs > 0) {
    startMs = startTimeMs
  } else {
    const start = parseDateTime(startTime)
    if (!start) return null
    startMs = start.getTime()
  }
  const elapsedMs = Math.max(0, nowMs - startMs)
  const totalMinutes = Math.floor(elapsedMs / 60_000)
  if (totalMinutes < 60) {
    return { kind: 'min', count: Math.max(0, totalMinutes) }
  }
  const totalHours = Math.floor(elapsedMs / 3_600_000)
  if (totalHours < 24) {
    return { kind: 'hours', count: totalHours }
  }
  const days = Math.floor(totalHours / 24)
  const hours = totalHours % 24
  if (hours === 0) return { kind: 'days', count: days }
  return { kind: 'daysHours', days, hours }
}

export function workerShortVersion(worker: Record<string, unknown> | null | undefined): string {
  if (!worker) return ''
  const version = worker.software_version ?? worker.version
  if (typeof version !== 'string') return ''
  return version.trim()
}

export function versionsMismatch(left: string | null | undefined, right: string | null | undefined): boolean {
  const a = String(left || '').trim()
  const b = String(right || '').trim()
  return Boolean(a && b && a !== b)
}
