import type { Overview } from '@/lib/types'

/**
 * When the master started, in milliseconds since the Unix epoch, or null.
 *
 * `/api/overview` sends `start_time_ms` for this. Its `start_time` is the
 * master's local wall-clock time without a time zone, which a browser in
 * another zone reads hours off, so it is only used for masters that do not
 * send `start_time_ms` yet.
 */
export function masterStartMillis(overview: Overview | null): number | null {
  if (!overview) {
    return null
  }
  if (Number.isFinite(overview.start_time_ms) && overview.start_time_ms > 0) {
    return overview.start_time_ms
  }
  const parsed = overview.start_time ? new Date(overview.start_time).getTime() : NaN
  return Number.isNaN(parsed) ? null : parsed
}

/**
 * Time since `startMillis` as "Xd Yh Zm". A browser clock that runs behind the
 * master's shows 0m rather than a negative uptime.
 */
export function uptimeText(startMillis: number | null, nowMillis = Date.now()): string {
  if (startMillis === null) {
    return 'N/A'
  }
  const diff = Math.max(0, nowMillis - startMillis)
  const days = Math.floor(diff / (1000 * 60 * 60 * 24))
  const hours = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60))
  const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60))
  return `${days}d ${hours}h ${minutes}m`
}
