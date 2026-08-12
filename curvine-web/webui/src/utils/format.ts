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
