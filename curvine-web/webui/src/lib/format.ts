export function formatCount(value: number | null | undefined): string {
  return (value ?? 0).toLocaleString()
}

export function formatGB(bytes: number | null | undefined, digits = 2): string {
  if (!bytes) {
    return (0).toFixed(digits)
  }
  return (bytes / 1024 ** 3).toFixed(digits)
}

/**
 * A byte count in the largest binary unit that keeps the number at 1 or more,
 * with up to two decimals: 134217728 gives "128 MB", 1536 gives "1.5 KB".
 */
export function formatBytes(bytes: number | null | undefined): string {
  if (!(bytes && bytes > 0)) {
    return '0 B'
  }
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
  return `${parseFloat((bytes / 1024 ** exponent).toFixed(2))} ${units[exponent]}`
}

export function formatDate(millis: number | null | undefined): string {
  if (millis === null || millis === undefined) {
    return 'N/A'
  }
  return new Date(millis).toLocaleString()
}

export function formatTimestamp(millis: number | null | undefined): string {
  if (!millis) {
    return 'N/A'
  }
  const date = new Date(millis)
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  const hours = String(date.getHours()).padStart(2, '0')
  const minutes = String(date.getMinutes()).padStart(2, '0')
  const seconds = String(date.getSeconds()).padStart(2, '0')
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`
}
