export function formatCount(value: number | null | undefined): string {
  return (value ?? 0).toLocaleString()
}

export function formatGB(bytes: number | null | undefined, digits = 2): string {
  if (!bytes) {
    return (0).toFixed(digits)
  }
  return (bytes / 1024 ** 3).toFixed(digits)
}

export function formatDate(millis: number | null | undefined): string {
  if (millis === null || millis === undefined) {
    return 'N/A'
  }
  return new Date(millis).toLocaleString()
}
