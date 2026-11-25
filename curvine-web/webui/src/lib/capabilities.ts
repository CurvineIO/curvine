import { api } from '@/lib/api'

/**
 * The master and the transfer process serve the same SPA bundle, so the UI has
 * to find out which API it is talking to before it can pick a navigation.
 */
export type WebMode = 'master' | 'transfer'

let detection: Promise<WebMode> | undefined

async function probe(): Promise<WebMode> {
  try {
    // The master answers unknown /api paths with index.html, which the client
    // rejects; only the transfer API returns a `jobs` array.
    const response = await api.transferJobs(
      { limit: 1 },
      { signal: AbortSignal.timeout(5000), quiet: true },
    )
    return Array.isArray(response.jobs) ? 'transfer' : 'master'
  } catch {
    return 'master'
  }
}

export function detectWebMode(): Promise<WebMode> {
  detection ??= probe()
  return detection
}
