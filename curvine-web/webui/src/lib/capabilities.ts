/**
 * The master and the transfer process serve the same SPA bundle, so the UI has
 * to find out which API it is talking to before it can pick a navigation.
 */
const API_BASE = import.meta.env.VITE_API_BASE || '/api'

export type WebMode = 'master' | 'transfer'

let detection: Promise<WebMode> | undefined

async function probe(): Promise<WebMode> {
  try {
    const response = await fetch(`${API_BASE}/transfer/jobs?limit=1`, {
      headers: { Accept: 'application/json' },
      signal: AbortSignal.timeout(5000),
    })
    if (!response.ok) {
      return 'master'
    }
    const body: unknown = await response.json()
    // The master answers unknown /api paths with index.html, which is not JSON;
    // only the transfer API returns a `jobs` array.
    if (body && typeof body === 'object' && Array.isArray((body as { jobs?: unknown }).jobs)) {
      return 'transfer'
    }
    return 'master'
  } catch {
    return 'master'
  }
}

export function detectWebMode(): Promise<WebMode> {
  detection ??= probe()
  return detection
}
