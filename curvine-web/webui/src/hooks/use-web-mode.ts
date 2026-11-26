import { useEffect, useState } from 'react'
import { detectWebMode, type WebMode } from '@/lib/capabilities'

/** Resolves to `null` until the API probe has answered. */
export function useWebMode(): WebMode | null {
  const [mode, setMode] = useState<WebMode | null>(null)

  useEffect(() => {
    let cancelled = false
    void detectWebMode().then(result => {
      if (!cancelled) {
        setMode(result)
      }
    })
    return () => {
      cancelled = true
    }
  }, [])

  return mode
}
