import * as React from 'react'

interface RefreshContextValue {
  /** Increments on every refresh request; pages watch it to re-fetch. */
  signal: number
  triggerRefresh(): void
  autoRefresh: boolean
  setAutoRefresh(enabled: boolean): void
}

const RefreshContext = React.createContext<RefreshContextValue | null>(null)

/** Legacy auto-refresh cadence, kept identical to the Vue shell. */
const AUTO_REFRESH_INTERVAL_MS = 5000

/** Replaces the old mitt `admin-refresh` event bus with a React signal. */
export function RefreshProvider({ children }: { children: React.ReactNode }) {
  const [signal, setSignal] = React.useState(0)
  const [autoRefresh, setAutoRefresh] = React.useState(false)

  const triggerRefresh = React.useCallback(() => {
    setSignal((value) => value + 1)
  }, [])

  React.useEffect(() => {
    if (!autoRefresh) return
    const timer = window.setInterval(() => {
      setSignal((value) => value + 1)
    }, AUTO_REFRESH_INTERVAL_MS)
    return () => window.clearInterval(timer)
  }, [autoRefresh])

  const value = React.useMemo<RefreshContextValue>(
    () => ({ signal, triggerRefresh, autoRefresh, setAutoRefresh }),
    [signal, triggerRefresh, autoRefresh]
  )

  return <RefreshContext.Provider value={value}>{children}</RefreshContext.Provider>
}

export function useRefresh(): RefreshContextValue {
  const context = React.useContext(RefreshContext)
  if (!context) {
    throw new Error('useRefresh must be used within RefreshProvider')
  }
  return context
}

/**
 * Re-run `refresh` whenever the global refresh signal changes.
 *
 * Uses the silent `refresh()` (not `reload()`) so an auto-refresh tick does not
 * flash the loading state over data the user is already looking at.
 */
export function useRefreshSignal(refresh: () => Promise<void>) {
  const { signal } = useRefresh()
  React.useEffect(() => {
    if (signal > 0) void refresh()
  }, [signal, refresh])
}
