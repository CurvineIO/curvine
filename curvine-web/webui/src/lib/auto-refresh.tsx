/* eslint-disable react-refresh/only-export-components -- context and hook share one module */
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from 'react'

const STORAGE_KEY = 'curvine-auto-refresh'
const REFRESH_INTERVAL_MS = 5000

type Listener = () => void

interface AutoRefreshContextValue {
  enabled: boolean
  setEnabled: (enabled: boolean) => void
  subscribe: (listener: Listener) => () => void
}

const AutoRefreshContext = createContext<AutoRefreshContextValue | null>(null)

export function AutoRefreshProvider({ children }: { children: ReactNode }) {
  const [enabled, setEnabledState] = useState(() => localStorage.getItem(STORAGE_KEY) === 'true')
  const listeners = useRef(new Set<Listener>())

  const subscribe = useCallback((listener: Listener) => {
    listeners.current.add(listener)
    return () => {
      listeners.current.delete(listener)
    }
  }, [])

  const setEnabled = useCallback((next: boolean) => {
    setEnabledState(next)
    localStorage.setItem(STORAGE_KEY, String(next))
  }, [])

  useEffect(() => {
    if (!enabled) {
      return
    }
    const timer = window.setInterval(() => {
      listeners.current.forEach(listener => listener())
    }, REFRESH_INTERVAL_MS)
    return () => window.clearInterval(timer)
  }, [enabled])

  return (
    <AutoRefreshContext.Provider value={{ enabled, setEnabled, subscribe }}>
      {children}
    </AutoRefreshContext.Provider>
  )
}

export function useAutoRefresh(): AutoRefreshContextValue {
  const context = useContext(AutoRefreshContext)
  if (!context) {
    throw new Error('useAutoRefresh must be used inside AutoRefreshProvider')
  }
  return context
}

/** Registers the page's refetch callback with the auto refresh timer. */
export function useAutoRefreshEffect(refetch: () => void) {
  const { subscribe } = useAutoRefresh()
  const refetchRef = useRef(refetch)

  useEffect(() => {
    refetchRef.current = refetch
  })

  useEffect(() => subscribe(() => refetchRef.current()), [subscribe])
}
