import * as React from 'react'

import { useApi } from '@/api/api-context'
import type { OverviewData } from '@/api/types'

export interface MasterRuntime {
  version: string
  versionCommit: string
  startTime: string
  startTimeMs: number | null
}

const EMPTY_RUNTIME: MasterRuntime = {
  version: '',
  versionCommit: '',
  startTime: '',
  startTimeMs: null
}

interface MasterRuntimeContextValue {
  runtime: MasterRuntime
  setFromOverview(payload: OverviewData | null | undefined): void
  refreshMasterRuntime(): Promise<void>
}

const MasterRuntimeContext = React.createContext<MasterRuntimeContextValue | null>(null)

/**
 * Master version/start-time shown in the sidebar and compared against worker
 * versions. Refreshes are de-duplicated: concurrent callers share one request.
 */
export function MasterRuntimeProvider({ children }: { children: React.ReactNode }) {
  const api = useApi()
  const [runtime, setRuntime] = React.useState<MasterRuntime>(EMPTY_RUNTIME)
  const inFlight = React.useRef<Promise<void> | null>(null)

  const setFromOverview = React.useCallback((payload: OverviewData | null | undefined) => {
    const startTimeMs = Number(payload?.start_time_ms)
    setRuntime({
      version: String(payload?.version || '').trim(),
      versionCommit: String(payload?.version_commit || '').trim(),
      startTime: String(payload?.start_time || '').trim(),
      startTimeMs: Number.isFinite(startTimeMs) && startTimeMs > 0 ? startTimeMs : null
    })
  }, [])

  const refreshMasterRuntime = React.useCallback(async () => {
    if (inFlight.current) return inFlight.current
    inFlight.current = (async () => {
      try {
        setFromOverview(await api.fetchOverviewData())
      } catch {
        // Keep the last known runtime; the sidebar stays usable without overview.
      } finally {
        inFlight.current = null
      }
    })()
    return inFlight.current
  }, [api, setFromOverview])

  const value = React.useMemo<MasterRuntimeContextValue>(
    () => ({ runtime, setFromOverview, refreshMasterRuntime }),
    [runtime, setFromOverview, refreshMasterRuntime]
  )

  return <MasterRuntimeContext.Provider value={value}>{children}</MasterRuntimeContext.Provider>
}

export function useMasterRuntime(): MasterRuntimeContextValue {
  const context = React.useContext(MasterRuntimeContext)
  if (!context) {
    throw new Error('useMasterRuntime must be used within MasterRuntimeProvider')
  }
  return context
}
