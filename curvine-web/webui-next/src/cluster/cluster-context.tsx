import * as React from 'react'

import { useApi } from '@/api/api-context'
import type { ClusterSummary } from '@/api/types'
import { useToast } from '@/components/ui/toast'
import { useT } from '@/i18n'
import { useRefreshSignal } from '@/refresh/refresh-context'

/**
 * Selected cluster, persisted the same way the locale and theme are
 * (`curvine-theme`, `curvine-locale` — the same dash-separated key convention).
 * An empty value means "not selected": the request then carries no cluster
 * header and the backend answers with its own default cluster, which is the
 * behaviour of a single-cluster deployment.
 */
export const CLUSTER_STORAGE_KEY = 'curvine-cluster'

/** Sentinel `Select` value for "follow the backend default cluster". */
export const DEFAULT_CLUSTER_OPTION = 'default'

function readStoredCluster(): string {
  try {
    return window.localStorage.getItem(CLUSTER_STORAGE_KEY) ?? ''
  } catch {
    // Storage can be unavailable (private mode); no selection is a safe default.
    return ''
  }
}

interface ClusterContextValue {
  /** Empty means "follow the backend default cluster". */
  clusterId: string
  setClusterId(id: string): void
  clusters: ClusterSummary[]
  setClusters(clusters: ClusterSummary[]): void
  clustersLoading: boolean
  setClustersLoading(loading: boolean): void
  clustersError: string | null
  setClustersError(error: string | null): void
  /** Bumped by `refreshClusters`; `ClusterListBridge` re-reads the list on change. */
  reloadToken: number
  refreshClusters(): void
}

const ClusterContext = React.createContext<ClusterContextValue | null>(null)

/**
 * Selection state only — no network, no api client.
 *
 * It sits above `AuthProvider` because the API client is built *from* the
 * selected cluster: switching clusters has to rebuild the client, so the
 * selection cannot live below it. The list itself is fetched by
 * `ClusterListBridge`, which does sit inside the `ApiProvider`.
 */
export function ClusterSelectionProvider({ children }: { children: React.ReactNode }) {
  const [clusterId, setClusterIdState] = React.useState<string>(readStoredCluster)
  const [clusters, setClusters] = React.useState<ClusterSummary[]>([])
  const [clustersLoading, setClustersLoading] = React.useState(false)
  const [clustersError, setClustersError] = React.useState<string | null>(null)
  const [reloadToken, setReloadToken] = React.useState(0)

  const setClusterId = React.useCallback((id: string) => {
    setClusterIdState(id)
    try {
      window.localStorage.setItem(CLUSTER_STORAGE_KEY, id)
    } catch {
      // Persisting is best-effort; the in-memory choice still applies.
    }
  }, [])

  const refreshClusters = React.useCallback(() => {
    setReloadToken((token) => token + 1)
  }, [])

  const value = React.useMemo<ClusterContextValue>(
    () => ({
      clusterId,
      setClusterId,
      clusters,
      setClusters,
      clustersLoading,
      setClustersLoading,
      clustersError,
      setClustersError,
      reloadToken,
      refreshClusters
    }),
    [clusterId, setClusterId, clusters, clustersLoading, clustersError, reloadToken, refreshClusters]
  )

  return <ClusterContext.Provider value={value}>{children}</ClusterContext.Provider>
}

/**
 * Keeps the cluster list in the context in sync with the backend.
 *
 * Rendered inside `ApiProvider` (the layout element in App.tsx) and renders
 * nothing: the list is context state for the switcher, not a page.
 *
 * Without a `ClusterSelectionProvider` above it (a component test that mounts
 * the shell on its own) it is a no-op instead of a crash: there is no selection
 * to keep in sync, and the switcher falls back to its disabled default state.
 */
export function ClusterListBridge() {
  const cluster = useOptionalCluster()
  if (!cluster) return null
  return <ClusterListSync cluster={cluster} />
}

function ClusterListSync({ cluster }: { cluster: ClusterContextValue }) {
  const api = useApi()
  const t = useT()
  const toast = useToast()
  const { clusterId, setClusterId, setClusters, setClustersLoading, setClustersError, reloadToken } =
    cluster

  // Read through a ref so a fallback (which changes `clusterId`) does not turn
  // `load` into a new callback and re-fetch the list once more.
  const clusterIdRef = React.useRef(clusterId)
  React.useEffect(() => {
    clusterIdRef.current = clusterId
  }, [clusterId])

  const load = React.useCallback(async () => {
    setClustersLoading(true)
    try {
      const list = await api.fetchClusters()
      const next = Array.isArray(list) ? list : []
      setClusters(next)
      setClustersError(null)

      // A stored id that the backend no longer knows about (cluster removed, or a
      // stale localStorage from another deployment) falls back to the default
      // cluster instead of leaving every page on a 404. An empty list is *not* a
      // removal signal: it means the list could not be read, so the selection is
      // kept rather than silently reset by a degraded backend.
      const stored = clusterIdRef.current
      if (stored && next.length > 0 && !next.some((cluster) => cluster.id === stored)) {
        setClusterId('')
        toast.error(t('layout.clusterInvalidToast', { id: stored }))
      }
    } catch (error) {
      setClustersError(error instanceof Error ? error.message : String(error))
    } finally {
      setClustersLoading(false)
    }
  }, [api, setClusterId, setClusters, setClustersError, setClustersLoading, t, toast])

  React.useEffect(() => {
    void load()
  }, [load, reloadToken])
  useRefreshSignal(load)

  return null
}

export function useCluster(): ClusterContextValue {
  const context = useOptionalCluster()
  if (!context) {
    throw new Error('useCluster must be used within ClusterSelectionProvider')
  }
  return context
}

/**
 * Non-throwing variant.
 *
 * `AuthProvider` and `ConsoleLayout` read it: they also render in component
 * tests that mount a subtree without the full provider stack, where "no
 * selection" (the default cluster) is the correct fallback rather than a crash.
 */
export function useOptionalCluster(): ClusterContextValue | null {
  return React.useContext(ClusterContext)
}
