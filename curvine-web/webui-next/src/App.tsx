import { lazy } from 'react'
import { Navigate, Route, Routes } from 'react-router-dom'

import { RequireAuth } from '@/auth/auth-context'
import { ClusterListBridge } from '@/cluster/cluster-context'
import { ConsoleLayout } from '@/components/console-layout'
import { LoginPage } from '@/pages/login-page'

/**
 * Every page is its own chunk.
 *
 * Shipping them all up front put the whole console in one ~690 kB entry file,
 * even though a visitor only ever opens one page: the observability page alone
 * drags in ECharts (~1 MB). The shell is what loads eagerly, and ConsoleLayout
 * holds the Suspense boundary so the sidebar and topbar stay mounted while a
 * route chunk arrives. Login stays eager — it is the pre-auth screen.
 *
 * `import()` inside `lazy` is required for the split; a static import at the top
 * of this file would pull the page back into the entry chunk.
 */
const OverviewPage = lazy(() =>
  import('@/pages/overview-page').then((module) => ({ default: module.OverviewPage }))
)
const BrowsePage = lazy(() =>
  import('@/pages/browse-page').then((module) => ({ default: module.BrowsePage }))
)
const MountsPage = lazy(() =>
  import('@/pages/mounts-page').then((module) => ({ default: module.MountsPage }))
)
const WorkersPage = lazy(() =>
  import('@/pages/workers-page').then((module) => ({ default: module.WorkersPage }))
)
const JobsPage = lazy(() =>
  import('@/pages/jobs-page').then((module) => ({ default: module.JobsPage }))
)
const TransfersPage = lazy(() =>
  import('@/pages/transfers-page').then((module) => ({ default: module.TransfersPage }))
)
const SyncJobsPage = lazy(() =>
  import('@/pages/sync-jobs-page').then((module) => ({ default: module.SyncJobsPage }))
)
const ObservabilityPage = lazy(() =>
  import('@/pages/observability-page').then((module) => ({ default: module.ObservabilityPage }))
)
const OperationsPage = lazy(() =>
  import('@/pages/operations-page').then((module) => ({ default: module.OperationsPage }))
)
const ConfigPage = lazy(() =>
  import('@/pages/config-page').then((module) => ({ default: module.ConfigPage }))
)
const BlocksPage = lazy(() =>
  import('@/pages/blocks-page').then((module) => ({ default: module.BlocksPage }))
)
const ClustersPage = lazy(() =>
  import('@/pages/clusters-page').then((module) => ({ default: module.ClustersPage }))
)

/**
 * Route paths must stay in sync with the SPA whitelist hardcoded in
 * curvine-web/src/router/admin_handler.rs — the backend answers these exact
 * paths with index.html.
 */
export function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route
        element={
          <RequireAuth>
            <ClusterListBridge />
            <ConsoleLayout />
          </RequireAuth>
        }
      >
        <Route index element={<Navigate to="/overview" replace />} />
        <Route path="overview" element={<OverviewPage />} />
        <Route path="browse" element={<BrowsePage />} />
        <Route path="mounts" element={<MountsPage />} />
        <Route path="workers/:id?" element={<WorkersPage />} />
        <Route path="jobs" element={<JobsPage />} />
        <Route path="transfers" element={<TransfersPage />} />
        <Route path="sync-jobs" element={<SyncJobsPage />} />
        <Route path="observability" element={<ObservabilityPage />} />
        <Route path="operations" element={<OperationsPage />} />
        <Route path="config" element={<ConfigPage />} />
        <Route path="clusters" element={<ClustersPage />} />
        <Route path="blocks" element={<BlocksPage />} />
      </Route>
      <Route path="*" element={<Navigate to="/overview" replace />} />
    </Routes>
  )
}
