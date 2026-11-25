import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { AppShell } from '@/components/app-shell'
import { BlocksPage } from '@/pages/blocks'
import { BrowsePage } from '@/pages/browse'
import { ConfigPage } from '@/pages/config'
import { OverviewPage } from '@/pages/overview'
import { PlaceholderPage } from '@/pages/placeholder'
import { TransferDetailRoute } from '@/pages/transfer-detail'
import { TransfersPage } from '@/pages/transfers'
import { WorkersPage } from '@/pages/workers'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<AppShell />}>
          <Route index element={<Navigate to="/overview" replace />} />
          <Route path="overview" element={<OverviewPage />} />
          <Route path="browse" element={<BrowsePage />} />
          <Route path="blocks" element={<BlocksPage />} />
          <Route path="workers" element={<WorkersPage />} />
          <Route path="config" element={<ConfigPage />} />
          <Route path="transfers" element={<TransfersPage />} />
          <Route path="transfers/:jobId" element={<TransferDetailRoute />} />
          <Route path="transfer-tenants" element={<PlaceholderPage title="Tenants" />} />
          <Route path="*" element={<Navigate to="/overview" replace />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
