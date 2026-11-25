import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { AppShell } from '@/components/app-shell'
import { OverviewPage } from '@/pages/overview'
import { PlaceholderPage } from '@/pages/placeholder'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<AppShell />}>
          <Route index element={<Navigate to="/overview" replace />} />
          <Route path="overview" element={<OverviewPage />} />
          <Route path="browse" element={<PlaceholderPage title="Browse" />} />
          <Route path="blocks" element={<PlaceholderPage title="Blocks" />} />
          <Route path="workers" element={<PlaceholderPage title="Workers" />} />
          <Route path="config" element={<PlaceholderPage title="Configuration" />} />
          <Route path="transfers" element={<PlaceholderPage title="Transfers" />} />
          <Route path="transfers/:jobId" element={<PlaceholderPage title="Transfer detail" />} />
          <Route path="transfer-tenants" element={<PlaceholderPage title="Tenants" />} />
          <Route path="*" element={<Navigate to="/overview" replace />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
