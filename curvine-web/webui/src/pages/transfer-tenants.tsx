import { useCallback, useEffect, useState } from 'react'
import { ArrowLeft, RefreshCw, Users } from 'lucide-react'
import { Link } from 'react-router-dom'
import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { api } from '@/lib/api'
import { useAutoRefreshEffect } from '@/lib/auto-refresh'
import type { TransferTenant } from '@/lib/transfer-types'

const PAGE_SIZE = 50

export function TransferTenantsPage() {
  const [tenants, setTenants] = useState<TransferTenant[]>([])
  const [loaded, setLoaded] = useState(false)
  const [pageIndex, setPageIndex] = useState(0)
  const [pageTokens, setPageTokens] = useState<(string | null)[]>([null])
  const [nextPageToken, setNextPageToken] = useState<string | null>(null)
  const [refreshKey, setRefreshKey] = useState(0)

  useEffect(() => {
    let cancelled = false
    api
      .transferTenants({ limit: PAGE_SIZE, page_token: pageTokens[pageIndex] })
      .then(data => {
        if (!cancelled) {
          setTenants(data.tenants ?? [])
          setNextPageToken(data.next_page_token ?? null)
        }
      })
      .catch(() => {})
      .finally(() => {
        if (!cancelled) {
          setLoaded(true)
        }
      })
    return () => {
      cancelled = true
    }
  }, [pageIndex, pageTokens, refreshKey])

  const autoRefresh = useCallback(() => setRefreshKey(key => key + 1), [])
  useAutoRefreshEffect(autoRefresh)

  const refresh = () => {
    setPageTokens([null])
    setPageIndex(0)
    setRefreshKey(key => key + 1)
  }

  const nextPage = () => {
    if (!nextPageToken) {
      return
    }
    setPageTokens(tokens => [...tokens.slice(0, pageIndex + 1), nextPageToken])
    setPageIndex(index => index + 1)
  }

  const prevPage = () => {
    setPageIndex(index => Math.max(0, index - 1))
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <Link
            to="/transfers"
            className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-sky-400"
          >
            <ArrowLeft className="h-3.5 w-3.5" />
            Transfers
          </Link>
          <h2 className="mt-1 flex items-center gap-2 text-2xl font-bold tracking-tight">
            <Users className="h-5 w-5 text-sky-400" />
            Tenant Summary
          </h2>
          <p className="mt-1 text-sm text-muted-foreground">Transfer jobs grouped by tenant</p>
        </div>
        <button
          type="button"
          onClick={refresh}
          className="inline-flex items-center gap-2 rounded-md bg-sky-500/15 px-3 py-2 text-sm font-medium text-sky-400 transition-colors hover:bg-sky-500/25"
        >
          <RefreshCw className="h-4 w-4" />
          Refresh
        </button>
      </div>

      <Card>
        <CardHeader className="flex-row items-center justify-between border-b py-4">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            {tenants.length} tenants on this page
          </CardTitle>
          <Badge variant="muted">Page {pageIndex + 1}</Badge>
        </CardHeader>
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <table className="w-full min-w-[880px] text-sm">
              <thead>
                <tr className="border-b bg-secondary/40 text-xs uppercase tracking-wider text-muted-foreground">
                  <th className="px-4 py-3 text-left">Tenant</th>
                  <th className="px-4 py-3 text-right">Total</th>
                  <th className="px-4 py-3 text-right">Pending</th>
                  <th className="px-4 py-3 text-right">Executing</th>
                  <th className="px-4 py-3 text-right">Completed</th>
                  <th className="px-4 py-3 text-right">Failed</th>
                  <th className="px-4 py-3 text-right">Canceled</th>
                  <th className="px-4 py-3 text-right">Partial success</th>
                </tr>
              </thead>
              <tbody>
                {tenants.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="px-4 py-16 text-center text-muted-foreground">
                      {loaded ? 'No tenants' : 'Loading tenants…'}
                    </td>
                  </tr>
                ) : (
                  tenants.map(tenant => (
                    <tr key={tenant.tenant} className="border-b last:border-b-0 hover:bg-accent/40">
                      <td className="px-4 py-3 font-mono font-semibold text-sky-400">
                        {tenant.tenant}
                      </td>
                      <td className="px-4 py-3 text-right font-mono font-semibold">{tenant.total}</td>
                      <td className="px-4 py-3 text-right font-mono text-muted-foreground">
                        {tenant.pending}
                      </td>
                      <td className="px-4 py-3 text-right font-mono text-sky-400">
                        {tenant.executing}
                      </td>
                      <td className="px-4 py-3 text-right font-mono text-emerald-400">
                        {tenant.completed}
                      </td>
                      <td className="px-4 py-3 text-right font-mono text-red-400">{tenant.failed}</td>
                      <td className="px-4 py-3 text-right font-mono text-muted-foreground">
                        {tenant.canceled}
                      </td>
                      <td className="px-4 py-3 text-right font-mono text-amber-300">
                        {tenant.partial_success}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
          <div className="flex items-center justify-end gap-3 border-t px-4 py-3">
            <button
              type="button"
              onClick={prevPage}
              disabled={pageIndex === 0}
              className="rounded-md border px-3 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground disabled:cursor-not-allowed disabled:opacity-40"
            >
              Previous
            </button>
            <span className="text-xs text-muted-foreground">Page {pageIndex + 1}</span>
            <button
              type="button"
              onClick={nextPage}
              disabled={!nextPageToken}
              className="rounded-md border px-3 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground disabled:cursor-not-allowed disabled:opacity-40"
            >
              Next
            </button>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
