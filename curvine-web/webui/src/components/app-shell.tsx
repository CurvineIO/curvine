import { useEffect, useState } from 'react'
import { Menu, RefreshCw, X } from 'lucide-react'
import { Navigate, NavLink, Outlet, useLocation } from 'react-router-dom'
import logoUrl from '@/assets/logo.svg'
import { Badge } from '@/components/ui/badge'
import { useWebMode } from '@/hooks/use-web-mode'
import { clearApiError, useApiError } from '@/lib/api-error'
import { AutoRefreshProvider, useAutoRefresh } from '@/lib/auto-refresh'
import { isTransferPath, navigationFor, pageTitle } from '@/lib/navigation'
import { cn } from '@/lib/utils'
import type { WebMode } from '@/lib/capabilities'

export function AppShell() {
  const mode = useWebMode()
  const location = useLocation()
  const [mobileNavOpen, setMobileNavOpen] = useState(false)

  useEffect(() => {
    setMobileNavOpen(false)
  }, [location.pathname])

  if (!mode) {
    return (
      <div className="flex min-h-screen items-center justify-center text-sm text-muted-foreground">
        Loading…
      </div>
    )
  }

  // The same bundle is served by both back ends; keep the user on the pages the
  // detected API can actually answer.
  if (mode === 'transfer' && !isTransferPath(location.pathname)) {
    return <Navigate to="/transfers" replace />
  }
  if (mode === 'master' && isTransferPath(location.pathname)) {
    return <Navigate to="/overview" replace />
  }

  return (
    <AutoRefreshProvider>
      <div className="flex min-h-screen bg-background text-foreground">
        {mobileNavOpen && (
          <button
            type="button"
            aria-label="Close navigation"
            className="fixed inset-0 z-30 bg-black/60 md:hidden"
            onClick={() => setMobileNavOpen(false)}
          />
        )}
        <aside
          className={cn(
            'fixed inset-y-0 left-0 z-40 flex w-60 shrink-0 flex-col border-r bg-card transition-transform md:static md:translate-x-0',
            mobileNavOpen ? 'translate-x-0' : '-translate-x-full',
          )}
        >
          <Sidebar mode={mode} onNavigate={() => setMobileNavOpen(false)} />
        </aside>
        <div className="flex min-w-0 flex-1 flex-col">
          <header className="sticky top-0 z-20 flex h-14 items-center gap-3 border-b bg-background/80 px-4 backdrop-blur md:px-6">
            <button
              type="button"
              aria-label="Open navigation"
              className="inline-flex h-9 w-9 items-center justify-center rounded-md text-muted-foreground hover:bg-accent hover:text-foreground md:hidden"
              onClick={() => setMobileNavOpen(true)}
            >
              <Menu className="h-4 w-4" />
            </button>
            <h1 className="truncate text-sm font-semibold">{pageTitle(location.pathname)}</h1>
            <div className="ml-auto">
              <AutoRefreshToggle />
            </div>
          </header>
          <ApiErrorBanner />
          <main className="mx-auto w-full max-w-7xl flex-1 p-4 md:p-6">
            <Outlet />
          </main>
        </div>
      </div>
    </AutoRefreshProvider>
  )
}

function Sidebar({ mode, onNavigate }: { mode: WebMode; onNavigate: () => void }) {
  return (
    <>
      <div className="flex h-14 items-center gap-2 border-b px-4">
        <img src={logoUrl} alt="" className="h-7 w-7" />
        <span className="text-sm font-semibold tracking-tight">Curvine</span>
        <Badge variant={mode === 'transfer' ? 'info' : 'muted'} className="ml-auto uppercase">
          {mode}
        </Badge>
      </div>
      <nav className="flex-1 space-y-1 overflow-y-auto p-3">
        {navigationFor(mode).map(item => {
          const Icon = item.icon
          return (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              onClick={onNavigate}
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors',
                  isActive
                    ? 'bg-accent text-accent-foreground'
                    : 'text-muted-foreground hover:bg-accent/60 hover:text-foreground',
                )
              }
            >
              <Icon className="h-4 w-4" />
              {item.label}
            </NavLink>
          )
        })}
      </nav>
      <div className="border-t p-4 text-xs text-muted-foreground">Curvine distributed cache</div>
    </>
  )
}

function ApiErrorBanner() {
  const message = useApiError()
  if (!message) {
    return null
  }
  return (
    <div className="border-b border-red-500/30 bg-red-500/10 px-4 py-2 text-sm text-red-300 md:px-6">
      <div className="mx-auto flex w-full max-w-7xl items-center gap-4">
        <span className="flex-1">{message}</span>
        <button
          type="button"
          aria-label="Dismiss error"
          className="rounded p-1 text-red-300 hover:bg-red-500/20"
          onClick={clearApiError}
        >
          <X className="h-4 w-4" />
        </button>
      </div>
    </div>
  )
}

function AutoRefreshToggle() {
  const { enabled, setEnabled } = useAutoRefresh()

  return (
    <button
      type="button"
      aria-pressed={enabled}
      onClick={() => setEnabled(!enabled)}
      className={cn(
        'inline-flex items-center gap-2 rounded-md border px-3 py-1.5 text-xs font-medium transition-colors',
        enabled
          ? 'border-sky-500/40 bg-sky-500/10 text-sky-400'
          : 'text-muted-foreground hover:bg-accent hover:text-foreground',
      )}
    >
      <RefreshCw className={cn('h-3.5 w-3.5', enabled && 'motion-safe:animate-spin')} />
      <span className="hidden sm:inline">Auto refresh</span>
      <span>{enabled ? 'ON' : 'OFF'}</span>
    </button>
  )
}
