import { Suspense } from 'react'
import { LogOutIcon, MoonIcon, RefreshCwIcon, SunIcon, TimerIcon } from 'lucide-react'
import { Outlet, useLocation, useNavigate } from 'react-router-dom'

import { AppSidebar } from '@/components/app-sidebar'
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator
} from '@/components/ui/breadcrumb'
import { Button } from '@/components/ui/button'
import { Separator } from '@/components/ui/separator'
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/components/ui/select'
import { SidebarInset, SidebarProvider, SidebarTrigger } from '@/components/ui/sidebar'
import { Toggle } from '@/components/ui/toggle'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { Spinner } from '@/components/ui/spinner'
import { useAuth } from '@/auth/auth-context'
import { useOptionalCluster } from '@/cluster/cluster-context'
import type { ClusterStatus } from '@/api/types'
import { useI18n, localeOptions, type AppLocale } from '@/i18n'
import { cn } from '@/lib/utils'
import { useRefresh } from '@/refresh/refresh-context'
import { useTheme } from '@/theme/theme-context'
import { findNavigation } from '@/navigation'

/** Status → dot colour. Semantic tokens only, so dark mode follows the theme. */
const CLUSTER_STATUS_DOT: Record<ClusterStatus, string> = {
  ready: 'bg-emerald-500',
  invalid: 'bg-destructive',
  unreachable: 'bg-muted-foreground'
}

function ClusterStatusDot({ status }: { status: ClusterStatus }) {
  return (
    <span
      aria-hidden="true"
      data-status={status}
      className={cn('size-2 shrink-0 rounded-full', CLUSTER_STATUS_DOT[status])}
    />
  )
}

/**
 * Cluster switcher in the topbar.
 *
 * Options are the real clusters, labelled by cluster id. There is no "default
 * cluster" pseudo-option: the backend's own cluster is already a normal entry
 * in the list (it reports `source: 'default'`) and is preselected when the
 * console carries no usable stored choice, so a user always picks one concrete
 * cluster. The control only sits disabled while the registry serves no list at
 * all, which is exactly the single-cluster deployment that has nothing to
 * switch between.
 */
function ClusterSwitcher() {
  const { t } = useI18n()
  const { triggerRefresh } = useRefresh()
  const cluster = useOptionalCluster()

  const clusters = cluster?.clusters ?? []
  const clusterId = cluster?.clusterId ?? ''
  const loading = cluster?.clustersLoading ?? false
  // A Radix `Select` can only display a value that exists as an item, so an id
  // stored for a list that has not arrived yet would blank the trigger. Fall
  // back to `''` and show the pending id as placeholder text instead.
  const value = clusters.some((item) => item.id === clusterId) ? clusterId : ''

  return (
    <Select
      value={value}
      disabled={clusters.length === 0}
      onValueChange={(next) => {
        // Switching cluster invalidates every page's data: the client is rebuilt
        // from the new id, and the refresh signal makes mounted pages re-read.
        cluster?.setClusterId(next)
        triggerRefresh()
      }}
    >
      <SelectTrigger
        size="sm"
        className="w-[11.25rem]"
        aria-label={t('layout.clusterLabel')}
        aria-busy={loading || undefined}
        data-testid="cluster-switcher"
      >
        <SelectValue placeholder={clusterId || t('layout.clusterPlaceholder')} />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          {clusters.map((item) => (
            <SelectItem
              key={item.id}
              value={item.id}
              textValue={item.id}
              title={item.error || item.display_name || undefined}
            >
              <ClusterStatusDot status={item.status} />
              <span className="truncate">{item.id}</span>
            </SelectItem>
          ))}
        </SelectGroup>
      </SelectContent>
    </Select>
  )
}

export function ConsoleLayout() {
  const { t, locale, setLocale } = useI18n()
  const { signOut } = useAuth()
  const { triggerRefresh, autoRefresh, setAutoRefresh } = useRefresh()
  const { resolved, setPreference } = useTheme()
  const { pathname } = useLocation()
  const navigate = useNavigate()

  const current = findNavigation(pathname)

  async function handleSignOut() {
    await signOut()
    navigate('/login', { replace: true })
  }

  return (
    <SidebarProvider>
      <AppSidebar />
      <SidebarInset>
        <header className="sticky top-0 z-10 flex h-14 shrink-0 items-center gap-2 border-b border-border bg-background/95 px-3 backdrop-blur">
          <SidebarTrigger data-testid="sidebar-trigger" />
          <Separator orientation="vertical" className="mr-1 h-4" />
          <Breadcrumb className="min-w-0">
            <BreadcrumbList>
              {current && (
                <>
                  <BreadcrumbItem className="hidden md:block">
                    {t(current.group.titleKey)}
                  </BreadcrumbItem>
                  <BreadcrumbSeparator className="hidden md:block" />
                  <BreadcrumbItem>
                    <BreadcrumbPage className="truncate">{t(current.item.labelKey)}</BreadcrumbPage>
                  </BreadcrumbItem>
                </>
              )}
            </BreadcrumbList>
          </Breadcrumb>

          <div className="ml-auto flex items-center gap-2">
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="outline"
                  size="icon-sm"
                  onClick={() => setPreference(resolved === 'dark' ? 'light' : 'dark')}
                  aria-label={resolved === 'dark' ? t('common.switchToLight') : t('common.switchToDark')}
                  data-testid="theme-toggle"
                >
                  {resolved === 'dark' ? <SunIcon /> : <MoonIcon />}
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                {resolved === 'dark' ? t('common.switchToLight') : t('common.switchToDark')}
              </TooltipContent>
            </Tooltip>

            <ClusterSwitcher />

            <Select value={locale} onValueChange={(value) => setLocale(value as AppLocale)}>
              <SelectTrigger
                size="sm"
                className="w-[7.5rem]"
                aria-label={t('common.language')}
                data-testid="locale-select"
              >
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {localeOptions.map((option) => (
                    <SelectItem key={option.value} value={option.value}>
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>

            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={triggerRefresh}
                  data-testid="global-refresh"
                >
                  <RefreshCwIcon data-icon="inline-start" />
                  {t('common.refresh')}
                </Button>
              </TooltipTrigger>
              <TooltipContent>{t('layout.masterReadyHint')}</TooltipContent>
            </Tooltip>

            <Tooltip>
              <TooltipTrigger asChild>
                <Toggle
                  size="sm"
                  variant="outline"
                  pressed={autoRefresh}
                  onPressedChange={setAutoRefresh}
                  aria-label={t('common.autoRefresh')}
                  data-testid="auto-refresh-toggle"
                  className="gap-1.5 px-2 text-[13px]"
                >
                  <TimerIcon className="size-3.5" />
                  {autoRefresh ? t('common.on') : t('common.off')}
                </Toggle>
              </TooltipTrigger>
              <TooltipContent>{t('common.autoRefresh')}</TooltipContent>
            </Tooltip>

            <Button
              variant="outline"
              size="sm"
              onClick={() => void handleSignOut()}
              data-testid="sign-out"
            >
              <LogOutIcon data-icon="inline-start" />
              {t('common.logout')}
            </Button>
          </div>
        </header>
        <main className="flex-1 bg-background">
          {/* Full-width dense shell: no mx-auto/max-w, tighter gap than the skill default.
              `data-console-content` is the box a full page fills; panels that should match
              a page's size measure it (see worker-detail-sheet). */}
          <div data-console-content className="flex w-full flex-col gap-4 p-3 lg:p-4">
            {/* Single boundary for every lazily-loaded route: the sidebar and
                topbar stay mounted while the page chunk arrives. */}
            <Suspense
              fallback={
                <div
                  className="flex items-center justify-center gap-2 rounded-xl border border-dashed border-border p-8 text-sm text-muted-foreground"
                  data-testid="route-loading"
                >
                  <Spinner className="size-4" />
                </div>
              }
            >
              <Outlet />
            </Suspense>
          </div>
        </main>
      </SidebarInset>
    </SidebarProvider>
  )
}
