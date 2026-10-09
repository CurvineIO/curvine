import { useEffect, useState } from 'react'
import { NavLink, useLocation } from 'react-router-dom'

import logoUrl from '@/assets/logo.svg'
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
  SidebarSeparator,
  useSidebar
} from '@/components/ui/sidebar'
import { useT } from '@/i18n'
import { getUptimeParts } from '@/lib/format'
import { useMasterRuntime } from '@/master-runtime/master-runtime-context'
import { isItemActive, navigationGroups } from '@/navigation'

/** Re-render the uptime label periodically; a per-second timer would be wasteful. */
function useNow(intervalMs = 30_000): number {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), intervalMs)
    return () => window.clearInterval(timer)
  }, [intervalMs])
  return now
}

function useUptimeLabel(): string {
  const t = useT()
  const { runtime } = useMasterRuntime()
  const now = useNow()

  const parts = getUptimeParts(runtime.startTime, now, runtime.startTimeMs)
  if (!parts) return t('layout.masterReadyHint')
  if (parts.kind === 'min') return t('common.uptime.min', { count: parts.count })
  if (parts.kind === 'hours') return t('common.uptime.hours', { count: parts.count })
  if (parts.kind === 'days') return t('common.uptime.days', { count: parts.count })
  return t('common.uptime.daysHours', { days: parts.days, hours: parts.hours })
}

export function AppSidebar() {
  const t = useT()
  const { pathname } = useLocation()
  const { isMobile, setOpenMobile } = useSidebar()
  const uptimeLabel = useUptimeLabel()

  return (
    <Sidebar collapsible="icon">
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton size="lg" asChild>
              <NavLink to="/overview" data-testid="sidebar-brand">
                <img src={logoUrl} alt="Curvine" className="size-8 shrink-0" />
                <div className="flex flex-col gap-0.5 leading-none group-data-[collapsible=icon]:hidden">
                  <span className="font-heading font-semibold">Curvine</span>
                  <span className="text-xs text-muted-foreground">{t('layout.brandSubtitle')}</span>
                </div>
              </NavLink>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarSeparator />
      <SidebarContent>
        {navigationGroups.map((group) => (
          <SidebarGroup key={group.titleKey}>
            <SidebarGroupLabel>{t(group.titleKey)}</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {group.items.map((item) => {
                  const Icon = item.icon
                  return (
                    <SidebarMenuItem key={item.to}>
                      <SidebarMenuButton
                        asChild
                        isActive={isItemActive(item, pathname)}
                        tooltip={t(item.labelKey)}
                      >
                        <NavLink
                          to={item.to}
                          end={item.end}
                          data-testid={`nav-${item.to.replace(/^\//, '')}`}
                          onClick={() => {
                            if (isMobile) setOpenMobile(false)
                          }}
                        >
                          <Icon />
                          <span>{t(item.labelKey)}</span>
                        </NavLink>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  )
                })}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        ))}
      </SidebarContent>
      <SidebarFooter>
        <div
          className="flex items-start gap-2 rounded-lg border border-sidebar-border bg-muted/40 p-2.5 group-data-[collapsible=icon]:hidden"
          data-testid="master-status"
        >
          <span aria-hidden className="mt-1 size-2 shrink-0 rounded-full bg-success" />
          <div className="flex min-w-0 flex-col">
            <strong className="text-xs font-medium">{t('layout.masterReady')}</strong>
            <small className="truncate text-xs text-muted-foreground">{uptimeLabel}</small>
          </div>
        </div>
      </SidebarFooter>
      <SidebarRail />
    </Sidebar>
  )
}
