import {
  ArrowLeftRight,
  FolderTree,
  LayoutDashboard,
  Server,
  Settings,
  Users,
  type LucideIcon,
} from 'lucide-react'
import type { WebMode } from '@/lib/capabilities'

export interface NavItem {
  to: string
  label: string
  icon: LucideIcon
  /** Match only the exact path; false for entries that own sub-routes. */
  end?: boolean
}

const MASTER_NAV: NavItem[] = [
  { to: '/overview', label: 'Overview', icon: LayoutDashboard, end: true },
  { to: '/browse', label: 'Browse', icon: FolderTree, end: true },
  { to: '/workers', label: 'Workers', icon: Server, end: true },
  { to: '/config', label: 'Configuration', icon: Settings, end: true },
]

const TRANSFER_NAV: NavItem[] = [
  { to: '/transfers', label: 'Transfers', icon: ArrowLeftRight },
  { to: '/transfer-tenants', label: 'Tenants', icon: Users, end: true },
]

export function navigationFor(mode: WebMode): NavItem[] {
  return mode === 'transfer' ? TRANSFER_NAV : MASTER_NAV
}

export function isTransferPath(pathname: string): boolean {
  return pathname === '/transfers' || pathname.startsWith('/transfers/') || pathname === '/transfer-tenants'
}

export function pageTitle(pathname: string): string {
  if (pathname.startsWith('/transfers/')) {
    return 'Transfer detail'
  }
  if (pathname === '/blocks') {
    return 'Blocks'
  }
  const item = [...MASTER_NAV, ...TRANSFER_NAV].find(entry => entry.to === pathname)
  return item ? item.label : 'Curvine'
}
