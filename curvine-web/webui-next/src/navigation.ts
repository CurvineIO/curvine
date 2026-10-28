import {
  ActivityIcon,
  ArrowLeftRightIcon,
  FolderTreeIcon,
  HardDriveIcon,
  LayersIcon,
  LayoutDashboardIcon,
  ListChecksIcon,
  RefreshCwIcon,
  ServerIcon,
  SettingsIcon,
  ShieldCheckIcon,
  type LucideIcon
} from 'lucide-react'

export interface NavigationItem {
  to: string
  /** i18n key for the sidebar label. */
  labelKey: string
  /** i18n key for the page subtitle, also used as the PageHeader description default. */
  subtitleKey: string
  icon: LucideIcon
  /** Exact match only — used for index routes that would otherwise match children. */
  end?: boolean
}

export interface NavigationGroup {
  titleKey: string
  items: NavigationItem[]
}

/**
 * The single navigation source: the sidebar and the breadcrumb both read from
 * here, so a page is only ever added in one place.
 */
export const navigationGroups: NavigationGroup[] = [
  {
    titleKey: 'nav.groupOverview',
    items: [
      {
        to: '/overview',
        labelKey: 'nav.overview',
        subtitleKey: 'nav.overviewSubtitle',
        icon: LayoutDashboardIcon,
        end: true
      }
    ]
  },
  {
    titleKey: 'nav.groupFilesystem',
    items: [
      {
        to: '/browse',
        labelKey: 'nav.browse',
        subtitleKey: 'nav.browseSubtitle',
        icon: FolderTreeIcon
      },
      { to: '/mounts', labelKey: 'nav.mounts', subtitleKey: 'nav.mountsSubtitle', icon: HardDriveIcon },
      {
        to: '/clusters',
        labelKey: 'nav.clusters',
        subtitleKey: 'nav.clustersSubtitle',
        icon: LayersIcon
      }
    ]
  },
  {
    titleKey: 'nav.groupRuntime',
    items: [
      { to: '/workers', labelKey: 'nav.workers', subtitleKey: 'nav.workersSubtitle', icon: ServerIcon },
      { to: '/config', labelKey: 'nav.config', subtitleKey: 'nav.configSubtitle', icon: SettingsIcon }
    ]
  },
  {
    titleKey: 'nav.groupJobs',
    items: [
      { to: '/jobs', labelKey: 'nav.jobs', subtitleKey: 'nav.jobsSubtitle', icon: ListChecksIcon },
      {
        to: '/transfers',
        labelKey: 'nav.transfers',
        subtitleKey: 'nav.transfersSubtitle',
        icon: ArrowLeftRightIcon
      },
      {
        to: '/sync-jobs',
        labelKey: 'nav.syncJobs',
        subtitleKey: 'nav.syncJobsSubtitle',
        icon: RefreshCwIcon
      }
    ]
  },
  {
    titleKey: 'nav.groupObservability',
    items: [
      {
        to: '/observability',
        labelKey: 'nav.observability',
        subtitleKey: 'nav.observabilitySubtitle',
        icon: ActivityIcon
      },
      {
        to: '/operations',
        labelKey: 'nav.operations',
        subtitleKey: 'nav.operationsSubtitle',
        icon: ShieldCheckIcon
      }
    ]
  }
]

export function isItemActive(item: NavigationItem, pathname: string): boolean {
  return item.end
    ? pathname === item.to
    : pathname === item.to || pathname.startsWith(`${item.to}/`)
}

/** Breadcrumb lookup: which group + item the current path belongs to. */
export function findNavigation(
  pathname: string
): { group: NavigationGroup; item: NavigationItem } | null {
  for (const group of navigationGroups) {
    for (const item of group.items) {
      if (isItemActive(item, pathname)) return { group, item }
    }
  }
  return null
}
