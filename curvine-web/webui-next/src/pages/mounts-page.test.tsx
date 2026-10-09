import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiProvider } from '@/api/api-context'
import type { ApiClient } from '@/api/client'
import type { MountInfo } from '@/api/types'
import { ToastProvider } from '@/components/ui/toast'
import { TooltipProvider } from '@/components/ui/tooltip'
import { I18nProvider, LOCALE_STORAGE_KEY, type AppLocale } from '@/i18n'
import { RefreshProvider } from '@/refresh/refresh-context'
import { MountsPage } from './mounts-page'

const mounts = [
  { cv_path: '/lake/raw', ufs_path: 'file:///lake/raw', write_type: 1 },
  { cv_path: '/lake/curated', ufs_path: 'file:///lake/curated', write_type: 1 }
] as MountInfo[]

function makeClient(list: MountInfo[] = mounts) {
  const fetchMountsData = vi.fn(async () => ({ items: list }))
  const client = { fetchMountsData } as unknown as ApiClient
  return { client, fetchMountsData }
}

function renderPage(locale: AppLocale, client: ApiClient) {
  window.localStorage.setItem(LOCALE_STORAGE_KEY, locale)
  return render(
    <ApiProvider client={client}>
      <I18nProvider>
        <RefreshProvider>
          <ToastProvider>
            <TooltipProvider>
              <MountsPage />
            </TooltipProvider>
          </ToastProvider>
        </RefreshProvider>
      </I18nProvider>
    </ApiProvider>
  )
}

describe('MountsPage views', () => {
  beforeEach(() => {
    window.localStorage.clear()
  })

  it('lists mounts flat by default', async () => {
    renderPage('en', makeClient().client)

    await waitFor(() => expect(screen.getByTestId('mount-row-/lake/raw')).toBeInTheDocument())
    expect(screen.getByText('/lake/raw')).toBeInTheDocument()
    expect(screen.getByText('/lake/curated')).toBeInTheDocument()
  })

  it('nests mounts in a tree and hides children until the node is expanded', async () => {
    renderPage('en', makeClient().client)
    await waitFor(() => expect(screen.getByTestId('mount-row-/lake/raw')).toBeInTheDocument())

    await userEvent.click(screen.getByTestId('mounts-view-tree'))

    // Collapsed by default: only the shared `/lake` node is shown.
    expect(screen.getByTestId('mount-tree-dir-/lake')).toBeInTheDocument()
    expect(screen.queryByTestId('mount-row-/lake/raw')).not.toBeInTheDocument()

    await userEvent.click(screen.getByTestId('mount-tree-toggle-/lake'))

    expect(screen.getByTestId('mount-row-/lake/raw')).toBeInTheDocument()
    expect(screen.getByTestId('mount-row-/lake/curated')).toBeInTheDocument()
  })

  it('narrows the list with an exclusion token', async () => {
    renderPage('en', makeClient().client)
    await waitFor(() => expect(screen.getByTestId('mount-row-/lake/raw')).toBeInTheDocument())

    await userEvent.type(screen.getByTestId('mounts-search'), '-curated')

    expect(screen.queryByTestId('mount-row-/lake/curated')).not.toBeInTheDocument()
    expect(screen.getByTestId('mount-row-/lake/raw')).toBeInTheDocument()
  })
})
