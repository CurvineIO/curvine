import { render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { describe, expect, it } from 'vitest'

import { ApiProvider } from '@/api/api-context'
import type { ApiClient } from '@/api/client'
import { CopyFallbackProvider } from '@/components/copy-fallback'
import { ToastProvider } from '@/components/ui/toast'
import { TooltipProvider } from '@/components/ui/tooltip'
import { SidebarInset } from '@/components/ui/sidebar'
import { I18nProvider, LOCALE_STORAGE_KEY } from '@/i18n'
import { RefreshProvider } from '@/refresh/refresh-context'
import { BrowsePage } from '../browse-page'

/**
 * A narrow window used to squeeze this listing: `Size`, `Modified` and
 * `Expire time` wrapped onto two or three lines, so rows grew from a single
 * line's ~37px to as much as 73px and the list became a wall of broken dates.
 * jsdom has no layout engine, so the invariant is asserted on the classes that
 * produce it (the same way `transfers-responsive.test.tsx` pins the compact
 * transfers table); the measured pixel behaviour is covered by the Chromium
 * harness in /tmp/browse-layout.
 */

/** Wide enough to force wrapping if the table were allowed to shrink. */
const LONG_NAME = `${'a-very-long-file-name-that-keeps-going-'.repeat(3)}.parquet`

const longEntry = {
  path: `/${LONG_NAME}`,
  name: LONG_NAME,
  is_dir: false,
  len: 1024,
  mtime: 1_750_000_000_000,
  replicas: 2,
  mode: 0o644,
  owner: 'alice',
  group: 'staff',
  nlink: 1,
  block_size: 16 * 1024 * 1024,
  storage_policy: { storage_type: 'Disk', ttl_ms: 604800000, ttl_action: 'Delete', state: 'Both' }
}

function stubClient(): ApiClient {
  return {
    fetchMountsData: async () => [],
    fetchBrowseData: async () => ({ path: '/', items: [longEntry] }),
    fetchUfsSyncJobs: async () => ({ items: [], path: '/', total: 0 }),
    fetchUfsSyncStatus: async () => null,
    fetchDirSpace: async () => ({ enabled: false, paths: [] }),
    fetchDirSpaceStatus: async () => null,
    fetchDirSpaceSummaries: async () => ({ items: [] })
  } as unknown as ApiClient
}

function renderPage() {
  window.localStorage.setItem(LOCALE_STORAGE_KEY, 'en')
  return render(
    <MemoryRouter initialEntries={['/browse']}>
      <ApiProvider client={stubClient()}>
        <I18nProvider>
          <RefreshProvider>
            <ToastProvider>
              <CopyFallbackProvider>
                <TooltipProvider>
                  <BrowsePage />
                </TooltipProvider>
              </CopyFallbackProvider>
            </ToastProvider>
          </RefreshProvider>
        </I18nProvider>
      </ApiProvider>
    </MemoryRouter>
  )
}

describe('BrowsePage narrow-window layout', () => {
  it('keeps every listing row on one line instead of wrapping it', async () => {
    renderPage()
    const row = await screen.findByTestId(`browse-row-/${LONG_NAME}`)
    const table = row.closest('table') as HTMLElement

    // `whitespace-nowrap` is what stops a date or name from breaking into two
    // lines and inflating the row height.
    expect(row.querySelectorAll('td').length).toBeGreaterThan(0)
    for (const cell of Array.from(row.querySelectorAll('td'))) {
      expect(cell.className).not.toMatch(/whitespace-normal/)
    }
    // Applied to every content cell at once, so no column can be forgotten.
    expect(table.className).toMatch(/\[&_td\]:whitespace-nowrap/)

    // Sized to its content and overflowing inside its own scroll container, so a
    // narrow viewport scrolls the list rather than compressing the columns.
    expect(table).toHaveClass('w-max', 'min-w-full')
    expect(table.className).not.toMatch(/table-fixed/)
    expect(table.parentElement).toHaveClass('overflow-x-auto')
  })

  it('keeps a very long file name from stretching the table across the window', async () => {
    renderPage()
    const row = await screen.findByTestId(`browse-row-/${LONG_NAME}`)
    const nameCell = row.querySelectorAll('td')[1] as HTMLElement

    // The name cell caps and truncates its own contents; the table can then be
    // content-sized without one pathological name widening it indefinitely.
    const nameBox = nameCell.querySelector('div') as HTMLElement
    expect(nameBox).toHaveClass('min-w-0', 'max-w-96')
    const button = within(nameBox).getByTestId(`browse-preview-/${LONG_NAME}`)
    expect(button).toHaveClass('truncate')
    expect(button).toHaveAttribute('title', `/${LONG_NAME}`)
  })

  it('lets the path row shrink instead of pushing Go past the edge', () => {
    // A 16rem path input is a flex floor: on a 320px window the row could not go
    // under it, so the Go button hung past the right edge and the whole page
    // scrolled. The form and the input both carry `min-w-0` so the row gives way.
    renderPage()
    return screen.findByTestId('browse-path').then((input) => {
      expect(input).toHaveClass('w-64', 'min-w-0')
      const form = input.closest('form') as HTMLElement
      expect(form).toHaveClass('min-w-0')
    })
  })

  it('lets the page shell shrink so the table does not widen the page', () => {
    // The shell is a flex item, and a flex item's default `min-width: auto` is a
    // floor at its content's min-content width. The content-sized table above
    // sets that floor past a narrow viewport, so without `min-w-0` the whole page
    // (header and sidebar included) is pushed sideways and the card's own
    // horizontal scrolling never engages. This is the one lever that fixes it:
    // `min-width` on the table container or the card has no effect.
    render(<SidebarInset>content</SidebarInset>)
    expect(document.querySelector('[data-slot="sidebar-inset"]')).toHaveClass('min-w-0')
  })
})
