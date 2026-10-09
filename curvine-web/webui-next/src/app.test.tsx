import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { describe, expect, it, vi } from 'vitest'

import { App } from './App'
import type { ApiClient } from '@/api/client'
import { AuthProvider } from '@/auth/auth-context'
import { ToastProvider } from '@/components/ui/toast'
import { TooltipProvider } from '@/components/ui/tooltip'
import { I18nProvider } from '@/i18n'
import { MasterRuntimeProvider } from '@/master-runtime/master-runtime-context'
import { RefreshProvider } from '@/refresh/refresh-context'
import { THEME_STORAGE_KEY, ThemeProvider } from '@/theme/theme-context'

/** One transfer, for the list actions that report back through a toast. */
function transferClient(): Partial<ApiClient> {
  const transfer = {
    job_id: 'job-abc123',
    state: 'running',
    kind: 'load',
    path: '/source/a.parquet',
    target_path: '/target/a.parquet',
    progress: 42,
    total_files: 10,
    completed_files: 4,
    submitter: 'alice'
  }
  return {
    fetchTransfersData: vi.fn(async () => ({ items: [transfer] })),
    fetchTransferStatus: vi.fn(async () => transfer)
  } as unknown as Partial<ApiClient>
}

/** One live worker plus its detail payload, for the workers list and its panel. */
function workerClient(): Partial<ApiClient> {
  const version = '0.2.2 (commit: 5897a118, branch: web2)'
  const worker = {
    worker_id: 1,
    hostname: 'worker-01',
    software_version: version,
    status: 'Live',
    capacity: 2048,
    available: 1024,
    block_num: 7,
    address: { rpc_port: 8995, ip_addr: '10.0.0.1', web_port: 9001 },
    storage_map: {}
  }
  return {
    fetchWorkersData: vi.fn(async () => ({
      live_workers: [worker],
      blacklist_workers: [],
      decommission_workers: [],
      lost_workers: []
    })),
    fetchWorkerDetail: vi.fn(async () => ({
      state: 'Live',
      version,
      version_commit: '5897a118',
      file_count: 12,
      worker
    }))
  } as unknown as Partial<ApiClient>
}

/** Stub client covering the endpoints the phase-1 pages call. */
function createStubClient(overrides: Partial<ApiClient> = {}): ApiClient {
  const stub = {
    login: vi.fn(async () => ({})),
    logout: vi.fn(async () => ({})),
    fetchAuthSession: vi.fn(async () => ({ authenticated: true })),
    fetchOverviewData: vi.fn(async () => ({
      cluster_id: 'test-cluster',
      version: '0.4.0',
      start_time: '2026-01-01 00:00:00',
      master_state: 'Active',
      live_workers: 2,
      lost_workers: 0,
      capacity: 2048,
      fs_used: 1024,
      available: 1024,
      reserved_bytes: 0,
      files_total: 10,
      dir_total: 2,
      block_total: 7
    })),
    fetchMasterHa: vi.fn(async () => ({ nodes: [], failover_supported: false })),
    fetchWorkersData: vi.fn(async () => ({
      live_workers: [],
      blacklist_workers: [],
      decommission_workers: [],
      lost_workers: []
    })),
    fetchConfigData: vi.fn(async () => ({ master: { hostname: 'master-01' } })),
    fetchBrowseData: vi.fn(async () => ({ path: '/', items: [] })),
    fetchBlocksData: vi.fn(async () => ({})),
    downloadFile: vi.fn(async () => new Blob(['hello curvine'])),
    fetchMountsData: vi.fn(async () => [
      {
        mount_id: 'm1',
        cv_path: '/lake',
        ufs_path: 's3://bucket/lake',
        write_type: 'FsMode',
        ttl_ms: 604800000,
        provider: 'Opendal'
      }
    ]),
    fetchJobsData: vi.fn(async () => ({ items: [] })),
    fetchTransfersData: vi.fn(async () => ({ items: [] })),
    fetchObservabilityHealth: vi.fn(async () => ({
      enabled: true,
      mode: 'builtin',
      active_provider: 'builtin',
      scrape_interval_ms: 5000,
      retention_ms: 3600000,
      series_count: 0,
      sample_count: 0
    })),
    fetchObservabilitySettings: vi.fn(async () => ({
      scrape_interval_ms: 5000,
      retention_ms: 3600000,
      scrape_interval_options_ms: [5000],
      retention_options_ms: [3600000],
      max_retention_ms: 86400000,
      max_samples_per_series_cap: 1000,
      max_total_samples: 100000,
      max_series: 1000,
      estimated_peak_samples: 0
    })),
    fetchObservabilityTargets: vi.fn(async () => []),
    fetchObservabilityNames: vi.fn(async () => []),
    fetchObservabilityDashboard: vi.fn(async () => ({
      window: { range: '15m', step: '15s', start_ms: 0, end_ms: 0 },
      cards: [],
      groups: []
    })),
    fetchObservabilityOperations: vi.fn(async () => ({
      generated_ms: 1_750_000_000_000,
      diagnostics: {
        generated_ms: 1_750_000_000_000,
        active_provider: 'builtin',
        overall_status: 'pass',
        checks: [],
        discovered_master_targets: 1,
        discovered_live_worker_targets: 1,
        discovered_transfer_targets: 0,
        prometheus_covered_targets: 0,
        active_covered_targets: 2,
        expected_targets: 2,
        missing_targets: []
      },
      alerts: []
    })),
    ...overrides
  }
  return stub as unknown as ApiClient
}

function renderApp(path: string, client: ApiClient) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <ThemeProvider>
        <I18nProvider>
          <AuthProvider client={client}>
            <MasterRuntimeProvider>
              <RefreshProvider>
                <ToastProvider>
                  <TooltipProvider>
                    <App />
                  </TooltipProvider>
                </ToastProvider>
              </RefreshProvider>
            </MasterRuntimeProvider>
          </AuthProvider>
        </I18nProvider>
      </ThemeProvider>
    </MemoryRouter>
  )
}

describe('App routing and auth gating', () => {
  it('sends an unauthenticated visitor to the login form', async () => {
    const client = createStubClient({
      fetchAuthSession: vi.fn(async () => ({ authenticated: false }))
    })
    renderApp('/overview', client)

    expect(await screen.findByTestId('login-panel')).toBeInTheDocument()
    expect(screen.queryByTestId('global-refresh')).not.toBeInTheDocument()
  })

  it('keeps a public /login route reachable', async () => {
    renderApp('/login', createStubClient())
    expect(await screen.findByTestId('login-submit')).toBeInTheDocument()
  })
})

describe('Console shell', () => {
  it('renders navigation, breadcrumb and the overview metrics', async () => {
    renderApp('/overview', createStubClient())

    expect(await screen.findByTestId('sidebar-brand')).toBeInTheDocument()
    expect(screen.getByTestId('nav-overview')).toBeInTheDocument()
    expect(screen.getByTestId('nav-browse')).toBeInTheDocument()
    expect(screen.getByTestId('nav-workers')).toBeInTheDocument()
    expect(screen.getByTestId('nav-config')).toBeInTheDocument()
    expect(screen.getByTestId('locale-select')).toBeInTheDocument()
    expect(screen.getByTestId('auto-refresh-toggle')).toBeInTheDocument()

    // Overview data has landed and is rendered (cluster id shows in both the
    // metric card and the master details panel).
    expect((await screen.findAllByText('test-cluster')).length).toBeGreaterThan(0)
    expect(screen.getByText('Active')).toBeInTheDocument()
  })

  it('renders the workers page filters and empty state', async () => {
    renderApp('/workers', createStubClient())

    expect(await screen.findByTestId('workers-filter-live')).toBeInTheDocument()
    expect(await screen.findByTestId('workers-search')).toBeInTheDocument()
  })

  it('gives the reported worker version its own column', async () => {
    renderApp('/workers', createStubClient(workerClient()))

    expect(await screen.findByTestId('worker-version-worker-01:8995')).toHaveTextContent(
      '0.2.2 (commit: 5897a118, branch: web2)'
    )
    expect(screen.getByRole('columnheader', { name: 'Version' })).toBeInTheDocument()
  })

  it('opens the worker detail as a panel over the still-mounted list', async () => {
    renderApp('/workers', createStubClient(workerClient()))
    await screen.findByTestId('worker-row-worker-01:8995')

    await userEvent.click(screen.getByTestId('worker-open-worker-01:8995'))

    expect(await screen.findByTestId('worker-detail-sheet')).toBeInTheDocument()
    // The panel layers over the list rather than replacing it.
    expect(screen.getByTestId('workers-search')).toBeInTheDocument()
    expect(screen.getByText('worker-01 - worker-01:8995 - IP 10.0.0.1')).toBeInTheDocument()
  })

  it('opens the panel directly from a deep link', async () => {
    renderApp('/workers/worker-01:8995', createStubClient(workerClient()))

    expect(await screen.findByTestId('worker-detail-sheet')).toBeInTheDocument()
    expect(screen.getByTestId('workers-search')).toBeInTheDocument()
  })

  it('sizes the worker panel at 80% of the content box, above the variant cap', async () => {
    renderApp('/workers/worker-01:8995', createStubClient(workerClient()))
    await screen.findByTestId('worker-detail-sheet')

    // jsdom does no layout, so the shell the panel measures has to be given a box,
    // and the panel told to measure again (it re-measures on resize).
    const shell = document.querySelector('[data-console-content]') as HTMLElement
    shell.getBoundingClientRect = () =>
      ({
        left: 256,
        right: 1256,
        width: 1000,
        top: 0,
        bottom: 800,
        height: 800,
        x: 256,
        y: 0,
        toJSON: () => ({})
      }) as DOMRect
    window.dispatchEvent(new Event('resize'))

    const panel = screen.getByTestId('worker-detail-sheet')
    await waitFor(() => expect(panel.style.width).toBe('800px'))
    // Every side sheet carries `sm:max-w-sm` (24rem) from its variant; without
    // dropping it the width above is capped and the panel renders unreadably narrow.
    expect(panel.className).toContain('sm:max-w-none')
    expect(panel.className).not.toContain('sm:max-w-sm')
    // Only the width is measured. The panel keeps the variant's `right-0`, so it sits
    // flush with the window edge like every other side sheet; an inset here left a
    // strip of background down the right-hand side.
    expect(panel.style.right).toBe('')
    expect(panel.className).toContain('right-0')
  })

  it('flattens the readonly config table and keeps nested keys', async () => {
    renderApp('/config', createStubClient())

    expect(await screen.findByText('master.hostname')).toBeInTheDocument()
    expect(screen.getByText('master-01')).toBeInTheDocument()
  })

  it('renders the browse toolbar', async () => {
    renderApp('/browse', createStubClient())

    expect(await screen.findByTestId('browse-path')).toBeInTheDocument()
    expect(screen.getByTestId('browse-mkdir-open')).toBeInTheDocument()
    expect(screen.getByTestId('browse-columns-trigger')).toBeInTheDocument()
    expect(screen.getByTestId('browse-upload-file')).toBeInTheDocument()

    // Root / Up buttons are gone — the trail's root crumb and its ancestor
    // crumbs already do both jobs — while the manual path jump stays.
    expect(screen.queryByTestId('browse-root')).not.toBeInTheDocument()
    expect(screen.queryByTestId('browse-up')).not.toBeInTheDocument()
    expect(screen.getByTestId('browse-crumb-root')).toBeInTheDocument()
    expect(screen.getByTestId('browse-go')).toBeInTheDocument()
  })

  it('distinguishes directories from files and offers both copy targets', async () => {
    const client = createStubClient({
      fetchBrowseData: vi.fn(async () => ({
        path: '/',
        items: [
          { name: 'docs', path: '/docs', is_dir: true },
          { name: 'hello.txt', path: '/hello.txt', is_dir: false, len: 14, replicas: 1 }
        ]
      }))
    })
    renderApp('/browse', client)

    expect(await screen.findByTestId('browse-icon-dir-/docs')).toBeInTheDocument()
    expect(screen.getByTestId('browse-icon-file-/hello.txt')).toBeInTheDocument()

    await userEvent.click(screen.getByTestId('browse-copy-/hello.txt'))
    expect(await screen.findByTestId('browse-copy-path-/hello.txt')).toBeInTheDocument()
    expect(screen.getByTestId('browse-copy-name-/hello.txt')).toBeInTheDocument()
  })

  it('keeps an unsaved mount form and asks before discarding it', async () => {
    renderApp('/mounts', createStubClient())

    await userEvent.click(await screen.findByTestId('mounts-add'))
    const pathInput = await screen.findByTestId('mount-cv-path')
    await userEvent.type(pathInput, '/lake')
    expect(pathInput).toHaveValue('/lake')

    // Clicking the page behind the drawer must not throw the input away.
    fireEvent.pointerDown(document.body)
    expect(screen.getByTestId('mount-form-sheet')).toBeInTheDocument()
    expect(screen.queryByTestId('unsaved-changes-dialog')).not.toBeInTheDocument()
    expect(screen.getByTestId('mount-cv-path')).toHaveValue('/lake')

    // An explicit cancel asks first, and keeping editing preserves the values.
    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(await screen.findByTestId('unsaved-changes-dialog')).toBeInTheDocument()
    await userEvent.click(screen.getByTestId('unsaved-keep-editing'))
    expect(screen.getByTestId('mount-cv-path')).toHaveValue('/lake')

    // Discarding is explicit and only then closes the drawer.
    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    await userEvent.click(await screen.findByTestId('unsaved-discard'))
    expect(screen.queryByTestId('mount-form-sheet')).not.toBeInTheDocument()
  })

  it('closes an untouched mount form without asking', async () => {
    renderApp('/mounts', createStubClient())

    await userEvent.click(await screen.findByTestId('mounts-add'))
    await screen.findByTestId('mount-cv-path')

    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(screen.queryByTestId('unsaved-changes-dialog')).not.toBeInTheDocument()
    expect(screen.queryByTestId('mount-form-sheet')).not.toBeInTheDocument()
  })

  it('reaches blocks from the row action rather than a page-level button', async () => {
    const client = createStubClient({
      fetchBrowseData: vi.fn(async () => ({
        path: '/',
        items: [{ name: 'hello.txt', path: '/hello.txt', is_dir: false, len: 14, replicas: 1 }]
      }))
    })
    renderApp('/browse', client)

    // A file row still offers Blocks, so the header button was redundant.
    expect(await screen.findByTestId('browse-row-/hello.txt')).toBeInTheDocument()
    expect(screen.queryByTestId('browse-view-blocks')).not.toBeInTheDocument()
  })

  it('still lands on the block locations from a /blocks link', async () => {
    renderApp('/blocks?path=%2Fdemo%2Fdocs%2Fhello.txt', createStubClient())

    // The route redirects into the browser with the drawer open, so an old link or a
    // bookmark reaches the same thing it always did.
    expect(screen.queryByTestId('blocks-back')).not.toBeInTheDocument()
    const sheet = await screen.findByTestId('browse-blocks-sheet')

    // The way back is the trail, inside the drawer.
    expect(within(sheet).getByTestId('browse-crumb-current')).toHaveTextContent('hello.txt')

    // Clicking an ancestor closes the drawer and opens that directory.
    await userEvent.click(within(sheet).getByTestId('browse-crumb-1'))
    expect(await screen.findByTestId('browse-path')).toHaveValue('/demo/docs')
    await waitFor(() => expect(screen.queryByTestId('browse-blocks-sheet')).not.toBeInTheDocument())
  })

  it('previews a file from its name, leaving blocks to the 块 button', async () => {
    const client = createStubClient({
      fetchBrowseData: vi.fn(async () => ({
        path: '/',
        items: [{ name: 'hello.txt', path: '/hello.txt', is_dir: false, len: 14, replicas: 1 }]
      })),
      downloadFile: vi.fn(async () => new Blob(['hello curvine']))
    })
    renderApp('/browse', client)

    await userEvent.click(await screen.findByTestId('browse-preview-/hello.txt'))

    expect(await screen.findByTestId('browse-preview-sheet')).toBeInTheDocument()
    expect(screen.queryByTestId('browse-blocks-sheet')).not.toBeInTheDocument()
    expect(await screen.findByTestId('browse-preview-text')).toHaveTextContent('hello curvine')
    expect(client.downloadFile).toHaveBeenCalledWith('/hello.txt')
  })

  it('opens the block locations from the row button without leaving the browser', async () => {
    const client = createStubClient({
      fetchBrowseData: vi.fn(async () => ({
        path: '/',
        items: [{ name: 'hello.txt', path: '/hello.txt', is_dir: false, len: 14, replicas: 1 }]
      }))
    })
    renderApp('/browse', client)

    await userEvent.click(await screen.findByTestId('browse-blocks-/hello.txt'))

    expect(await screen.findByTestId('browse-blocks-sheet')).toBeInTheDocument()
    expect(screen.queryByTestId('browse-preview-sheet')).not.toBeInTheDocument()
    // Still the browser behind the drawer: blocks are no longer a route.
    expect(screen.getByTestId('browse-path')).toHaveValue('/')

    // A muted panel with normal text. These tokens used to be swapped, which is what made
    // the block JSON black in light mode and white in dark mode.
    const json = screen.getByTestId('blocks-json')
    expect(json.className).toContain('bg-muted')
    expect(json.className).not.toContain('bg-foreground')
  })

  it('caps a long text preview and says so', async () => {
    const client = createStubClient({
      fetchBrowseData: vi.fn(async () => ({
        path: '/',
        items: [{ name: 'big.log', path: '/big.log', is_dir: false, len: 1024 * 1024, replicas: 1 }]
      })),
      // Under the fetch limit, over the display cap.
      downloadFile: vi.fn(async () => new Blob(['x'.repeat(300 * 1024)]))
    })
    renderApp('/browse', client)

    await userEvent.click(await screen.findByTestId('browse-preview-/big.log'))

    expect(await screen.findByTestId('browse-preview-truncated')).toBeInTheDocument()
    // Read from a slice, so the tab never holds the whole file as text.
    expect(screen.getByTestId('browse-preview-text').textContent).toHaveLength(256 * 1024)
  })

  it('says so, and offers the download, for a type it cannot preview', async () => {
    const client = createStubClient({
      fetchBrowseData: vi.fn(async () => ({
        path: '/',
        items: [{ name: 'archive.bin', path: '/archive.bin', is_dir: false, len: 4096, replicas: 1 }]
      }))
    })
    renderApp('/browse', client)

    await userEvent.click(await screen.findByTestId('browse-preview-/archive.bin'))

    expect(await screen.findByTestId('browse-preview-unsupported')).toBeInTheDocument()
    expect(screen.getByTestId('browse-preview-download')).toBeInTheDocument()
    expect(client.downloadFile).not.toHaveBeenCalled()
  })

  it('draws PDFs itself rather than handing them to the browser viewer', async () => {
    const client = createStubClient({
      fetchBrowseData: vi.fn(async () => ({
        path: '/',
        items: [{ name: 'report.pdf', path: '/report.pdf', is_dir: false, len: 2048, replicas: 1 }]
      })),
      // The endpoint answers octet-stream for every file, which is one of the reasons the
      // bytes are handed to a renderer we control instead of to an <iframe>.
      downloadFile: vi.fn(
        async () => new Blob(['%PDF-1.4'], { type: 'application/octet-stream' })
      )
    })
    renderApp('/browse', client)

    await userEvent.click(await screen.findByTestId('browse-preview-/report.pdf'))

    // jsdom cannot run the renderer, so this asserts the shape of the choice: our own
    // surface, never the browser's frame.
    const surface = await screen.findByTestId('browse-preview-pdf')
    expect(surface.querySelector('iframe')).toBeNull()
    expect(client.downloadFile).toHaveBeenCalledWith('/report.pdf')
  })

  it('translates the drawer controls rather than rendering keys', async () => {
    const client = createStubClient({
      fetchBrowseData: vi.fn(async () => ({
        path: '/',
        items: [{ name: 'hello.txt', path: '/hello.txt', is_dir: false, len: 14, replicas: 1 }]
      }))
    })
    renderApp('/browse', client)

    await userEvent.click(await screen.findByTestId('browse-preview-/hello.txt'))

    // `common.close` was missing from both locales, so this button read "common.close".
    expect(await screen.findByTestId('browse-preview-close')).toHaveTextContent('Close')
    expect(screen.getByTestId('browse-preview-close')).not.toHaveTextContent('common.close')
    expect(document.querySelector('[data-slot="sheet-close"]')).toHaveAttribute(
      'aria-label',
      'Close'
    )
  })

  it('refuses to preview a file over the limit, without fetching it', async () => {
    const client = createStubClient({
      fetchBrowseData: vi.fn(async () => ({
        path: '/',
        items: [
          { name: 'huge.log', path: '/huge.log', is_dir: false, len: 48 * 1024 * 1024, replicas: 1 }
        ]
      }))
    })
    renderApp('/browse', client)

    await userEvent.click(await screen.findByTestId('browse-preview-/huge.log'))

    expect(await screen.findByTestId('browse-preview-too-large')).toBeInTheDocument()
    // The gate runs before the request: the endpoint buffers whole files server-side.
    expect(client.downloadFile).not.toHaveBeenCalled()
  })

  it('switches the theme from the topbar and remembers the choice', async () => {
    // Start from a clean slate: the storage and the <html> class are shared
    // across the cases in this file.
    window.localStorage.removeItem(THEME_STORAGE_KEY)
    document.documentElement.classList.remove('dark')

    renderApp('/overview', createStubClient())

    const toggle = await screen.findByTestId('theme-toggle')
    expect(document.documentElement).not.toHaveClass('dark')

    await userEvent.click(toggle)
    expect(document.documentElement).toHaveClass('dark')
    expect(window.localStorage.getItem(THEME_STORAGE_KEY)).toBe('dark')

    await userEvent.click(screen.getByTestId('theme-toggle'))
    expect(document.documentElement).not.toHaveClass('dark')
    expect(window.localStorage.getItem(THEME_STORAGE_KEY)).toBe('light')
  })

  it('resizes the sidebar from the rail and collapses it when dragged past the minimum', async () => {
    // The width and the open state live in cookies, which outlive a single case.
    document.cookie = 'sidebar_state=; max-age=0; path=/'
    document.cookie = 'sidebar_width=; max-age=0; path=/'

    renderApp('/overview', createStubClient())
    await screen.findByTestId('sidebar-trigger')
    // By attribute: the topbar trigger carries the same accessible name.
    const rail = document.querySelector('[data-slot="sidebar-rail"]') as HTMLElement
    const sidebar = document.querySelector('[data-slot="sidebar"]') as HTMLElement
    const wrapper = document.querySelector('[data-slot="sidebar-wrapper"]') as HTMLElement
    // jsdom has no PointerEvent, and the rail only reads `button` and `clientX`.
    const pointer = (kind: string, clientX = 0) =>
      new MouseEvent(kind, { bubbles: true, button: 0, clientX })

    fireEvent(rail, pointer('pointerdown', 256))
    fireEvent(rail, pointer('pointermove', 316))
    fireEvent(rail, pointer('pointerup'))
    expect(wrapper.style.getPropertyValue('--sidebar-width')).toBe('316px')
    expect(sidebar).toHaveAttribute('data-state', 'expanded')

    // Dragging well past the minimum hands the sidebar over to its icon rail rather
    // than squeezing the labels down to nothing.
    fireEvent(rail, pointer('pointerdown', 316))
    fireEvent(rail, pointer('pointermove', 100))
    expect(sidebar).toHaveAttribute('data-state', 'collapsed')
    fireEvent(rail, pointer('pointerup'))

    // A drag ends with a click on the rail, and that click must not also toggle.
    fireEvent.click(rail)
    expect(sidebar).toHaveAttribute('data-state', 'collapsed')

    // Pulling back out of the zone restores the full sidebar.
    fireEvent(rail, pointer('pointerdown', 48))
    fireEvent(rail, pointer('pointermove', 260))
    fireEvent(rail, pointer('pointerup'))
    expect(sidebar).toHaveAttribute('data-state', 'expanded')

    document.cookie = 'sidebar_state=; max-age=0; path=/'
    document.cookie = 'sidebar_width=; max-age=0; path=/'
  })

  it('leaves the wide tables no floor that would push the page sideways', async () => {
    renderApp('/workers', createStubClient(workerClient()))
    const row = await screen.findByTestId('worker-row-worker-01:8995')
    const table = row.closest('table') as HTMLElement

    // `table-fixed` is what keeps a long value inside its own column; a `min-w-[…]`
    // floor wider than the window is what used to make the whole page scroll
    // sideways on a small screen, header and sidebar included.
    expect(table).toHaveClass('table-fixed')
    expect(table.className).not.toMatch(/min-w-\[/)
  })

  it('reports a copied transfer id in the corner instead of a page row', async () => {
    renderApp('/transfers', createStubClient(transferClient()))

    await userEvent.click(await screen.findByTestId('transfer-copy-job-abc123'))

    const toast = await screen.findByTestId('toast-success')
    expect(toast).toHaveTextContent('Transfer id copied')
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith('job-abc123')

    // Out of flow in the bottom-right corner, so it cannot push the table down.
    const region = screen.getByTestId('toast-region')
    expect(region.className).toContain('fixed')
    expect(region.className).toContain('bottom-4')
    expect(region.className).toContain('right-4')
    // The full-width notice that used to occupy a row of the page is gone.
    expect(screen.queryByTestId('transfers-notice')).not.toBeInTheDocument()
  })

  it('refreshes the toast for a repeated action instead of stacking copies', async () => {
    renderApp('/transfers', createStubClient(transferClient()))
    const copy = await screen.findByTestId('transfer-copy-job-abc123')

    await userEvent.click(copy)
    await userEvent.click(copy)

    expect(await screen.findAllByTestId('toast-success')).toHaveLength(1)
  })

  it('lets a toast be dismissed by hand', async () => {
    renderApp('/transfers', createStubClient(transferClient()))

    await userEvent.click(await screen.findByTestId('transfer-copy-job-abc123'))
    await userEvent.click(await screen.findByTestId('toast-dismiss-success'))

    await waitFor(() => expect(screen.queryByTestId('toast-success')).not.toBeInTheDocument())
  })

  it('redirects an unknown path to the overview', async () => {
    renderApp('/does-not-exist', createStubClient())
    expect((await screen.findAllByText('test-cluster')).length).toBeGreaterThan(0)
  })

  it('renders the mounts table with mode filters and the add action', async () => {
    renderApp('/mounts', createStubClient())

    expect(await screen.findByTestId('mounts-add')).toBeInTheDocument()
    expect(screen.getByTestId('mounts-mode-fs_mode')).toBeInTheDocument()
    expect(await screen.findByText('/lake')).toBeInTheDocument()
    expect(screen.getByText('s3://bucket/lake')).toBeInTheDocument()
  })

  it('renders the jobs page filters and empty state', async () => {
    renderApp('/jobs', createStubClient())

    expect(await screen.findByTestId('jobs-open-load-form')).toBeInTheDocument()
    expect(screen.getByTestId('jobs-status-failed')).toBeInTheDocument()
    expect(screen.getByTestId('jobs-search')).toBeInTheDocument()
  })

  it('renders the transfers page filters and empty state', async () => {
    renderApp('/transfers', createStubClient())

    expect(await screen.findByTestId('transfers-open-submit')).toBeInTheDocument()
    expect(screen.getByTestId('transfers-filter-kind')).toBeInTheDocument()
    expect(screen.getByTestId('transfers-search')).toBeInTheDocument()
  })

  it('keeps the sidebar ruler inside the panel at any width', async () => {
    renderApp('/overview', createStubClient())
    await screen.findByTestId('sidebar-brand')

    // jsdom does no layout, so this pins the class contract that keeps it inside:
    // the Separator's own `data-[orientation=horizontal]:w-full` outranks a plain
    // `w-*`, and with the `mx-2` margins on top the rule ran 8px past the panel — into
    // the content beside it, at every sidebar width, which is what was reported.
    const ruler = document.querySelector('[data-sidebar="separator"]')
    expect(ruler?.className).toContain('w-[calc(100%-1rem)]!')
    expect(ruler?.className).toContain('mx-2')
  })

  it('hides the brand labels when the sidebar collapses to the icon rail', async () => {
    renderApp('/overview', createStubClient())
    const brand = await screen.findByTestId('sidebar-brand')

    // Otherwise the labels keep their box and spill 52px past the 48px rail.
    const labels = brand.querySelector('div')
    expect(labels?.className).toContain('group-data-[collapsible=icon]:hidden')
  })

  it('lazy-loads the observability page and its toolbar', async () => {
    renderApp('/observability', createStubClient())

    expect(await screen.findByTestId('observability-toolbar')).toBeInTheDocument()
    expect(screen.getByTestId('observability-time-range')).toBeInTheDocument()
    expect(screen.getByTestId('observability-provider-mode')).toBeInTheDocument()
  })

  it('pins the time range with the charts, leaving provider mode up in the toolbar', async () => {
    renderApp('/observability', createStubClient())

    const toolbar = await screen.findByTestId('observability-toolbar')
    const controls = screen.getByTestId('observability-chart-controls')

    // The range travels with the charts and is pinned, so scrolling — or a zoomed panel —
    // cannot take it away. `z-[70]` outranks the panel, which opens below it.
    expect(within(controls).getByTestId('observability-time-range')).toBeInTheDocument()
    expect(controls.className).toContain('sticky')
    expect(controls.className).toContain('z-[70]')
    expect(within(toolbar).queryByTestId('observability-time-range')).not.toBeInTheDocument()

    // Provider mode is a setting rather than a chart control, and leads its card.
    expect(within(toolbar).getByTestId('observability-provider-mode')).toBeInTheDocument()
    const providerRow = screen.getByTestId('observability-provider-mode').closest('.order-1')
    const filterRow = screen.getByTestId('observability-target-filter').closest('.order-2')
    expect(providerRow).not.toBeNull()
    expect(filterRow).not.toBeNull()
  })

  it('renders the operations page summary', async () => {
    renderApp('/operations', createStubClient())

    expect(await screen.findByTestId('operations-summary')).toBeInTheDocument()
    expect(screen.getByTestId('operations-health-badge')).toBeInTheDocument()
  })
})
