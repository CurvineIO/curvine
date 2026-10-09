import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiError } from '@/api/error'
import { ApiProvider } from '@/api/api-context'
import type { ApiClient } from '@/api/client'
import type { TransferInfo } from '@/api/types'
import { ToastProvider } from '@/components/ui/toast'
import { CopyFallbackProvider } from '@/components/copy-fallback'
import { TooltipProvider } from '@/components/ui/tooltip'
import { I18nProvider, LOCALE_STORAGE_KEY, type AppLocale } from '@/i18n'
import { RefreshProvider } from '@/refresh/refresh-context'
import { TransfersPage } from './transfers-page'

const transfer = {
  job_id: 'job-abc123',
  state: 'running',
  kind: 'load',
  path: '/source/a.parquet',
  target_path: '/target/a.parquet',
  progress: 42,
  total_files: 10,
  completed_files: 4,
  submitter: 'alice',
  update_time_ms: 1_750_000_000_000
} as TransferInfo

function makeClient(
  listResponse: unknown = { items: [transfer] },
  transferStatus: (jobId: string, params?: Record<string, unknown>) => Promise<TransferInfo> =
    async () => transfer
) {
  const fetchTransfersData = vi.fn(async () => listResponse)
  const fetchTransferStatus = vi.fn(transferStatus)
  const client = {
    fetchTransfersData,
    fetchTransferStatus,
    cancelTransfer: async () => transfer,
    retryTransfer: async () => transfer
  } as unknown as ApiClient
  return { client, fetchTransfersData, fetchTransferStatus }
}

function renderPageWithClient(locale: AppLocale, client: ApiClient) {
  window.localStorage.setItem(LOCALE_STORAGE_KEY, locale)
  return render(
    <ApiProvider client={client}>
      <I18nProvider>
        <RefreshProvider>
          <ToastProvider>
            <CopyFallbackProvider>
              <TooltipProvider>
                <TransfersPage />
              </TooltipProvider>
            </CopyFallbackProvider>
          </ToastProvider>
        </RefreshProvider>
      </I18nProvider>
    </ApiProvider>
  )
}

function renderPage(locale: AppLocale, listResponse?: unknown) {
  return renderPageWithClient(locale, makeClient(listResponse).client)
}

describe('TransfersPage localization', () => {
  // The page restores its column selection on mount, so a leak between tests
  // would change what each one starts from.
  beforeEach(() => {
    window.localStorage.removeItem('curvine.web.transfers.columns.v1')
  })

  it('renders the page and its table in Chinese', async () => {
    renderPage('zh-CN')

    expect(await screen.findByRole('heading', { level: 1, name: '传输作业' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '提交传输' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '刷新' })).toBeInTheDocument()

    for (const header of [
      '作业 ID',
      '提交者',
      '类型',
      '路径',
      '状态',
      '进度',
      '文件',
      '传输时间',
      '更新时间',
      '操作'
    ]) {
      expect(screen.getByText(header, { selector: 'th' })).toBeInTheDocument()
    }

    // The state badge goes through the shared status vocabulary rather than
    // printing the raw backend value.
    await waitFor(() => expect(screen.getByTestId('transfer-row-job-abc123')).toBeInTheDocument())
    expect(screen.getByText('运行中')).toBeInTheDocument()
    expect(screen.getByText('详情')).toBeInTheDocument()
  })

  it('renders the same page in English', async () => {
    renderPage('en')

    expect(await screen.findByRole('heading', { level: 1, name: 'Transfer Jobs' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Submit Transfer' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Refresh' })).toBeInTheDocument()

    await waitFor(() => expect(screen.getByTestId('transfer-row-job-abc123')).toBeInTheDocument())
    expect(screen.getByText('Running')).toBeInTheDocument()
    expect(screen.getByText('Details')).toBeInTheDocument()
  })

  it('pages with the server and shows the reported total', async () => {
    const { client, fetchTransfersData } = makeClient({
      items: [transfer],
      page: 1,
      page_size: 50,
      total: 120,
      counts: { all: 120, active: 1, completed: 118, failed: 1, canceled: 0 }
    })
    renderPageWithClient('en', client)

    await waitFor(() => expect(screen.getByTestId('transfer-row-job-abc123')).toBeInTheDocument())
    // The first request asks for the first page at the default size.
    expect(fetchTransfersData).toHaveBeenCalledWith(
      expect.objectContaining({ page: 1, page_size: 50 })
    )
    expect(screen.getByTestId('transfers-total')).toHaveTextContent('120 total')
    expect(screen.getByRole('button', { name: /Previous/ })).toBeDisabled()

    await userEvent.click(screen.getByTestId('transfers-next-page'))

    await waitFor(() =>
      expect(fetchTransfersData).toHaveBeenCalledWith(expect.objectContaining({ page: 2 }))
    )
  })

  it('finds a transfer by a full job id outside the current page', async () => {
    const jobId = '12345678-1234-1234-1234-123456789abc'
    const target = { ...transfer, job_id: jobId, path: '/history/old.parquet' } as TransferInfo
    const { client, fetchTransferStatus } = makeClient(
      { items: [transfer], total: 1200, counts: { all: 1200 } },
      async () => target
    )
    renderPageWithClient('en', client)

    await screen.findByTestId('transfer-row-job-abc123')
    await userEvent.type(screen.getByTestId('transfers-search'), jobId)

    const row = await screen.findByTestId(`transfer-row-${jobId}`)
    expect(within(row).getByText(jobId)).toBeInTheDocument()
    expect(screen.queryByTestId('transfer-row-job-abc123')).not.toBeInTheDocument()
    expect(screen.getByTestId('transfers-total')).toHaveTextContent('1 total')
    expect(fetchTransferStatus).toHaveBeenCalledWith(jobId, { page_size: 0 })

    await userEvent.clear(screen.getByTestId('transfers-search'))
    expect(await screen.findByTestId('transfer-row-job-abc123')).toBeInTheDocument()
    expect(screen.queryByTestId(`transfer-row-${jobId}`)).not.toBeInTheDocument()
    expect(screen.getByTestId('transfers-total')).toHaveTextContent('1200 total')

    await userEvent.type(screen.getByTestId('transfers-search'), jobId)
    await screen.findByTestId(`transfer-row-${jobId}`)
    await userEvent.click(screen.getByTestId(`transfer-details-${jobId}`))
    expect(await screen.findByTestId('transfer-diagnostics-sheet')).toBeInTheDocument()
  })

  it('treats a missing full job id as an empty search result', async () => {
    const jobId = '12345678-1234-1234-1234-123456789abc'
    const { client } = makeClient(
      { items: [], total: 0, counts: { all: 0 } },
      async () => {
        throw new ApiError(500, `Job ${jobId} not found: `)
      }
    )
    renderPageWithClient('en', client)

    await screen.findByText(/No transfer jobs tracked yet/)
    await userEvent.type(screen.getByTestId('transfers-search'), jobId)

    await waitFor(() => expect(screen.getByTestId('transfers-total')).toHaveTextContent('0 total'))
    expect(screen.queryByTestId(`transfer-row-${jobId}`)).not.toBeInTheDocument()
    expect(await screen.findByText(/No transfers matched the current filters/)).toBeInTheDocument()
    expect(screen.queryByText(/No transfer jobs tracked yet/)).not.toBeInTheDocument()
    expect(screen.queryByText(/Transfer lookup failed/)).not.toBeInTheDocument()
  })

  it('shows lookup failures without claiming the job was not found', async () => {
    const jobId = '12345678-1234-1234-1234-123456789abc'
    const { client } = makeClient({ items: [], total: 0, counts: { all: 0 } }, async () => {
      throw new ApiError(503, 'Service unavailable')
    })
    renderPageWithClient('en', client)

    await userEvent.type(screen.getByTestId('transfers-search'), jobId)

    expect(await screen.findByText(/Transfer lookup failed: Service unavailable/)).toBeInTheDocument()
    expect(screen.queryByText(/No transfers matched the current filters/)).not.toBeInTheDocument()
  })

  it('heads the row with the job id', async () => {
    renderPage('en')
    const row = await screen.findByTestId('transfer-row-job-abc123')
    expect(within(row).getByTestId('transfer-copy-job-abc123')).toHaveTextContent('job-abc123')
  })

  it('hides a column from the header and the rows together', async () => {
    renderPage('en')
    await waitFor(() => expect(screen.getByTestId('transfer-row-job-abc123')).toBeInTheDocument())
    expect(screen.getByRole('columnheader', { name: 'Files' })).toBeInTheDocument()

    await userEvent.click(screen.getByTestId('transfers-columns-trigger'))
    await userEvent.click(screen.getByTestId('transfers-columns-option-files'))

    expect(screen.queryByRole('columnheader', { name: 'Files' })).not.toBeInTheDocument()
    // Job and Actions are fixed, so they survive hiding everything optional.
    expect(screen.getByRole('columnheader', { name: 'Job ID' })).toBeInTheDocument()
    expect(screen.getByRole('columnheader', { name: 'Actions' })).toBeInTheDocument()
  })

  it('restores the defaults on reset', async () => {
    renderPage('en')
    await waitFor(() => expect(screen.getByTestId('transfer-row-job-abc123')).toBeInTheDocument())

    await userEvent.click(screen.getByTestId('transfers-columns-trigger'))
    await userEvent.click(screen.getByTestId('transfers-columns-option-files'))
    expect(screen.queryByRole('columnheader', { name: 'Files' })).not.toBeInTheDocument()

    await userEvent.click(screen.getByTestId('transfers-columns-reset'))

    expect(screen.getByRole('columnheader', { name: 'Files' })).toBeInTheDocument()
  })

  it('remembers a hidden column across a remount', async () => {
    const first = renderPage('en')
    await waitFor(() => expect(screen.getByTestId('transfer-row-job-abc123')).toBeInTheDocument())

    await userEvent.click(screen.getByTestId('transfers-columns-trigger'))
    await userEvent.click(screen.getByTestId('transfers-columns-option-updated'))
    expect(screen.queryByRole('columnheader', { name: 'Updated' })).not.toBeInTheDocument()

    first.unmount()
    renderPage('en')

    await waitFor(() => expect(screen.getByTestId('transfer-row-job-abc123')).toBeInTheDocument())
    expect(screen.queryByRole('columnheader', { name: 'Updated' })).not.toBeInTheDocument()
  })

  it('keeps the state column when the stored selection predates the pin', async () => {
    // Not a selection the picker can produce any more, but it is what an older
    // browser profile holds — and the execution state has to survive it.
    window.localStorage.setItem(
      'curvine.web.transfers.columns.v1',
      JSON.stringify(['kind', 'path', 'progress', 'files', 'updated'])
    )
    renderPage('en')

    const row = await screen.findByTestId('transfer-row-job-abc123')
    expect(screen.getByRole('columnheader', { name: 'Status' })).toBeInTheDocument()
    expect(within(row).getByText('Running')).toBeInTheDocument()
  })

  it('does not offer the state column for hiding', async () => {
    renderPage('en')
    await waitFor(() => expect(screen.getByTestId('transfer-row-job-abc123')).toBeInTheDocument())

    await userEvent.click(screen.getByTestId('transfers-columns-trigger'))

    for (const key of ['kind', 'path', 'progress', 'files', 'updated']) {
      expect(screen.getByTestId(`transfers-columns-option-${key}`)).toBeInTheDocument()
    }
    expect(screen.queryByTestId('transfers-columns-option-status')).not.toBeInTheDocument()
  })

  it('still shows the state after every optional column is hidden', async () => {
    renderPage('en')
    await waitFor(() => expect(screen.getByTestId('transfer-row-job-abc123')).toBeInTheDocument())

    await userEvent.click(screen.getByTestId('transfers-columns-trigger'))
    for (const key of ['kind', 'path', 'progress', 'files', 'updated']) {
      await userEvent.click(screen.getByTestId(`transfers-columns-option-${key}`))
    }

    expect(screen.getByRole('columnheader', { name: 'Status' })).toBeInTheDocument()
    expect(within(screen.getByTestId('transfer-row-job-abc123')).getByText('Running')).toBeInTheDocument()
  })

  it('shows a warning icon instead of a message line when a transfer has one', async () => {
    renderPage('en', {
      items: [{ ...transfer, message: 'waiting for scheduling capacity' }]
    })

    await waitFor(() => expect(screen.getByTestId('transfer-row-job-abc123')).toBeInTheDocument())
    // One line per row: the message is reachable through the icon's hover title.
    const icon = screen.getByTestId('transfer-message-job-abc123')
    expect(icon.closest('span')).toHaveAttribute('title', 'waiting for scheduling capacity')
  })
})
