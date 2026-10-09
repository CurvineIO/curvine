import { render, screen, within } from '@testing-library/react'
import { describe, expect, it } from 'vitest'

import { ApiProvider } from '@/api/api-context'
import type { ApiClient } from '@/api/client'
import type { TransferInfo } from '@/api/types'
import { CopyFallbackProvider } from '@/components/copy-fallback'
import { ToastProvider } from '@/components/ui/toast'
import { TooltipProvider } from '@/components/ui/tooltip'
import { I18nProvider, LOCALE_STORAGE_KEY } from '@/i18n'
import { RefreshProvider } from '@/refresh/refresh-context'
import { TransfersPage } from '../../transfers-page'

const transfer = {
  job_id: 'job-responsive-test',
  state: 'running',
  kind: 'load'
} as TransferInfo

describe('TransfersPage mobile layout', () => {
  it('wraps status filters and uses a compact table with stacked actions', async () => {
    window.localStorage.setItem(LOCALE_STORAGE_KEY, 'en')
    const client = {
      fetchTransfersData: async () => ({ items: [transfer] }),
      fetchTransferStatus: async () => transfer,
      cancelTransfer: async () => transfer,
      retryTransfer: async () => transfer
    } as unknown as ApiClient

    render(
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

    const row = await screen.findByTestId('transfer-row-job-responsive-test')
    const headers = Array.from(document.querySelectorAll('th'))
    const pathHeader = headers.find((header) => header.textContent === 'Path')
    const progressHeader = headers.find((header) => header.textContent === 'Progress')
    const statusHeader = headers.find((header) => header.textContent === 'Status')

    expect(screen.getByTestId('transfers-status-filter')).toHaveClass('max-w-full', 'flex-wrap')
    expect(pathHeader).toHaveClass('hidden', 'sm:table-cell')
    expect(progressHeader).toHaveClass('hidden', 'sm:table-cell')
    expect(statusHeader).toHaveClass('w-[35%]')
    expect(within(row).getByTestId('transfer-details-job-responsive-test').parentElement).toHaveClass(
      'flex-col',
      'sm:flex-row'
    )
  })
})
