import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'

import { ApiProvider } from '@/api/api-context'
import type { ApiClient } from '@/api/client'
import type { MountInfo } from '@/api/types'
import { I18nProvider, LOCALE_STORAGE_KEY } from '@/i18n'
import { MountFormSheet } from '../mount-form-sheet'

function renderSheet(mount: MountInfo | null, saveMount: ReturnType<typeof vi.fn>) {
  window.localStorage.setItem(LOCALE_STORAGE_KEY, 'en')
  const client = { saveMount } as unknown as ApiClient
  return render(
    <ApiProvider client={client}>
      <I18nProvider>
        <MountFormSheet open mount={mount} onClose={vi.fn()} onSaved={vi.fn()} />
      </I18nProvider>
    </ApiProvider>
  )
}

describe('MountFormSheet ttl type', () => {
  it('defaults the ttl type to mtime when adding a mount', async () => {
    const saveMount = vi.fn(async () => ({}))
    renderSheet(null, saveMount)

    expect(screen.getByTestId('mount-ttl-type')).toHaveTextContent('mtime')

    await userEvent.type(screen.getByTestId('mount-cv-path'), '/lake/raw')
    await userEvent.type(screen.getByTestId('mount-ufs-path'), '/data/raw')
    await userEvent.click(screen.getByTestId('mount-submit'))

    await waitFor(() => expect(saveMount).toHaveBeenCalledTimes(1))
    expect(saveMount).toHaveBeenCalledWith(expect.objectContaining({ ttl_type: 'mtime' }))
  })

  it('prefills the ttl type from the mount being updated and submits it', async () => {
    const saveMount = vi.fn(async () => ({}))
    const mount = {
      cv_path: '/lake/raw',
      ufs_path: 'file:///lake/raw',
      write_type: 1,
      ttl_ms: 604800000,
      ttl_type: 'Atime'
    } as MountInfo
    renderSheet(mount, saveMount)

    expect(screen.getByTestId('mount-ttl-type')).toHaveTextContent('atime')

    await userEvent.click(screen.getByTestId('mount-submit'))

    await waitFor(() => expect(saveMount).toHaveBeenCalledTimes(1))
    expect(saveMount).toHaveBeenCalledWith(expect.objectContaining({ ttl_type: 'atime' }))
  })
})
