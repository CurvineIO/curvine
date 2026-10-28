import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiProvider } from '@/api/api-context'
import type { ApiClient } from '@/api/client'
import { ApiError } from '@/api/error'
import type { ClusterSummary, ClusterValidation } from '@/api/types'
import { CLUSTER_STORAGE_KEY, ClusterSelectionProvider } from '@/cluster/cluster-context'
import { CopyFallbackProvider } from '@/components/copy-fallback'
import { ToastProvider } from '@/components/ui/toast'
import { TooltipProvider } from '@/components/ui/tooltip'
import { I18nProvider, LOCALE_STORAGE_KEY } from '@/i18n'
import { RefreshProvider } from '@/refresh/refresh-context'
import { ClustersPage } from './clusters-page'

const DEFAULT_CLUSTER: ClusterSummary = {
  id: 'default',
  display_name: 'Default cluster',
  source: 'default',
  master_addrs: ['127.0.0.1:8995'],
  status: 'ready',
  error: null,
  editable: false,
  removable: false
}

const FILE_CLUSTER: ClusterSummary = {
  id: 'analytics',
  display_name: 'Analytics',
  source: 'file',
  master_addrs: ['10.0.0.7:8995', '10.0.0.8:8995'],
  status: 'invalid',
  error: 'master_addrs is empty',
  editable: true,
  removable: true
}

const FAILED_VALIDATION: ClusterValidation = {
  ok: false,
  cluster: null,
  errors: ['cluster.name is required'],
  warnings: ['credentials are inherited from the default cluster']
}

/** Hand-maintained file: the editor must hand it back byte for byte, comments included. */
const STORED_TOML = `# analytics cluster, maintained by hand
# the console writes back exactly what it shows
[cluster]
name = "analytics"

[master]
host = "10.0.0.7"
rpc_port = 8995
`

function stubClient(overrides: Partial<ApiClient> = {}): ApiClient {
  return {
    fetchClusters: async () => [DEFAULT_CLUSTER, FILE_CLUSTER],
    validateCluster: async () => ({
      ok: true,
      cluster: FILE_CLUSTER,
      errors: [],
      warnings: []
    }),
    createCluster: async () => FILE_CLUSTER,
    updateCluster: async () => FILE_CLUSTER,
    deleteCluster: async () => {},
    probeCluster: async () => ({
      reachable: true,
      active_master: '127.0.0.1:8995',
      latency_ms: 3
    }),
    reloadClusters: async () => [DEFAULT_CLUSTER, FILE_CLUSTER],
    getClusterConfig: async () => STORED_TOML,
    ...overrides
  } as unknown as ApiClient
}

function renderPage(client: ApiClient = stubClient()) {
  return render(
    <MemoryRouter initialEntries={['/clusters']}>
      <ClusterSelectionProvider>
        <ApiProvider client={client}>
          <I18nProvider>
            <RefreshProvider>
              <ToastProvider>
                <CopyFallbackProvider>
                  <TooltipProvider>
                    <ClustersPage />
                  </TooltipProvider>
                </CopyFallbackProvider>
              </ToastProvider>
            </RefreshProvider>
          </I18nProvider>
        </ApiProvider>
      </ClusterSelectionProvider>
    </MemoryRouter>
  )
}

beforeEach(() => {
  window.localStorage.clear()
  window.localStorage.setItem(LOCALE_STORAGE_KEY, 'en')
})

describe('ClustersPage', () => {
  it('lists every cluster with its status and source', async () => {
    renderPage()

    await waitFor(() => expect(screen.getByTestId('cluster-row-default')).toBeInTheDocument())
    expect(screen.getByTestId('cluster-row-analytics')).toBeInTheDocument()

    const defaultRow = screen.getByTestId('cluster-row-default')
    expect(defaultRow).toHaveTextContent('Default cluster')
    expect(defaultRow).toHaveTextContent('127.0.0.1:8995')
    expect(defaultRow).toHaveTextContent('Ready')
    expect(defaultRow).toHaveTextContent('Default')

    const fileRow = screen.getByTestId('cluster-row-analytics')
    expect(fileRow).toHaveTextContent('Invalid')
    expect(fileRow).toHaveTextContent('File')
    // The backend explanation for an invalid file is surfaced next to its id.
    expect(fileRow).toHaveTextContent('master_addrs is empty')
  })

  it('keeps save disabled while validation reports errors', async () => {
    const user = userEvent.setup()
    const validateCluster = vi.fn(async () => FAILED_VALIDATION)
    renderPage(stubClient({ validateCluster }))

    await user.click(await screen.findByTestId('cluster-import'))
    expect(screen.getByTestId('cluster-save')).toBeDisabled()

    // userEvent's `type` parses `[Key]` as a key descriptor, so the payload
    // deliberately avoids TOML table headers here.
    await user.type(screen.getByTestId('cluster-toml'), 'name = "analytics"')
    await user.click(screen.getByTestId('cluster-validate'))

    await waitFor(() =>
      expect(screen.getByTestId('cluster-validation-errors')).toHaveTextContent(
        'cluster.name is required'
      )
    )
    expect(screen.getByTestId('cluster-validation-warnings')).toHaveTextContent(
      'credentials are inherited from the default cluster'
    )
    expect(screen.getByTestId('cluster-save')).toBeDisabled()
    expect(validateCluster).toHaveBeenCalledWith('name = "analytics"')
  })

  it('only deletes a cluster once its id has been typed', async () => {
    const user = userEvent.setup()
    const deleteCluster = vi.fn(async () => {})
    renderPage(stubClient({ deleteCluster }))

    await user.click(await screen.findByTestId('cluster-delete-analytics'))
    await user.click(await screen.findByTestId('cluster-delete-confirm'))

    expect(deleteCluster).not.toHaveBeenCalled()
    await waitFor(() =>
      expect(screen.getByTestId('cluster-delete-error')).toBeInTheDocument()
    )

    await user.type(screen.getByTestId('cluster-delete-input'), 'analytics')
    await user.click(screen.getByTestId('cluster-delete-confirm'))

    await waitFor(() => expect(deleteCluster).toHaveBeenCalledWith('analytics'))
  })

  it('switches the selected cluster and persists it', async () => {
    const user = userEvent.setup()
    renderPage()

    // Nothing is pinned, so the default cluster is current and its button is idle.
    const defaultSwitch = await screen.findByTestId('cluster-switch-default')
    expect(defaultSwitch).toBeDisabled()

    await user.click(screen.getByTestId('cluster-switch-analytics'))

    expect(window.localStorage.getItem(CLUSTER_STORAGE_KEY)).toBe('analytics')
    await waitFor(() => expect(screen.getByTestId('cluster-switch-analytics')).toBeDisabled())
  })

  it('prefills the editor with the stored configuration', async () => {
    const user = userEvent.setup()
    const getClusterConfig = vi.fn(async () => STORED_TOML)
    renderPage(stubClient({ getClusterConfig }))

    await user.click(await screen.findByTestId('cluster-edit-analytics'))

    const textarea = await screen.findByTestId('cluster-toml')
    await waitFor(() => expect(textarea).toHaveValue(STORED_TOML))
    expect(getClusterConfig).toHaveBeenCalledWith('analytics')
    // Comments survive the round trip, so editing really is in place.
    expect((textarea as HTMLTextAreaElement).value).toContain('# analytics cluster')
    expect(textarea).not.toBeDisabled()
    expect(screen.queryByTestId('cluster-config-error')).not.toBeInTheDocument()
    // A prefilled document still has to be validated before it can be written back.
    expect(screen.getByTestId('cluster-save')).toBeDisabled()
  })

  it('falls back to pasting when the stored configuration cannot be read', async () => {
    const user = userEvent.setup()
    const getClusterConfig = vi.fn(async () => {
      throw new Error('cluster file disappeared')
    })
    renderPage(stubClient({ getClusterConfig }))

    await user.click(await screen.findByTestId('cluster-edit-analytics'))

    const fallback = await screen.findByTestId('cluster-config-error')
    expect(fallback).toHaveTextContent(/Could not read the stored configuration/)
    expect(fallback).toHaveTextContent('cluster file disappeared')

    // The sheet stays open on an empty, still editable box.
    const textarea = screen.getByTestId('cluster-toml')
    expect(textarea).toHaveValue('')
    expect(textarea).not.toBeDisabled()
    expect(screen.getByTestId('cluster-save')).toBeDisabled()

    await user.type(textarea, 'name = "analytics"')
    expect(textarea).toHaveValue('name = "analytics"')
  })

  it('blocks editing when the configuration is not editable by design', async () => {
    const user = userEvent.setup()
    const getClusterConfig = vi.fn(async () => {
      throw new ApiError(409, 'cluster is not editable', 'not_removable')
    })
    renderPage(stubClient({ getClusterConfig }))

    await user.click(await screen.findByTestId('cluster-edit-analytics'))

    const blocked = await screen.findByTestId('cluster-config-error')
    expect(blocked).toHaveTextContent('This cluster cannot be edited.')
    expect(screen.getByTestId('cluster-toml')).toBeDisabled()
    expect(screen.getByTestId('cluster-validate')).toBeDisabled()
    expect(screen.getByTestId('cluster-save')).toBeDisabled()
  })
})
