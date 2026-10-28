import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { ApiProvider } from '@/api/api-context'
import type { ApiClient } from '@/api/client'
import type { ClusterSummary } from '@/api/types'
import { ToastProvider } from '@/components/ui/toast'
import { I18nProvider } from '@/i18n'
import { RefreshProvider } from '@/refresh/refresh-context'

import {
  CLUSTER_STORAGE_KEY,
  ClusterListBridge,
  ClusterSelectionProvider,
  useCluster
} from './cluster-context'

const EAST: ClusterSummary = {
  id: 'east',
  display_name: 'East Lake',
  source: 'file',
  master_addrs: ['10.0.0.1:9000'],
  status: 'ready',
  editable: true,
  removable: true
}

const WEST: ClusterSummary = {
  id: 'west',
  display_name: 'West Lake',
  source: 'file',
  master_addrs: ['10.0.0.2:9000'],
  status: 'ready',
  editable: true,
  removable: true
}

/** The `--conf` cluster: an ordinary list entry that reports `source: 'default'`. */
const HOME: ClusterSummary = {
  id: 'home',
  display_name: 'Home',
  source: 'default',
  master_addrs: ['10.0.0.9:9000'],
  status: 'ready',
  editable: false,
  removable: false
}

/** Exposes the selection state so tests can assert on it without a page. */
function Probe() {
  const { clusterId, clusters, clustersLoading } = useCluster()
  return (
    <>
      <span data-testid="cluster-id">{clusterId}</span>
      <span data-testid="cluster-count">{String(clusters.length)}</span>
      <span data-testid="cluster-loading">{String(clustersLoading)}</span>
    </>
  )
}

function SwitchTo({ id }: { id: string }) {
  const { setClusterId } = useCluster()
  return (
    <button type="button" onClick={() => setClusterId(id)}>
      switch to {id || 'default'}
    </button>
  )
}

function makeClient(clusters: ClusterSummary[]) {
  const fetchClusters = vi.fn(async () => clusters)
  return { client: { fetchClusters } as unknown as ApiClient, fetchClusters }
}

function renderTree(client: ApiClient, { bridge = false }: { bridge?: boolean } = {}) {
  return render(
    <I18nProvider>
      <ClusterSelectionProvider>
        <ApiProvider client={client}>
          <RefreshProvider>
            <ToastProvider>
              {bridge ? <ClusterListBridge /> : null}
              <Probe />
              <SwitchTo id="east" />
              <SwitchTo id="" />
            </ToastProvider>
          </RefreshProvider>
        </ApiProvider>
      </ClusterSelectionProvider>
    </I18nProvider>
  )
}

describe('ClusterSelectionProvider', () => {
  beforeEach(() => {
    window.localStorage.clear()
  })

  it('restores the cluster from localStorage', () => {
    window.localStorage.setItem(CLUSTER_STORAGE_KEY, 'east')

    renderTree(makeClient([EAST]).client)

    expect(screen.getByTestId('cluster-id')).toHaveTextContent('east')
  })

  it('starts with no selection when storage is empty', () => {
    renderTree(makeClient([EAST]).client)

    // '' is "follow the backend default cluster", not a missing value.
    expect(screen.getByTestId('cluster-id').textContent).toBe('')
  })

  it('persists a selection and accepts the unresolved (empty) value', async () => {
    const user = userEvent.setup()
    renderTree(makeClient([EAST]).client)

    await user.click(screen.getByRole('button', { name: 'switch to east' }))
    expect(screen.getByTestId('cluster-id')).toHaveTextContent('east')
    expect(window.localStorage.getItem(CLUSTER_STORAGE_KEY)).toBe('east')

    await user.click(screen.getByRole('button', { name: 'switch to default' }))
    expect(screen.getByTestId('cluster-id').textContent).toBe('')
    expect(window.localStorage.getItem(CLUSTER_STORAGE_KEY)).toBe('')
  })
})

describe('ClusterListBridge', () => {
  beforeEach(() => {
    window.localStorage.clear()
  })

  it('loads the cluster list from the backend', async () => {
    const { client, fetchClusters } = makeClient([EAST])

    renderTree(client, { bridge: true })

    await waitFor(() => {
      expect(screen.getByTestId('cluster-count')).toHaveTextContent('1')
    })
    expect(fetchClusters).toHaveBeenCalledTimes(1)
    expect(screen.getByTestId('cluster-loading')).toHaveTextContent('false')
  })

  it('keeps a stored id that the backend still reports', async () => {
    window.localStorage.setItem(CLUSTER_STORAGE_KEY, 'east')

    renderTree(makeClient([EAST]).client, { bridge: true })

    await waitFor(() => {
      expect(screen.getByTestId('cluster-count')).toHaveTextContent('1')
    })
    expect(screen.getByTestId('cluster-id')).toHaveTextContent('east')
  })

  it('selects the backend default cluster when nothing is stored', async () => {
    renderTree(makeClient([EAST, HOME, WEST]).client, { bridge: true })

    // No "follow the default" state: the switcher lists concrete clusters, so
    // the backend's own cluster is preselected and persisted.
    await waitFor(() => {
      expect(screen.getByTestId('cluster-id')).toHaveTextContent('home')
    })
    expect(window.localStorage.getItem(CLUSTER_STORAGE_KEY)).toBe('home')
  })

  it('falls back to the default cluster when the stored id is unknown', async () => {
    window.localStorage.setItem(CLUSTER_STORAGE_KEY, 'gone')

    renderTree(makeClient([WEST, HOME]).client, { bridge: true })

    await waitFor(() => {
      expect(screen.getByTestId('cluster-id')).toHaveTextContent('home')
    })
    expect(window.localStorage.getItem(CLUSTER_STORAGE_KEY)).toBe('home')
    // The disappearance is explained, not silent.
    expect(await screen.findByTestId('toast-error')).toHaveTextContent(/no longer exists/i)
  })

  it('prefers a ready cluster when the declared default is not usable', async () => {
    const brokenHome: ClusterSummary = { ...HOME, status: 'invalid', error: 'bad toml' }

    renderTree(makeClient([brokenHome, WEST]).client, { bridge: true })

    await waitFor(() => {
      expect(screen.getByTestId('cluster-id')).toHaveTextContent('west')
    })
  })

  it('leaves an empty list alone: no registry, so no header and no selection', async () => {
    renderTree(makeClient([]).client, { bridge: true })

    await waitFor(() => {
      expect(screen.getByTestId('cluster-loading')).toHaveTextContent('false')
    })
    expect(screen.getByTestId('cluster-count')).toHaveTextContent('0')
    expect(screen.getByTestId('cluster-id').textContent).toBe('')
  })
})
