import type { AxiosInstance, AxiosResponse, InternalAxiosRequestConfig } from 'axios'
import { describe, expect, it } from 'vitest'

import { ApiError } from './error'
import { CLUSTER_HEADER, createDownloadClient, createHttpClient } from './http'

/**
 * Replaces the transport with a recorder.
 *
 * `createHttpClient` keeps its interceptors when the adapter is swapped, so the
 * request interceptor still runs and the assertions see exactly what would have
 * gone on the wire. No extra mocking dependency needed.
 */
function recordRequests(client: AxiosInstance): InternalAxiosRequestConfig[] {
  const seen: InternalAxiosRequestConfig[] = []
  client.defaults.adapter = async (config: InternalAxiosRequestConfig): Promise<AxiosResponse> => {
    seen.push(config)
    return { data: {}, status: 200, statusText: 'OK', headers: {}, config }
  }
  return seen
}

/** An error shaped the way the http/xhr adapters reject, so the 401 path runs. */
function rejectWithStatus(status: number) {
  return async (config: InternalAxiosRequestConfig): Promise<AxiosResponse> => {
    throw Object.assign(new Error(`Request failed with status code ${status}`), {
      isAxiosError: true,
      config,
      response: { data: {}, status, statusText: '', headers: {}, config }
    })
  }
}

describe('createHttpClient cluster header', () => {
  it('stamps the selected cluster on requests', async () => {
    const client = createHttpClient({ clusterId: 'east' })
    const seen = recordRequests(client)

    await client.get('/v1/overview')
    await client.post('/v1/fs/mkdir', { path: '/a' })

    expect(seen).toHaveLength(2)
    for (const request of seen) {
      expect(request.headers.get(CLUSTER_HEADER)).toBe('east')
    }
  })

  it('omits the header when no cluster is selected', async () => {
    const client = createHttpClient({ onUnauthorized: () => {} })
    const seen = recordRequests(client)

    await client.get('/v1/overview')

    expect(seen).toHaveLength(1)
    // Missing header means "the backend's own default cluster"; an empty value
    // would be an invalid cluster id instead.
    expect(seen[0].headers.has(CLUSTER_HEADER)).toBe(false)
  })

  it('respects a cluster the caller set explicitly', async () => {
    const client = createHttpClient({ clusterId: 'east' })
    const seen = recordRequests(client)

    await client.get('/v1/overview', { headers: { [CLUSTER_HEADER]: 'west' } })

    expect(seen[0].headers.get(CLUSTER_HEADER)).toBe('west')
  })

  it('treats an empty cluster id as no selection', async () => {
    const client = createHttpClient({ clusterId: '' })
    const seen = recordRequests(client)

    await client.get('/v1/overview')

    expect(seen[0].headers.has(CLUSTER_HEADER)).toBe(false)
  })

  it('still routes a 401 to onUnauthorized', async () => {
    let unauthorized = 0
    const client = createHttpClient({ onUnauthorized: () => (unauthorized += 1), clusterId: 'east' })
    client.defaults.adapter = rejectWithStatus(401)

    await expect(client.get('/v1/overview')).rejects.toBeInstanceOf(ApiError)
    expect(unauthorized).toBe(1)
  })
})

describe('createDownloadClient cluster header', () => {
  it('stamps the selected cluster on transfers', async () => {
    const client = createDownloadClient('east')
    const seen = recordRequests(client)

    await client.get('/v1/fs/download')

    expect(seen[0].headers.get(CLUSTER_HEADER)).toBe('east')
    expect(seen[0].baseURL).toBe('/api')
  })

  it('omits the header when no cluster is selected', async () => {
    const client = createDownloadClient()
    const seen = recordRequests(client)

    await client.get('/v1/fs/download')

    expect(seen[0].headers.has(CLUSTER_HEADER)).toBe(false)
  })
})
