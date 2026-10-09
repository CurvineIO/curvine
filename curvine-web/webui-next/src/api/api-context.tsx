import * as React from 'react'

import type { ApiClient } from './client'

const ApiContext = React.createContext<ApiClient | null>(null)

export function ApiProvider({
  client,
  children
}: {
  client: ApiClient
  children: React.ReactNode
}) {
  return <ApiContext.Provider value={client}>{children}</ApiContext.Provider>
}

export function useApi(): ApiClient {
  const client = React.useContext(ApiContext)
  if (!client) {
    throw new Error('useApi must be used within ApiProvider')
  }
  return client
}
