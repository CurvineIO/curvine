import * as React from 'react'
import { useLocation, useNavigate } from 'react-router-dom'

import { ApiProvider } from '@/api/api-context'
import { createApiClient, type ApiClient } from '@/api/client'
import { ApiError } from '@/api/error'
import type { AuthSession } from '@/api/types'
import { useOptionalCluster } from '@/cluster/cluster-context'

export type AuthStatus = 'unknown' | 'authenticated' | 'unauthenticated'

interface AuthContextValue {
  status: AuthStatus
  session: AuthSession | null
  signIn(username: string, password: string): Promise<void>
  signOut(): Promise<void>
}

const AuthContext = React.createContext<AuthContextValue | null>(null)

export function AuthProvider({
  children,
  client: injectedClient
}: {
  children: React.ReactNode
  /** Test seam: lets a test supply a stub client instead of hitting the network. */
  client?: ApiClient
}) {
  const [status, setStatus] = React.useState<AuthStatus>('unknown')
  const [session, setSession] = React.useState<AuthSession | null>(null)

  // Switching clusters rebuilds the client below, which changes the `useApi()`
  // identity every page depends on — so pages re-fetch on their own (useAsync /
  // useEffect deps) without any per-page cluster logic.
  const clusterId = useOptionalCluster()?.clusterId ?? ''

  // 401 from any request clears the session; RequireAuth then redirects to /login.
  const client = React.useMemo<ApiClient>(
    () =>
      injectedClient ??
      createApiClient({
        onUnauthorized: () => {
          setSession(null)
          setStatus('unauthenticated')
        },
        clusterId
      }),
    [injectedClient, clusterId]
  )

  const refreshSession = React.useCallback(async () => {
    try {
      const next = await client.fetchAuthSession()
      setSession(next ?? null)
      setStatus(next?.authenticated ? 'authenticated' : 'unauthenticated')
    } catch (error) {
      setSession(null)
      // A 401 is a definitive answer; anything else (backend down, network) is
      // treated the same for gating purposes, so the login page can retry.
      setStatus(error instanceof ApiError && error.status === 401 ? 'unauthenticated' : 'unauthenticated')
    }
  }, [client])

  React.useEffect(() => {
    void refreshSession()
  }, [refreshSession])

  const signIn = React.useCallback(
    async (username: string, password: string) => {
      await client.login({ username, password })
      await refreshSession()
    },
    [client, refreshSession]
  )

  const signOut = React.useCallback(async () => {
    try {
      await client.logout()
    } finally {
      setSession(null)
      setStatus('unauthenticated')
    }
  }, [client])

  const value = React.useMemo<AuthContextValue>(
    () => ({ status, session, signIn, signOut }),
    [status, session, signIn, signOut]
  )

  return (
    <ApiProvider client={client}>
      <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
    </ApiProvider>
  )
}

export function useAuth(): AuthContextValue {
  const context = React.useContext(AuthContext)
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider')
  }
  return context
}

/**
 * Gate for authenticated routes. Keeps the attempted location in `?redirect=`
 * so the login page can send the user back where they were.
 */
export function RequireAuth({ children }: { children: React.ReactNode }) {
  const { status } = useAuth()
  const location = useLocation()
  const navigate = useNavigate()

  React.useEffect(() => {
    if (status === 'unauthenticated') {
      navigate(`/login?redirect=${encodeURIComponent(location.pathname + location.search)}`, {
        replace: true
      })
    }
  }, [status, navigate, location.pathname, location.search])

  if (status === 'authenticated') {
    return <>{children}</>
  }

  return <div className="min-h-svh bg-background" data-testid="auth-pending" />
}
