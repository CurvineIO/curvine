import { useCallback, useEffect, useState, type DependencyList } from 'react'

export interface AsyncState<T> {
  data: T | null
  loading: boolean
  error: string | null
  reload(): Promise<void>
  refresh(): Promise<void>
}

export function useAsync<T>(loader: () => Promise<T>, dependencies: DependencyList): AsyncState<T> {
  const [data, setData] = useState<T | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const reload = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      setData(await loader())
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : '请求失败')
    } finally {
      setLoading(false)
    }
  }, dependencies)

  // refresh updates in place without toggling the loading state, for silent polling.
  const refresh = useCallback(async () => {
    try {
      setData(await loader())
      setError(null)
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : '请求失败')
    }
  }, dependencies)

  useEffect(() => {
    void reload()
  }, [reload])

  return { data, loading, error, reload, refresh }
}
