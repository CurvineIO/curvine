import { Navigate, useSearchParams } from 'react-router-dom'

import { normalizePath } from './browse/utils'

/**
 * `/blocks` used to be its own page; block locations are now a sheet over the file browser,
 * opened with the row's 块 button. The route stays because the backend's SPA whitelist serves
 * it (`admin-router`, `/blocks`), so an old link or bookmark lands on the same thing: the
 * parent directory, with the sheet open for whatever the last segment names.
 */
export function BlocksPage() {
  const [searchParams] = useSearchParams()
  const path = normalizePath(searchParams.get('path') || '/')
  const cut = path.lastIndexOf('/')
  const directory = cut > 0 ? path.slice(0, cut) : '/'
  const name = path.slice(cut + 1)

  const search = name
    ? `?path=${encodeURIComponent(directory)}&open=blocks&file=${encodeURIComponent(name)}`
    : `?path=${encodeURIComponent(directory)}`

  return <Navigate to={`/browse${search}`} replace />
}
