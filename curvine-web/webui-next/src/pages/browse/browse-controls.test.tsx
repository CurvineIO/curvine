import { fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'

import { PathBreadcrumb } from './browse-controls'
import { I18nProvider } from '@/i18n'

function renderTrail(path: string, onNavigate = vi.fn()) {
  render(
    <I18nProvider>
      <PathBreadcrumb path={path} onNavigate={onNavigate} />
    </I18nProvider>
  )
  return onNavigate
}

describe('PathBreadcrumb', () => {
  it('offers every intermediate level as a jump target', () => {
    const onNavigate = renderTrail('/warehouse/orders/2026')

    fireEvent.click(screen.getByTestId('browse-crumb-root'))
    expect(onNavigate).toHaveBeenLastCalledWith('/')

    fireEvent.click(screen.getByTestId('browse-crumb-0'))
    expect(onNavigate).toHaveBeenLastCalledWith('/warehouse')

    fireEvent.click(screen.getByTestId('browse-crumb-1'))
    expect(onNavigate).toHaveBeenLastCalledWith('/warehouse/orders')
  })

  it('keeps the last segment inert and shows it as the current directory', () => {
    renderTrail('/warehouse/orders/2026')

    expect(screen.getByTestId('browse-crumb-current')).toHaveTextContent('2026')
    // The current directory is a label, not another jump target.
    expect(screen.queryByTestId('browse-crumb-2')).not.toBeInTheDocument()
  })

  it('shows only the root at the filesystem root', () => {
    renderTrail('/')

    expect(screen.getByTestId('browse-crumb-root')).toBeInTheDocument()
    expect(screen.queryByTestId('browse-crumb-current')).not.toBeInTheDocument()
    expect(screen.queryByTestId('browse-crumb-0')).not.toBeInTheDocument()
  })

  it('ignores trailing and repeated slashes', () => {
    renderTrail('//warehouse//orders/')

    expect(screen.getByTestId('browse-crumb-current')).toHaveTextContent('orders')
    expect(screen.queryByTestId('browse-crumb-1')).not.toBeInTheDocument()
  })

  it('collapses a deep path to its tail and keeps the rest reachable', async () => {
    const onNavigate = renderTrail('/a/b/c/d/e/f')

    // Only the last three levels stay visible, so the current directory cannot
    // be pushed off the end of the strip.
    expect(screen.getByTestId('browse-crumb-current')).toHaveTextContent('f')
    expect(screen.getByTestId('browse-crumb-3')).toHaveTextContent('d')
    expect(screen.getByTestId('browse-crumb-4')).toHaveTextContent('e')
    expect(screen.queryByTestId('browse-crumb-2')).not.toBeInTheDocument()

    // The hidden ancestors are still jump targets, behind the ellipsis.
    await userEvent.click(screen.getByTestId('browse-crumb-hidden'))
    await userEvent.click(await screen.findByTestId('browse-crumb-hidden-1'))
    expect(onNavigate).toHaveBeenLastCalledWith('/a/b')
  })

  it('does not collapse a path at the visible limit', () => {
    renderTrail('/a/b/c')

    expect(screen.queryByTestId('browse-crumb-hidden')).not.toBeInTheDocument()
    expect(screen.getByTestId('browse-crumb-current')).toHaveTextContent('c')
  })
})
