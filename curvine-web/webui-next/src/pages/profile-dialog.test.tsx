import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { I18nProvider } from '@/i18n'
import { ThemeProvider } from '@/theme/theme-context'

import {
  availableDurations,
  DEFAULT_PROFILE_SECONDS,
  PROFILE_DURATIONS,
  ProfileDialog,
  profileUrl,
  workerProfileUrl
} from './profile-dialog'

function wrap(ui: React.ReactNode) {
  return (
    <ThemeProvider>
      <I18nProvider>{ui}</I18nProvider>
    </ThemeProvider>
  )
}

function renderDialog(props: Partial<React.ComponentProps<typeof ProfileDialog>> = {}) {
  return render(
    wrap(<ProfileDialog host="10.0.0.1" webPort={9002} open onOpenChange={() => {}} {...props} />)
  )
}

describe('profileUrl', () => {
  it('targets the process web port, not the rpc port', () => {
    expect(profileUrl('10.0.0.1', 9002)).toBe(
      'http://10.0.0.1:9002/api/v1/profiling/cpu?seconds=5&frequency=200&theme=light'
    )
  })

  it('carries the requested duration and theme', () => {
    // The SVG bakes its colours, so the theme has to travel in the URL.
    expect(profileUrl('10.0.0.1', 9002, 60, 'dark')).toBe(
      'http://10.0.0.1:9002/api/v1/profiling/cpu?seconds=60&frequency=200&theme=dark'
    )
  })

  it('offers a one-minute window', () => {
    expect(PROFILE_DURATIONS).toContain(60)
  })

  it('defaults to the shortest window', () => {
    expect(DEFAULT_PROFILE_SECONDS).toBe(PROFILE_DURATIONS[0])
  })

  it('targets the heap endpoint without a duration', () => {
    // The heap graph reads samples jemalloc already recorded, so there is no
    // window to request; sending one would imply a wait that never happens.
    expect(profileUrl('10.0.0.1', 9002, 60, 'dark', 'memory')).toBe(
      'http://10.0.0.1:9002/api/v1/profiling/memory?theme=dark'
    )
  })

  it('returns null when the web port is unknown', () => {
    // A missing web port means the endpoint cannot be addressed at all, so
    // the caller must not build a host-only URL that silently 404s.
    expect(profileUrl('10.0.0.1', null)).toBeNull()
    expect(profileUrl('10.0.0.1', undefined)).toBeNull()
    expect(profileUrl(null, 9002)).toBeNull()
  })

  it('prefers the reporter display ip for workers', () => {
    expect(
      workerProfileUrl({
        display_ip: '10.0.0.9',
        address: { hostname: 'localhost', ip_addr: '10.0.0.1', web_port: 9002 }
      } as never)
    ).toContain('10.0.0.9:9002')
  })
})

describe('availableDurations', () => {
  it('keeps every preset when the server allows them', () => {
    expect(availableDurations({ max_duration_secs: 120 })).toEqual([...PROFILE_DURATIONS])
  })

  it('drops presets above the server cap', () => {
    // Offering 120s while the cap is 60 would let the operator believe a
    // 120s window ran; the server clamps and the graph covers 60s.
    expect(availableDurations({ max_duration_secs: 60 })).toEqual([5, 15, 30, 60])
  })

  it('still offers one entry when the cap is below every preset', () => {
    expect(availableDurations({ max_duration_secs: 3 })).toEqual([3])
  })

  it('falls back to the full list when the capability is unknown', () => {
    // The server clamps regardless, so a superset is safe offline.
    expect(availableDurations(null)).toEqual([...PROFILE_DURATIONS])
    expect(availableDurations({ max_duration_secs: null })).toEqual([...PROFILE_DURATIONS])
  })
})

describe('fullscreen', () => {
  const originalRequest = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'requestFullscreen')

  afterEach(() => {
    cleanup()
    // jsdom has no Fullscreen API; restore exactly what each case installed.
    delete (document as unknown as { fullscreenElement?: unknown }).fullscreenElement
    delete (document as unknown as { exitFullscreen?: unknown }).exitFullscreen
    if (originalRequest) {
      Object.defineProperty(HTMLElement.prototype, 'requestFullscreen', originalRequest)
    } else {
      delete (HTMLElement.prototype as unknown as { requestFullscreen?: unknown })
        .requestFullscreen
    }
  })

  function installFullscreen() {
    const exit = vi.fn(async () => {
      delete (document as unknown as { fullscreenElement?: unknown }).fullscreenElement
      document.dispatchEvent(new Event('fullscreenchange'))
    })
    Object.defineProperty(document, 'exitFullscreen', { value: exit, configurable: true })
    const request = vi.fn(async function (this: HTMLElement) {
      Object.defineProperty(document, 'fullscreenElement', { value: this, configurable: true })
      document.dispatchEvent(new Event('fullscreenchange'))
    })
    Object.defineProperty(HTMLElement.prototype, 'requestFullscreen', {
      value: request,
      configurable: true
    })
    return { request, exit }
  }

  const label = () => screen.getByTestId('profile-fullscreen').getAttribute('aria-label') || ''

  it('fullscreens the frame container, not the iframe', async () => {
    const { request } = installFullscreen()
    renderDialog()
    expect(label()).toBe('Fullscreen')

    await act(async () => {
      fireEvent.click(screen.getByTestId('profile-fullscreen'))
    })

    expect(request).toHaveBeenCalledTimes(1)
    // The container is what grows; the iframe has no chrome of its own.
    expect(request.mock.instances[0]).toBe(screen.getByTestId('profile-frame-container'))
    expect(label()).toBe('Exit fullscreen')
  })

  it('returns to the idle label after exit', async () => {
    const { exit } = installFullscreen()
    renderDialog()

    await act(async () => {
      fireEvent.click(screen.getByTestId('profile-fullscreen'))
    })
    expect(label()).toBe('Exit fullscreen')

    await act(async () => {
      fireEvent.click(screen.getByTestId('profile-fullscreen'))
    })
    expect(exit).toHaveBeenCalledTimes(1)
    expect(label()).toBe('Fullscreen')
  })

  it('does not flip state when the browser denies fullscreen', async () => {
    Object.defineProperty(HTMLElement.prototype, 'requestFullscreen', {
      value: vi.fn(async () => {
        throw new Error('denied')
      }),
      configurable: true
    })
    renderDialog()

    await act(async () => {
      fireEvent.click(screen.getByTestId('profile-fullscreen'))
    })
    // A rejected request must not leave the button claiming fullscreen.
    expect(label()).toBe('Fullscreen')
  })
})

describe('ProfileDialog', () => {
  it('shows a sampling indicator until the frame loads', async () => {
    renderDialog()
    // The window blocks for its whole duration, so the empty iframe must be
    // explained rather than looking frozen.
    expect(screen.getByTestId('profile-loading')).toBeInTheDocument()

    await act(async () => {
      screen.getByTestId('profile-frame').dispatchEvent(new Event('load'))
    })

    await waitFor(() => {
      expect(screen.queryByTestId('profile-loading')).not.toBeInTheDocument()
    })
  })

  it('renders the iframe instead of an image so the svg script runs', () => {
    renderDialog()
    expect(screen.getByTestId('profile-frame').tagName).toBe('IFRAME')
  })

  it('exposes a duration control defaulting to 5s', () => {
    renderDialog()
    const trigger = screen.getByTestId('profile-duration')
    expect(trigger).toBeInTheDocument()
    expect(trigger.textContent).toContain(String(DEFAULT_PROFILE_SECONDS))
  })

  it('explains the missing web port instead of rendering a broken frame', () => {
    renderDialog({ webPort: null })
    expect(screen.queryByTestId('profile-frame')).not.toBeInTheDocument()
    expect(screen.getByText(/web port/i)).toBeInTheDocument()
  })

  it('restarts the indicator for a new target', async () => {
    const { rerender } = renderDialog()
    await act(async () => {
      screen.getByTestId('profile-frame').dispatchEvent(new Event('load'))
    })
    await waitFor(() => {
      expect(screen.queryByTestId('profile-loading')).not.toBeInTheDocument()
    })

    // Switching target must not inherit the previous "already loaded" state.
    rerender(
      wrap(<ProfileDialog host="10.0.0.2" webPort={9002} open onOpenChange={() => {}} />)
    )
    await waitFor(() => {
      expect(screen.getByTestId('profile-loading')).toBeInTheDocument()
    })
  })
})
