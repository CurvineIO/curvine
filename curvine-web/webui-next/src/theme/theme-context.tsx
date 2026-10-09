import * as React from 'react'

/** Keep in sync with the inline bootstrap script in index.html. */
export const THEME_STORAGE_KEY = 'curvine-theme'

export const THEME_PREFERENCES = ['system', 'light', 'dark'] as const
export type ThemePreference = (typeof THEME_PREFERENCES)[number]
export type ResolvedTheme = 'light' | 'dark'

function isPreference(value: unknown): value is ThemePreference {
  return typeof value === 'string' && (THEME_PREFERENCES as readonly string[]).includes(value)
}

function prefersDark(): boolean {
  if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return false
  return window.matchMedia('(prefers-color-scheme: dark)').matches
}

function readPreference(): ThemePreference {
  try {
    const stored = window.localStorage.getItem(THEME_STORAGE_KEY)
    if (isPreference(stored)) return stored
  } catch {
    // Storage can be unavailable (private mode); 'system' is the safe default.
  }
  return 'system'
}

interface ThemeContextValue {
  preference: ThemePreference
  resolved: ResolvedTheme
  setPreference(next: ThemePreference): void
}

const ThemeContext = React.createContext<ThemeContextValue | null>(null)

/**
 * Drives the `.dark` class on `<html>` from a stored preference.
 *
 * The stylesheet already defines both token sets; this is the missing half —
 * nothing toggled the class before. The inline script in index.html applies the
 * same rule before the bundle loads, so a dark-mode reload does not flash white.
 */
export function ThemeProvider({ children }: { children: React.ReactNode }) {
  const [preference, setPreferenceState] = React.useState<ThemePreference>(readPreference)
  const [systemPrefersDark, setSystemPrefersDark] = React.useState(prefersDark)

  const resolved: ResolvedTheme =
    preference === 'system' ? (systemPrefersDark ? 'dark' : 'light') : preference

  // Track the OS only while the user has not made an explicit choice.
  React.useEffect(() => {
    if (preference !== 'system') return
    if (typeof window.matchMedia !== 'function') return
    const query = window.matchMedia('(prefers-color-scheme: dark)')
    const onChange = () => setSystemPrefersDark(query.matches)
    query.addEventListener('change', onChange)
    return () => query.removeEventListener('change', onChange)
  }, [preference])

  React.useEffect(() => {
    document.documentElement.classList.toggle('dark', resolved === 'dark')
    // Keeps native controls (scrollbars, form widgets) in the same scheme.
    document.documentElement.style.colorScheme = resolved
  }, [resolved])

  const setPreference = React.useCallback((next: ThemePreference) => {
    setPreferenceState(next)
    try {
      window.localStorage.setItem(THEME_STORAGE_KEY, next)
    } catch {
      // Persisting is best-effort; the in-memory choice still applies.
    }
  }, [])

  const value = React.useMemo<ThemeContextValue>(
    () => ({ preference, resolved, setPreference }),
    [preference, resolved, setPreference]
  )

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
}

export function useTheme(): ThemeContextValue {
  const context = React.useContext(ThemeContext)
  if (!context) {
    throw new Error('useTheme must be used within ThemeProvider')
  }
  return context
}
