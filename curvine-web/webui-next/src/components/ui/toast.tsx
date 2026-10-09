import * as React from 'react'
import { CircleAlertIcon, CircleCheckIcon, InfoIcon, XIcon } from 'lucide-react'

import { cn } from '@/lib/utils'
import { useT } from '@/i18n'

export type ToastTone = 'success' | 'error' | 'info'

interface ToastItem {
  id: number
  tone: ToastTone
  message: string
  /** Set while the exit animation plays; the item is dropped once it finishes. */
  leaving: boolean
}

interface ToastApi {
  success(message: string): void
  error(message: string): void
  info(message: string): void
}

const ToastContext = React.createContext<ToastApi | null>(null)

/** Long enough to read a path or an id, short enough not to linger over the UI. */
const LIFETIME: Record<ToastTone, number> = {
  success: 3200,
  info: 3200,
  error: 6000
}

/** Matches the `duration-*` on the toast's closed state. */
const EXIT_MS = 160

const TONE_STYLES: Record<ToastTone, { icon: React.ReactNode; className: string }> = {
  success: {
    icon: <CircleCheckIcon className="size-4 shrink-0 text-success" />,
    className: 'border-border'
  },
  error: {
    icon: <CircleAlertIcon className="size-4 shrink-0 text-destructive" />,
    className: 'border-destructive/40'
  },
  info: {
    icon: <InfoIcon className="size-4 shrink-0 text-muted-foreground" />,
    className: 'border-border'
  }
}

/**
 * Action feedback that must not disturb the layout it was triggered from.
 *
 * These notices used to render as an `<Alert>` in the page body, so confirming a
 * copied id pushed the whole table down for a moment. Toasts are out of flow and
 * stack in the bottom-right, and repeating the same action refreshes the existing
 * toast instead of piling up duplicates.
 */
export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [items, setItems] = React.useState<ToastItem[]>([])
  const nextId = React.useRef(1)
  const timers = React.useRef(new Map<number, ReturnType<typeof setTimeout>>())

  const clearTimer = React.useCallback((id: number) => {
    const timer = timers.current.get(id)
    if (timer !== undefined) {
      clearTimeout(timer)
      timers.current.delete(id)
    }
  }, [])

  const dismiss = React.useCallback(
    (id: number) => {
      clearTimer(id)
      setItems((prev) => prev.map((item) => (item.id === id ? { ...item, leaving: true } : item)))
      timers.current.set(
        id,
        setTimeout(() => {
          timers.current.delete(id)
          setItems((prev) => prev.filter((item) => item.id !== id))
        }, EXIT_MS)
      )
    },
    [clearTimer]
  )

  const show = React.useCallback(
    (tone: ToastTone, message: string) => {
      const lifetime = LIFETIME[tone]
      const duplicate = items.find((item) => item.tone === tone && item.message === message)
      if (duplicate) {
        clearTimer(duplicate.id)
        setItems((prev) =>
          prev.map((item) => (item.id === duplicate.id ? { ...item, leaving: false } : item))
        )
        timers.current.set(
          duplicate.id,
          setTimeout(() => dismiss(duplicate.id), lifetime)
        )
        return
      }

      const id = nextId.current++
      setItems((prev) => [...prev, { id, tone, message, leaving: false }])
      timers.current.set(
        id,
        setTimeout(() => dismiss(id), lifetime)
      )
    },
    [clearTimer, dismiss, items]
  )

  React.useEffect(() => {
    const pending = timers.current
    return () => {
      pending.forEach((timer) => clearTimeout(timer))
      pending.clear()
    }
  }, [])

  const api = React.useMemo<ToastApi>(
    () => ({
      success: (message: string) => show('success', message),
      error: (message: string) => show('error', message),
      info: (message: string) => show('info', message)
    }),
    [show]
  )

  return (
    <ToastContext.Provider value={api}>
      {children}
      <ToastRegion items={items} onDismiss={dismiss} />
    </ToastContext.Provider>
  )
}

function ToastRegion({ items, onDismiss }: { items: ToastItem[]; onDismiss(id: number): void }) {
  const t = useT()

  return (
    <div
      data-testid="toast-region"
      aria-live="polite"
      className="pointer-events-none fixed right-4 bottom-4 z-100 flex w-[calc(100%-2rem)] max-w-sm flex-col gap-2"
    >
      {items.map((item) => (
        <div
          key={item.id}
          role={item.tone === 'error' ? 'alert' : 'status'}
          data-testid={`toast-${item.tone}`}
          data-state={item.leaving ? 'closed' : 'open'}
          className={cn(
            'pointer-events-auto flex items-start gap-2 rounded-lg border bg-card p-3 text-sm shadow-lg',
            'data-[state=open]:animate-in data-[state=open]:fade-in data-[state=open]:slide-in-from-bottom-2 data-[state=open]:duration-200',
            'data-[state=closed]:animate-out data-[state=closed]:fade-out data-[state=closed]:slide-out-to-right data-[state=closed]:duration-150',
            TONE_STYLES[item.tone].className
          )}
        >
          {TONE_STYLES[item.tone].icon}
          <p className="min-w-0 flex-1 break-words">{item.message}</p>
          <button
            type="button"
            onClick={() => onDismiss(item.id)}
            aria-label={t('common.close')}
            data-testid={`toast-dismiss-${item.tone}`}
            className="-m-1 shrink-0 rounded-md p-1 text-muted-foreground transition-colors hover:text-foreground focus-visible:ring-[3px] focus-visible:ring-ring/30 focus-visible:outline-none"
          >
            <XIcon className="size-3.5" />
          </button>
        </div>
      ))}
    </div>
  )
}

export function useToast(): ToastApi {
  const api = React.useContext(ToastContext)
  if (!api) throw new Error('useToast must be used inside a ToastProvider')
  return api
}
