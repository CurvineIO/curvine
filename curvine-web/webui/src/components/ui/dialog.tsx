import { useEffect, type ReactNode } from 'react'
import { X } from 'lucide-react'
import { cn } from '@/lib/utils'

interface DialogProps {
  open: boolean
  onClose: () => void
  title: ReactNode
  children: ReactNode
  /** Optional action row pinned below the scrollable body. */
  footer?: ReactNode
  className?: string
}

/**
 * A minimal modal matching the card/badge primitives: a dimmed overlay that
 * closes on click, a centered panel, and Escape-to-close. The body scrolls so
 * long forms and property tables stay usable on small screens.
 */
export function Dialog({ open, onClose, title, children, footer, className }: DialogProps) {
  useEffect(() => {
    if (!open) {
      return
    }
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onClose()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, onClose])

  if (!open) {
    return null
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <button
        type="button"
        aria-label="Close dialog"
        className="absolute inset-0 cursor-default bg-black/60"
        onClick={onClose}
      />
      <div
        role="dialog"
        aria-modal="true"
        className={cn(
          'relative z-10 flex max-h-[85vh] w-full max-w-lg flex-col rounded-xl border bg-card text-card-foreground shadow-lg',
          className,
        )}
      >
        <div className="flex items-center justify-between gap-4 border-b p-4">
          <h3 className="font-semibold leading-none tracking-tight">{title}</h3>
          <button
            type="button"
            aria-label="Close"
            onClick={onClose}
            className="rounded p-1 text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        <div className="flex-1 overflow-y-auto p-4">{children}</div>
        {footer && <div className="flex flex-wrap justify-end gap-2 border-t p-4">{footer}</div>}
      </div>
    </div>
  )
}