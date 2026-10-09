import * as React from 'react'

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useToast } from '@/components/ui/toast'
import { useT } from '@/i18n'
import { copyToClipboard, type CopyOutcome } from '@/lib/clipboard'

interface CopyApi {
  /**
   * Copies through whatever the browser allows, and reports which happened.
   *
   * `manual` means the text is now on screen, selected, for a real Ctrl/Cmd+C — callers that
   * show their own "copied" state should only do so for `copied`.
   */
  copy(text: string, successMessage: string): Promise<CopyOutcome>
}

const CopyContext = React.createContext<CopyApi | null>(null)

/**
 * Where a copy lands when the browser will not do it for us.
 *
 * On a plain-HTTP origin there is no clipboard API and no way to verify the legacy one, so the
 * text is handed over in a focused, fully selected, read-only field: a real Ctrl/Cmd+C is the
 * only path the browser trusts, and the only one that cannot silently do nothing.
 */
export function CopyFallbackProvider({ children }: { children: React.ReactNode }) {
  const t = useT()
  const toast = useToast()
  const [pending, setPending] = React.useState<string | null>(null)
  const inputRef = React.useRef<HTMLInputElement | null>(null)

  const copy = React.useCallback(
    async (text: string, successMessage: string): Promise<CopyOutcome> => {
      let outcome: CopyOutcome = 'manual'
      try {
        outcome = await copyToClipboard(text)
      } catch (error) {
        // Nothing to copy: worth saying out loud, since the click looked like it did something.
        toast.error(error instanceof Error ? error.message : String(error))
        return 'manual'
      }
      if (outcome === 'copied') {
        toast.success(successMessage)
        return outcome
      }
      setPending(text)
      return outcome
    },
    [toast]
  )

  const api = React.useMemo<CopyApi>(() => ({ copy }), [copy])

  // Radix focuses the first control in the dialog; selecting its contents is what makes the
  // fallback one keystroke. Runs on the frame after the field appears.
  React.useEffect(() => {
    if (pending === null) return
    const node = inputRef.current
    if (!node) return
    node.focus()
    node.select()
  }, [pending])

  return (
    <CopyContext.Provider value={api}>
      {children}
      <Dialog open={pending !== null} onOpenChange={(next) => !next && setPending(null)}>
        <DialogContent data-testid="copy-fallback" className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>{t('common.copyManually')}</DialogTitle>
            <DialogDescription>{t('common.copyManuallyHint')}</DialogDescription>
          </DialogHeader>
          <Input
            ref={inputRef}
            readOnly
            value={pending ?? ''}
            data-testid="copy-fallback-value"
            className="font-mono text-xs"
            onFocus={(event) => event.currentTarget.select()}
          />
          <DialogFooter>
            <Button size="sm" onClick={() => setPending(null)} data-testid="copy-fallback-close">
              {t('common.close')}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </CopyContext.Provider>
  )
}

export function useCopy(): CopyApi {
  const api = React.useContext(CopyContext)
  if (!api) throw new Error('useCopy must be used inside a CopyFallbackProvider')
  return api
}
