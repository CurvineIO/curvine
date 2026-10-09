import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle
} from '@/components/ui/alert-dialog'
import { useT } from '@/i18n'

/**
 * Confirmation shown when a drawer with a filled-in form is dismissed.
 *
 * Pair this with `onInteractOutside` prevented on the drawer: an accidental
 * click on the page behind it should be inert rather than a data-loss trap,
 * while an explicit close still asks before throwing the input away.
 */
export function UnsavedChangesDialog({
  open,
  onDiscard,
  onKeepEditing
}: {
  open: boolean
  onDiscard(): void
  onKeepEditing(): void
}) {
  const t = useT()

  return (
    <AlertDialog
      open={open}
      onOpenChange={(next) => {
        if (!next) onKeepEditing()
      }}
    >
      <AlertDialogContent data-testid="unsaved-changes-dialog">
        <AlertDialogHeader>
          <AlertDialogTitle>{t('common.discardChanges')}</AlertDialogTitle>
          <AlertDialogDescription>{t('common.discardChangesHint')}</AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel data-testid="unsaved-keep-editing">
            {t('common.keepEditing')}
          </AlertDialogCancel>
          <AlertDialogAction
            variant="destructive"
            data-testid="unsaved-discard"
            onClick={(event) => {
              event.preventDefault()
              onDiscard()
            }}
          >
            {t('common.discard')}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}
