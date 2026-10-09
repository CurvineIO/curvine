import type { ReactNode } from 'react'
import { TriangleAlert } from 'lucide-react'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { Spinner } from '@/components/ui/spinner'
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

/**
 * Confirmation for state-changing actions.
 *
 * The action stays open until the caller resolves: `preventDefault` suppresses
 * Radix's default close, so a rejected request can surface its error in place.
 */
export function ConfirmDialog({
  open,
  title,
  description,
  warning,
  confirmLabel,
  cancelLabel,
  pending = false,
  destructive = false,
  onOpenChange,
  onConfirm,
  testId
}: {
  open: boolean
  title: string
  description: string
  warning?: ReactNode
  confirmLabel: string
  cancelLabel: string
  pending?: boolean
  destructive?: boolean
  onOpenChange(open: boolean): void
  onConfirm(): void
  testId?: string
}) {
  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{title}</AlertDialogTitle>
          <AlertDialogDescription>{description}</AlertDialogDescription>
        </AlertDialogHeader>
        {warning && (
          <Alert
            variant="destructive"
            data-testid={testId ? `${testId}-warning` : undefined}
          >
            <TriangleAlert />
            <AlertDescription className="text-destructive">{warning}</AlertDescription>
          </Alert>
        )}
        <AlertDialogFooter>
          <AlertDialogCancel disabled={pending}>{cancelLabel}</AlertDialogCancel>
          <AlertDialogAction
            variant={destructive ? 'destructive' : undefined}
            disabled={pending}
            data-testid={testId}
            onClick={(event) => {
              event.preventDefault()
              onConfirm()
            }}
          >
            {pending && <Spinner data-icon="inline-start" />}
            {confirmLabel}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}
