import { useMemo, useState } from 'react'

import { useApi } from '@/api/api-context'
import type { TransferInfo } from '@/api/types'
import { ErrorAlert } from '@/components/data-state'
import { SelectField, SubmitButton, TextField } from '@/components/form-fields'
import { Button } from '@/components/ui/button'
import { FieldGroup } from '@/components/ui/field'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { UnsavedChangesDialog } from '@/components/unsaved-changes-dialog'
import { useT } from '@/i18n'
import { hasUnsavedChanges } from '@/lib/form-dirty'

interface TransferFormState {
  kind: 'load' | 'export'
  source_path: string
  target_path: string
  overwrite: string
}

function emptyForm(): TransferFormState {
  return { kind: 'load', source_path: '', target_path: '', overwrite: 'true' }
}

export function SubmitTransferSheet({
  open,
  onClose,
  onSubmitted
}: {
  open: boolean
  onClose(): void
  onSubmitted(transfer: TransferInfo): void
}) {
  const api = useApi()
  const t = useT()
  const [form, setForm] = useState<TransferFormState>(emptyForm)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [confirmDiscard, setConfirmDiscard] = useState(false)

  const initial = useMemo(() => emptyForm(), [])

  /** Dismissal asks before discarding; an outside click is inert instead. */
  function requestClose() {
    if (hasUnsavedChanges(initial, form)) {
      setConfirmDiscard(true)
      return
    }
    onClose()
  }

  const isLoad = form.kind === 'load'
  const sourcePlaceholder = isLoad
    ? t('transfers.form.sourcePathPlaceholderLoad')
    : t('transfers.form.sourcePathPlaceholderExport')
  const targetPlaceholder = isLoad
    ? t('transfers.form.targetPathPlaceholderLoad')
    : t('transfers.form.targetPathPlaceholderExport')

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const sourcePath = form.source_path.trim()
    if (!sourcePath) {
      setError(t('transfers.alerts.sourceRequired'))
      return
    }
    setError(null)
    setSubmitting(true)
    try {
      const transfer = (await api.submitTransfer({
        kind: form.kind,
        source_path: sourcePath,
        target_path: form.target_path.trim() || null,
        overwrite: form.overwrite === 'true'
      })) as TransferInfo
      onSubmitted(transfer)
    } catch (nextError) {
      setError(
        t('transfers.alerts.submitFailed', {
          error: nextError instanceof Error ? nextError.message : String(nextError)
        })
      )
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Sheet open={open} onOpenChange={(next) => !next && requestClose()}>
      <SheetContent
        side="right"
        className="data-[side=right]:sm:max-w-xl"
        data-testid="submit-transfer-sheet"
        onInteractOutside={(event) => event.preventDefault()}
      >
        <SheetHeader>
          <SheetTitle>{t('transfers.form.title')}</SheetTitle>
          <SheetDescription>{t('transfers.form.subtitle')}</SheetDescription>
        </SheetHeader>

        <form className="flex min-h-0 flex-1 flex-col gap-4 overflow-auto px-4 pb-4" onSubmit={submit}>
          <FieldGroup className="md:grid md:grid-cols-2 md:gap-4">
            <SelectField
              label={t('transfers.form.transferType')}
              value={form.kind}
              onChange={(value) => setForm((current) => ({ ...current, kind: value as 'load' | 'export' }))}
              options={[
                { value: 'load', label: t('transfers.form.kindLoad') },
                { value: 'export', label: t('transfers.form.kindExport') }
              ]}
              testId="transfer-kind"
            />
            <SelectField
              label={t('transfers.form.overwrite')}
              value={form.overwrite}
              onChange={(overwrite) => setForm((current) => ({ ...current, overwrite }))}
              options={[
                { value: 'true', label: t('transfers.form.trueValue') },
                { value: 'false', label: t('transfers.form.falseValue') }
              ]}
              testId="transfer-overwrite"
            />
            <TextField
              label={t('transfers.form.sourcePath')}
              value={form.source_path}
              onChange={(source_path) => setForm((current) => ({ ...current, source_path }))}
              placeholder={sourcePlaceholder}
              required
              className="md:col-span-2"
              testId="transfer-source-path"
            />
            <TextField
              label={t('transfers.form.targetPath')}
              value={form.target_path}
              onChange={(target_path) => setForm((current) => ({ ...current, target_path }))}
              placeholder={targetPlaceholder}
              className="md:col-span-2"
              testId="transfer-target-path"
            />
          </FieldGroup>

          <p className="text-xs text-muted-foreground">{t('transfers.form.targetHint')}</p>

          {error && <ErrorAlert message={error} />}

          <div className="flex flex-wrap items-center gap-2">
            <SubmitButton
              pending={submitting}
              pendingLabel={t('transfers.actions.working')}
              testId="transfer-submit"
            >
              {t('transfers.form.submit')}
            </SubmitButton>
            <Button
              variant="outline"
              type="button"
              disabled={submitting}
              onClick={() => {
                setForm(emptyForm())
                setError(null)
              }}
            >
              {t('common.reset')}
            </Button>
            <Button variant="ghost" type="button" onClick={requestClose}>
              {t('common.cancel')}
            </Button>
          </div>
        </form>
      </SheetContent>

      <UnsavedChangesDialog
        open={confirmDiscard}
        onDiscard={() => {
          setConfirmDiscard(false)
          onClose()
        }}
        onKeepEditing={() => setConfirmDiscard(false)}
      />
    </Sheet>
  )
}
