import { useMemo, useState } from 'react'

import { useApi } from '@/api/api-context'
import type { JobInfo, MountInfo } from '@/api/types'
import { SelectField, SubmitButton, TextAreaField, TextField } from '@/components/form-fields'
import { ErrorAlert } from '@/components/data-state'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { FieldGroup } from '@/components/ui/field'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { UnsavedChangesDialog } from '@/components/unsaved-changes-dialog'
import { useT } from '@/i18n'
import { hasUnsavedChanges } from '@/lib/form-dirty'
import { joinPath, parseConfigs } from './utils'

interface LoadForm {
  source_mode: 'manual' | 'mount'
  mount_cv_path: string
  relative_path: string
  path: string
  target_path: string
  ttl: string
  ttl_action: string
  recursive: boolean
  replicas: string
  block_size: string
  storage_type: string
  overwrite: string
}

function emptyForm(): LoadForm {
  return {
    source_mode: 'manual',
    mount_cv_path: '',
    relative_path: '',
    path: '',
    target_path: '',
    ttl: '7d',
    ttl_action: '',
    recursive: false,
    replicas: '',
    block_size: '',
    storage_type: '',
    overwrite: 'true'
  }
}

export function SubmitLoadSheet({
  open,
  mounts,
  onClose,
  onSubmitted
}: {
  open: boolean
  mounts: MountInfo[]
  onClose(): void
  onSubmitted(job: JobInfo): void
}) {
  const t = useT()
  const api = useApi()

  const [form, setForm] = useState<LoadForm>(emptyForm)
  const [configsText, setConfigsText] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [confirmDiscard, setConfirmDiscard] = useState(false)

  const initial = useMemo(() => ({ form: emptyForm(), configsText: '' }), [])

  /** Dismissal asks before discarding; an outside click is inert instead. */
  function requestClose() {
    if (hasUnsavedChanges(initial, { form, configsText })) {
      setConfirmDiscard(true)
      return
    }
    onClose()
  }

  const selectedMount = mounts.find((mount) => mount.cv_path === form.mount_cv_path) || null
  const fromMount = form.source_mode === 'mount'

  const generatedSourcePath = fromMount && selectedMount
    ? joinPath(selectedMount.ufs_path, form.relative_path)
    : form.path.trim()

  const generatedTargetPath =
    fromMount && selectedMount ? joinPath(selectedMount.cv_path, form.relative_path) : ''

  function buildPayload() {
    if (fromMount && !selectedMount) throw new Error(t('jobs.alerts.mountRequired'))
    const path = generatedSourcePath
    if (!path) throw new Error(t('jobs.alerts.sourceRequired'))
    const replicas = Number(form.replicas)
    return {
      path,
      target_path: form.target_path.trim() || null,
      ttl: form.ttl.trim() || null,
      ttl_action: form.ttl_action || null,
      recursive: form.recursive,
      replicas: form.replicas && Number.isFinite(replicas) ? replicas : null,
      block_size: form.block_size || null,
      storage_type: form.storage_type || null,
      overwrite: form.overwrite === 'true',
      configs: parseConfigs(t, configsText)
    }
  }

  /** A missing backing path is the common failure mode; surface it before submitting. */
  async function validateMountedSource(path: string) {
    if (!fromMount || !selectedMount) return
    try {
      await api.validateMount({
        ufs_path: path,
        provider: selectedMount.provider || null,
        properties: selectedMount.properties || {}
      })
    } catch (nextError) {
      throw new Error(
        t('jobs.alerts.mountedSourceUnavailable', { path, error: String(nextError) })
      )
    }
  }

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setError(null)
    setSubmitting(true)
    try {
      const payload = buildPayload()
      await validateMountedSource(payload.path)
      onSubmitted((await api.submitLoadJob(payload)) as JobInfo)
    } catch (nextError) {
      setError(
        nextError instanceof Error
          ? nextError.message
          : t('jobs.alerts.submitFailed', { error: String(nextError) })
      )
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Sheet open={open} onOpenChange={(next) => !next && requestClose()}>
      <SheetContent
        side="right"
        className="data-[side=right]:sm:max-w-2xl"
        data-testid="submit-load-sheet"
        onInteractOutside={(event) => event.preventDefault()}
      >
        <SheetHeader>
          <SheetTitle>{t('jobs.form.title')}</SheetTitle>
          <SheetDescription>{t('jobs.form.subtitle')}</SheetDescription>
        </SheetHeader>

        <form className="flex min-h-0 flex-1 flex-col gap-4 overflow-auto px-4 pb-4" onSubmit={submit}>
          <FieldGroup className="md:grid md:grid-cols-2 md:gap-4">
            <SelectField
              label={t('jobs.form.sourceMode')}
              value={form.source_mode}
              onChange={(value) =>
                setForm((current) => ({ ...current, source_mode: value as LoadForm['source_mode'] }))
              }
              options={[
                { value: 'manual', label: t('jobs.form.manualPath') },
                { value: 'mount', label: t('jobs.form.fromMount') }
              ]}
              testId="load-source-mode"
            />

            {fromMount && (
              <SelectField
                label={t('jobs.form.mountPrefix')}
                value={form.mount_cv_path}
                onChange={(mount_cv_path) => setForm((current) => ({ ...current, mount_cv_path }))}
                placeholder={t('jobs.form.selectMount')}
                options={mounts.map((mount) => ({
                  value: mount.cv_path,
                  label: `${mount.cv_path} -> ${mount.ufs_path}`
                }))}
                testId="load-mount-prefix"
              />
            )}

            {fromMount ? (
              <TextField
                label={t('jobs.form.relativePath')}
                value={form.relative_path}
                onChange={(relative_path) => setForm((current) => ({ ...current, relative_path }))}
                placeholder={t('jobs.form.relativePathPlaceholder')}
                testId="load-relative-path"
              />
            ) : (
              <TextField
                label={t('jobs.form.sourcePath')}
                value={form.path}
                onChange={(path) => setForm((current) => ({ ...current, path }))}
                placeholder={t('jobs.form.sourcePathPlaceholder')}
                testId="load-source-path"
              />
            )}

            <TextField
              label={t('jobs.form.targetPath')}
              value={form.target_path}
              onChange={(target_path) => setForm((current) => ({ ...current, target_path }))}
              placeholder={generatedTargetPath || t('jobs.form.targetPathPlaceholder')}
              className="md:col-span-2"
              testId="load-target-path"
            />
          </FieldGroup>

          {fromMount && (
            <div className="grid gap-2 rounded-lg border border-border bg-muted/30 p-3">
              <div className="grid grid-cols-[4rem_minmax(0,1fr)] items-center gap-3 text-xs">
                <span className="text-muted-foreground">{t('jobs.form.source')}</span>
                <strong className="truncate font-mono" title={generatedSourcePath}>
                  {generatedSourcePath || '-'}
                </strong>
              </div>
              <div className="grid grid-cols-[4rem_minmax(0,1fr)] items-center gap-3 text-xs">
                <span className="text-muted-foreground">{t('jobs.form.autoTarget')}</span>
                <strong className="truncate font-mono" title={generatedTargetPath}>
                  {generatedTargetPath || '-'}
                </strong>
              </div>
            </div>
          )}

          <p className="text-xs text-muted-foreground">{t('jobs.form.targetHint')}</p>

          <FieldGroup className="md:grid md:grid-cols-2 md:gap-4">
            <TextField
              label={t('jobs.form.ttl')}
              value={form.ttl}
              onChange={(ttl) => setForm((current) => ({ ...current, ttl }))}
              placeholder="7d"
              testId="load-ttl"
            />
            <SelectField
              label={t('jobs.form.ttlAction')}
              value={form.ttl_action}
              onChange={(ttl_action) => setForm((current) => ({ ...current, ttl_action }))}
              options={[
                { value: '', label: t('jobs.form.default') },
                { value: 'delete', label: t('jobs.form.delete') },
                { value: 'free', label: t('jobs.form.free') },
                { value: 'none', label: t('jobs.form.none') }
              ]}
              testId="load-ttl-action"
            />
            <TextField
              label={t('jobs.form.replicas')}
              type="number"
              value={form.replicas}
              onChange={(replicas) => setForm((current) => ({ ...current, replicas }))}
              placeholder={t('jobs.form.default')}
              testId="load-replicas"
            />
            <TextField
              label={t('jobs.form.blockSize')}
              value={form.block_size}
              onChange={(block_size) => setForm((current) => ({ ...current, block_size }))}
              placeholder="128MB"
              testId="load-block-size"
            />
            <SelectField
              label={t('jobs.form.storage')}
              value={form.storage_type}
              onChange={(storage_type) => setForm((current) => ({ ...current, storage_type }))}
              options={[
                { value: '', label: t('jobs.form.default') },
                { value: 'disk', label: t('jobs.form.disk') },
                { value: 'mem', label: t('jobs.form.mem') },
                { value: 'ssd', label: t('jobs.form.ssd') },
                { value: 'hdd', label: t('jobs.form.hdd') },
                { value: 'ufs', label: t('jobs.form.ufs') }
              ]}
              testId="load-storage-type"
            />
            <SelectField
              label={t('jobs.form.overwrite')}
              value={form.overwrite}
              onChange={(overwrite) => setForm((current) => ({ ...current, overwrite }))}
              options={[
                { value: 'true', label: t('jobs.form.trueValue') },
                { value: 'false', label: t('jobs.form.falseValue') }
              ]}
              testId="load-overwrite"
            />
          </FieldGroup>

          <label className="flex items-center gap-2 text-sm">
            <Checkbox
              checked={form.recursive}
              onCheckedChange={(checked) =>
                setForm((current) => ({ ...current, recursive: checked === true }))
              }
              data-testid="load-recursive"
            />
            {t('jobs.form.recursive')}
          </label>

          <TextAreaField
            label={t('jobs.form.configs')}
            value={configsText}
            onChange={setConfigsText}
            rows={5}
            placeholder={t('jobs.form.configsPlaceholder')}
            testId="load-configs"
          />

          {error && <ErrorAlert message={error} />}

          <div className="flex flex-wrap items-center gap-2">
            <SubmitButton pending={submitting} pendingLabel={t('jobs.actions.working')} testId="load-submit">
              {t('jobs.form.submit')}
            </SubmitButton>
            <Button
              variant="outline"
              type="button"
              disabled={submitting}
              onClick={() => {
                setForm(emptyForm())
                setConfigsText('')
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
