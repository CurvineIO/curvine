import { useMemo, useState } from 'react'

import { useApi } from '@/api/api-context'
import type { MountInfo } from '@/api/types'
import { SelectField, SubmitButton, TextAreaField, TextField } from '@/components/form-fields'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { FieldGroup } from '@/components/ui/field'
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle
} from '@/components/ui/sheet'
import { UnsavedChangesDialog } from '@/components/unsaved-changes-dialog'
import { useT } from '@/i18n'
import { hasUnsavedChanges } from '@/lib/form-dirty'
import {
  accessModeLabel,
  defaultPropertyKeys,
  formatMountTtl,
  isSecretKey,
  nextPropertyRowId,
  normalizeUfsPath,
  parsePropertiesText,
  propertiesHintKey,
  propertiesTextPlaceholder,
  propertyKeyPlaceholder,
  providerLabel,
  rowsFromProperties,
  schemeOf,
  textFromProperties,
  writeTypeLabel,
  type PropertyRow
} from './utils'

interface MountForm {
  cv_path: string
  ufs_path: string
  update: boolean
  write_type: string
  ttl: string
  read_verify_ufs: boolean
  auto_cache: boolean
  access_mode: string
  replicas: string
  block_size: string
  storage_type: string
  provider: string
}

function emptyForm(): MountForm {
  return {
    cv_path: '',
    ufs_path: '',
    update: false,
    write_type: 'fs_mode',
    ttl: '7d',
    read_verify_ufs: false,
    auto_cache: true,
    access_mode: 'read_only',
    replicas: '',
    block_size: '',
    storage_type: '',
    provider: 'auto'
  }
}

function formFromMount(mount: MountInfo): MountForm {
  return {
    cv_path: String(mount.cv_path || ''),
    ufs_path: String(mount.ufs_path || ''),
    update: true,
    write_type: writeTypeLabel(mount.write_type),
    ttl: formatMountTtl(mount.ttl_ms),
    read_verify_ufs: Boolean(mount.read_verify_ufs),
    auto_cache: (mount as { auto_cache?: boolean }).auto_cache !== false,
    access_mode: accessModeLabel(mount.access_mode),
    replicas:
      (mount as { replicas?: number | null }).replicas != null
        ? String((mount as { replicas?: number | null }).replicas)
        : '',
    block_size:
      (mount as { block_size?: number | string | null }).block_size != null
        ? String((mount as { block_size?: number | string | null }).block_size)
        : '',
    storage_type: String((mount as { storage_type?: string | null }).storage_type || ''),
    provider: providerLabel(mount.provider)
  }
}

/**
 * Create/update drawer. Uses the skill's Sheet contract `{ onClose, onSaved }`
 * rather than the legacy draggable modal; field behaviour is unchanged.
 */
export function MountFormSheet({
  open,
  mount,
  onClose,
  onSaved
}: {
  open: boolean
  mount: MountInfo | null
  onClose(): void
  onSaved(): void
}) {
  const t = useT()
  const api = useApi()

  // Snapshot of what the drawer opened with, so dismissing it can tell whether
  // anything would actually be thrown away.
  const initial = useMemo(() => {
    const properties = (mount as { properties?: Record<string, string> } | null)?.properties
    return {
      form: mount ? formFromMount(mount) : emptyForm(),
      text: mount ? textFromProperties(properties) : '',
      rows: mount ? rowsFromProperties(properties) : []
    }
  }, [mount])

  const [form, setForm] = useState<MountForm>(initial.form)
  const [propertiesText, setPropertiesText] = useState(initial.text)
  const [propertyRows, setPropertyRows] = useState<PropertyRow[]>(initial.rows)
  const [feedback, setFeedback] = useState<{ type: 'success' | 'error'; title: string; message: string } | null>(
    null
  )
  const [submitError, setSubmitError] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [validating, setValidating] = useState(false)
  const [confirmDiscard, setConfirmDiscard] = useState(false)

  const scheme = schemeOf(form.ufs_path)
  const keyPlaceholder = propertyKeyPlaceholder(form.provider, scheme)
  const hintKey = propertiesHintKey(form.provider, scheme)

  const invalidProperty = (line: string) => t('mounts.validation.invalidProperty', { line })

  /** Dismissal asks before discarding; an outside click is inert instead. */
  function requestClose() {
    if (hasUnsavedChanges(initial, { form, text: propertiesText, rows: propertyRows })) {
      setConfirmDiscard(true)
      return
    }
    onClose()
  }

  const parsedProperties = useMemo(() => {
    try {
      return parsePropertiesText(propertiesText, invalidProperty)
    } catch {
      return {} as Record<string, string>
    }
  }, [propertiesText, invalidProperty])

  function propertiesFromRows(): Record<string, string> {
    const props: Record<string, string> = {}
    for (const row of propertyRows) {
      const key = String(row.key || '').trim()
      if (!key) continue
      props[key] = String(row.value ?? '').trim()
    }
    return props
  }

  function buildProperties(): Record<string, string> {
    return { ...parsedProperties, ...propertiesFromRows() }
  }

  function usedPropertyKeys(): Set<string> {
    const used = new Set<string>(Object.keys(parsedProperties))
    propertyRows.forEach((row) => {
      const key = String(row.key || '').trim()
      if (key) used.add(key)
    })
    return used
  }

  function nextPropertyKey(): string {
    const used = usedPropertyKeys()
    return defaultPropertyKeys(form.provider, scheme).find((key) => !used.has(key)) || keyPlaceholder
  }

  function addPropertyRow() {
    // Free-form text wins until the first structured row exists.
    const base =
      propertyRows.length === 0 && propertiesText.trim()
        ? rowsFromProperties(parsedProperties)
        : propertyRows
    setPropertyRows([...base, { id: nextPropertyRowId(), key: nextPropertyKey(), value: '' }])
    setFeedback(null)
  }

  function removePropertyRow(index: number) {
    const next = propertyRows.filter((_, position) => position !== index)
    setPropertyRows(next)
    if (next.length > 0) setPropertiesText(textFromProperties(propertiesFromRows()))
    setFeedback(null)
  }

  function updateRow(id: string, patch: Partial<PropertyRow>) {
    setPropertyRows((current) => current.map((row) => (row.id === id ? { ...row, ...patch } : row)))
    setFeedback(null)
  }

  function ensureFormValid() {
    if (!form.cv_path.trim()) throw new Error(t('mounts.validation.curvinePathRequired'))
    if (!form.ufs_path.trim()) throw new Error(t('mounts.validation.ufsUriRequired'))
  }

  function payload() {
    ensureFormValid()
    const replicas = Number(form.replicas)
    return {
      cv_path: form.cv_path.trim(),
      ufs_path: normalizeUfsPath(form.ufs_path),
      update: Boolean(form.update),
      write_type: form.write_type,
      ttl: form.ttl,
      read_verify_ufs: Boolean(form.read_verify_ufs),
      auto_cache: Boolean(form.auto_cache),
      access_mode: form.access_mode,
      replicas: form.replicas && Number.isFinite(replicas) ? replicas : null,
      block_size: form.block_size || null,
      storage_type: form.storage_type || null,
      provider: form.provider === 'auto' ? null : form.provider,
      properties: buildProperties()
    }
  }

  function validateErrorMessage(error: unknown): string {
    const message = error instanceof Error ? error.message : String(error || t('common.unknown'))
    return message.replace(/^Error:\s*/i, '')
  }

  async function submitForm(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setSubmitError(null)
    setSubmitting(true)
    try {
      await api.saveMount(payload())
      onSaved()
    } catch (error) {
      setSubmitError(t('mounts.alerts.mountFailed', { error: validateErrorMessage(error) }))
    } finally {
      setSubmitting(false)
    }
  }

  async function validateForm() {
    setValidating(true)
    setFeedback(null)
    try {
      ensureFormValid()
      const data = (await api.validateMount({
        ufs_path: normalizeUfsPath(form.ufs_path),
        provider: form.provider || null,
        properties: buildProperties()
      })) as { ufs_path?: string; is_dir?: boolean; entries?: number } | null

      const ufsPath = data?.ufs_path || normalizeUfsPath(form.ufs_path)
      setFeedback({
        type: 'success',
        title: t('mounts.validation.succeeded'),
        message: data?.is_dir
          ? t('mounts.validation.reachableDir', { path: ufsPath, count: Number(data?.entries || 0) })
          : t('mounts.validation.reachableFile', { path: ufsPath })
      })
    } catch (error) {
      setFeedback({
        type: 'error',
        title: t('mounts.validation.failed'),
        message: validateErrorMessage(error)
      })
    } finally {
      setValidating(false)
    }
  }

  return (
    <Sheet open={open} onOpenChange={(next) => !next && requestClose()}>
      <SheetContent
        side="right"
        className="data-[side=right]:sm:max-w-2xl"
        data-testid="mount-form-sheet"
        onInteractOutside={(event) => event.preventDefault()}
      >
        <SheetHeader>
          <SheetTitle>{form.update ? t('mounts.form.updateTitle') : t('mounts.form.addTitle')}</SheetTitle>
          <SheetDescription>{t('mounts.form.subtitle')}</SheetDescription>
        </SheetHeader>

        <form className="flex min-h-0 flex-1 flex-col gap-4 overflow-auto px-4 pb-4" onSubmit={submitForm}>
          <FieldGroup className="md:grid md:grid-cols-2 md:gap-4">
            <TextField
              label={t('mounts.form.curvinePath')}
              value={form.cv_path}
              onChange={(cv_path) => setForm((current) => ({ ...current, cv_path }))}
              placeholder={t('mounts.form.curvinePathPlaceholder')}
              required
              testId="mount-cv-path"
            />
            <TextField
              label={t('mounts.form.ufsUri')}
              value={form.ufs_path}
              onChange={(ufs_path) => setForm((current) => ({ ...current, ufs_path }))}
              placeholder={t('mounts.form.ufsUriPlaceholder')}
              required
              testId="mount-ufs-path"
            />
            <SelectField
              label={t('mounts.form.writeType')}
              value={form.write_type}
              onChange={(write_type) => setForm((current) => ({ ...current, write_type }))}
              disabled={form.update}
              description={form.update ? t('mounts.form.writeTypeLocked') : undefined}
              options={[
                { value: 'fs_mode', label: t('mounts.form.fs_mode') },
                { value: 'cache_mode', label: t('mounts.form.cache_mode') }
              ]}
              testId="mount-write-type"
            />
            <TextField
              label={t('mounts.form.ttl')}
              value={form.ttl}
              onChange={(ttl) => setForm((current) => ({ ...current, ttl }))}
              placeholder={t('mounts.form.ttlPlaceholder')}
              testId="mount-ttl"
            />
            <SelectField
              label={t('mounts.form.provider')}
              value={form.provider}
              onChange={(provider) => setForm((current) => ({ ...current, provider }))}
              options={[
                { value: 'auto', label: t('mounts.form.auto') },
                { value: 'opendal', label: t('mounts.form.opendal') },
                { value: 'oss-hdfs', label: t('mounts.form.ossHdfs') }
              ]}
              testId="mount-provider"
            />
            <TextField
              label={t('mounts.form.replicas')}
              type="number"
              value={form.replicas}
              onChange={(replicas) => setForm((current) => ({ ...current, replicas }))}
              placeholder={t('mounts.form.replicasPlaceholder')}
              testId="mount-replicas"
            />
            <TextField
              label={t('mounts.form.blockSize')}
              value={form.block_size}
              onChange={(block_size) => setForm((current) => ({ ...current, block_size }))}
              placeholder={t('mounts.form.blockSizePlaceholder')}
              className="md:col-span-2"
              testId="mount-block-size"
            />
          </FieldGroup>

          <section className="flex flex-col gap-3 rounded-lg border border-border bg-muted/30 p-3">
            <div className="flex flex-col gap-0.5">
              <h3 className="text-sm font-medium">{t('mounts.form.cacheConsistencyTitle')}</h3>
              <p className="text-xs text-muted-foreground">{t('mounts.form.cacheConsistencyHint')}</p>
            </div>
            <div className="grid gap-3 md:grid-cols-2">
              <label className="flex flex-col gap-1.5 rounded-lg border border-border bg-card p-3">
                <span className="flex items-center gap-2">
                  <Checkbox
                    checked={form.auto_cache}
                    onCheckedChange={(checked) =>
                      setForm((current) => ({ ...current, auto_cache: checked === true }))
                    }
                    data-testid="mount-auto-cache"
                  />
                  <span className="text-[13px] font-medium">{t('mounts.form.autoCache')}</span>
                </span>
                <span className="text-xs text-muted-foreground">{t('mounts.form.autoCacheHint')}</span>
              </label>

              <div
                className={
                  form.write_type === 'fs_mode'
                    ? 'rounded-lg border border-border bg-muted/50 p-3 opacity-70'
                    : 'rounded-lg border border-border bg-card p-3'
                }
              >
                <SelectField
                  label={t('mounts.form.accessMode')}
                  value={form.access_mode}
                  onChange={(access_mode) => setForm((current) => ({ ...current, access_mode }))}
                  disabled={form.write_type === 'fs_mode'}
                  description={
                    form.write_type === 'fs_mode'
                      ? t('mounts.form.accessModeLocked')
                      : t('mounts.form.accessModeHint')
                  }
                  options={[
                    { value: 'read_only', label: 'read_only' },
                    { value: 'read_write', label: 'read_write' }
                  ]}
                  testId="mount-access-mode"
                />
              </div>

              <label className="flex flex-col gap-1.5 rounded-lg border border-border bg-card p-3 md:col-span-2">
                <span className="flex items-center gap-2">
                  <Checkbox
                    checked={form.read_verify_ufs}
                    onCheckedChange={(checked) =>
                      setForm((current) => ({ ...current, read_verify_ufs: checked === true }))
                    }
                    data-testid="mount-read-verify"
                  />
                  <span className="text-[13px] font-medium">{t('mounts.form.readVerifyUfs')}</span>
                </span>
                <span className="text-xs text-muted-foreground">{t('mounts.form.readVerifyHint')}</span>
              </label>
            </div>
          </section>

          <section className="flex flex-col gap-3 rounded-lg border border-border p-3">
            <div className="flex items-center justify-between gap-3">
              <strong className="text-sm font-medium">{t('mounts.form.properties')}</strong>
              <Button
                variant="outline"
                size="xs"
                type="button"
                onClick={addPropertyRow}
                data-testid="mount-add-property"
              >
                {t('mounts.form.addProperty')}
              </Button>
            </div>
            <p className="text-xs text-muted-foreground">{t(hintKey)}</p>

            <TextAreaField
              label={t('mounts.form.properties')}
              value={propertiesText}
              onChange={(value) => {
                setPropertiesText(value)
                setFeedback(null)
              }}
              rows={5}
              placeholder={propertiesTextPlaceholder(keyPlaceholder)}
              className="[&_label]:sr-only"
              testId="mount-properties-text"
            />

            {propertyRows.length > 0 && (
              <div className="flex flex-col gap-2 border-t border-dashed border-border pt-2">
                {propertyRows.map((row, index) => (
                  <div key={row.id} className="grid items-center gap-2 md:grid-cols-[minmax(0,1fr)_minmax(0,1.4fr)_auto]">
                    <input
                      value={row.key}
                      onChange={(event) => updateRow(row.id, { key: event.target.value })}
                      placeholder={keyPlaceholder}
                      aria-label={`${t('mounts.form.properties')} ${index + 1}`}
                      className="h-8 w-full rounded-lg border border-input bg-card px-2.5 text-sm outline-none focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/25"
                    />
                    <input
                      value={row.value}
                      type={isSecretKey(row.key) ? 'password' : 'text'}
                      onChange={(event) => updateRow(row.id, { value: event.target.value })}
                      placeholder={t('mounts.form.valuePlaceholder')}
                      aria-label={`${t('mounts.form.valuePlaceholder')} ${index + 1}`}
                      className="h-8 w-full rounded-lg border border-input bg-card px-2.5 text-sm outline-none focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/25"
                    />
                    <Button
                      variant="ghost"
                      size="xs"
                      type="button"
                      className="justify-self-end text-destructive"
                      onClick={() => removePropertyRow(index)}
                    >
                      {t('mounts.form.remove')}
                    </Button>
                  </div>
                ))}
              </div>
            )}
          </section>

          {feedback && (
            <Alert variant={feedback.type === 'error' ? 'destructive' : 'default'} data-testid="mount-validate-feedback">
              <AlertTitle>{feedback.title}</AlertTitle>
              <AlertDescription>{feedback.message}</AlertDescription>
            </Alert>
          )}

          {submitError && <Alert variant="destructive">
            <AlertDescription>{submitError}</AlertDescription>
          </Alert>}

          <div className="flex flex-wrap items-center gap-2">
            <SubmitButton pending={submitting} pendingLabel={t('common.working')} testId="mount-submit">
              {form.update ? t('mounts.form.update') : t('mounts.form.mount')}
            </SubmitButton>
            <Button
              variant="outline"
              type="button"
              disabled={validating || submitting}
              onClick={() => void validateForm()}
              data-testid="mount-validate"
            >
              {t('common.validate')}
            </Button>
            <Button variant="ghost" type="button" onClick={requestClose}>
              {t('mounts.form.cancel')}
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
