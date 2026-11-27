import { useState, type FormEvent, type ReactNode } from 'react'
import { Dialog } from '@/components/ui/dialog'
import { api } from '@/lib/api'
import type { AccessMode, MountInfo, MountRequest, Provider, StorageType, WriteType } from '@/lib/types'

/**
 * Parsers mirroring the CLI `DurationUnit::from_str` / `ByteUnit::from_str` so
 * the web form accepts the same strings as `cv mount`. Both allow a decimal or
 * scientific-notation number followed by a unit; a bare number means
 * milliseconds (duration) or bytes (size).
 */
function parseDurationMs(input: string): number {
  const s = input.trim().toLowerCase()
  if (!s || /[^\x20-\x7E]/.test(s)) {
    throw new Error(`Invalid duration: "${input}"`)
  }
  const match = s.match(/^([0-9.eE+-]+)(.*)$/)
  if (!match) {
    throw new Error(`Invalid duration: "${input}" (only d, h, m, s, ms are supported)`)
  }
  const [, num, rawUnit] = match
  const unit = rawUnit.trim()
  let factor: number
  switch (unit) {
    case 's':
    case 'second':
      factor = 1000
      break
    case 'm':
    case 'minute':
      factor = 60_000
      break
    case 'h':
    case 'hour':
      factor = 3_600_000
      break
    case 'd':
    case 'day':
      factor = 86_400_000
      break
    case 'ms':
    case '':
      if (/^[0-9]+$/.test(num)) {
        const n = Number(num)
        if (!Number.isSafeInteger(n)) {
          throw new Error(`Invalid duration: "${input}"`)
        }
        return n
      }
      factor = 1
      break
    default:
      throw new Error(`Invalid duration unit: "${input}" (only d, h, m, s, ms are supported)`)
  }
  const n = Number(num)
  if (!Number.isFinite(n) || n < 0) {
    throw new Error(`Invalid duration: "${input}"`)
  }
  return Math.floor(n * factor)
}

function parseBytes(input: string): number {
  const s = input.trim().toUpperCase()
  if (!s || /[^\x20-\x7E]/.test(s)) {
    throw new Error(`Invalid size: "${input}"`)
  }
  const match = s.match(/^([0-9.eE+-]+)(.*)$/)
  if (!match) {
    throw new Error(`Invalid size: "${input}" (only B, KB, MB, GB, TB, PB are supported)`)
  }
  const [, num, rawUnit] = match
  const unit = rawUnit.trim()
  let factor: number
  switch (unit) {
    case 'K':
    case 'KB':
      factor = 1024
      break
    case 'M':
    case 'MB':
      factor = 1024 ** 2
      break
    case 'G':
    case 'GB':
      factor = 1024 ** 3
      break
    case 'T':
    case 'TB':
      factor = 1024 ** 4
      break
    case 'P':
    case 'PB':
      factor = 1024 ** 5
      break
    case 'B':
    case '':
      if (/^[0-9]+$/.test(num)) {
        const n = Number(num)
        if (!Number.isSafeInteger(n)) {
          throw new Error(`Invalid size: "${input}"`)
        }
        return n
      }
      factor = 1
      break
    default:
      throw new Error(`Invalid size unit: "${input}" (only B, KB, MB, GB, TB, PB are supported)`)
  }
  const n = Number(num)
  if (!Number.isFinite(n) || n < 0) {
    throw new Error(`Invalid size: "${input}"`)
  }
  return Math.floor(n * factor)
}

/**
 * Exact single-unit renderings mirroring the CLI `Display` impls. Unlike
 * `formatDuration` / `formatBytes` these never round, so a prefilled value
 * parses back to the identical number and the update diff sees no change.
 */
function durationToUnit(ms: number): string {
  if (ms === 0) return '0ms'
  if (ms % 86_400_000 === 0) return `${ms / 86_400_000}d`
  if (ms % 3_600_000 === 0) return `${ms / 3_600_000}h`
  if (ms % 60_000 === 0) return `${ms / 60_000}m`
  if (ms % 1000 === 0) return `${ms / 1000}s`
  return `${ms}ms`
}

function bytesToUnit(bytes: number): string {
  if (bytes === 0) return '0B'
  if (bytes % 1024 ** 5 === 0) return `${bytes / 1024 ** 5}PB`
  if (bytes % 1024 ** 4 === 0) return `${bytes / 1024 ** 4}TB`
  if (bytes % 1024 ** 3 === 0) return `${bytes / 1024 ** 3}GB`
  if (bytes % 1024 ** 2 === 0) return `${bytes / 1024 ** 2}MB`
  if (bytes % 1024 === 0) return `${bytes / 1024}KB`
  return `${bytes}B`
}

/** One `key=value` per line; the value is everything after the first `=`. */
function parseProperties(text: string): Record<string, string> {
  const props: Record<string, string> = {}
  for (const line of text.split('\n')) {
    const trimmed = line.trim()
    if (!trimmed) {
      continue
    }
    const eq = trimmed.indexOf('=')
    const key = eq > 0 ? trimmed.slice(0, eq).trim() : ''
    if (!key) {
      throw new Error(`Invalid property line (expected key=value): "${trimmed}"`)
    }
    props[key] = trimmed.slice(eq + 1).trim()
  }
  return props
}

interface FormState {
  cvPath: string
  ufsPath: string
  writeType: WriteType
  accessMode: AccessMode
  ttl: string
  blockSize: string
  replicas: string
  storageType: '' | StorageType
  provider: '' | Provider
  checkPathConsist: boolean
  readVerifyUfs: boolean
  autoCache: boolean
  writeCache: boolean
  properties: string
}

const INITIAL: FormState = {
  cvPath: '',
  ufsPath: '',
  writeType: 'CacheMode',
  accessMode: 'ReadOnly',
  ttl: '7d',
  blockSize: '',
  replicas: '',
  storageType: '',
  provider: '',
  checkPathConsist: true,
  readVerifyUfs: false,
  autoCache: true,
  writeCache: false,
  properties: '',
}

/** Prefill from the stored mount. Properties stay empty: the master masks
 * secret values, so they must never be echoed back into an update. */
function prefill(mount: MountInfo): FormState {
  return {
    cvPath: mount.cv_path,
    ufsPath: mount.ufs_path,
    writeType: mount.write_type,
    accessMode: mount.access_mode,
    ttl: durationToUnit(mount.ttl_ms),
    blockSize: mount.block_size === null ? '' : bytesToUnit(mount.block_size),
    replicas: mount.replicas === null ? '' : String(mount.replicas),
    storageType: mount.storage_type ?? '',
    provider: mount.provider ?? '',
    checkPathConsist: true,
    readVerifyUfs: mount.read_verify_ufs,
    autoCache: mount.auto_cache,
    writeCache: mount.write_cache,
    properties: '',
  }
}

const inputCls =
  'h-9 w-full rounded-md border bg-secondary/40 px-3 font-mono text-sm outline-none placeholder:text-muted-foreground/70 focus:border-sky-500/60 focus:ring-2 focus:ring-sky-500/20'

function Field({ label, hint, children }: { label: string; hint?: string; children: ReactNode }) {
  return (
    <label className="block space-y-1">
      <span className="text-xs uppercase tracking-widest text-muted-foreground">{label}</span>
      {children}
      {hint && <span className="block text-[11px] text-muted-foreground/70">{hint}</span>}
    </label>
  )
}

function ReadOnly({ label, value }: { label: string; value: string }) {
  return (
    <div className="space-y-1">
      <span className="text-xs uppercase tracking-widest text-muted-foreground">{label}</span>
      <div className="break-all rounded-md border bg-secondary/20 px-3 py-2 font-mono text-sm text-muted-foreground">
        {value}
      </div>
    </div>
  )
}

function Check({
  label,
  checked,
  onChange,
}: {
  label: string
  checked: boolean
  onChange: (value: boolean) => void
}) {
  return (
    <label className="flex items-center gap-2 text-xs text-muted-foreground">
      <input
        type="checkbox"
        checked={checked}
        onChange={event => onChange(event.target.checked)}
        className="h-3.5 w-3.5 accent-sky-500"
      />
      {label}
    </label>
  )
}

/**
 * Create (`mount` null) or update (`mount` set) a mount point via
 * `POST /api/mount`. On create, optional fields left empty are omitted so the
 * master keeps its builder defaults. On update, only fields that differ from
 * the stored mount are sent; the UFS path and write type are immutable, and
 * masked properties are never written back. Errors render inside the dialog
 * because it covers the shared error banner.
 */
export function MountFormDialog({
  mount,
  onClose,
  onSaved,
}: {
  mount: MountInfo | null
  onClose: () => void
  onSaved: () => void
}) {
  const isUpdate = mount !== null
  const [form, setForm] = useState<FormState>(() => (mount ? prefill(mount) : INITIAL))
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const set = <K extends keyof FormState>(key: K, value: FormState[K]) =>
    setForm(prev => ({ ...prev, [key]: value }))

  const buildCreate = (): MountRequest => {
    const cvPath = form.cvPath.trim()
    const ufsPath = form.ufsPath.trim()
    if (!cvPath) {
      throw new Error('Curvine path is required')
    }
    if (!ufsPath) {
      throw new Error('UFS path is required')
    }
    const body: MountRequest = {
      cv_path: cvPath,
      ufs_path: ufsPath,
      write_type: form.writeType,
      access_mode: form.accessMode,
      ttl_ms: parseDurationMs(form.ttl),
      check_path_consist: form.checkPathConsist,
      read_verify_ufs: form.readVerifyUfs,
      auto_cache: form.autoCache,
      write_cache: form.writeCache,
    }
    if (form.blockSize.trim()) {
      body.block_size = parseBytes(form.blockSize)
    }
    if (form.replicas.trim()) {
      const n = Number(form.replicas)
      if (!Number.isInteger(n) || n <= 0) {
        throw new Error('Replicas must be a positive integer')
      }
      body.replicas = n
    }
    if (form.storageType) {
      body.storage_type = form.storageType
    }
    if (form.provider) {
      body.provider = form.provider
    }
    const props = parseProperties(form.properties)
    if (Object.keys(props).length > 0) {
      body.properties = props
    }
    return body
  }

  const buildUpdate = (stored: MountInfo): MountRequest => {
    // The request ufs_path is required by the API but ignored on update; the
    // stored UFS path and write type are never changed.
    const body: MountRequest = {
      cv_path: stored.cv_path,
      ufs_path: stored.ufs_path,
      update: true,
    }
    const ttlMs = parseDurationMs(form.ttl)
    if (ttlMs !== stored.ttl_ms) {
      body.ttl_ms = ttlMs
    }
    if (form.accessMode !== stored.access_mode) {
      body.access_mode = form.accessMode
    }
    if (form.readVerifyUfs !== stored.read_verify_ufs) {
      body.read_verify_ufs = form.readVerifyUfs
    }
    if (form.autoCache !== stored.auto_cache) {
      body.auto_cache = form.autoCache
    }
    if (form.writeCache !== stored.write_cache) {
      body.write_cache = form.writeCache
    }
    // An empty optional field means "keep the stored value": the update API
    // treats an absent field as no change and cannot unset it to the default.
    if (form.blockSize.trim()) {
      const bs = parseBytes(form.blockSize)
      if (bs !== stored.block_size) {
        body.block_size = bs
      }
    }
    if (form.replicas.trim()) {
      const n = Number(form.replicas)
      if (!Number.isInteger(n) || n <= 0) {
        throw new Error('Replicas must be a positive integer')
      }
      if (n !== stored.replicas) {
        body.replicas = n
      }
    }
    if (form.storageType && form.storageType !== (stored.storage_type ?? '')) {
      body.storage_type = form.storageType
    }
    if (form.provider && form.provider !== (stored.provider ?? '')) {
      body.provider = form.provider
    }
    const props = parseProperties(form.properties)
    if (Object.keys(props).length > 0) {
      body.properties = props
    }
    return body
  }

  const submit = (event: FormEvent) => {
    event.preventDefault()
    if (busy) {
      return
    }
    setError(null)
    let body: MountRequest
    try {
      body = mount ? buildUpdate(mount) : buildCreate()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
      return
    }
    setBusy(true)
    api
      .mount(body, { quiet: true })
      .then(() => {
        onSaved()
        onClose()
      })
      .catch(err => setError(err instanceof Error ? err.message : String(err)))
      .finally(() => setBusy(false))
  }

  return (
    <Dialog
      open
      onClose={onClose}
      title={isUpdate ? `Update ${mount.cv_path}` : 'New mount'}
      className="max-w-xl"
      footer={
        <>
          <button
            type="button"
            onClick={onClose}
            disabled={busy}
            className="rounded-md bg-secondary px-4 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-accent disabled:cursor-not-allowed disabled:opacity-60"
          >
            Cancel
          </button>
          <button
            type="submit"
            form="mount-form"
            disabled={busy}
            className="rounded-md bg-sky-500/15 px-4 py-2 text-sm font-medium text-sky-400 transition-colors hover:bg-sky-500/25 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {busy ? (isUpdate ? 'Saving…' : 'Creating…') : isUpdate ? 'Save changes' : 'Create mount'}
          </button>
        </>
      }
    >
      <form id="mount-form" onSubmit={submit}>
        <fieldset disabled={busy} className="space-y-4">
          {error && (
            <p className="rounded-md border border-red-500/40 bg-red-500/10 px-3 py-2 text-xs text-red-400">
              {error}
            </p>
          )}
          {isUpdate ? (
            <>
              <ReadOnly label="Curvine path" value={form.cvPath} />
              <ReadOnly label="UFS path (immutable)" value={form.ufsPath} />
            </>
          ) : (
            <>
              <Field label="Curvine path">
                <input
                  value={form.cvPath}
                  onChange={event => set('cvPath', event.target.value)}
                  placeholder="/data"
                  className={inputCls}
                  required
                />
              </Field>
              <Field label="UFS path">
                <input
                  value={form.ufsPath}
                  onChange={event => set('ufsPath', event.target.value)}
                  placeholder="oss://bucket/data"
                  className={inputCls}
                  required
                />
              </Field>
            </>
          )}
          <div className="grid grid-cols-2 gap-3">
            {isUpdate ? (
              <ReadOnly label="Write type (immutable)" value={form.writeType} />
            ) : (
              <Field label="Write type">
                <select
                  value={form.writeType}
                  onChange={event => set('writeType', event.target.value as WriteType)}
                  className={inputCls}
                >
                  <option value="CacheMode">CacheMode</option>
                  <option value="FsMode">FsMode</option>
                </select>
              </Field>
            )}
            <Field label="Access mode">
              <select
                value={form.accessMode}
                onChange={event => set('accessMode', event.target.value as AccessMode)}
                className={inputCls}
              >
                <option value="ReadOnly">ReadOnly</option>
                <option value="ReadWrite">ReadWrite</option>
              </select>
            </Field>
            <Field label="TTL" hint="e.g. 7d, 12h, 90s; 0 = never expire">
              <input
                value={form.ttl}
                onChange={event => set('ttl', event.target.value)}
                placeholder="7d"
                className={inputCls}
              />
            </Field>
            <Field label="Replicas" hint={isUpdate ? 'empty = keep current' : 'empty = cluster default'}>
              <input
                value={form.replicas}
                onChange={event => set('replicas', event.target.value)}
                placeholder="1"
                className={inputCls}
              />
            </Field>
            <Field label="Block size" hint={isUpdate ? 'empty = keep current' : 'e.g. 128MB; empty = default'}>
              <input
                value={form.blockSize}
                onChange={event => set('blockSize', event.target.value)}
                placeholder="128MB"
                className={inputCls}
              />
            </Field>
            <Field label="Storage type">
              <select
                value={form.storageType}
                onChange={event => set('storageType', event.target.value as FormState['storageType'])}
                className={inputCls}
              >
                <option value="">{isUpdate ? 'Keep current' : 'Cluster default'}</option>
                <option value="Mem">Mem</option>
                <option value="Ssd">Ssd</option>
                <option value="Hdd">Hdd</option>
                <option value="Ufs">Ufs</option>
                <option value="Disk">Disk</option>
                <option value="SpdkDisk">SpdkDisk</option>
              </select>
            </Field>
            <Field label="Provider">
              <select
                value={form.provider}
                onChange={event => set('provider', event.target.value as FormState['provider'])}
                className={inputCls}
              >
                <option value="">{isUpdate ? 'Keep current' : 'Cluster default'}</option>
                <option value="Auto">Auto</option>
                <option value="OssHdfs">OssHdfs</option>
                <option value="Opendal">Opendal</option>
                <option value="Cosn">Cosn</option>
              </select>
            </Field>
          </div>
          <div className="grid grid-cols-2 gap-2 border-t pt-3">
            {!isUpdate && (
              <Check
                label="Check path consist"
                checked={form.checkPathConsist}
                onChange={value => set('checkPathConsist', value)}
              />
            )}
            <Check
              label="Read verify UFS"
              checked={form.readVerifyUfs}
              onChange={value => set('readVerifyUfs', value)}
            />
            <Check label="Auto cache" checked={form.autoCache} onChange={value => set('autoCache', value)} />
            <Check label="Write cache" checked={form.writeCache} onChange={value => set('writeCache', value)} />
          </div>
          <Field
            label="Properties"
            hint={
              isUpdate
                ? 'Leave empty to keep existing; add or override keys only (key=value per line). Secret values are masked and never shown.'
                : 'One key=value per line; secret values are masked after save'
            }
          >
            <textarea
              value={form.properties}
              onChange={event => set('properties', event.target.value)}
              placeholder={'fs.oss.accessKeyId=...\nfs.oss.accessKeySecret=...'}
              rows={4}
              className="w-full rounded-md border bg-secondary/40 px-3 py-2 font-mono text-sm outline-none placeholder:text-muted-foreground/70 focus:border-sky-500/60 focus:ring-2 focus:ring-sky-500/20"
            />
          </Field>
        </fieldset>
      </form>
    </Dialog>
  )
}
