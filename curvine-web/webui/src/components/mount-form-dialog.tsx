import { useState, type FormEvent, type ReactNode } from 'react'
import { Dialog } from '@/components/ui/dialog'
import { api } from '@/lib/api'
import type { AccessMode, MountRequest, Provider, StorageType, WriteType } from '@/lib/types'

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
 * Create a mount point (`POST /api/mount`). Optional numeric/enum fields left
 * empty are omitted so the master keeps its CLI builder defaults. Errors are
 * shown inside the dialog because it covers the shared error banner.
 */
export function MountFormDialog({
  onClose,
  onCreated,
}: {
  onClose: () => void
  onCreated: () => void
}) {
  const [form, setForm] = useState<FormState>(INITIAL)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const set = <K extends keyof FormState>(key: K, value: FormState[K]) =>
    setForm(prev => ({ ...prev, [key]: value }))

  const submit = (event: FormEvent) => {
    event.preventDefault()
    if (busy) {
      return
    }
    setError(null)
    let body: MountRequest
    try {
      const cvPath = form.cvPath.trim()
      const ufsPath = form.ufsPath.trim()
      if (!cvPath) {
        throw new Error('Curvine path is required')
      }
      if (!ufsPath) {
        throw new Error('UFS path is required')
      }
      body = {
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
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
      return
    }
    setBusy(true)
    api
      .mount(body, { quiet: true })
      .then(() => {
        onCreated()
        onClose()
      })
      .catch(err => setError(err instanceof Error ? err.message : String(err)))
      .finally(() => setBusy(false))
  }

  return (
    <Dialog
      open
      onClose={onClose}
      title="New mount"
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
            {busy ? 'Creating…' : 'Create mount'}
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
          <div className="grid grid-cols-2 gap-3">
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
            <Field label="Replicas" hint="empty = cluster default">
              <input
                value={form.replicas}
                onChange={event => set('replicas', event.target.value)}
                placeholder="1"
                className={inputCls}
              />
            </Field>
            <Field label="Block size" hint="e.g. 128MB; empty = cluster default">
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
                <option value="">Cluster default</option>
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
                <option value="">Cluster default</option>
                <option value="Auto">Auto</option>
                <option value="OssHdfs">OssHdfs</option>
                <option value="Opendal">Opendal</option>
                <option value="Cosn">Cosn</option>
              </select>
            </Field>
          </div>
          <div className="grid grid-cols-2 gap-2 border-t pt-3">
            <Check
              label="Check path consist"
              checked={form.checkPathConsist}
              onChange={value => set('checkPathConsist', value)}
            />
            <Check
              label="Read verify UFS"
              checked={form.readVerifyUfs}
              onChange={value => set('readVerifyUfs', value)}
            />
            <Check label="Auto cache" checked={form.autoCache} onChange={value => set('autoCache', value)} />
            <Check label="Write cache" checked={form.writeCache} onChange={value => set('writeCache', value)} />
          </div>
          <Field label="Properties" hint="One key=value per line; secret values are masked after save">
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
