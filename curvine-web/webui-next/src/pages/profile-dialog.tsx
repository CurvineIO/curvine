import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { Maximize2Icon, Minimize2Icon } from 'lucide-react'

import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/components/ui/select'
import { Spinner } from '@/components/ui/spinner'
import { useT } from '@/i18n'
import { useTheme } from '@/theme/theme-context'
import type { WorkerInfo } from '@/api/types'

/** Sampling windows offered in the dialog, in seconds. */
export const PROFILE_DURATIONS = [5, 15, 30, 60, 120] as const
export const DEFAULT_PROFILE_SECONDS = 5

/** What the sampled process reports it can serve. */
export interface ProfileCapability {
  max_duration_secs?: number | null
}

/**
 * Trims the offered windows to what the server will actually honour.
 *
 * Without this the picker could offer 120s while `max_duration_secs` is 60:
 * the server clamps, the SVG is drawn for 60s, and the dialog still claims
 * the operator asked for 120s.
 */
export function availableDurations(capability: ProfileCapability | null): number[] {
  const max = Number(capability?.max_duration_secs)
  if (!Number.isFinite(max) || max <= 0) return [...PROFILE_DURATIONS]
  const allowed = PROFILE_DURATIONS.filter((value) => value <= max)
  // A cap below the smallest preset still needs one selectable entry;
  // the server will clamp it, and the dialog reports the clamped duration.
  return allowed.length ? allowed : [max]
}

/** Reads the process's profiling capability; null when unavailable. */
export async function fetchProfileCapability(
  host: string,
  webPort: number
): Promise<ProfileCapability | null> {
  try {
    const response = await fetch(`http://${host}:${webPort}/api/v1/profiling/cpu/status`)
    if (!response.ok) return null
    return (await response.json()) as ProfileCapability
  } catch {
    // A cross-origin or offline process must not break the dialog; the
    // default presets are a safe superset that the server clamps anyway.
    return null
  }
}
/** Sampling frequency in Hz. Lower than the server default keeps the SVG small. */
export const PROFILE_FREQUENCY = 200

/** Parses an i18n count-ish value back to a number, ignoring junk. */
function toSeconds(value: string): number {
  const parsed = Number(value)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_PROFILE_SECONDS
}

/**
 * Builds a profiling URL from an explicit host and the process's web port.
 *
 * Profiling is process-local: only the process being sampled answers, on its
 * own `web_port`. That is why the caller must supply the address instead of
 * routing through the master — there is nothing to proxy to.
 *
 * `theme` is passed because the returned SVG is a static document with baked
 * colours; it cannot read the console's CSS variables.
 */
export function profileUrl(
  host: string | null | undefined,
  webPort: number | null | undefined,
  seconds: number = DEFAULT_PROFILE_SECONDS,
  theme: 'light' | 'dark' = 'light'
) {
  if (!host || !webPort) return null
  return (
    `http://${host}:${webPort}/api/v1/profiling/cpu` +
    `?seconds=${seconds}&frequency=${PROFILE_FREQUENCY}&theme=${theme}`
  )
}

/**
 * Tracks whether `ref` currently occupies the screen.
 *
 * Listens to `fullscreenchange` instead of tracking state locally: the browser
 * also exits fullscreen on Esc, so a purely local flag would go stale and the
 * toggle would need two clicks to work again.
 */
export function useFullscreen(ref: React.RefObject<HTMLElement | null>) {
  const [active, setActive] = useState(false)

  useEffect(() => {
    const sync = () => setActive(document.fullscreenElement === ref.current)
    document.addEventListener('fullscreenchange', sync)
    return () => {
      document.removeEventListener('fullscreenchange', sync)
    }
  }, [ref])

  const toggle = useCallback(async () => {
    const node = ref.current
    if (!node) return
    try {
      if (document.fullscreenElement === node) {
        await document.exitFullscreen()
      } else {
        await node.requestFullscreen()
      }
    } catch {
      // Denied (no user gesture) or unsupported; leave the state alone so the
      // button keeps matching reality rather than flipping optimistically.
    }
  }, [ref])

  return { active, toggle }
}

/** Resolves a worker's profiling URL, or null when it reported no web port. */
export function workerProfileUrl(worker: WorkerInfo): string | null {
  const address = worker.address
  const host = worker.display_ip || address?.ip_addr || address?.hostname || worker.ip_addr
  return profileUrl(host, address?.web_port)
}

export interface ProfileDialogProps {
  /** Host of the process to sample; null closes the content path. */
  host?: string | null
  /** The process's web port. */
  webPort?: number | null
  /** Process name shown in the title, e.g. `Worker 10.0.0.1:8997`. */
  target?: string
  open: boolean
  onOpenChange(open: boolean): void
}

/**
 * Shows a process's CPU flame graph in place.
 *
 * An iframe rather than an `<img>`: the SVG ships its own zoom/search script,
 * and only a document context runs it. `<img>` would render a static picture
 * with the interaction silently gone.
 */
export function ProfileDialog({ host, webPort, target, open, onOpenChange }: ProfileDialogProps) {
  const t = useT()
  const { resolved } = useTheme()
  const [seconds, setSeconds] = useState<number>(DEFAULT_PROFILE_SECONDS)
  const [capability, setCapability] = useState<ProfileCapability | null>(null)
  // The sampler blocks for the whole window, so the iframe stays blank until
  // the response arrives. Track load state so that wait is explained.
  const [loaded, setLoaded] = useState(false)
  // Fullscreening the frame container (not the iframe) keeps the toolbar
  // usable and lets the graph grow; the SVG's own controls stay inside it.
  const frameRef = useRef<HTMLDivElement | null>(null)
  const fullscreen = useFullscreen(frameRef)

  // Ask the process what it can serve, then size the picker to that instead of
  // offering windows the server would clamp.
  useEffect(() => {
    if (!open || !host || !webPort) return
    let cancelled = false
    void fetchProfileCapability(host, webPort).then((next) => {
      if (!cancelled) setCapability(next)
    })
    return () => {
      cancelled = true
    }
  }, [open, host, webPort])

  const durations = useMemo(() => availableDurations(capability), [capability])

  // Keying on the URL makes a new duration start a fresh sample instead of
  // leaving the previous graph on screen while the new one is pending.
  // The server clamps to `max_duration_secs`; report that number rather than
  // the one the operator picked when the two differ.
  const effectiveSeconds = useMemo(() => {
    const cap = Number(capability?.max_duration_secs)
    return Number.isFinite(cap) && cap > 0 ? Math.min(seconds, cap) : seconds
  }, [seconds, capability])

  const url = useMemo(
    () => profileUrl(host, webPort, effectiveSeconds, resolved),
    [host, webPort, effectiveSeconds, resolved]
  )

  // A new request (duration, theme, target) or a reopened dialog starts over;
  // without this the spinner would be skipped because the last frame loaded.
  useEffect(() => {
    setLoaded(false)
  }, [url, open])

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[92vw] lg:max-w-6xl">
        <DialogHeader>
          <DialogTitle>
            {target ? t('workers.profile.titleWithTarget', { target }) : t('workers.profile.title')}
          </DialogTitle>
          <DialogDescription>
            {url
              ? t('workers.profile.description', { seconds: effectiveSeconds })
              : t('workers.profile.unsupported')}
          </DialogDescription>
        </DialogHeader>

        {url ? (
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <label
                className="text-xs text-muted-foreground"
                htmlFor="profile-duration"
              >
                {t('workers.profile.duration')}
              </label>
              <Select
                value={String(seconds)}
                onValueChange={(value) => setSeconds(toSeconds(value))}
              >
                <SelectTrigger
                  id="profile-duration"
                  size="sm"
                  className="w-28"
                  data-testid="profile-duration"
                >
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {durations.map((value) => (
                    <SelectItem key={value} value={String(value)}>
                      {t('workers.profile.seconds', { count: value })}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Button
                variant="ghost"
                size="sm"
                className="ml-auto h-7 px-2"
                onClick={() => void fullscreen.toggle()}
                title={
                  fullscreen.active
                    ? t('workers.profile.exitFullscreen')
                    : t('workers.profile.fullscreen')
                }
                aria-label={
                  fullscreen.active
                    ? t('workers.profile.exitFullscreen')
                    : t('workers.profile.fullscreen')
                }
                data-testid="profile-fullscreen"
              >
                {fullscreen.active ? (
                  <Minimize2Icon className="size-4" />
                ) : (
                  <Maximize2Icon className="size-4" />
                )}
              </Button>
            </div>

            <div
              ref={frameRef}
              className={
                fullscreen.active
                  ? 'relative h-screen w-screen overflow-hidden bg-background'
                  : 'relative h-[70vh] overflow-hidden rounded-md border border-border bg-muted/20'
              }
              data-testid="profile-frame-container"
            >
              <iframe
                key={url}
                src={url}
                title={t('workers.profile.title')}
                className="size-full"
                onLoad={() => setLoaded(true)}
                data-testid="profile-frame"
              />
              {!loaded && (
                <div
                  className="absolute inset-0 flex flex-col items-center justify-center gap-3 bg-background/85"
                  role="status"
                  aria-live="polite"
                  data-testid="profile-loading"
                >
                  <Spinner className="size-6 text-primary" />
                  <p className="text-sm text-muted-foreground">
                    {t('workers.profile.sampling', { seconds: effectiveSeconds })}
                  </p>
                </div>
              )}
            </div>
            <a
              className="inline-block text-xs text-primary underline-offset-2 hover:underline"
              href={url}
              target="_blank"
              rel="noreferrer"
            >
              {t('workers.profile.openInNewTab')}
            </a>
          </div>
        ) : (
          <p className="text-sm text-muted-foreground">{t('workers.profile.noWebPort')}</p>
        )}
      </DialogContent>
    </Dialog>
  )
}
