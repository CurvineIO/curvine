import { useEffect, useState } from 'react'

import { useApi } from '@/api/api-context'
import { EmptyNotice, ErrorAlert } from '@/components/data-state'
import { Button } from '@/components/ui/button'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Skeleton } from '@/components/ui/skeleton'
import { useT } from '@/i18n'
import { formatBytes } from '@/lib/format'
import {
  MAX_PREVIEW_BYTES,
  TEXT_PREVIEW_BYTES,
  previewKindFor,
  previewMimeTypeFor,
  previewSizeVerdict
} from '@/lib/previewKind'
import type { BrowseRow } from './types'
import { PdfPreview } from './pdf-preview'
import { fileName } from './utils'

type PreviewState =
  | { status: 'loading' }
  | { status: 'error'; message: string }
  /** Text arrives as a string; PDFs keep their bytes; the rest get an object URL. */
  | {
      status: 'ready'
      text?: string
      url?: string
      blob?: Blob
      size: number
      truncated?: boolean
    }

/**
 * Preview of one file, as a drawer over the list.
 *
 * The size is checked from the listing before anything is fetched, because the download
 * endpoint buffers the whole file server-side — a preview must not ask for a gigabyte to
 * find out it cannot show it. Formats come from the name for the same reason: the endpoint
 * answers `application/octet-stream` for every file.
 */
export function PreviewSheet({
  open,
  item,
  onClose,
  onDownload,
  onShowBlocks
}: {
  open: boolean
  item: BrowseRow | null
  onClose(): void
  onDownload(item: BrowseRow): void
  onShowBlocks(item: BrowseRow): void
}) {
  const t = useT()
  const api = useApi()

  const path = String(item?.path || '')
  const name = fileName(path)
  const kind = previewKindFor(name)
  const verdict = previewSizeVerdict(item?.len)
  const previewable = kind !== 'none' && verdict === 'ok' && Boolean(path)

  const [state, setState] = useState<PreviewState>({ status: 'loading' })

  useEffect(() => {
    if (!open || !previewable) return
    let cancelled = false
    let createdUrl: string | null = null
    setState({ status: 'loading' })

    void (async () => {
      try {
        const blob = await api.downloadFile(path)
        if (cancelled) return
        if (kind === 'text') {
          // A slice, not the whole blob: a 9 MB log must not become a 9 MB text node.
          const text = await blob.slice(0, TEXT_PREVIEW_BYTES).text()
          if (cancelled) return
          setState({
            status: 'ready',
            text,
            size: blob.size,
            truncated: blob.size > TEXT_PREVIEW_BYTES
          })
          return
        }
        if (kind === 'pdf') {
          // The pages are drawn by us, so the bytes are handed over as they came.
          setState({ status: 'ready', blob, size: blob.size })
          return
        }
        createdUrl = URL.createObjectURL(renderableBlob(blob))
        if (cancelled) {
          URL.revokeObjectURL(createdUrl)
          return
        }
        setState({ status: 'ready', url: createdUrl, size: blob.size })
      } catch (error) {
        if (!cancelled) {
          setState({
            status: 'error',
            message: error instanceof Error ? error.message : ''
          })
        }
      }
    })()

    // Revoked on close and on unmount, so a previewed file does not pin its bytes.
    return () => {
      cancelled = true
      if (createdUrl) URL.revokeObjectURL(createdUrl)
    }
  }, [api, kind, open, path, previewable])

  const sizeText = Number.isFinite(Number(item?.len)) ? formatBytes(Number(item?.len)) : ''

  /**
   * The bytes as the browser should see them. Without a type, a blob is an unknown download,
   * so a PDF went to the download bar instead of the viewer — and the media elements had no
   * format to decode.
   */
  function renderableBlob(blob: Blob): Blob {
    const mime = previewMimeTypeFor(name)
    return mime ? blob.slice(0, blob.size, mime) : blob
  }

  function previewBody() {
    if (kind === 'none') {
      return (
        <div data-testid="browse-preview-unsupported">
          <EmptyNotice title={t('browse.preview.unsupported', { name })} />
        </div>
      )
    }
    if (verdict === 'too-large') {
      return (
        <div data-testid="browse-preview-too-large">
          <EmptyNotice
            title={t('browse.preview.tooLarge', { limit: formatBytes(MAX_PREVIEW_BYTES) })}
            description={t('browse.preview.tooLargeHint')}
          />
        </div>
      )
    }
    if (verdict === 'unknown') {
      return (
        <div data-testid="browse-preview-unknown-size">
          <EmptyNotice
            title={t('browse.preview.unknownSize')}
            description={t('browse.preview.unknownSizeHint')}
          />
        </div>
      )
    }
    if (state.status === 'loading') return <Skeleton className="h-64 w-full" />
    if (state.status === 'error') {
      return <ErrorAlert message={state.message || t('browse.preview.failed')} />
    }

    if (kind === 'text') {
      if (!state.text) {
        return (
          <div data-testid="browse-preview-empty">
            <EmptyNotice title={t('browse.preview.empty')} />
          </div>
        )
      }
      return (
        <div className="flex min-h-0 flex-1 flex-col gap-2">
          {state.truncated && (
            <p className="text-xs text-muted-foreground" data-testid="browse-preview-truncated">
              {t('browse.preview.truncated', { shown: formatBytes(TEXT_PREVIEW_BYTES) })}
            </p>
          )}
          <pre
            className="max-h-[70vh] min-h-0 flex-1 overflow-auto rounded-lg bg-muted p-3 font-mono text-xs whitespace-pre text-foreground"
            data-testid="browse-preview-text"
          >
            {state.text}
          </pre>
        </div>
      )
    }

    if (kind === 'image') {
      return (
        <div className="flex min-h-0 flex-1 items-center justify-center rounded-lg border border-border bg-muted/30 p-3">
          <img
            src={state.url}
            alt={t('browse.preview.imageAlt', { name })}
            className="max-h-[70vh] max-w-full object-contain"
            data-testid="browse-preview-image"
          />
        </div>
      )
    }

    if (kind === 'pdf') {
      // Ours, not the browser's viewer: that one paints its own dark canvas and follows the
      // browser's preference rather than the console's theme.
      return state.blob ? (
        <PdfPreview blob={state.blob} />
      ) : (
        <Skeleton className="h-64 w-full" />
      )
    }

    if (kind === 'audio') {
      return (
        <audio
          src={state.url}
          controls
          className="w-full"
          data-testid="browse-preview-audio"
        />
      )
    }

    return (
      <video
        src={state.url}
        controls
        className="max-h-[70vh] w-full rounded-lg border border-border bg-muted/30"
        data-testid="browse-preview-video"
      />
    )
  }

  return (
    <Sheet open={open} onOpenChange={(next) => !next && onClose()}>
      <SheetContent
        side="right"
        className="data-[side=right]:sm:max-w-3xl"
        data-testid="browse-preview-sheet"
      >
        <SheetHeader>
          <SheetTitle className="truncate" title={name}>
            {name}
          </SheetTitle>
          <SheetDescription className="flex flex-wrap items-center gap-x-2 truncate" title={path}>
            <span className="truncate">{path}</span>
            {sizeText && <span className="shrink-0 tabular-nums">· {sizeText}</span>}
          </SheetDescription>
        </SheetHeader>

        <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-auto px-4 pb-4">{previewBody()}</div>

        <div className="flex items-center justify-end gap-2 border-t border-border px-4 py-3">
          {item && kind !== 'none' && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => onShowBlocks(item)}
              data-testid="browse-preview-blocks"
            >
              {t('common.blocks')}
            </Button>
          )}
          <Button
            variant="ghost"
            size="sm"
            onClick={onClose}
            data-testid="browse-preview-close"
          >
            {t('common.close')}
          </Button>
          {item && (
            <Button size="sm" onClick={() => onDownload(item)} data-testid="browse-preview-download">
              {t('browse.preview.downloadFile')}
            </Button>
          )}
        </div>
      </SheetContent>
    </Sheet>
  )
}
