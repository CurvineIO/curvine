import { useCallback, useEffect, useRef, useState } from 'react'
import { ChevronLeftIcon, ChevronRightIcon } from 'lucide-react'
import type { PDFDocumentLoadingTask, PDFDocumentProxy, PageViewport } from 'pdfjs-dist'

import { ErrorAlert } from '@/components/data-state'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Spinner } from '@/components/ui/spinner'
import { useT } from '@/i18n'
import { cn } from '@/lib/utils'

/** A link annotation, positioned as a percentage of the page, like pdf.js's own layer. */
interface PageLink {
  key: string
  style: { left: string; top: string; width: string; height: string }
  /** External targets open in a new tab; internal ones move to another page. */
  url?: string
  targetPage?: number
  title: string
}

/**
 * Renders PDF pages ourselves rather than handing the blob to the browser's viewer.
 *
 * The built-in viewer paints its own dark-grey canvas around the page and follows the
 * browser's preference instead of the app's theme — measured at 87% of the frame, and
 * unchanged by `color-scheme` on the frame, on its parent, or in a wrapper document, and
 * barely moved by the PDF open parameters. Drawing the pages means the surface is ours:
 * light paper on a light panel, inverted paper in dark mode, and the same result in every
 * browser.
 *
 * On top of the canvas sits pdf.js's text layer, so the page's text can be selected and
 * copied, and a layer of link annotations, so links in the document are clickable.
 *
 * `pdfjs-dist` is imported on demand so it never reaches the initial bundle and only loads
 * for someone actually looking at a PDF.
 */
async function loadPdfJs() {
  const [pdfjs, worker] = await Promise.all([
    import('pdfjs-dist'),
    import('pdfjs-dist/build/pdf.worker.min.mjs?url')
  ])
  pdfjs.GlobalWorkerOptions.workerSrc = worker.default
  return pdfjs
}

/**
 * Annotation rectangles are in PDF space (y from the bottom), and the layer in the official
 * viewer places them as percentages of the page. Copying that convention means the boxes
 * follow the canvas at any zoom, without duplicating the viewport's scale maths.
 */
export function linkStyle(viewport: { rawDims: unknown }, rect: number[], view: number[]) {
  const normalized = [
    rect[0],
    view[3] - rect[1] + view[1],
    rect[2],
    view[3] - rect[3] + view[1]
  ]
  const left = Math.min(normalized[0], normalized[2])
  const top = Math.min(normalized[1], normalized[3])
  const width = Math.abs(normalized[2] - normalized[0])
  const height = Math.abs(normalized[3] - normalized[1])
  // Typed as `Object` in pdf.js's own declarations.
  const { pageWidth, pageHeight, pageX, pageY } = viewport.rawDims as {
    pageWidth: number
    pageHeight: number
    pageX: number
    pageY: number
  }
  return {
    left: `${(100 * (left - pageX)) / pageWidth}%`,
    top: `${(100 * (top - pageY)) / pageHeight}%`,
    width: `${(100 * width) / pageWidth}%`,
    height: `${(100 * height) / pageHeight}%`
  }
}

export function PdfPreview({ blob, className }: { blob: Blob; className?: string }) {
  const t = useT()
  const canvasRef = useRef<HTMLCanvasElement | null>(null)
  const textRef = useRef<HTMLDivElement | null>(null)
  const surfaceRef = useRef<HTMLDivElement | null>(null)
  const [document, setDocument] = useState<PDFDocumentProxy | null>(null)
  const [links, setLinks] = useState<PageLink[]>([])
  const [page, setPage] = useState(1)
  const [failed, setFailed] = useState('')
  const [width, setWidth] = useState(0)
  const [rendering, setRendering] = useState(true)

  useEffect(() => {
    const node = surfaceRef.current
    if (!node) return
    const measure = () => setWidth(Math.round(node.getBoundingClientRect().width))
    measure()
    const observer = new ResizeObserver(measure)
    observer.observe(node)
    return () => observer.disconnect()
  }, [])

  // Load the document once per blob, and tear it down with the sheet. `destroy()` lives on
  // the loading task, not on the document proxy.
  useEffect(() => {
    let cancelled = false
    let task: PDFDocumentLoadingTask | null = null
    setFailed('')
    setPage(1)

    void (async () => {
      try {
        const pdfjs = await loadPdfJs()
        const data = new Uint8Array(await blob.arrayBuffer())
        task = pdfjs.getDocument({ data })
        const loaded = await task.promise
        if (cancelled) {
          void task.destroy()
          return
        }
        setDocument(loaded)
      } catch (error) {
        if (!cancelled) {
          setDocument(null)
          setFailed(error instanceof Error ? error.message : '')
        }
      }
    })()

    return () => {
      cancelled = true
      setDocument(null)
      void task?.destroy()
    }
  }, [blob])

  /** Internal links carry a destination; resolve it to a page number up front. */
  const resolveLinks = useCallback(
    async (
      target: Awaited<ReturnType<PDFDocumentProxy['getPage']>>,
      source: PDFDocumentProxy,
      viewport: PageViewport
    ) => {
      const annotations = await target.getAnnotations()
      const view = (target.view as number[]) || [0, 0, 1, 1]
      const resolved: PageLink[] = []
      for (const [index, annotation] of annotations.entries()) {
        if (annotation.subtype !== 'Link') continue
        const rect = annotation.rect
        if (!Array.isArray(rect)) continue
        const style = linkStyle(viewport, rect as number[], view)
        const url = annotation.url || annotation.unsafeUrl
        if (url) {
          resolved.push({ key: `link-${index}`, style, url: String(url), title: String(url) })
          continue
        }
        // A destination is `[pageRef, {name}|number, ...]`; only page jumps are handled.
        const dest = annotation.dest
        if (Array.isArray(dest) && dest.length > 0) {
          try {
            const pageIndex = await source.getPageIndex(dest[0])
            if (Number.isFinite(pageIndex)) {
              resolved.push({
                key: `dest-${index}`,
                style,
                targetPage: pageIndex + 1,
                title: t('browse.preview.goToPage', { page: pageIndex + 1 })
              })
            }
          } catch {
            // A named or malformed destination is not worth failing the page for.
          }
        }
      }
      return resolved
    },
    [t]
  )

  // Draw the current page, then lay the text and link layers over it.
  useEffect(() => {
    const canvas = canvasRef.current
    if (!document || !canvas || width <= 0) return
    let cancelled = false
    let textLayer: { render(): Promise<unknown>; cancel(): void } | null = null
    setRendering(true)

    void (async () => {
      try {
        const target = await document.getPage(page)
        if (cancelled) return
        const unscaled = target.getViewport({ scale: 1 })
        const scale = width / unscaled.width
        const viewport = target.getViewport({ scale })
        const ratio = Math.min(window.devicePixelRatio || 1, 2)
        const context = canvas.getContext('2d')
        if (!context) return
        canvas.width = Math.floor(viewport.width * ratio)
        canvas.height = Math.floor(viewport.height * ratio)
        canvas.style.width = `${Math.floor(viewport.width)}px`
        canvas.style.height = `${Math.floor(viewport.height)}px`
        await target.render({
          canvas,
          canvasContext: context,
          viewport,
          transform: ratio === 1 ? undefined : [ratio, 0, 0, ratio, 0, 0]
        }).promise
        if (cancelled) return

        const surface = textRef.current
        if (surface) {
          surface.replaceChildren()
          // `--total-scale-factor` is what the layer's font maths scales against.
          surface.style.setProperty('--total-scale-factor', String(viewport.scale))
          surface.style.width = `${Math.floor(viewport.width)}px`
          surface.style.height = `${Math.floor(viewport.height)}px`
          const pdfjs = await loadPdfJs()
          if (cancelled) return
          textLayer = new pdfjs.TextLayer({
            textContentSource: await target.getTextContent(),
            container: surface,
            viewport
          })
          await textLayer.render()
          if (cancelled) return
        }

        const resolved = await resolveLinks(target, document, viewport)
        if (!cancelled) setLinks(resolved)
      } catch (error) {
        if (!cancelled) setFailed(error instanceof Error ? error.message : '')
      } finally {
        if (!cancelled) setRendering(false)
      }
    })()

    return () => {
      cancelled = true
      textLayer?.cancel()
    }
  }, [document, page, width, resolveLinks])

  const go = useCallback(
    (step: number) =>
      setPage((current) => Math.min(Math.max(current + step, 1), document?.numPages || 1)),
    [document]
  )

  if (failed) {
    return (
      <div className={className} data-testid="browse-preview-pdf">
        <ErrorAlert message={failed} />
      </div>
    )
  }

  return (
    <div className={cn('flex min-h-0 flex-1 flex-col gap-2', className)}>
      <div
        ref={surfaceRef}
        data-testid="browse-preview-pdf"
        className="flex min-h-[40vh] flex-1 justify-center overflow-auto rounded-lg border border-border bg-muted/30 p-3"
      >
        {document ? (
          <div className="relative">
            {/* `dark:invert` turns the paper dark in dark mode; the hue rotation keeps
                colours from turning into their complements. The text layer is a sibling so
                it is not inverted with the paper — it is transparent anyway — and the link
                boxes sit above both. */}
            <canvas
              ref={canvasRef}
              data-testid="browse-preview-pdf-canvas"
              className="rounded-sm shadow-sm dark:invert dark:hue-rotate-180"
            />
            <div ref={textRef} className="pdf-text-surface" data-testid="browse-preview-pdf-text" />
            {links.map((link) =>
              link.url ? (
                <a
                  key={link.key}
                  href={link.url}
                  target="_blank"
                  rel="noreferrer"
                  title={link.title}
                  data-testid="browse-preview-pdf-link"
                  className="absolute rounded-sm transition-colors hover:bg-primary/15 focus-visible:bg-primary/15 focus-visible:ring-[3px] focus-visible:ring-ring/30 focus-visible:outline-none"
                  style={link.style}
                />
              ) : (
                <button
                  key={link.key}
                  type="button"
                  title={link.title}
                  data-testid="browse-preview-pdf-destination"
                  onClick={() => link.targetPage && setPage(link.targetPage)}
                  className="absolute cursor-pointer rounded-sm transition-colors hover:bg-primary/15 focus-visible:bg-primary/15 focus-visible:ring-[3px] focus-visible:ring-ring/30 focus-visible:outline-none"
                  style={link.style}
                />
              )
            )}
            {rendering && (
              <div className="absolute inset-0 grid place-items-center bg-muted/40">
                <Spinner className="size-5" />
              </div>
            )}
          </div>
        ) : (
          <div className="w-full max-w-md self-center">
            <Skeleton className="h-[60vh] w-full rounded-lg" />
          </div>
        )}
      </div>

      {document && document.numPages > 1 && (
        <div className="flex items-center justify-end gap-2">
          <Button
            variant="ghost"
            size="xs"
            disabled={page <= 1}
            onClick={() => go(-1)}
            data-testid="browse-preview-pdf-prev"
          >
            <ChevronLeftIcon />
            {t('browse.preview.previousPage')}
          </Button>
          <span className="text-xs tabular-nums text-muted-foreground">
            {t('browse.preview.pageOf', { page, total: document.numPages })}
          </span>
          <Button
            variant="ghost"
            size="xs"
            disabled={page >= document.numPages}
            onClick={() => go(1)}
            data-testid="browse-preview-pdf-next"
          >
            {t('browse.preview.nextPage')}
            <ChevronRightIcon />
          </Button>
        </div>
      )}
    </div>
  )
}
