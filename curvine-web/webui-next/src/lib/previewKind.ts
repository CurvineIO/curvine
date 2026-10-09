/**
 * How a file can be shown in the browser, and whether it is small enough to fetch.
 *
 * The download endpoint answers `application/octet-stream` for every file and sends no
 * size header, so the name is the only signal available for the format and the listing's
 * `len` is the only one for the size.
 */
export type PreviewKind = 'text' | 'image' | 'pdf' | 'audio' | 'video' | 'none'

/**
 * Refused above this. The endpoint buffers the whole file in memory before replying, so a
 * preview of a large file costs the server more than it costs the browser.
 */
export const MAX_PREVIEW_BYTES = 10 * 1024 * 1024

/** Text is read from a slice: a 9 MB log would otherwise become a 9 MB DOM text node. */
export const TEXT_PREVIEW_BYTES = 256 * 1024

const KINDS: Record<Exclude<PreviewKind, 'none'>, string[]> = {
  text: [
    'txt', 'text', 'log', 'md', 'markdown', 'rst', 'adoc', 'tex',
    'json', 'jsonl', 'ndjson', 'json5', 'csv', 'tsv',
    'yaml', 'yml', 'toml', 'ini', 'conf', 'cfg', 'properties', 'env', 'list',
    'xml', 'html', 'htm', 'xhtml', 'css', 'scss', 'sass', 'less',
    'js', 'mjs', 'cjs', 'jsx', 'ts', 'tsx', 'vue', 'svelte',
    'py', 'rb', 'go', 'rs', 'java', 'kt', 'kts', 'scala', 'groovy', 'lua', 'pl', 'r',
    'c', 'h', 'cc', 'cpp', 'cxx', 'hpp', 'hh', 'cs', 'swift', 'm', 'mm', 'dart', 'php',
    'sh', 'bash', 'zsh', 'fish', 'ps1', 'bat', 'cmd',
    'sql', 'graphql', 'gql', 'proto', 'tf', 'hcl',
    'diff', 'patch', 'lock', 'srt', 'vtt', 'pem', 'crt', 'pub', 'key', 'asc'
  ],
  image: ['png', 'jpg', 'jpeg', 'gif', 'webp', 'svg', 'bmp', 'ico', 'avif'],
  pdf: ['pdf'],
  audio: ['mp3', 'wav', 'ogg', 'oga', 'm4a', 'aac', 'flac', 'opus', 'weba'],
  video: ['mp4', 'm4v', 'webm', 'mov', 'mkv', 'avi', 'ogv']
}

/** Extension-less files that are text by name rather than by suffix. */
const TEXT_FILENAMES = [
  'makefile', 'dockerfile', 'license', 'licence', 'notice', 'readme', 'changelog',
  'authors', 'contributing', 'codeowners', 'gemfile', 'rakefile', 'procfile'
]

const BY_EXTENSION = new Map<string, PreviewKind>(
  (Object.entries(KINDS) as [Exclude<PreviewKind, 'none'>, string[]][]).flatMap(
    ([kind, extensions]) => extensions.map((extension) => [extension, kind] as const)
  )
)

function extensionOf(name: string): string {
  const base = String(name || '').trim().toLowerCase()
  const dot = base.lastIndexOf('.')
  // A leading dot is a hidden file name, not an extension (`.env`, `.gitignore`).
  if (dot <= 0 || dot === base.length - 1) return ''
  return base.slice(dot + 1)
}

export function previewKindFor(name: string): PreviewKind {
  const base = String(name || '').trim().toLowerCase()
  if (!base) return 'none'

  const extension = extensionOf(base)
  const byExtension = extension ? BY_EXTENSION.get(extension) : undefined
  if (byExtension) return byExtension

  // `.env`, `.gitignore`, `.npmrc`: a dotfile with no known extension is config text.
  // Checked after the extension, so `.logo.png` is still an image.
  if (base.length > 1 && base.startsWith('.')) return 'text'
  return TEXT_FILENAMES.includes(base) ? 'text' : 'none'
}

export type PreviewSizeVerdict = 'ok' | 'too-large' | 'unknown'

/**
 * What the fetched bytes should be labelled as.
 *
 * The download endpoint answers `application/octet-stream` for everything, and a blob with
 * that type is treated as an unknown download — which is why a PDF opened the browser's
 * download bar instead of rendering in the viewer. Labelling the blob by extension is what
 * makes the browser show it inline.
 */
const MIME_TYPES: Record<string, string> = {
  pdf: 'application/pdf',
  png: 'image/png',
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  gif: 'image/gif',
  webp: 'image/webp',
  svg: 'image/svg+xml',
  bmp: 'image/bmp',
  ico: 'image/x-icon',
  avif: 'image/avif',
  mp3: 'audio/mpeg',
  wav: 'audio/wav',
  ogg: 'audio/ogg',
  oga: 'audio/ogg',
  opus: 'audio/ogg',
  m4a: 'audio/mp4',
  aac: 'audio/aac',
  flac: 'audio/flac',
  weba: 'audio/webm',
  mp4: 'video/mp4',
  m4v: 'video/mp4',
  webm: 'video/webm',
  mov: 'video/quicktime',
  mkv: 'video/x-matroska',
  avi: 'video/x-msvideo',
  ogv: 'video/ogg'
}

/** `null` when the extension is unknown, in which case the bytes keep their own type. */
export function previewMimeTypeFor(name: string): string | null {
  const extension = extensionOf(String(name || '').toLowerCase())
  return extension ? MIME_TYPES[extension] ?? null : null
}

/** `unknown` is refused too: an unreadable size means an unbounded request. */
export function previewSizeVerdict(len: unknown): PreviewSizeVerdict {
  const size = Number(len)
  if (len === null || len === undefined || !Number.isFinite(size) || size < 0) return 'unknown'
  return size > MAX_PREVIEW_BYTES ? 'too-large' : 'ok'
}
