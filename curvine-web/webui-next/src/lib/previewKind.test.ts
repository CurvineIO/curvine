import { describe, expect, it } from 'vitest'

import {
  MAX_PREVIEW_BYTES,
  TEXT_PREVIEW_BYTES,
  previewKindFor,
  previewMimeTypeFor,
  previewSizeVerdict
} from './previewKind'

describe('previewKindFor', () => {
  it('recognises the text family, including source and config', () => {
    for (const name of [
      'app.log',
      'notes.txt',
      'README.md',
      'data.json',
      'rows.csv',
      'grid.tsv',
      'conf.yaml',
      'conf.yml',
      'conf.toml',
      'app.ini',
      'main.rs',
      'index.tsx',
      'deploy.sh',
      'schema.sql',
      'Makefile',
      'Dockerfile'
    ]) {
      expect(previewKindFor(name), name).toBe('text')
    }
  })

  it('recognises images, PDFs and media', () => {
    expect(previewKindFor('logo.png')).toBe('image')
    expect(previewKindFor('photo.jpeg')).toBe('image')
    expect(previewKindFor('icon.svg')).toBe('image')
    expect(previewKindFor('report.pdf')).toBe('pdf')
    expect(previewKindFor('clip.mp3')).toBe('audio')
    expect(previewKindFor('clip.wav')).toBe('audio')
    expect(previewKindFor('demo.mp4')).toBe('video')
    expect(previewKindFor('demo.webm')).toBe('video')
  })

  it('ignores case and a leading dot', () => {
    expect(previewKindFor('README.MD')).toBe('text')
    expect(previewKindFor('PHOTO.PNG')).toBe('image')
    expect(previewKindFor('.env')).toBe('text')
    expect(previewKindFor('.gitignore')).toBe('text')
    // A dotfile with a known image extension is still an image.
    expect(previewKindFor('.logo.png')).toBe('image')
  })

  it('refuses names it cannot place, and the empty name', () => {
    expect(previewKindFor('notes')).toBe('none')
    expect(previewKindFor('dataset.bin')).toBe('none')
    expect(previewKindFor('archive.tar.gz')).toBe('none')
    expect(previewKindFor('trailing.')).toBe('none')
    expect(previewKindFor('')).toBe('none')
  })
})

describe('previewMimeTypeFor', () => {
  it('labels the bytes by extension, so the browser renders instead of downloading', () => {
    // The endpoint answers application/octet-stream for everything, and a blob with that
    // type is treated as an unknown download: a PDF went to the download bar.
    expect(previewMimeTypeFor('report.pdf')).toBe('application/pdf')
    expect(previewMimeTypeFor('PHOTO.PNG')).toBe('image/png')
    expect(previewMimeTypeFor('clip.mp4')).toBe('video/mp4')
    expect(previewMimeTypeFor('clip.mp3')).toBe('audio/mpeg')
    expect(previewMimeTypeFor('icon.svg')).toBe('image/svg+xml')
  })

  it('has no answer for names it cannot place', () => {
    expect(previewMimeTypeFor('archive.bin')).toBeNull()
    expect(previewMimeTypeFor('notes')).toBeNull()
  })
})

describe('previewSizeVerdict', () => {
  it('accepts anything up to the limit, including the limit itself', () => {
    expect(previewSizeVerdict(0)).toBe('ok')
    expect(previewSizeVerdict(1)).toBe('ok')
    expect(previewSizeVerdict(MAX_PREVIEW_BYTES)).toBe('ok')
  })

  it('refuses one byte over the limit', () => {
    expect(previewSizeVerdict(MAX_PREVIEW_BYTES + 1)).toBe('too-large')
  })

  it('treats an unreadable size as unknowable rather than small', () => {
    // An unknown size means an unbounded request, which the endpoint buffers whole.
    expect(previewSizeVerdict(undefined)).toBe('unknown')
    expect(previewSizeVerdict(null)).toBe('unknown')
    expect(previewSizeVerdict(Number.NaN)).toBe('unknown')
    expect(previewSizeVerdict('not a number')).toBe('unknown')
    expect(previewSizeVerdict(-1)).toBe('unknown')
  })

  it('keeps the text cap below the fetch limit', () => {
    expect(TEXT_PREVIEW_BYTES).toBeLessThan(MAX_PREVIEW_BYTES)
  })
})
