export async function copyToClipboard(text: string) {
  const value = String(text || '').trim()
  if (!value || value === '-') {
    throw new Error('Nothing to copy')
  }

  if (navigator.clipboard?.writeText && window.isSecureContext) {
    await navigator.clipboard.writeText(value)
    return
  }

  const input = document.createElement('textarea')
  input.value = value
  input.setAttribute('readonly', '')
  input.style.position = 'fixed'
  input.style.left = '-9999px'
  input.style.top = '0'
  input.style.opacity = '0'
  document.body.appendChild(input)
  input.focus()
  input.select()
  input.setSelectionRange(0, value.length)
  const copied = document.execCommand('copy')
  document.body.removeChild(input)
  if (!copied) {
    throw new Error('Browser blocked clipboard access')
  }
}
