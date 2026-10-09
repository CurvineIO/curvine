/**
 * Whether a form's current values differ from the snapshot it was opened with.
 *
 * Compares structurally rather than by reference: the form state is rebuilt on
 * every keystroke, so identity comparison would report every render as dirty.
 */
export function hasUnsavedChanges(initial: unknown, current: unknown): boolean {
  return JSON.stringify(initial) !== JSON.stringify(current)
}
