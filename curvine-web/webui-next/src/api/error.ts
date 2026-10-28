/** Error thrown for every failed API call; `status` is 0 for network/transport failures. */
export class ApiError extends Error {
  readonly status: number
  /**
   * Machine-readable code from the backend error envelope
   * (`not_removable`, `unknown_cluster`, …) when the response carried one.
   */
  readonly code?: string

  constructor(status: number, message: string, code?: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
  }
}

export function isUnauthorized(error: unknown): boolean {
  return error instanceof ApiError && error.status === 401
}
