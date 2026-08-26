/**
 * Canonical AppError envelope.
 *
 * Mirrors ``aidp_common.errors.AppError`` so the UI can decode a 4xx
 * / 5xx response without sniffing the HTTP status. The four fields
 * are the contract; the rest of the body is ignored.
 */
export interface AppErrorBody {
  code: string;
  message: string;
  details?: Record<string, unknown>;
  trace_id?: string;
}

/**
 * Error thrown by the API client when a request fails.
 *
 * Carries the parsed :class:`AppErrorBody` plus the originating
 * ``status`` so callers can branch on 401/403/404 without a string
 * match on the message.
 */
export class ApiError extends Error {
  public readonly status: number;
  public readonly code: string;
  public readonly details: Record<string, unknown> | undefined;
  public readonly traceId: string | undefined;

  constructor(
    status: number,
    body: Partial<AppErrorBody> | undefined,
    fallbackMessage: string,
  ) {
    super(body?.message ?? fallbackMessage);
    this.name = "ApiError";
    this.status = status;
    this.code = body?.code ?? "INTERNAL";
    this.details = body?.details;
    this.traceId = body?.trace_id;
  }
}
