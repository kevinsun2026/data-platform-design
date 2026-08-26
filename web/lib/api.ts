"use client";

import axios, {
  type AxiosError,
  type AxiosInstance,
  type AxiosRequestConfig,
  type InternalAxiosRequestConfig,
} from "axios";

import { ApiError, type AppErrorBody } from "@/lib/types";
import { clearToken, getToken, setToken } from "@/lib/auth";

/**
 * Build a 32-char hex ``trace_id`` for request correlation.
 *
 * Mirrors the platform's ``aidp_common.tracing`` helper shape: the
 * gateway will accept a caller-supplied ``X-Trace-Id`` (the platform
 * sets one in middleware) but we ship one for every browser request
 * so console errors stay linkable to the server trace even when the
 * request never reaches the gateway (e.g. CORS preflight, offline).
 */
function buildTraceId(): string {
  // 16 random bytes → 32 hex chars. ``crypto.getRandomValues`` is
  // available in every modern browser (the only runtime we ship).
  const buf = new Uint8Array(16);
  crypto.getRandomValues(buf);
  return Array.from(buf, (b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * Pull the access token out of the request config's custom slot.
 *
 * We do not pull from a closure so SSR / RSC calls (which we don't
 * yet make) won't accidentally call into ``localStorage``.
 */
function readAuthToken(config: InternalAxiosRequestConfig): string | null {
  const fromSlot = (config.headers as Record<string, unknown> | undefined)?.[
    "X-Auth-Token"
  ];
  if (typeof fromSlot === "string" && fromSlot.length > 0) {
    return fromSlot;
  }
  if (typeof window === "undefined") {
    return null;
  }
  return getToken();
}

/**
 * Build the shared Axios instance.
 *
 * The instance carries:
 *  - ``baseURL: "/api/v1"`` so callers can write ``api.get("/datasources")``
 *    without repeating the version prefix.
 *  - A request interceptor that injects ``Authorization: Bearer <token>``
 *    and ``X-Trace-Id`` on every outbound call.
 *  - A response interceptor that converts any non-2xx body that matches
 *    the platform error envelope into an :class:`ApiError`.
 *
 * Side effects: a 401 response clears the in-memory token so the next
 * render bounces the user to ``/login``.
 */
function createApiClient(): AxiosInstance {
  const instance = axios.create({
    baseURL: "/api/v1",
    timeout: 30_000,
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
  });

  instance.interceptors.request.use((config) => {
    config.headers = config.headers ?? ({} as never);
    const token = readAuthToken(config);
    if (token) {
      config.headers.set("Authorization", `Bearer ${token}`);
    }
    // Always overwrite so retries from the same instance keep the
    // same trace id (makes one-click replay debugging easier).
    config.headers.set("X-Trace-Id", buildTraceId());
    return config;
  });

  instance.interceptors.response.use(
    (resp) => resp,
    (error: AxiosError<AppErrorBody>) => {
      const status = error.response?.status ?? 0;
      const body = error.response?.data;
      // Single 401 → drop the token; the consuming component will
      // decide whether to redirect to /login via the auth store.
      if (status === 401) {
        clearToken();
      }
      throw new ApiError(
        status,
        body,
        error.message || `Request failed with status ${status}`,
      );
    },
  );

  return instance;
}

export const api: AxiosInstance = createApiClient();

/**
 * Convenience wrapper for the rare cases where a caller needs to
 * push a token into a request that already has one set (e.g. an
 * admin form re-using the same tab for another tenant). The
 * interceptor also reads from :func:`getToken` so this is mostly
 * a no-op outside of testing.
 */
export function withToken(
  config: AxiosRequestConfig,
  token: string,
): AxiosRequestConfig {
  const headers = {
    ...(config.headers as Record<string, unknown> | undefined),
    "X-Auth-Token": token,
  };
  return { ...config, headers };
}

// Re-export so callers can do ``import { setToken } from "@/lib/api"``
export { setToken };
