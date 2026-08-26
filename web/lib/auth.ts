"use client";

import { create } from "zustand";
import { persist, createJSONStorage } from "zustand/middleware";

/**
 * Auth store.
 *
 * The store keeps the access token, refresh token, and the current
 * user. Persistence is via ``localStorage`` under the
 * ``aidp.auth.v1`` key — deliberately namespaced + versioned so
 * future breaking changes bump the key and old sessions are dropped
 * cleanly.
 *
 * Why Zustand and not React Context? Two reasons:
 *  1. Selectors avoid re-rendering the entire tree on a single
 *     field change. With React Query, the form components only need
 *     the auth user; the token changes ripple through the Axios
 *     interceptor instead.
 *  2. The Axios interceptor in :mod:`@/lib/api` lives outside React
 *     and needs synchronous access to the token. A Zustand store
 *     exposes ``getToken`` / ``setToken`` as plain functions, which
 *     keeps the API client React-free.
 */

export interface AuthUser {
  id: string;
  tenant_id: string;
  username: string;
  email: string;
  display_name: string | null;
  status: string;
  mfa_enabled: boolean;
  roles: string[];
  scopes: string[];
  last_login_at: string | null;
  created_at: string;
}

export interface AuthTokens {
  access_token: string;
  refresh_token: string;
  token_type: "Bearer";
  expires_in: number;
}

interface AuthState {
  token: AuthTokens | null;
  user: AuthUser | null;
  setSession: (payload: { token: AuthTokens; user: AuthUser }) => void;
  setUser: (user: AuthUser) => void;
  clear: () => void;
}

/* ------------------------------------------------------------------
 * Token helpers — exported so the Axios interceptor can call them
 * without going through the React store API (no re-render cost).
 * ------------------------------------------------------------------ */

const TOKEN_STORAGE_KEY = "aidp.auth.v1";

let inMemoryToken: AuthTokens | null = null;

export function getToken(): string | null {
  if (inMemoryToken) {
    return inMemoryToken.access_token;
  }
  if (typeof window === "undefined") {
    return null;
  }
  try {
    const raw = window.localStorage.getItem(TOKEN_STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as { state?: { token?: AuthTokens | null } };
    inMemoryToken = parsed?.state?.token ?? null;
    return inMemoryToken?.access_token ?? null;
  } catch {
    return null;
  }
}

export function setToken(token: AuthTokens): void {
  inMemoryToken = token;
}

export function clearToken(): void {
  inMemoryToken = null;
  if (typeof window !== "undefined") {
    try {
      window.localStorage.removeItem(TOKEN_STORAGE_KEY);
    } catch {
      /* swallow — localStorage may be disabled */
    }
  }
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set) => ({
      token: null,
      user: null,
      setSession: ({ token, user }) => {
        inMemoryToken = token;
        set({ token, user });
      },
      setUser: (user) => set({ user }),
      clear: () => {
        inMemoryToken = null;
        set({ token: null, user: null });
      },
    }),
    {
      name: TOKEN_STORAGE_KEY,
      storage: createJSONStorage(() =>
        typeof window === "undefined"
          ? {
              getItem: () => null,
              setItem: () => undefined,
              removeItem: () => undefined,
            }
          : window.localStorage,
      ),
      // Only persist the user + token. The store API itself is not
      // serializable.
      partialize: (state) => ({
        token: state.token,
        user: state.user,
      }),
      onRehydrateStorage: () => (state) => {
        if (state?.token) {
          inMemoryToken = state.token;
        }
      },
    },
  ),
);
