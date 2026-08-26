"use client";

import * as React from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

/**
 * Client-side provider tree.
 *
 * Kept separate from :file:`app/layout.tsx` so the root layout stays
 * a server component (smaller initial JS bundle, no need to mark
 * ``<html>`` as a client boundary).
 */
export function Providers({ children }: { children: React.ReactNode }) {
  // One QueryClient per browser tab; rebuilt only when the tab is
  // reloaded. Stale time of 30s keeps the list page snappy on
  // navigation without over-fetching when the user revisits.
  const [client] = React.useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            staleTime: 30_000,
            retry: 1,
            refetchOnWindowFocus: false,
          },
        },
      }),
  );

  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}
