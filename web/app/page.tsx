"use client";

import * as React from "react";
import { useRouter } from "next/navigation";

import { useAuthStore } from "@/lib/auth";

/**
 * Root page — just dispatches the user to the right place.
 *
 * The brief lists ``/login`` and ``/datasources`` as the two real
 * entry points; we keep ``/`` as a tiny dispatcher so the dev
 * server's default URL doesn't 404.
 */
export default function HomePage() {
  const router = useRouter();
  const user = useAuthStore((s) => s.user);

  React.useEffect(() => {
    router.replace(user ? "/datasources" : "/login");
  }, [user, router]);

  return (
    <main className="flex min-h-screen items-center justify-center">
      <p className="text-sm text-muted-foreground">Loading…</p>
    </main>
  );
}
