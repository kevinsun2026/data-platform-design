"use client";

import * as React from "react";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { LogOut } from "lucide-react";

import { useAuthStore } from "@/lib/auth";
import { Button } from "@/components/ui/button";

/**
 * Authenticated console shell.
 *
 * Wraps every page under :file:`app/(console)/` with a top bar and
 * a guard that bounces the user to ``/login`` if no session is
 * present. The guard is intentionally client-side only — the
 * server is the source of truth and any state-mutating API call
 * would 401 without a valid token, but a quick bounce keeps the
 * UX honest.
 */
export default function ConsoleLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const user = useAuthStore((s) => s.user);
  const clear = useAuthStore((s) => s.clear);

  React.useEffect(() => {
    if (!user) {
      router.replace("/login");
    }
  }, [user, router]);

  const handleLogout = () => {
    clear();
    router.replace("/login");
  };

  if (!user) {
    return null;
  }

  return (
    <div className="flex min-h-screen flex-col">
      <header className="sticky top-0 z-30 border-b bg-background">
        <div className="container flex h-14 items-center justify-between">
          <div className="flex items-center gap-6">
            <Link
              href="/datasources"
              className="text-lg font-semibold tracking-tight"
            >
              AIDP
            </Link>
            <nav className="flex items-center gap-4 text-sm text-muted-foreground">
              <Link
                href="/datasources"
                className={
                  pathname?.startsWith("/datasources")
                    ? "font-medium text-foreground"
                    : "hover:text-foreground"
                }
              >
                Datasources
              </Link>
            </nav>
          </div>
          <div className="flex items-center gap-3 text-sm">
            <span className="text-muted-foreground">
              {user.display_name || user.email}
            </span>
            <Button
              type="button"
              variant="ghost"
              size="sm"
              onClick={handleLogout}
              aria-label="Sign out"
            >
              <LogOut className="mr-1.5 h-4 w-4" />
              Sign out
            </Button>
          </div>
        </div>
      </header>
      <main className="container flex-1 py-8">{children}</main>
    </div>
  );
}
