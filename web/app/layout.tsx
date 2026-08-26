import type { Metadata } from "next";
import * as React from "react";

import { Providers } from "@/components/providers";

import "./globals.css";

export const metadata: Metadata = {
  title: {
    default: "AIDP Console",
    template: "%s · AIDP Console",
  },
  description:
    "AIDP admin console — manage datasources, agents, and policies.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className="min-h-screen bg-background font-sans antialiased">
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
