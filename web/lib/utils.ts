import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

/**
 * Tailwind-aware class-name concat.
 *
 * Mirrors the canonical shadcn helper so every component in
 * ``components/ui`` can pass its slot list through here and get
 * deterministic dedup (``px-2 px-4`` → ``px-4``) plus conditional
 * ``clsx`` semantics for free.
 */
export function cn(...inputs: ClassValue[]): string {
  return twMerge(clsx(inputs));
}
