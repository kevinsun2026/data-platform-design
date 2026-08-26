# web/

AIDP admin console — Next.js 14 (App Router) + TypeScript + Tailwind + shadcn/ui
+ Zustand. Replaces the Phase-1-monorepo stub with a real application:
login (`/login`), datasources list (`/datasources`), and the new-datasource
form (`/datasources/new`).

## Quick start

```bash
cd web
pnpm install
pnpm dev          # serves http://127.0.0.1:3000
```

The dev server proxies `/api/v1/*` to the gateway (default
`http://127.0.0.1:8000`). Override with `AIDP_GATEWAY_URL=http://…`
when running against a non-local stack.

## Scripts

| Script             | Purpose                                             |
|--------------------|-----------------------------------------------------|
| `pnpm dev`         | Next.js dev server on port 3000.                    |
| `pnpm build`       | Production build (`.next/`).                        |
| `pnpm start`       | Run the production build on port 3000.             |
| `pnpm lint`        | `next lint` over `app/`, `components/`, `lib/`, `tests/`. |
| `pnpm typecheck`   | `tsc --noEmit`.                                     |
| `pnpm test`        | Playwright e2e (`tests/e2e/*.spec.ts`).             |
| `pnpm format`      | Prettier (skip globs from the monorepo root).       |

## Layout

```
web/
├── app/                 # Next.js App Router
│   ├── (auth)/login/    # /login — sign-in form
│   ├── (console)/       # authenticated shell + datasources pages
│   ├── globals.css      # Tailwind base + shadcn tokens
│   ├── layout.tsx       # root layout
│   └── page.tsx         # dispatch to /login or /datasources
├── components/
│   ├── providers.tsx    # React Query client provider
│   └── ui/              # shadcn-style primitives
├── lib/
│   ├── api.ts           # Axios client + token + trace_id interceptors
│   ├── auth.ts          # Zustand auth store
│   ├── types.ts         # AppError envelope
│   └── utils.ts         # `cn` helper (clsx + tailwind-merge)
├── tests/e2e/           # Playwright specs
├── next.config.js       # /api/v1/* → gateway rewrite
├── tailwind.config.ts
├── tsconfig.json
└── playwright.config.ts
```

## Notes

- **shadcn/ui** primitives are hand-rolled under `components/ui/` so the
  build doesn't depend on `pnpm dlx shadcn init`. They cover the same
  variant slots as the canonical components — drop in a `pnpm dlx shadcn
  add …` later if you want the upstream versions.
- **Error envelope**: the API client surfaces the platform's
  `AppError { code, message, details, trace_id }` body as a typed
  `ApiError`. The console layout / forms display `message` and attach
  `trace_id` to retry buttons so on-call can grep server logs.
- **Token storage**: Zustand persists the session to `localStorage`
  (`aidp.auth.v1`). A 401 from any API call clears the token and the
  console layout bounces the user to `/login` on next render.
