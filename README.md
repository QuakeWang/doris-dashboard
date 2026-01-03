# Doris Dashboard

Doris Dashboard is a frontend-only, offline analysis tool for Doris diagnostic artifacts.
The current MVP focuses on FE audit logs (`fe.audit.log`).

Key features:

- Runs fully in the browser. No backend required; data stays local by default.
- Import `fe.audit.log` → parse → load into `duckdb-wasm` → aggregate with SQL → visualize (TopSQL, share chart, drill-down).
- CPU-heavy parsing/import/query runs in a Web Worker; the main thread focuses on rendering and interactions.

See `docs/architecture.md` and `docs/requirements.md` for design notes.

## Quick Start

Prerequisites:

- Node.js >= 18 (Node.js 20 recommended)

Start dev server:

```bash
cd apps/web
npm ci
npm run dev
```

Open the printed local URL (default: `http://localhost:12305`) and import a FE audit log.

Sample data: `data/fe.audit.log`.

## Build & Preview

```bash
cd apps/web
npm run build
npm run preview
```

Output directory: `apps/web/dist`.

## Lint, Type Check, Tests

```bash
cd apps/web
npm run lint
npm run typecheck
npm test
```

## Deployment Notes (COOP/COEP / crossOriginIsolated)

DuckDB WASM multithreading (pthreads) typically requires `crossOriginIsolated=true`.
In production, serve assets with COOP/COEP headers:

- `Cross-Origin-Opener-Policy: same-origin`
- `Cross-Origin-Embedder-Policy: require-corp`

Minimal Nginx example:

```nginx
location / {
  add_header Cross-Origin-Opener-Policy "same-origin" always;
  add_header Cross-Origin-Embedder-Policy "require-corp" always;

  root /var/www/doris-dashboard;
  try_files $uri $uri/ /index.html;
}
```

If you cannot set these headers, the app may still work but will fall back to a single-threaded bundle, and large imports/queries will be noticeably slower.

## Storage & Reset

The app prefers an OPFS-backed persistent DuckDB file (browser storage) and falls back to an in-memory database if OPFS is unavailable.

To reset local data, use "Clear site data" in your browser.

## CI

GitHub Actions workflow: `.github/workflows/ci.yml`. It runs:

- `npm ci`
- `npm run lint`
- `npm run typecheck`
- `npm test`
- `npm run build`
