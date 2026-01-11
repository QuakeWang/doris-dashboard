# Doris Dashboard

Doris Dashboard is a browser-first analysis UI for Doris audit logs. Parsing/importing/aggregation run locally in the browser via `duckdb-wasm` + Web Worker.

- Offline import: supports `fe.audit.log` (FE native log) and `__internal_schema.audit_log` [OUTFILE CSV](https://doris.apache.org/docs/3.x/data-operate/export/outfile/) exports.
- Online import (optional): import `__internal_schema.audit_log` directly from a running Doris cluster (via `agentd`).

## Features

- Runs locally in your browser; data stays on your machine by default.
- Offline import via drag-and-drop.
- Online import via `agentd` (export TSV from Doris, then import locally).
- Visualizations: TopSQL / Share / Template drill-down.

## Quick Start

### 1 Start Web (offline analysis)

Prerequisites:

- Node.js >= 18 (Node.js 20 recommended)

```bash
cd apps/web
npm ci
npm run dev
```

Open the printed local URL (default: `http://localhost:12305`), then drag-and-drop `fe.audit.log`
or OUTFILE CSV/TSV in `Import Audit Log`.

### 2 Optional: start agentd (online import)

Prerequisites:

- Go >= 1.21

Start `agentd`:

```bash
cd apps/agentd
mkdir -p .gocache
GOCACHE=$(pwd)/.gocache go run . --listen 127.0.0.1:12306 --export-timeout 60s
```

Then in the Web UI, click `Doris` to save the connection and click `Import from Doris` to import.

## Configuration

- Dev proxy: `apps/web/vite.config.ts` proxies `/api/*` to `http://127.0.0.1:12306`.
- Without proxy: set `VITE_AGENT_BASE_URL` (e.g. `http://127.0.0.1:12306`).
  - Note: `VITE_*` env vars are build-time variables (baked into the bundle). If you change it, rebuild/restart the dev server.

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

## Storage & Reset

The app prefers an OPFS-backed persistent DuckDB file (browser storage) and falls back to an in-memory database if OPFS is unavailable.

To reset local data, use "Clear site data" in your browser.
