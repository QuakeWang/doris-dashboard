import * as duckdb from "@duckdb/duckdb-wasm";
import type { DuckDBBundles } from "@duckdb/duckdb-wasm";
import duckdbCoiPthreadWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-coi.pthread.worker.js?url";
import duckdbCoiWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-coi.worker.js?url";
import duckdbEhWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";
import duckdbMvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdbCoiWasm from "@duckdb/duckdb-wasm/dist/duckdb-coi.wasm?url";
import duckdbEhWasm from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import duckdbMvpWasm from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import { log } from "./messaging";
import { createSchema } from "./schema";

const DUCKDB_BUNDLES: DuckDBBundles = {
  mvp: { mainModule: duckdbMvpWasm, mainWorker: duckdbMvpWorker },
  eh: { mainModule: duckdbEhWasm, mainWorker: duckdbEhWorker },
  coi: {
    mainModule: duckdbCoiWasm,
    mainWorker: duckdbCoiWorker,
    pthreadWorker: duckdbCoiPthreadWorker,
  },
};

let db: duckdb.AsyncDuckDB | null = null;
let conn: duckdb.AsyncDuckDBConnection | null = null;
let connPromise: Promise<duckdb.AsyncDuckDBConnection> | null = null;

const SCRATCH_OPFS_BASENAME = "doris_dashboard_scratch";

const fallbackScratchId = crypto.randomUUID();
let tabSessionId: string | undefined;

export function setTabSessionId(value?: string): void {
  if (value) tabSessionId = value;
}

function getScratchOpfsName(): string {
  const raw = (tabSessionId ?? fallbackScratchId)
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .slice(0, 64);
  return raw ? `${SCRATCH_OPFS_BASENAME}_${raw}.duckdb` : `${SCRATCH_OPFS_BASENAME}.duckdb`;
}

export async function ensureDb(): Promise<duckdb.AsyncDuckDBConnection> {
  if (conn) return conn;
  if (connPromise) return await connPromise;
  connPromise = (async (): Promise<duckdb.AsyncDuckDBConnection> => {
    const bundle = await duckdb.selectBundle(DUCKDB_BUNDLES);
    const engineWorker = new Worker(bundle.mainWorker!, { type: "module" });
    const logger = new duckdb.ConsoleLogger();
    db = new duckdb.AsyncDuckDB(logger, engineWorker);
    await db.instantiate(bundle.mainModule!, bundle.pthreadWorker);

    const hc = typeof navigator !== "undefined" ? navigator.hardwareConcurrency : 4;
    const threads = Math.max(1, Math.min(8, Number.isFinite(hc) ? hc : 4));

    try {
      const opfsName = getScratchOpfsName();
      await db.registerOPFSFileName(opfsName);
      await db.dropFile(opfsName).catch(() => {});
      await db.open({
        path: opfsName,
        accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
        maximumThreads: threads,
      });
      log(`DuckDB storage: OPFS scratch (${opfsName})`);
    } catch (e) {
      log(`DuckDB storage fallback: in-memory (${e instanceof Error ? e.message : String(e)})`);
      await db.open({ maximumThreads: threads });
    }

    conn = await db.connect();
    await createSchema(conn);
    try {
      await conn.query(`PRAGMA threads=${threads}`);
      log(`DuckDB threads=${threads}`);
    } catch (e) {
      log(`PRAGMA threads skipped: ${e instanceof Error ? e.message : String(e)}`);
    }
    return conn;
  })();

  try {
    const c = await connPromise;
    return c;
  } catch (e) {
    connPromise = null;
    throw e;
  } finally {
    if (conn) connPromise = null;
  }
}
