import * as arrow from "apache-arrow";
import { parseAuditLogRecordBlock } from "../../import/auditLogParser";
import { iterateRecordBlocks } from "../../import/recordReader";
import { ensureDb } from "./engine";
import { log, reply } from "./messaging";
import { createAuditLogIndexes, dropAuditLogIndexes } from "./schema";
import { insertArrowViaStaging, queryWithParams } from "./sql";

let abortController: AbortController | null = null;

const MAX_STMT_RAW_CHARS = 4096;
const BATCH_SIZE = 10000;
const MAX_BATCH_BYTES = 16 * 1024 * 1024;

const RECORD_COLS =
  "dataset_id,record_id,event_time_ms,is_internal,query_id,user_name,client_ip,fe_ip,db_name,state,error_code,query_time_ms,cpu_time_ms,scan_bytes,scan_rows,return_rows,peak_memory_bytes,stmt_raw,stripped_template_id".split(
    ","
  ) as readonly string[];
const RECORD_INSERT_SQL = `INSERT INTO audit_log_records (${RECORD_COLS.join(", ")}) VALUES (${RECORD_COLS.map(() => "?").join(", ")})`;

const STRIPPED_TPL_COLS = "dataset_id,stripped_template_id,sql_template_stripped,table_guess".split(
  ","
) as readonly string[];

const pushRow = (arrays: unknown[][], values: unknown[]) =>
  values.forEach((v, i) => arrays[i].push(v));
const clearArrays = (arrays: unknown[][]) => {
  for (const a of arrays) a.length = 0;
};
const tableFromCols = (cols: readonly string[], arrays: unknown[][]) =>
  arrow.tableFromArrays(
    Object.fromEntries(cols.map((c, i) => [c, arrays[i]])) as Record<string, unknown[]>
  );

const getOrCreateId = (
  map: Map<string, number>,
  key: string,
  next: () => number,
  onNew: (id: number) => void
): number => {
  const existing = map.get(key);
  if (existing != null) return existing;
  const id = next();
  map.set(key, id);
  onNew(id);
  return id;
};

export async function handleCancel(requestId: string): Promise<void> {
  abortController?.abort();
  abortController = null;
  reply({ type: "response", requestId, ok: true, result: { ok: true } });
}

export async function handleImportAuditLog(
  requestId: string,
  datasetId: string,
  file: File
): Promise<void> {
  const c = await ensureDb();

  abortController?.abort();
  abortController = new AbortController();
  const signal = abortController.signal;

  await dropAuditLogIndexes(c);

  for (const table of ["audit_log_records", "audit_sql_templates_stripped"])
    await queryWithParams(c, `DELETE FROM ${table} WHERE dataset_id = ?`, [datasetId]);

  const insertStmt = await c.prepare(RECORD_INSERT_SQL);

  const recordArrays = RECORD_COLS.map(() => [] as unknown[]);
  const strippedTplArrays = STRIPPED_TPL_COLS.map(() => [] as unknown[]);

  const strippedTemplateToId = new Map<string, number>();
  let nextStrippedTemplateId = 1;

  const bytesTotal = file.size;
  let bytesRead = 0;
  let recordsParsed = 0;
  let recordsInserted = 0;
  let badRecords = 0;
  let recordId = 0;

  let batchBytes = 0;

  let lastProgressEmitMs = 0;
  const maybeEmitProgress = (force = false) => {
    const now = performance.now();
    if (!force && now - lastProgressEmitMs < 200) return;
    lastProgressEmitMs = now;
    reply({
      type: "event",
      event: {
        type: "importProgress",
        requestId,
        progress: { bytesRead, bytesTotal, recordsParsed, recordsInserted, badRecords },
      },
    });
  };

  const flush = async (): Promise<void> => {
    const rowCount = recordArrays[0].length;
    if (rowCount === 0) return;
    try {
      await insertArrowViaStaging(
        c,
        "audit_log_records",
        tableFromCols(RECORD_COLS, recordArrays),
        "insert"
      );
      recordsInserted += rowCount;
    } catch (e) {
      log(
        `Arrow batch insert failed, fallback to row inserts: ${e instanceof Error ? e.message : String(e)}`
      );
      for (let r = 0; r < rowCount; r++) {
        if (signal.aborted) throw new DOMException("Aborted", "AbortError");
        await insertStmt.query(...(recordArrays.map((a) => a[r]) as any[]));
        recordsInserted++;
      }
    }
    clearArrays(recordArrays);
    batchBytes = 0;
  };

  const flushTemplates = async (): Promise<void> => {
    if (strippedTplArrays[0].length === 0) return;
    await insertArrowViaStaging(
      c,
      "audit_sql_templates_stripped",
      tableFromCols(STRIPPED_TPL_COLS, strippedTplArrays),
      "insertOrIgnore"
    );
    clearArrays(strippedTplArrays);
  };

  await c.query("BEGIN TRANSACTION");
  try {
    for await (const block of iterateRecordBlocks(file, {
      signal,
      onProgress: (p) => {
        bytesRead = p.bytesRead;
        maybeEmitProgress();
      },
    })) {
      if (signal.aborted) throw new DOMException("Aborted", "AbortError");
      const parsed = parseAuditLogRecordBlock(block.raw);
      recordsParsed++;
      if (!parsed?.sqlTemplateStripped) {
        badRecords++;
        continue;
      }
      const stmtRaw =
        parsed.stmtRaw && parsed.stmtRaw.length > MAX_STMT_RAW_CHARS
          ? `${parsed.stmtRaw.slice(0, MAX_STMT_RAW_CHARS)} ...[truncated]`
          : parsed.stmtRaw;

      const strippedTemplate = parsed.sqlTemplateStripped;
      const strippedId = getOrCreateId(
        strippedTemplateToId,
        strippedTemplate,
        () => nextStrippedTemplateId++,
        (id) => pushRow(strippedTplArrays, [datasetId, id, strippedTemplate, parsed.tableGuess])
      );
      pushRow(recordArrays, [
        datasetId,
        recordId++,
        parsed.eventTimeMs,
        parsed.isInternal,
        parsed.queryId,
        parsed.userName,
        parsed.clientIp,
        parsed.feIp,
        parsed.dbName,
        parsed.state,
        parsed.errorCode,
        parsed.queryTimeMs,
        parsed.cpuTimeMs,
        parsed.scanBytes,
        parsed.scanRows,
        parsed.returnRows,
        parsed.peakMemoryBytes,
        stmtRaw,
        strippedId,
      ]);

      batchBytes +=
        (typeof stmtRaw === "string" ? stmtRaw.length * 2 : 0) +
        (parsed.queryId ? parsed.queryId.length * 2 : 0);

      if (recordArrays[0].length >= BATCH_SIZE || batchBytes >= MAX_BATCH_BYTES) {
        await flushTemplates();
        await flush();
        maybeEmitProgress(true);
      }
    }
    await flushTemplates();
    await flush();

    await c.query("COMMIT");
    await createAuditLogIndexes(c);
  } catch (e) {
    await c.query("ROLLBACK");
    throw e;
  } finally {
    await insertStmt.close();
    maybeEmitProgress(true);
  }

  reply({ type: "response", requestId, ok: true, result: { recordsInserted, badRecords } });
}
