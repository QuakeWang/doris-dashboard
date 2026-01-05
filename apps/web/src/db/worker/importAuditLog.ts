import * as arrow from "apache-arrow";
import {
  type AuditLogOutfileDelimiter,
  detectOutfileDelimiter,
  parseAuditLogOutfileLine,
} from "../../import/auditLogOutfileCsv";
import { parseAuditLogRecordBlock } from "../../import/auditLogParser";
import { iterateLines, iterateRecordBlocks } from "../../import/recordReader";
import { ensureDb } from "./engine";
import { log, reply } from "./messaging";
import { createAuditLogIndexes, dropAuditLogIndexes } from "./schema";
import { insertArrowViaStaging, queryWithParams } from "./sql";

let abortController: AbortController | null = null;

const MAX_STMT_RAW_CHARS = 4096;
const BATCH_SIZE = 5000;
const MAX_BATCH_BYTES = 8 * 1024 * 1024;
const LARGE_FILE_CHUNKED_TXN_THRESHOLD_BYTES = 256 * 1024 * 1024;
const MIN_ARROW_INSERT_ROWS = 200;

type AuditLogInputFormat = "feAuditLog" | "auditLogOutfileCsv";

async function detectAuditLogInputFormat(file: File): Promise<{
  format: AuditLogInputFormat;
  outfileDelimiter?: AuditLogOutfileDelimiter;
}> {
  const sample = await file.slice(0, 256 * 1024).text();
  if (sample.includes("|Stmt=") || sample.includes("|QueryId=") || sample.includes("|Time(ms)=")) {
    return { format: "feAuditLog" };
  }

  const firstLine =
    sample
      .split(/\r?\n/)
      .find((l) => l.trim().length > 0)
      ?.trim() ?? null;
  if (firstLine) {
    const delimiter = detectOutfileDelimiter(firstLine);
    if (delimiter) return { format: "auditLogOutfileCsv", outfileDelimiter: delimiter };
  }

  return { format: "feAuditLog" };
}

const estimateUtf16Bytes = (v: string | null | undefined): number =>
  typeof v === "string" ? v.length * 2 : 0;

const estimateRecordBytes = (
  parsed: ReturnType<typeof parseAuditLogRecordBlock>,
  stmtRaw: string | null
): number => {
  if (!parsed) return 0;
  return (
    estimateUtf16Bytes(stmtRaw) +
    estimateUtf16Bytes(parsed.queryId) +
    estimateUtf16Bytes(parsed.userName) +
    estimateUtf16Bytes(parsed.clientIp) +
    estimateUtf16Bytes(parsed.feIp) +
    estimateUtf16Bytes(parsed.dbName) +
    estimateUtf16Bytes(parsed.state) +
    estimateUtf16Bytes(parsed.errorMessage) +
    estimateUtf16Bytes(parsed.workloadGroup) +
    estimateUtf16Bytes(parsed.cloudClusterName) +
    256
  );
};

const RECORD_COLS =
  "dataset_id,record_id,event_time_ms,is_internal,query_id,user_name,client_ip,fe_ip,db_name,state,error_code,error_message,query_time_ms,cpu_time_ms,scan_bytes,scan_rows,return_rows,peak_memory_bytes,workload_group,cloud_cluster_name,stmt_raw,stripped_template_id".split(
    ","
  ) as readonly string[];
const RECORD_INSERT_SQL = `INSERT INTO audit_log_records (${RECORD_COLS.join(", ")}) VALUES (${RECORD_COLS.map(() => "?").join(", ")})`;

const STRIPPED_TPL_COLS = "dataset_id,stripped_template_id,sql_template_stripped,table_guess".split(
  ","
) as readonly string[];
const STRIPPED_TPL_INSERT_SQL = `INSERT OR IGNORE INTO audit_sql_templates_stripped (${STRIPPED_TPL_COLS.join(", ")}) VALUES (${STRIPPED_TPL_COLS.map(() => "?").join(", ")})`;

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
  const insertTplStmt = await c.prepare(STRIPPED_TPL_INSERT_SQL);

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

  const useSingleTransaction = bytesTotal <= LARGE_FILE_CHUNKED_TXN_THRESHOLD_BYTES;
  if (!useSingleTransaction) {
    log(
      `Large file detected (${Math.round(bytesTotal / (1024 * 1024))} MiB), switching to chunked transactions to reduce wasm memory pressure.`
    );
  }

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

  const insertRecordRange = async (start: number, end: number): Promise<number> => {
    const count = end - start;
    if (count <= 0) return 0;

    try {
      const arrays = recordArrays.map((a) => a.slice(start, end));
      await insertArrowViaStaging(
        c,
        "audit_log_records",
        tableFromCols(RECORD_COLS, arrays),
        "insert"
      );
      return count;
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      log(
        `Arrow insert failed for audit_log_records (rows=${count}), retry smaller chunks: ${message}`
      );
      if (count <= MIN_ARROW_INSERT_ROWS) {
        let inserted = 0;
        for (let r = start; r < end; r++) {
          if (signal.aborted) throw new DOMException("Aborted", "AbortError");
          await insertStmt.query(...(recordArrays.map((a) => a[r]) as any[]));
          inserted++;
        }
        return inserted;
      }
      const mid = start + Math.floor(count / 2);
      const left = await insertRecordRange(start, mid);
      const right = await insertRecordRange(mid, end);
      return left + right;
    }
  };

  const flushRecords = async (): Promise<void> => {
    const rowCount = recordArrays[0].length;
    if (rowCount === 0) return;
    const inserted = await insertRecordRange(0, rowCount);
    recordsInserted += inserted;
    clearArrays(recordArrays);
    batchBytes = 0;
  };

  const insertTemplateRange = async (start: number, end: number): Promise<void> => {
    const count = end - start;
    if (count <= 0) return;
    try {
      const arrays = strippedTplArrays.map((a) => a.slice(start, end));
      await insertArrowViaStaging(
        c,
        "audit_sql_templates_stripped",
        tableFromCols(STRIPPED_TPL_COLS, arrays),
        "insertOrIgnore"
      );
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e);
      log(
        `Arrow insert failed for audit_sql_templates_stripped (rows=${count}), retry smaller chunks: ${message}`
      );
      if (count <= MIN_ARROW_INSERT_ROWS) {
        for (let r = start; r < end; r++) {
          if (signal.aborted) throw new DOMException("Aborted", "AbortError");
          await insertTplStmt.query(...(strippedTplArrays.map((a) => a[r]) as any[]));
        }
        return;
      }
      const mid = start + Math.floor(count / 2);
      await insertTemplateRange(start, mid);
      await insertTemplateRange(mid, end);
    }
  };

  const flushTemplates = async (): Promise<void> => {
    const rowCount = strippedTplArrays[0].length;
    if (rowCount === 0) return;
    await insertTemplateRange(0, rowCount);
    clearArrays(strippedTplArrays);
  };

  const flushBatch = async (): Promise<void> => {
    if (recordArrays[0].length === 0 && strippedTplArrays[0].length === 0) return;
    if (!useSingleTransaction) await c.query("BEGIN TRANSACTION");
    try {
      await flushTemplates();
      await flushRecords();
      if (!useSingleTransaction) await c.query("COMMIT");
    } catch (e) {
      if (!useSingleTransaction) await c.query("ROLLBACK");
      throw e;
    }
  };

  if (useSingleTransaction) await c.query("BEGIN TRANSACTION");
  try {
    const input = await detectAuditLogInputFormat(file);
    const onProgress = (p: { bytesRead: number }) => {
      bytesRead = p.bytesRead;
      maybeEmitProgress();
    };

    const processParsed = async (
      parsed: ReturnType<typeof parseAuditLogRecordBlock>,
      rawStmt: string | null
    ): Promise<void> => {
      if (!parsed?.sqlTemplateStripped) {
        badRecords++;
        return;
      }

      const stmtRaw =
        rawStmt && rawStmt.length > MAX_STMT_RAW_CHARS
          ? `${rawStmt.slice(0, MAX_STMT_RAW_CHARS)} ...[truncated]`
          : rawStmt;

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
        parsed.errorMessage,
        parsed.queryTimeMs,
        parsed.cpuTimeMs,
        parsed.scanBytes,
        parsed.scanRows,
        parsed.returnRows,
        parsed.peakMemoryBytes,
        parsed.workloadGroup,
        parsed.cloudClusterName,
        stmtRaw,
        strippedId,
      ]);

      batchBytes += estimateRecordBytes(parsed, stmtRaw);
      if (recordArrays[0].length >= BATCH_SIZE || batchBytes >= MAX_BATCH_BYTES) {
        await flushBatch();
        maybeEmitProgress(true);
      }
    };

    if (input.format === "auditLogOutfileCsv") {
      const delimiter = input.outfileDelimiter ?? "\t";
      for await (const line of iterateLines(file, { signal, onProgress })) {
        if (signal.aborted) throw new DOMException("Aborted", "AbortError");
        if (!line.trim()) continue;
        const res = parseAuditLogOutfileLine(line, delimiter);
        if (res.kind === "header") continue;
        recordsParsed++;
        if (res.kind === "invalid") {
          badRecords++;
          continue;
        }
        await processParsed(res.record, res.record.stmtRaw);
      }
    } else {
      for await (const block of iterateRecordBlocks(file, { signal, onProgress })) {
        if (signal.aborted) throw new DOMException("Aborted", "AbortError");
        const parsed = parseAuditLogRecordBlock(block.raw);
        recordsParsed++;
        await processParsed(parsed, parsed?.stmtRaw ?? null);
      }
    }
    await flushBatch();

    if (useSingleTransaction) await c.query("COMMIT");
    await createAuditLogIndexes(c);
  } catch (e) {
    if (useSingleTransaction) await c.query("ROLLBACK");
    if (!useSingleTransaction) {
      try {
        log("Import failed, cleaning up partially committed data for this dataset.");
        for (const table of ["audit_log_records", "audit_sql_templates_stripped"]) {
          await queryWithParams(c, `DELETE FROM ${table} WHERE dataset_id = ?`, [datasetId]);
        }
        await createAuditLogIndexes(c);
      } catch (cleanupErr) {
        log(
          `Cleanup after failed import skipped: ${
            cleanupErr instanceof Error ? cleanupErr.message : String(cleanupErr)
          }`
        );
      }
    }
    throw e;
  } finally {
    await insertStmt.close();
    await insertTplStmt.close();
    maybeEmitProgress(true);
  }

  reply({ type: "response", requestId, ok: true, result: { recordsInserted, badRecords } });
}
