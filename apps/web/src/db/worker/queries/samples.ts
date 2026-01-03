import type { QueryFilters, QuerySampleRow, SampleOrderBy } from "../../client/protocol";
import { ensureDb } from "../engine";
import { queryWithParams } from "../sql";
import {
  buildWhere,
  clampInt,
  num,
  numOrNull,
  parseTemplateId,
  replyOk,
  strOrNull,
  toRows,
} from "./common";

export async function handleQuerySamples(
  requestId: string,
  datasetId: string,
  templateHash: string,
  limit: number,
  orderBy: SampleOrderBy,
  filters: QueryFilters
): Promise<void> {
  const c = await ensureDb();
  const where = buildWhere(datasetId, filters);
  const orderExpr = orderBy === "cpuTimeMs" ? "cpu_time_ms" : "query_time_ms";
  const safeLimit = clampInt(limit, 1, 500);
  const templateId = parseTemplateId(templateHash);

  const res = await queryWithParams(
    c,
    `SELECT record_id, event_time_ms, query_id, user_name, db_name, client_ip, state, query_time_ms, cpu_time_ms, scan_bytes, scan_rows, return_rows, stmt_raw FROM audit_log_records WHERE ${where.whereSql} AND stripped_template_id = ? ORDER BY ${orderExpr} DESC NULLS LAST, event_time_ms DESC NULLS LAST, record_id DESC LIMIT ?;`,
    [...where.params, templateId, safeLimit]
  );

  const rows: QuerySampleRow[] = toRows(res).map((r) => {
    const eventTimeMs = numOrNull(r.event_time_ms);
    return {
      recordId: num(r.record_id),
      eventTimeMs,
      queryId: strOrNull(r.query_id),
      userName: strOrNull(r.user_name),
      dbName: strOrNull(r.db_name),
      clientIp: strOrNull(r.client_ip),
      state: strOrNull(r.state),
      queryTimeMs: numOrNull(r.query_time_ms),
      cpuTimeMs: numOrNull(r.cpu_time_ms),
      scanBytes: numOrNull(r.scan_bytes),
      scanRows: numOrNull(r.scan_rows),
      returnRows: numOrNull(r.return_rows),
      stmtRaw: strOrNull(r.stmt_raw),
    };
  });

  replyOk(requestId, rows);
}
