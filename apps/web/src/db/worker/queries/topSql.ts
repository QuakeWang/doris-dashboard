import type { QueryFilters, TopSqlRow } from "../../client/protocol";
import { ensureDb } from "../engine";
import { queryWithParams } from "../sql";
import { buildWhere, clampInt, num, numOrNull, replyOk, str, strOrNull, toRows } from "./common";

export async function handleQueryTopSql(
  requestId: string,
  datasetId: string,
  topN: number,
  filters: QueryFilters
): Promise<void> {
  const c = await ensureDb();
  const where = buildWhere(datasetId, filters, "r");
  const safeTopN = clampInt(topN, 1, 1000);

  const res = await queryWithParams(
    c,
    `SELECT r.stripped_template_id AS template_id, any_value(t.sql_template_stripped) AS template, any_value(t.table_guess) AS table_guess, count(*) AS exec_count, sum(r.cpu_time_ms) AS total_cpu_ms, sum(r.query_time_ms) AS total_time_ms, avg(r.query_time_ms) AS avg_time_ms, max(r.query_time_ms) AS max_time_ms, approx_quantile(r.query_time_ms, 0.95) AS p95_time_ms FROM audit_log_records r JOIN audit_sql_templates_stripped t ON t.dataset_id = r.dataset_id AND t.stripped_template_id = r.stripped_template_id WHERE ${where.whereSql} GROUP BY r.stripped_template_id ORDER BY total_cpu_ms DESC LIMIT ?;`,
    [...where.params, safeTopN]
  );

  const rows: TopSqlRow[] = toRows(res).map((r) => ({
    templateHash: str(r.template_id),
    template: str(r.template),
    tableGuess: strOrNull(r.table_guess),
    execCount: num(r.exec_count),
    totalCpuMs: num(r.total_cpu_ms),
    totalTimeMs: num(r.total_time_ms),
    avgTimeMs: num(r.avg_time_ms),
    maxTimeMs: num(r.max_time_ms),
    p95TimeMs: numOrNull(r.p95_time_ms),
  }));

  replyOk(requestId, rows);
}
