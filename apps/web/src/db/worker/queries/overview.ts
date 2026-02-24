import type {
  DbTopRow,
  DimensionTopRow,
  OverviewResult,
  QueryFilters,
  TableTopRow,
} from "../../client/protocol";
import { ensureDb } from "../engine";
import { queryWithParams } from "../sql";
import { buildWhere, mapDimRows, num, numOrNull, replyOk, str, toRows } from "./common";

type OverviewAggRow = {
  records: unknown;
  total_cpu_ms: unknown;
  total_time_ms: unknown;
  p95_time_ms: unknown;
  p99_time_ms: unknown;
  approx_distinct_users: unknown;
  approx_distinct_dbs: unknown;
  approx_distinct_client_ips: unknown;
  approx_distinct_stripped_templates: unknown;
  failed_count: unknown;
  min_time_ms: unknown;
  max_time_ms: unknown;
};

type TopTableAggRow = {
  table_guess: unknown;
  exec_count: unknown;
  total_cpu_ms: unknown;
  total_time_ms: unknown;
};

export async function handleQueryOverview(
  requestId: string,
  datasetId: string,
  filters: QueryFilters
): Promise<void> {
  const c = await ensureDb();
  const where = buildWhere(datasetId, filters);
  const topN = 20;

  const overviewRes = await queryWithParams(
    c,
    `SELECT count(*) AS records, sum(cpu_time_ms) AS total_cpu_ms, sum(query_time_ms) AS total_time_ms, approx_quantile(query_time_ms, 0.95) AS p95_time_ms, approx_quantile(query_time_ms, 0.99) AS p99_time_ms, approx_count_distinct(user_name) AS approx_distinct_users, approx_count_distinct(db_name) AS approx_distinct_dbs, approx_count_distinct(client_ip) AS approx_distinct_client_ips, approx_count_distinct(stripped_template_id) AS approx_distinct_stripped_templates, sum(CASE WHEN error_code IS NOT NULL AND error_code <> 0 THEN 1 WHEN state IN ('EOF', 'OK') THEN 0 ELSE 1 END) AS failed_count, min(event_time_ms) AS min_time_ms, max(event_time_ms) AS max_time_ms FROM audit_log_records WHERE ${where.whereSql};`,
    where.params
  );
  const row = toRows<OverviewAggRow>(overviewRes)[0] ?? null;

  const queryTopDim = async (
    col: "db_name" | "user_name" | "client_ip",
    orderExpr: "total_cpu_ms" | "exec_count"
  ): Promise<DimensionTopRow[]> => {
    const res = await queryWithParams(
      c,
      `SELECT ${col} AS name, count(*) AS exec_count, sum(cpu_time_ms) AS total_cpu_ms, sum(query_time_ms) AS total_time_ms FROM audit_log_records WHERE ${where.whereSql} AND ${col} IS NOT NULL AND ${col} <> '' GROUP BY ${col} ORDER BY ${orderExpr} DESC LIMIT ?;`,
      [...where.params, topN]
    );
    return mapDimRows(res);
  };

  const toDbRows = (rows: DimensionTopRow[]): DbTopRow[] =>
    rows.map((r) => ({
      dbName: r.name,
      execCount: r.execCount,
      totalCpuMs: r.totalCpuMs,
      totalTimeMs: r.totalTimeMs,
    }));

  const topDbsByCpu = toDbRows(await queryTopDim("db_name", "total_cpu_ms"));
  const topUsersByCpu = await queryTopDim("user_name", "total_cpu_ms");
  const topClientIpsByCpu = await queryTopDim("client_ip", "total_cpu_ms");

  const whereR = buildWhere(datasetId, filters, "r");
  const queryTopTables = async (
    orderExpr: "total_cpu_ms" | "exec_count"
  ): Promise<TableTopRow[]> => {
    const res = await queryWithParams(
      c,
      `SELECT t.table_guess AS table_guess, count(*) AS exec_count, sum(r.cpu_time_ms) AS total_cpu_ms, sum(r.query_time_ms) AS total_time_ms FROM audit_log_records r JOIN audit_sql_templates_stripped t ON t.dataset_id = r.dataset_id AND t.stripped_template_id = r.stripped_template_id WHERE ${whereR.whereSql} AND t.table_guess IS NOT NULL AND t.table_guess <> '' GROUP BY t.table_guess ORDER BY ${orderExpr} DESC LIMIT ?;`,
      [...whereR.params, topN]
    );
    return toRows<TopTableAggRow>(res).map((r) => {
      const guess = str(r.table_guess);
      const parts = guess.split(".").filter(Boolean);
      const tableName = parts.length > 0 ? parts[parts.length - 1] : guess;
      const dbName = parts.length >= 2 ? parts[parts.length - 2] : "";
      return {
        dbName,
        tableName,
        execCount: num(r.exec_count),
        totalCpuMs: num(r.total_cpu_ms),
        totalTimeMs: num(r.total_time_ms),
      };
    });
  };

  const topTablesByCpu = await queryTopTables("total_cpu_ms");

  const result: OverviewResult = {
    records: num(row?.records),
    totalCpuMs: num(row?.total_cpu_ms),
    totalTimeMs: num(row?.total_time_ms),
    failedCount: num(row?.failed_count),
    p95TimeMs: numOrNull(row?.p95_time_ms),
    p99TimeMs: numOrNull(row?.p99_time_ms),
    minTimeMs: numOrNull(row?.min_time_ms),
    maxTimeMs: numOrNull(row?.max_time_ms),
    approxDistinctUsers: num(row?.approx_distinct_users),
    approxDistinctDbs: num(row?.approx_distinct_dbs),
    approxDistinctClientIps: num(row?.approx_distinct_client_ips),
    approxDistinctStrippedTemplates: num(row?.approx_distinct_stripped_templates),
    topDbsByCpu,
    topTablesByCpu,
    topUsersByCpu,
    topClientIpsByCpu,
  };

  replyOk(requestId, result);
}
