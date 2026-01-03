import type { DimensionKey, DimensionRankBy, QueryFilters } from "../../client/protocol";
import { ensureDb } from "../engine";
import { queryWithParams } from "../sql";
import { buildWhere, clampInt, mapDimRows, parseTemplateId, replyOk } from "./common";

export async function handleQueryDimensionTop(
  requestId: string,
  datasetId: string,
  templateHash: string,
  dimension: DimensionKey,
  topN: number,
  rankBy: DimensionRankBy,
  filters: QueryFilters
): Promise<void> {
  const c = await ensureDb();
  const where = buildWhere(datasetId, filters);
  const templateId = parseTemplateId(templateHash);
  const dimCol =
    dimension === "dbName" ? "db_name" : dimension === "clientIp" ? "client_ip" : "user_name";
  const orderExpr =
    rankBy === "execCount"
      ? "exec_count"
      : rankBy === "totalTimeMs"
        ? "total_time_ms"
        : "total_cpu_ms";
  const safeTopN = clampInt(topN, 1, 50);

  const res = await queryWithParams(
    c,
    `SELECT ${dimCol} AS name, count(*) AS exec_count, sum(cpu_time_ms) AS total_cpu_ms, sum(query_time_ms) AS total_time_ms FROM audit_log_records WHERE ${where.whereSql} AND stripped_template_id = ? AND ${dimCol} IS NOT NULL AND ${dimCol} <> '' GROUP BY ${dimCol} ORDER BY ${orderExpr} DESC LIMIT ?;`,
    [...where.params, templateId, safeTopN]
  );

  replyOk(requestId, mapDimRows(res));
}
