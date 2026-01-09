import type { QueryFilters, ShareRankBy, ShareRow } from "../../client/protocol";
import { ensureDb } from "../engine";
import { queryWithParams } from "../sql";
import { buildWhere, clampInt, num, numOrNull, replyOk, str, toRows } from "./common";

export async function handleQueryShare(
  requestId: string,
  datasetId: string,
  topN: number,
  rankBy: ShareRankBy,
  filters: QueryFilters
): Promise<void> {
  const c = await ensureDb();
  const whereR = buildWhere(datasetId, filters, "r");
  const where = buildWhere(datasetId, filters);
  const orderExpr =
    rankBy === "maxPeakMemBytes"
      ? "max_peak_mem_bytes"
      : rankBy === "totalTimeMs"
        ? "total_time_ms"
        : "total_cpu_ms";
  const safeTopN = clampInt(topN, 1, 1000);

  const totalRes = await queryWithParams(
    c,
    `SELECT count(*) AS records, sum(cpu_time_ms) AS total_cpu_ms, sum(query_time_ms) AS total_time_ms FROM audit_log_records WHERE ${where.whereSql};`,
    where.params
  );
  const totalRow = toRows(totalRes)[0] ?? null;
  const totalRecords = num(totalRow?.records);
  const totalCpuMs = num(totalRow?.total_cpu_ms);
  const totalTimeMs = num(totalRow?.total_time_ms);
  if (!Number.isFinite(totalRecords) || totalRecords <= 0) {
    replyOk(requestId, [] satisfies ShareRow[]);
    return;
  }

  const topRes = await queryWithParams(
    c,
    `SELECT r.stripped_template_id AS template_id, any_value(t.sql_template_stripped) AS template, count(*) AS exec_count, sum(r.cpu_time_ms) AS total_cpu_ms, sum(r.query_time_ms) AS total_time_ms, max(r.peak_memory_bytes) AS max_peak_mem_bytes FROM audit_log_records r JOIN audit_sql_templates_stripped t ON t.dataset_id = r.dataset_id AND t.stripped_template_id = r.stripped_template_id WHERE ${whereR.whereSql} GROUP BY r.stripped_template_id ORDER BY ${orderExpr} DESC NULLS LAST LIMIT ?;`,
    [...whereR.params, safeTopN]
  );

  const rows: ShareRow[] = [];
  let sumTopCpu = 0;
  let sumTopTime = 0;
  let sumTopCount = 0;
  for (const r of toRows(topRes)) {
    const cpu = num(r.total_cpu_ms);
    const time = num(r.total_time_ms);
    const count = num(r.exec_count);
    sumTopCpu += cpu;
    sumTopTime += time;
    sumTopCount += count;
    rows.push({
      templateHash: str(r.template_id),
      template: str(r.template),
      execCount: count,
      totalCpuMs: cpu,
      totalTimeMs: time,
      cpuShare: totalCpuMs > 0 ? cpu / totalCpuMs : 0,
      timeShare: totalTimeMs > 0 ? time / totalTimeMs : 0,
      maxPeakMemBytes: numOrNull(r.max_peak_mem_bytes),
      isOthers: false,
    });
  }

  const othersCpu = Math.max(0, totalCpuMs - sumTopCpu);
  const othersTime = Math.max(0, totalTimeMs - sumTopTime);
  const othersCount = Math.max(0, totalRecords - sumTopCount);
  rows.push({
    templateHash: "__others__",
    template: "Others",
    execCount: othersCount,
    totalCpuMs: othersCpu,
    totalTimeMs: othersTime,
    cpuShare: totalCpuMs > 0 ? othersCpu / totalCpuMs : 0,
    timeShare: totalTimeMs > 0 ? othersTime / totalTimeMs : 0,
    maxPeakMemBytes: null,
    isOthers: true,
  });

  replyOk(requestId, rows);
}
