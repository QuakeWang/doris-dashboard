import type { QueryFilters, TemplateSeriesResult } from "../../client/protocol";
import { ensureDb } from "../engine";
import { queryWithParams } from "../sql";
import { buildWhere, num, numOrNull, parseTemplateId, replyOk, toRows } from "./common";

const NICE_BUCKET_SECONDS = [
  60, 300, 900, 1800, 3600, 7200, 21600, 43200, 86400, 172800, 604800,
] as const;

const emptySeriesResult = (bucketMs: number): TemplateSeriesResult => ({
  bucketSeconds: Math.floor(bucketMs / 1000),
  bucketStarts: [],
  execCounts: [],
  totalCpuMs: [],
  totalTimeMs: [],
});

function pickNiceBucketMs(minBucketMs: number): number {
  const minBucketSeconds = Math.max(1, Math.ceil(minBucketMs / 1000));
  for (const s of NICE_BUCKET_SECONDS) {
    if (s >= minBucketSeconds) return s * 1000;
  }
  const days = Math.ceil(minBucketSeconds / 86400);
  return Math.max(1, days) * 86400 * 1000;
}

export async function handleQueryTemplateSeries(
  requestId: string,
  datasetId: string,
  templateHash: string,
  bucketSeconds: number,
  filters: QueryFilters
): Promise<void> {
  const c = await ensureDb();
  const where = buildWhere(datasetId, filters);
  const templateId = parseTemplateId(templateHash);
  let bucketMs = Math.max(1, Math.floor(bucketSeconds)) * 1000;

  const rangeRes = await queryWithParams(
    c,
    `SELECT min(event_time_ms) AS min_time_ms, max(event_time_ms) AS max_time_ms FROM audit_log_records WHERE ${where.whereSql} AND stripped_template_id = ? AND event_time_ms IS NOT NULL;`,
    [...where.params, templateId]
  );
  const rangeRow = toRows(rangeRes)[0] ?? null;
  const minMs = numOrNull(rangeRow?.min_time_ms);
  const maxMs = numOrNull(rangeRow?.max_time_ms);
  if (minMs == null || maxMs == null || !Number.isFinite(minMs) || !Number.isFinite(maxMs)) {
    replyOk(requestId, emptySeriesResult(bucketMs));
    return;
  }

  const startMs = filters.startMs != null ? filters.startMs : minMs;
  const endMsExclusive = filters.endMs != null ? filters.endMs : maxMs + 1;
  const MAX_BUCKETS = 2000;
  let startBucketMs = 0;
  let endBucketExclusiveMs = 0;
  while (true) {
    startBucketMs = Math.floor(startMs / bucketMs) * bucketMs;
    endBucketExclusiveMs = Math.ceil(endMsExclusive / bucketMs) * bucketMs;
    const bucketCount = Math.floor((endBucketExclusiveMs - startBucketMs) / bucketMs);
    if (bucketCount <= MAX_BUCKETS) break;
    bucketMs = pickNiceBucketMs(Math.ceil((endBucketExclusiveMs - startBucketMs) / MAX_BUCKETS));
  }

  const bucketStartsMs: number[] = [];
  for (let t = startBucketMs; t < endBucketExclusiveMs; t += bucketMs) bucketStartsMs.push(t);
  if (bucketStartsMs.length > MAX_BUCKETS) {
    replyOk(requestId, emptySeriesResult(bucketMs));
    return;
  }
  const bucketStarts = bucketStartsMs.map((t) => new Date(t).toISOString());

  const seriesRes = await queryWithParams(
    c,
    `SELECT floor(event_time_ms / ?) * ? AS bucket_ms, count(*) AS exec_count, sum(cpu_time_ms) AS total_cpu_ms, sum(query_time_ms) AS total_time_ms FROM audit_log_records WHERE ${where.whereSql} AND stripped_template_id = ? AND event_time_ms IS NOT NULL GROUP BY bucket_ms ORDER BY bucket_ms;`,
    [bucketMs, bucketMs, ...where.params, templateId]
  );

  const byBucket = new Map<
    number,
    { execCount: number; totalCpuMs: number; totalTimeMs: number }
  >();
  for (const r of toRows(seriesRes)) {
    const bucket = num(r.bucket_ms);
    byBucket.set(bucket, {
      execCount: num(r.exec_count),
      totalCpuMs: num(r.total_cpu_ms),
      totalTimeMs: num(r.total_time_ms),
    });
  }

  const result: TemplateSeriesResult = {
    bucketSeconds: Math.floor(bucketMs / 1000),
    bucketStarts,
    execCounts: bucketStartsMs.map((b) => byBucket.get(b)?.execCount ?? 0),
    totalCpuMs: bucketStartsMs.map((b) => byBucket.get(b)?.totalCpuMs ?? 0),
    totalTimeMs: bucketStartsMs.map((b) => byBucket.get(b)?.totalTimeMs ?? 0),
  };
  replyOk(requestId, result);
}
