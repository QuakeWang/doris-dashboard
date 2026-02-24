import type { DimensionTopRow, QueryFilters } from "../../client/protocol";
import { reply } from "../messaging";

export type QueryResult = { toArray: () => unknown[] };
export type QueryRow = Record<string, unknown>;

export function replyOk(requestId: string, result: unknown): void {
  reply({ type: "response", requestId, ok: true, result });
}

export function toRows<T extends QueryRow = QueryRow>(res: QueryResult): T[] {
  return res.toArray() as T[];
}

export function num(value: unknown, fallback = 0): number {
  return value == null ? fallback : Number(value);
}

export function numOrNull(value: unknown): number | null {
  return value == null ? null : Number(value);
}

export function str(value: unknown, fallback = ""): string {
  return value == null ? fallback : String(value);
}

export function strOrNull(value: unknown): string | null {
  return value ? String(value) : null;
}

export function clampInt(n: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, Math.floor(n)));
}

export function parseTemplateId(templateHash: string): number {
  const templateId = Number.parseInt(templateHash, 10);
  if (!Number.isFinite(templateId)) throw new Error("Invalid template id");
  return templateId;
}

export function buildWhere(
  datasetId: string,
  filters: QueryFilters,
  tableAlias?: string
): { whereSql: string; params: unknown[] } {
  const col = (name: string) => (tableAlias ? `${tableAlias}.${name}` : name);
  const clauses = [`${col("dataset_id")} = ?`];
  const params: unknown[] = [datasetId];
  const strEq: Array<[string | undefined, string]> = [
    [filters.userName, "user_name"],
    [filters.dbName, "db_name"],
    [filters.clientIp, "client_ip"],
    [filters.state, "state"],
    [filters.cloudClusterName, "cloud_cluster_name"],
  ];
  for (const [v, name] of strEq) {
    const s = v?.trim();
    if (!s) continue;
    clauses.push(`${col(name)} = ?`);
    params.push(s);
  }
  if (filters.excludeInternal) clauses.push(`${col("is_internal")} IS NOT TRUE`);
  if (filters.startMs != null) {
    clauses.push(`${col("event_time_ms")} >= ?`);
    params.push(filters.startMs);
  }
  if (filters.endMs != null) {
    clauses.push(`${col("event_time_ms")} < ?`);
    params.push(filters.endMs);
  }
  return { whereSql: clauses.join(" AND "), params };
}

export function mapDimRows(res: QueryResult): DimensionTopRow[] {
  type DimAggRow = {
    name: unknown;
    exec_count: unknown;
    total_cpu_ms: unknown;
    total_time_ms: unknown;
  };
  return toRows<DimAggRow>(res).map((r) => ({
    name: str(r.name),
    execCount: num(r.exec_count),
    totalCpuMs: num(r.total_cpu_ms),
    totalTimeMs: num(r.total_time_ms),
  }));
}
