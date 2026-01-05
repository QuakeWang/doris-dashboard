import { extractKvAndStmt } from "./kvExtractor";
import { normalizeClientIp, parseBoolOrNull, parseEventTimeMs, parseIntOrNull } from "./parseUtils";
import { normalizeSqlBase } from "./sqlTemplate";
import { getTemplateInfo } from "./templateInfo";

export interface ParsedAuditLogRecord {
  eventTimeMs: number | null;
  isInternal: boolean | null;
  queryId: string | null;
  userName: string | null;
  clientIp: string | null;
  feIp: string | null;
  dbName: string | null;
  state: string | null;
  errorCode: number | null;
  errorMessage: string | null;
  queryTimeMs: number | null;
  cpuTimeMs: number | null;
  scanBytes: number | null;
  scanRows: number | null;
  returnRows: number | null;
  peakMemoryBytes: number | null;
  workloadGroup: string | null;
  cloudClusterName: string | null;
  stmtRaw: string | null;
  sqlTemplateStripped: string | null;
  tableGuess: string | null;
}

const PREFIX_RE =
  /^[\uFEFF\s]*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:[,.]\d{3,6})?(?:Z|[+-]\d{2}:?\d{2})?)\s+(?:\[[^\]]+\])?\s*(.*)$/s;

function parseFirstTableFromQueriedTablesAndViews(v: string | undefined): string | null {
  if (!v) return null;
  const s = v.trim();
  if (!s) return null;
  try {
    const parsed = JSON.parse(s) as unknown;
    if (!Array.isArray(parsed)) return null;
    for (const item of parsed) {
      if (typeof item === "string") {
        const t = item.trim();
        if (t) return t;
      }
    }
    return null;
  } catch {
    return null;
  }
}

function pickFirstStrOrNull(kv: Record<string, string>, ...keys: string[]): string | null {
  for (const key of keys) {
    const v = kv[key];
    if (!v) continue;
    const s = v.trim();
    if (s) return s;
  }
  return null;
}

function pickFirstIntOrNull(kv: Record<string, string>, ...keys: string[]): number | null {
  for (const key of keys) {
    const n = parseIntOrNull(kv[key]);
    if (n != null) return n;
  }
  return null;
}

export function parseAuditLogRecordBlock(block: string): ParsedAuditLogRecord | null {
  const m = PREFIX_RE.exec(block);
  if (!m) return null;
  const prefixTimeMs = parseEventTimeMs(m[1]);
  const body = m[2] ?? "";

  const { kv, stmtRaw } = extractKvAndStmt(body);
  const kvLower: Record<string, string> = Object.create(null);
  for (const [key, val] of Object.entries(kv)) {
    kvLower[key.toLowerCase()] = val;
  }

  const tsMs = kvLower["timestamp"] ? parseEventTimeMs(kvLower["timestamp"]) : null;
  const eventTimeMs = tsMs ?? prefixTimeMs;
  const isInternal = parseBoolOrNull(kvLower["isinternal"] ?? kvLower["is_internal"]);
  const queryId = pickFirstStrOrNull(kvLower, "queryid", "query_id");
  const userName = pickFirstStrOrNull(kvLower, "user", "user_name");
  const clientIp = normalizeClientIp(pickFirstStrOrNull(kvLower, "client", "client_ip"));
  const feIp = pickFirstStrOrNull(kvLower, "feip", "frontendip", "fe_ip", "frontend_ip");
  const dbName = pickFirstStrOrNull(kvLower, "db", "db_name");
  const state = pickFirstStrOrNull(kvLower, "state");
  const errorCode = pickFirstIntOrNull(kvLower, "errorcode", "error_code");
  const errorMessage = pickFirstStrOrNull(kvLower, "errormessage", "error_message");
  const queryTimeMs = parseIntOrNull(kvLower["time(ms)"]);
  const cpuTimeMs = parseIntOrNull(kvLower["cputimems"]);
  const scanBytes = parseIntOrNull(kvLower["scanbytes"]);
  const scanRows = parseIntOrNull(kvLower["scanrows"]);
  const returnRows = parseIntOrNull(kvLower["returnrows"]);
  const peakMemoryBytes = parseIntOrNull(kvLower["peakmemorybytes"]);
  const workloadGroup = pickFirstStrOrNull(kvLower, "workloadgroup", "workload_group");
  const cloudClusterName = pickFirstStrOrNull(
    kvLower,
    "cloudclustername",
    "cloud_cluster_name",
    "computegroupname",
    "compute_group_name",
    "computegroup",
    "compute_group"
  );

  const baseTemplate = stmtRaw ? normalizeSqlBase(stmtRaw) : null;
  const info = baseTemplate ? getTemplateInfo(baseTemplate) : null;
  const strippedTemplate = info ? info.strippedTemplate : null;
  const queriedTablesAndViews = parseFirstTableFromQueriedTablesAndViews(
    kvLower["queriedtablesandviews"]
  );
  const sqlGuess = info ? info.tableGuess : null;
  const tableGuess =
    queriedTablesAndViews ??
    (sqlGuess ? (sqlGuess.includes(".") || !dbName ? sqlGuess : `${dbName}.${sqlGuess}`) : null);

  return {
    eventTimeMs,
    isInternal,
    queryId,
    userName,
    clientIp,
    feIp,
    dbName,
    state,
    errorCode,
    errorMessage,
    queryTimeMs,
    cpuTimeMs,
    scanBytes,
    scanRows,
    returnRows,
    peakMemoryBytes,
    workloadGroup,
    cloudClusterName,
    stmtRaw,
    sqlTemplateStripped: strippedTemplate,
    tableGuess,
  };
}
