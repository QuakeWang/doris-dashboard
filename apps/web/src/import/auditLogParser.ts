import { extractKvAndStmt } from "./kvExtractor";
import { guessTableFromSqlTemplate, normalizeSqlBase } from "./sqlTemplate";
import { DEFAULT_COMPILED_STRIPPING_RULES, applyCompiledStrippingRules } from "./strippingRules";

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
  queryTimeMs: number | null;
  cpuTimeMs: number | null;
  scanBytes: number | null;
  scanRows: number | null;
  returnRows: number | null;
  peakMemoryBytes: number | null;
  stmtRaw: string | null;
  sqlTemplateStripped: string | null;
  tableGuess: string | null;
}

const PREFIX_RE =
  /^[\uFEFF\s]*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:[,.]\d{3,6})?(?:Z|[+-]\d{2}:?\d{2})?)\s+(?:\[[^\]]+\])?\s*(.*)$/s;
const TEMPLATE_INFO_CACHE_LIMIT = 50_000;
const templateInfoCache = new Map<
  string,
  { strippedTemplate: string; tableGuess: string | null }
>();

function parseIntOrNull(v: string | undefined): number | null {
  if (v == null || v === "") return null;
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) ? n : null;
}

function parseBoolOrNull(v: string | undefined): boolean | null {
  if (v == null) return null;
  const s = v.trim().toLowerCase();
  if (!s) return null;
  if (s === "true" || s === "t" || s === "1" || s === "yes" || s === "y") return true;
  if (s === "false" || s === "f" || s === "0" || s === "no" || s === "n") return false;
  return null;
}

function normalizeClientIp(v: string | undefined): string | null {
  if (v == null || v === "") return null;
  const s = v.trim();
  const idx = s.lastIndexOf(":");
  if (idx > 0) return s.slice(0, idx);
  return s;
}

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

function parseEventTimeMs(ts: string): number | null {
  const s = ts.trim();
  const m =
    /^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})(?:[,.](\d{3,6}))?(?:Z|([+-])(\d{2}):?(\d{2}))?$/.exec(
      s
    ) ?? null;
  if (!m) return null;
  const y = Number.parseInt(m[1], 10);
  const mo = Number.parseInt(m[2], 10);
  const d = Number.parseInt(m[3], 10);
  const h = Number.parseInt(m[4], 10);
  const mi = Number.parseInt(m[5], 10);
  const sec = Number.parseInt(m[6], 10);
  const frac = m[7];
  const ms = frac ? Number.parseInt(frac.slice(0, 3).padEnd(3, "0"), 10) : 0;
  if (![y, mo, d, h, mi, sec, ms].every(Number.isFinite)) return null;
  const hasZ = s.endsWith("Z");
  const sign = m[8];
  if (!sign && !hasZ) {
    const localMs = new Date(y, mo - 1, d, h, mi, sec, ms).getTime();
    return Number.isFinite(localMs) ? localMs : null;
  }
  const baseUtc = Date.UTC(y, mo - 1, d, h, mi, sec, ms);
  if (!sign) return baseUtc;
  const oh = Number.parseInt(m[9], 10);
  const om = Number.parseInt(m[10], 10);
  if (![oh, om].every(Number.isFinite)) return baseUtc;
  const offsetMs = (oh * 60 + om) * 60_000;
  return sign === "+" ? baseUtc - offsetMs : baseUtc + offsetMs;
}

function getTemplateInfo(baseTemplate: string): {
  strippedTemplate: string;
  tableGuess: string | null;
} {
  const cached = templateInfoCache.get(baseTemplate);
  if (cached) return cached;
  const strippedTemplate = applyCompiledStrippingRules(
    baseTemplate,
    DEFAULT_COMPILED_STRIPPING_RULES
  ).stripped;
  const tableGuess = guessTableFromSqlTemplate(baseTemplate);
  const next = { strippedTemplate, tableGuess };
  templateInfoCache.set(baseTemplate, next);
  if (templateInfoCache.size > TEMPLATE_INFO_CACHE_LIMIT) {
    const oldest = templateInfoCache.keys().next().value as string | undefined;
    if (oldest) templateInfoCache.delete(oldest);
  }
  return next;
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
  const isInternal = parseBoolOrNull(kvLower["isinternal"]);
  const queryId = kvLower["queryid"] ?? null;
  const userName = kvLower["user"] ?? null;
  const clientIp = normalizeClientIp(kvLower["client"]);
  const feIp = kvLower["feip"] ?? kvLower["frontendip"] ?? null;
  const dbName = kvLower["db"] ?? null;
  const state = kvLower["state"] ?? null;
  const errorCode = parseIntOrNull(kvLower["errorcode"]);
  const queryTimeMs = parseIntOrNull(kvLower["time(ms)"]);
  const cpuTimeMs = parseIntOrNull(kvLower["cputimems"]);
  const scanBytes = parseIntOrNull(kvLower["scanbytes"]);
  const scanRows = parseIntOrNull(kvLower["scanrows"]);
  const returnRows = parseIntOrNull(kvLower["returnrows"]);
  const peakMemoryBytes = parseIntOrNull(kvLower["peakmemorybytes"]);

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
    queryTimeMs,
    cpuTimeMs,
    scanBytes,
    scanRows,
    returnRows,
    peakMemoryBytes,
    stmtRaw,
    sqlTemplateStripped: strippedTemplate,
    tableGuess,
  };
}
