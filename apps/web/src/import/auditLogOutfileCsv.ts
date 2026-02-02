import type { ParsedAuditLogRecord } from "./auditLogParser";
import { splitDelimitedLine } from "./delimited";
import { normalizeClientIp, parseEventTimeMs } from "./parseUtils";
import { normalizeSqlBase } from "./sqlTemplate";
import { getTemplateInfo } from "./templateInfo";

export const AUDIT_LOG_OUTFILE_COLS = 29;

export type AuditLogOutfileDelimiter = "\t" | ",";

export type OutfileColumnKey =
  | "query_id"
  | "time"
  | "client_ip"
  | "user_name"
  | "db_name"
  | "state"
  | "error_code"
  | "error_message"
  | "query_time_ms"
  | "scan_bytes"
  | "scan_rows"
  | "return_rows"
  | "fe_ip"
  | "cpu_time_ms"
  | "peak_memory_bytes"
  | "workload_group"
  | "cloud_cluster_name"
  | "stmt_raw";

export type OutfileColumnIndex = Partial<Record<OutfileColumnKey, number>>;

const OUTFILE_HEADER_ALIASES: Record<string, OutfileColumnKey> = {
  query_id: "query_id",
  time: "time",
  client_ip: "client_ip",
  user: "user_name",
  user_name: "user_name",
  db: "db_name",
  db_name: "db_name",
  state: "state",
  error_code: "error_code",
  error_message: "error_message",
  "time(ms)": "query_time_ms",
  time_ms: "query_time_ms",
  query_time: "query_time_ms",
  query_time_ms: "query_time_ms",
  scan_bytes: "scan_bytes",
  scan_rows: "scan_rows",
  return_rows: "return_rows",
  fe_ip: "fe_ip",
  frontend_ip: "fe_ip",
  cpu_time_ms: "cpu_time_ms",
  peak_memory_bytes: "peak_memory_bytes",
  workload_group: "workload_group",
  cloud_cluster_name: "cloud_cluster_name",
  compute_group_name: "cloud_cluster_name",
  compute_group: "cloud_cluster_name",
  stmt: "stmt_raw",
  stmt_raw: "stmt_raw",
};

const OUTFILE_HEADER_REQUIRED: OutfileColumnKey[] = ["query_id", "time", "client_ip"];

export function unescapeOutfileText(s: string): string {
  if (!s.includes("\\")) return s;
  let out = "";
  for (let i = 0; i < s.length; i++) {
    const ch = s[i] as string;
    if (ch !== "\\") {
      out += ch;
      continue;
    }
    const next = s[i + 1];
    if (next == null) {
      out += "\\";
      break;
    }
    i++;
    switch (next) {
      case "n":
        out += "\n";
        break;
      case "r":
        out += "\r";
        break;
      case "t":
        out += "\t";
        break;
      case "\\":
        out += "\\";
        break;
      default:
        out += `\\${next}`;
        break;
    }
  }
  return out;
}

function normalizeOutfileNullableText(v: string | undefined): string | null {
  if (v == null) return null;
  const s = v.trim();
  if (!s) return null;
  if (s === "\\N") return null;
  if (s.toLowerCase() === "null") return null;
  return unescapeOutfileText(s);
}

function parseOutfileInt(v: string | undefined): number | null {
  const s = normalizeOutfileNullableText(v);
  if (s == null) return null;
  const n = Number.parseInt(s, 10);
  return Number.isFinite(n) ? n : null;
}

export function detectOutfileDelimiter(line: string): AuditLogOutfileDelimiter | null {
  const candidates: AuditLogOutfileDelimiter[] = ["\t", ","];
  for (const delimiter of candidates) {
    const fields = splitDelimitedLine(line, delimiter);
    if (!fields) continue;
    if (tryParseOutfileHeader(fields)) return delimiter;
    if (fields.length !== AUDIT_LOG_OUTFILE_COLS) continue;
    return delimiter;
  }
  return null;
}

export function isOutfileHeader(fields: readonly string[]): boolean {
  return tryParseOutfileHeader(fields) != null;
}

function normalizeHeaderKey(v: string): OutfileColumnKey | null {
  const key = v.trim().toLowerCase();
  return OUTFILE_HEADER_ALIASES[key] ?? null;
}

function tryParseOutfileHeader(fields: readonly string[]): OutfileColumnIndex | null {
  if (fields.length < 3) return null;
  const index: OutfileColumnIndex = {};
  for (let i = 0; i < fields.length; i++) {
    const key = normalizeHeaderKey(fields[i] ?? "");
    if (!key || index[key] != null) continue;
    index[key] = i;
  }
  for (const key of OUTFILE_HEADER_REQUIRED) {
    if (index[key] == null) return null;
  }
  return index;
}

function pickField(
  fields: readonly string[],
  header: OutfileColumnIndex,
  key: OutfileColumnKey
): string | undefined {
  const idx = header[key];
  if (idx == null || idx < 0 || idx >= fields.length) return undefined;
  return fields[idx];
}

export type AuditLogOutfileLineResult =
  | { kind: "header"; header: OutfileColumnIndex }
  | { kind: "invalid" }
  | { kind: "record"; record: ParsedAuditLogRecord };

export function parseAuditLogOutfileLine(
  line: string,
  delimiter: AuditLogOutfileDelimiter,
  header?: OutfileColumnIndex | null
): AuditLogOutfileLineResult {
  const fields = splitDelimitedLine(line, delimiter);
  if (!fields) return { kind: "invalid" };
  if (!header) {
    const parsedHeader = tryParseOutfileHeader(fields);
    if (parsedHeader) return { kind: "header", header: parsedHeader };
    if (fields.length !== AUDIT_LOG_OUTFILE_COLS) return { kind: "invalid" };
  }

  const getField = header
    ? (key: OutfileColumnKey) => pickField(fields, header, key)
    : (key: OutfileColumnKey) => {
        switch (key) {
          case "query_id":
            return fields[0];
          case "time":
            return fields[1];
          case "client_ip":
            return fields[2];
          case "user_name":
            return fields[3];
          case "db_name":
            return fields[5];
          case "state":
            return fields[6];
          case "error_code":
            return fields[7];
          case "error_message":
            return fields[8];
          case "query_time_ms":
            return fields[9];
          case "scan_bytes":
            return fields[10];
          case "scan_rows":
            return fields[11];
          case "return_rows":
            return fields[12];
          case "fe_ip":
            return fields[21];
          case "cpu_time_ms":
            return fields[22];
          case "peak_memory_bytes":
            return fields[25];
          case "workload_group":
            return fields[26];
          case "cloud_cluster_name":
            return fields[27];
          case "stmt_raw":
            return fields[28];
          default:
            return undefined;
        }
      };

  const queryId = normalizeOutfileNullableText(getField("query_id"));
  const time = normalizeOutfileNullableText(getField("time"));
  const eventTimeMs = time ? parseEventTimeMs(time) : null;
  const clientIp = normalizeClientIp(normalizeOutfileNullableText(getField("client_ip")));
  const userName = normalizeOutfileNullableText(getField("user_name"));
  const dbName = normalizeOutfileNullableText(getField("db_name"));
  const state = normalizeOutfileNullableText(getField("state"));
  const errorCode = parseOutfileInt(getField("error_code"));
  const errorMessage = normalizeOutfileNullableText(getField("error_message"));
  const queryTimeMs = parseOutfileInt(getField("query_time_ms"));
  const scanBytes = parseOutfileInt(getField("scan_bytes"));
  const scanRows = parseOutfileInt(getField("scan_rows"));
  const returnRows = parseOutfileInt(getField("return_rows"));
  const feIp = normalizeOutfileNullableText(getField("fe_ip"));
  const cpuTimeMs = parseOutfileInt(getField("cpu_time_ms"));
  const peakMemoryBytes = parseOutfileInt(getField("peak_memory_bytes"));
  const workloadGroup = normalizeOutfileNullableText(getField("workload_group"));
  const cloudClusterName = normalizeOutfileNullableText(getField("cloud_cluster_name"));
  const stmtRaw = normalizeOutfileNullableText(getField("stmt_raw"));

  const baseTemplate = stmtRaw ? normalizeSqlBase(stmtRaw) : null;
  const info = baseTemplate ? getTemplateInfo(baseTemplate) : null;
  const strippedTemplate = info ? info.strippedTemplate : null;
  const sqlGuess = info ? info.tableGuess : null;
  const tableGuess = sqlGuess
    ? sqlGuess.includes(".") || !dbName
      ? sqlGuess
      : `${dbName}.${sqlGuess}`
    : null;

  return {
    kind: "record",
    record: {
      eventTimeMs,
      isInternal: null,
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
    },
  };
}
