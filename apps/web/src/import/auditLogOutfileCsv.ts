import type { ParsedAuditLogRecord } from "./auditLogParser";
import { splitDelimitedLine } from "./delimited";
import { normalizeClientIp, parseEventTimeMs } from "./parseUtils";
import { normalizeSqlBase } from "./sqlTemplate";
import { getTemplateInfo } from "./templateInfo";

export const AUDIT_LOG_OUTFILE_COLS = 29;

export type AuditLogOutfileDelimiter = "\t" | ",";

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
    if (fields.length !== AUDIT_LOG_OUTFILE_COLS) continue;
    return delimiter;
  }
  return null;
}

export function isOutfileHeader(fields: readonly string[]): boolean {
  if (fields.length !== AUDIT_LOG_OUTFILE_COLS) return false;
  const h0 = fields[0]?.trim().toLowerCase();
  const h1 = fields[1]?.trim().toLowerCase();
  const h2 = fields[2]?.trim().toLowerCase();
  return h0 === "query_id" && h1 === "time" && h2 === "client_ip";
}

export type AuditLogOutfileLineResult =
  | { kind: "header" }
  | { kind: "invalid" }
  | { kind: "record"; record: ParsedAuditLogRecord };

export function parseAuditLogOutfileLine(
  line: string,
  delimiter: AuditLogOutfileDelimiter
): AuditLogOutfileLineResult {
  const fields = splitDelimitedLine(line, delimiter);
  if (!fields) return { kind: "invalid" };
  if (fields.length !== AUDIT_LOG_OUTFILE_COLS) return { kind: "invalid" };
  if (isOutfileHeader(fields)) return { kind: "header" };

  const queryId = normalizeOutfileNullableText(fields[0]);
  const time = normalizeOutfileNullableText(fields[1]);
  const eventTimeMs = time ? parseEventTimeMs(time) : null;
  const clientIp = normalizeClientIp(normalizeOutfileNullableText(fields[2]));
  const userName = normalizeOutfileNullableText(fields[3]);
  const dbName = normalizeOutfileNullableText(fields[5]);
  const state = normalizeOutfileNullableText(fields[6]);
  const errorCode = parseOutfileInt(fields[7]);
  const errorMessage = normalizeOutfileNullableText(fields[8]);
  const queryTimeMs = parseOutfileInt(fields[9]);
  const scanBytes = parseOutfileInt(fields[10]);
  const scanRows = parseOutfileInt(fields[11]);
  const returnRows = parseOutfileInt(fields[12]);
  const feIp = normalizeOutfileNullableText(fields[21]);
  const cpuTimeMs = parseOutfileInt(fields[22]);
  const peakMemoryBytes = parseOutfileInt(fields[25]);
  const workloadGroup = normalizeOutfileNullableText(fields[26]);
  const cloudClusterName = normalizeOutfileNullableText(fields[27]);
  const stmtRaw = normalizeOutfileNullableText(fields[28]);

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
