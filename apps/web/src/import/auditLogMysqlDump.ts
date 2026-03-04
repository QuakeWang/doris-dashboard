import type { ParsedAuditLogRecord } from "./auditLogParser";
import { normalizeClientIp, parseEventTimeMs } from "./parseUtils";
import { normalizeSqlBase } from "./sqlTemplate";
import { getTemplateInfo } from "./templateInfo";

export interface MysqlDumpReaderProgress {
  bytesRead: number;
  statementsScanned: number;
  insertStatementsMatched: number;
  tuplesParsed: number;
  badStatements: number;
}

export interface MysqlDumpBadStatement {
  statementIndex: number;
  reason: string;
  snippet: string;
}

export interface MysqlDumpReaderOptions {
  signal?: AbortSignal;
  onProgress?: (p: MysqlDumpReaderProgress) => void;
  onBadStatement?: (detail: MysqlDumpBadStatement) => void;
  maxStatementChars?: number;
}

type MysqlDumpColumnKey =
  | "is_internal"
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

type ColumnIndex = Partial<Record<MysqlDumpColumnKey, number>>;

const INSERT_MODIFIER_GROUP = "(?:low_priority|delayed|high_priority|ignore)";
const INSERT_PREFIX_PATTERN = `insert(?:\\s+${INSERT_MODIFIER_GROUP})*\\s+into`;
const AUDIT_LOG_TABLE_PATTERN = "(?:(?:`[^`]+`|[A-Za-z0-9_$]+)\\s*\\.\\s*)?(?:`?audit_log`?)";
const INSERT_OR_CREATE_AUDIT_LOG_RE = new RegExp(
  `(?:^|[\\r\\n])\\s*(?:${INSERT_PREFIX_PATTERN}|create\\s+table)\\s+${AUDIT_LOG_TABLE_PATTERN}(?:\\s|\\(|$)`,
  "im"
);

const INSERT_INTO_RE = new RegExp(`^\\s*${INSERT_PREFIX_PATTERN}\\s+`, "i");
const INSERT_AUDIT_LOG_RE = new RegExp(
  `^\\s*${INSERT_PREFIX_PATTERN}\\s+${AUDIT_LOG_TABLE_PATTERN}(?:\\s|\\(|$)`,
  "i"
);
const MAX_BAD_STATEMENT_SNIPPET_CHARS = 200;
const DEFAULT_MAX_SQL_STATEMENT_CHARS = 64 * 1024 * 1024;

const MYSQL_DUMP_NOISE_STMT_PREFIXES = [
  "select @@version_comment limit 1",
  "select $$",
  "show variables like ",
  "show tables like ",
  "show fields from `audit_log`",
  "show table status like ",
  "set session net_read_timeout",
  "set session transaction isolation level",
] as const;

const COLUMN_ALIASES: Record<string, MysqlDumpColumnKey> = {
  isinternal: "is_internal",
  is_internal: "is_internal",
  queryid: "query_id",
  query_id: "query_id",
  time: "time",
  client: "client_ip",
  client_ip: "client_ip",
  user: "user_name",
  user_name: "user_name",
  db: "db_name",
  db_name: "db_name",
  state: "state",
  errorcode: "error_code",
  error_code: "error_code",
  errormessage: "error_message",
  error_message: "error_message",
  query_time: "query_time_ms",
  query_time_ms: "query_time_ms",
  "time(ms)": "query_time_ms",
  scan_bytes: "scan_bytes",
  scan_rows: "scan_rows",
  return_rows: "return_rows",
  fe_ip: "fe_ip",
  frontend_ip: "fe_ip",
  frontendip: "fe_ip",
  cpu_time_ms: "cpu_time_ms",
  cputimems: "cpu_time_ms",
  peak_memory_bytes: "peak_memory_bytes",
  peakmemorybytes: "peak_memory_bytes",
  workload_group: "workload_group",
  workloadgroup: "workload_group",
  cloud_cluster_name: "cloud_cluster_name",
  compute_group_name: "cloud_cluster_name",
  compute_group: "cloud_cluster_name",
  computegroupname: "cloud_cluster_name",
  computegroup: "cloud_cluster_name",
  stmt: "stmt_raw",
  stmt_raw: "stmt_raw",
};

const DEFAULT_COLUMN_INDEX: ColumnIndex = {
  query_id: 0,
  time: 1,
  client_ip: 2,
  user_name: 3,
  db_name: 5,
  state: 6,
  error_code: 7,
  error_message: 8,
  query_time_ms: 9,
  scan_bytes: 10,
  scan_rows: 11,
  return_rows: 12,
  fe_ip: 21,
  cpu_time_ms: 22,
  peak_memory_bytes: 25,
  workload_group: 26,
  cloud_cluster_name: 27,
  stmt_raw: 28,
};

function skipSpaces(s: string, index: number): number {
  let i = index;
  while (i < s.length && /\s/.test(s[i] ?? "")) i++;
  return i;
}

function parseIdentifier(s: string, index: number): { value: string; next: number } | null {
  let i = skipSpaces(s, index);
  if (i >= s.length) return null;

  if (s[i] === "`") {
    i++;
    let out = "";
    while (i < s.length) {
      const ch = s[i] ?? "";
      if (ch === "`") {
        if (s[i + 1] === "`") {
          out += "`";
          i += 2;
          continue;
        }
        return { value: out, next: i + 1 };
      }
      out += ch;
      i++;
    }
    return null;
  }

  const start = i;
  while (i < s.length && /[A-Za-z0-9_$]/.test(s[i] ?? "")) i++;
  if (i === start) return null;
  return { value: s.slice(start, i), next: i };
}

function startsWithKeyword(s: string, index: number, keyword: string): boolean {
  const i = skipSpaces(s, index);
  const tail = s.slice(i, i + keyword.length);
  if (tail.toLowerCase() !== keyword) return false;
  const next = s[i + keyword.length];
  if (next && /[A-Za-z0-9_$]/.test(next)) return false;
  return true;
}

function decodeEscapedChar(ch: string): string {
  switch (ch) {
    case "0":
      return "\0";
    case "b":
      return "\b";
    case "n":
      return "\n";
    case "r":
      return "\r";
    case "t":
      return "\t";
    case "Z":
      return "\x1a";
    case "\\":
      return "\\";
    case '"':
      return '"';
    case "'":
      return "'";
    default:
      return ch;
  }
}

function parseSqlString(s: string, index: number): { value: string; next: number } | null {
  if (s[index] !== "'") return null;
  let i = index + 1;
  let out = "";

  while (i < s.length) {
    const ch = s[i] ?? "";
    if (ch === "\\") {
      const next = s[i + 1];
      if (next == null) {
        out += "\\";
        i++;
      } else {
        out += decodeEscapedChar(next);
        i += 2;
      }
      continue;
    }
    if (ch === "'") {
      if (s[i + 1] === "'") {
        out += "'";
        i += 2;
        continue;
      }
      return { value: out, next: i + 1 };
    }
    out += ch;
    i++;
  }
  return null;
}

function parseSqlValue(s: string, index: number): { value: unknown; next: number } | null {
  const i = skipSpaces(s, index);
  if (i >= s.length) return null;

  if (s[i] === "'") return parseSqlString(s, i);

  let j = i;
  while (j < s.length) {
    const ch = s[j] ?? "";
    if (ch === "," || ch === ")") break;
    j++;
  }

  const token = s.slice(i, j).trim();
  if (!token) return null;
  if (/^null$/i.test(token)) return { value: null, next: j };
  if (/^-?\d+$/.test(token)) {
    const n = Number.parseInt(token, 10);
    return { value: Number.isFinite(n) ? n : null, next: j };
  }
  if (/^-?\d+(?:\.\d+)?$/.test(token)) {
    const n = Number.parseFloat(token);
    return { value: Number.isFinite(n) ? n : null, next: j };
  }
  if (/^true$/i.test(token)) return { value: true, next: j };
  if (/^false$/i.test(token)) return { value: false, next: j };

  return { value: token, next: j };
}

function parseColumnList(s: string, index: number): { columns: string[]; next: number } | null {
  let i = skipSpaces(s, index);
  if (s[i] !== "(") return null;
  i++;

  const columns: string[] = [];
  while (i < s.length) {
    i = skipSpaces(s, i);
    if (s[i] === ")") return { columns, next: i + 1 };

    const parsed = parseIdentifier(s, i);
    if (!parsed) return null;
    columns.push(parsed.value);
    i = skipSpaces(s, parsed.next);

    if (s[i] === ",") {
      i++;
      continue;
    }
    if (s[i] === ")") return { columns, next: i + 1 };
    return null;
  }
  return null;
}

function parseTuple(s: string, index: number): { values: unknown[]; next: number } | null {
  let i = skipSpaces(s, index);
  if (s[i] !== "(") return null;
  i++;

  const values: unknown[] = [];
  while (i < s.length) {
    i = skipSpaces(s, i);
    if (s[i] === ")") return { values, next: i + 1 };

    const parsedValue = parseSqlValue(s, i);
    if (!parsedValue) return null;
    values.push(parsedValue.value);
    i = skipSpaces(s, parsedValue.next);

    if (s[i] === ",") {
      i++;
      continue;
    }
    if (s[i] === ")") return { values, next: i + 1 };
    return null;
  }

  return null;
}

function buildColumnIndex(columns: string[] | null): ColumnIndex {
  if (!columns || columns.length === 0) return DEFAULT_COLUMN_INDEX;

  const index: ColumnIndex = {};
  for (let i = 0; i < columns.length; i++) {
    const key = COLUMN_ALIASES[(columns[i] ?? "").trim().toLowerCase()];
    if (!key || index[key] != null) continue;
    index[key] = i;
  }
  return index;
}

function pickValue(values: unknown[], index: ColumnIndex, key: MysqlDumpColumnKey): unknown {
  const idx = index[key];
  if (idx == null || idx < 0 || idx >= values.length) return null;
  return values[idx] ?? null;
}

function toNullableText(value: unknown): string | null {
  if (value == null) return null;
  const s = String(value).trim();
  if (!s) return null;
  if (s.toLowerCase() === "null") return null;
  return s;
}

function toIntOrNull(value: unknown): number | null {
  if (value == null) return null;
  if (typeof value === "number") return Number.isFinite(value) ? Math.trunc(value) : null;
  const s = String(value).trim();
  if (!s) return null;
  const n = Number.parseInt(s, 10);
  return Number.isFinite(n) ? n : null;
}

function toBoolOrNull(value: unknown): boolean | null {
  if (value == null) return null;
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value === 0 ? false : value === 1 ? true : null;
  const s = String(value).trim().toLowerCase();
  if (!s) return null;
  if (s === "1" || s === "true" || s === "t" || s === "yes" || s === "y") return true;
  if (s === "0" || s === "false" || s === "f" || s === "no" || s === "n") return false;
  return null;
}

function mapTupleToRecord(values: unknown[], index: ColumnIndex): ParsedAuditLogRecord {
  const queryId = toNullableText(pickValue(values, index, "query_id"));
  const time = toNullableText(pickValue(values, index, "time"));
  const eventTimeMs = time ? parseEventTimeMs(time) : null;
  const clientIp = normalizeClientIp(toNullableText(pickValue(values, index, "client_ip")));
  const userName = toNullableText(pickValue(values, index, "user_name"));
  const dbName = toNullableText(pickValue(values, index, "db_name"));
  const state = toNullableText(pickValue(values, index, "state"));
  const errorCode = toIntOrNull(pickValue(values, index, "error_code"));
  const errorMessage = toNullableText(pickValue(values, index, "error_message"));
  const queryTimeMs = toIntOrNull(pickValue(values, index, "query_time_ms"));
  const scanBytes = toIntOrNull(pickValue(values, index, "scan_bytes"));
  const scanRows = toIntOrNull(pickValue(values, index, "scan_rows"));
  const returnRows = toIntOrNull(pickValue(values, index, "return_rows"));
  const feIp = toNullableText(pickValue(values, index, "fe_ip"));
  const cpuTimeMs = toIntOrNull(pickValue(values, index, "cpu_time_ms"));
  const peakMemoryBytes = toIntOrNull(pickValue(values, index, "peak_memory_bytes"));
  const workloadGroup = toNullableText(pickValue(values, index, "workload_group"));
  const cloudClusterName = toNullableText(pickValue(values, index, "cloud_cluster_name"));
  const stmtRaw = toNullableText(pickValue(values, index, "stmt_raw"));
  const isInternal = toBoolOrNull(pickValue(values, index, "is_internal"));

  const baseTemplate = stmtRaw ? normalizeSqlBase(stmtRaw) : null;
  const info = baseTemplate ? getTemplateInfo(baseTemplate) : null;
  const sqlTemplateStripped = info ? info.strippedTemplate : null;
  const sqlGuess = info ? info.tableGuess : null;
  const tableGuess = sqlGuess
    ? sqlGuess.includes(".") || !dbName
      ? sqlGuess
      : `${dbName}.${sqlGuess}`
    : null;

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
    sqlTemplateStripped,
    tableGuess,
  };
}

function* iterateRecordsFromInsertStatement(statement: string): Generator<ParsedAuditLogRecord> {
  const insertMatch = INSERT_INTO_RE.exec(statement);
  if (!insertMatch) throw new Error("statement is not INSERT INTO");

  let i = insertMatch[0].length;
  const firstIdent = parseIdentifier(statement, i);
  if (!firstIdent) throw new Error("missing table identifier after INSERT INTO");
  i = skipSpaces(statement, firstIdent.next);

  let tableName = firstIdent.value;
  if (statement[i] === ".") {
    i++;
    const secondIdent = parseIdentifier(statement, i);
    if (!secondIdent) throw new Error("malformed schema-qualified table name");
    tableName = secondIdent.value;
    i = secondIdent.next;
  }

  if (tableName.trim().toLowerCase() !== "audit_log") return;

  i = skipSpaces(statement, i);
  let columns: string[] | null = null;
  if (statement[i] === "(") {
    const parsedColumns = parseColumnList(statement, i);
    if (!parsedColumns) throw new Error("malformed column list");
    columns = parsedColumns.columns;
    i = parsedColumns.next;
  }

  i = skipSpaces(statement, i);
  if (!startsWithKeyword(statement, i, "values")) {
    throw new Error("expected VALUES clause");
  }
  i = skipSpaces(statement, i);
  i += "values".length;

  const index = buildColumnIndex(columns);

  while (i < statement.length) {
    i = skipSpaces(statement, i);
    const ch = statement[i];
    if (!ch || ch === ";") return;
    if (ch !== "(") throw new Error("expected tuple start '('");

    const parsedTuple = parseTuple(statement, i);
    if (!parsedTuple) throw new Error("malformed tuple in VALUES list");
    yield mapTupleToRecord(parsedTuple.values, index);
    i = skipSpaces(statement, parsedTuple.next);

    if (statement[i] === ",") {
      i++;
      continue;
    }
    if (!statement[i] || statement[i] === ";") return;
    throw new Error("unexpected token after tuple");
  }
}

interface SqlStatementScanOptions {
  signal?: AbortSignal;
  onBytesRead?: (bytesRead: number) => void;
  maxStatementChars?: number;
  onOversizedStatement?: (detail: { statementChars: number; snippet: string }) => void;
}

function normalizeStatementSnippet(statement: string): string {
  return statement.replace(/\s+/g, " ").trim().slice(0, MAX_BAD_STATEMENT_SNIPPET_CHARS);
}

async function* iterateSqlStatements(
  file: Blob,
  options: SqlStatementScanOptions = {}
): AsyncGenerator<string> {
  const stream = file.stream();
  const reader = stream.getReader();
  const decoder = new TextDecoder("utf-8");
  const maxStatementChars =
    options.maxStatementChars && options.maxStatementChars > 0
      ? options.maxStatementChars
      : DEFAULT_MAX_SQL_STATEMENT_CHARS;

  let bytesRead = 0;
  let buffer = "";
  let scanOffset = 0;
  let inString = false;
  let escaping = false;
  let droppingOversizedStatement = false;
  let oversizedStatementChars = 0;
  let oversizedSnippet = "";

  const emitProgress = () => {
    options.onBytesRead?.(bytesRead);
  };

  const reportOversizedStatement = () => {
    if (!droppingOversizedStatement || oversizedStatementChars <= 0) return;
    options.onOversizedStatement?.({
      statementChars: oversizedStatementChars,
      snippet: oversizedSnippet,
    });
    droppingOversizedStatement = false;
    oversizedStatementChars = 0;
    oversizedSnippet = "";
  };

  const emitBufferedStatements = function* (): Generator<string> {
    let start = 0;
    for (let i = scanOffset; i < buffer.length; i++) {
      const ch = buffer[i] ?? "";
      if (!droppingOversizedStatement && i - start + 1 > maxStatementChars) {
        droppingOversizedStatement = true;
        oversizedStatementChars = i - start + 1;
        oversizedSnippet = normalizeStatementSnippet(buffer.slice(start, i + 1));
        start = i + 1;
      } else if (droppingOversizedStatement) {
        oversizedStatementChars++;
      }

      if (inString) {
        if (escaping) {
          escaping = false;
          continue;
        }
        if (ch === "\\") {
          escaping = true;
          continue;
        }
        if (ch === "'") inString = false;
        continue;
      }

      if (ch === "'") {
        inString = true;
        continue;
      }

      if (droppingOversizedStatement) {
        if (ch === ";") {
          reportOversizedStatement();
          start = i + 1;
        }
        continue;
      }

      if (ch === ";") {
        const statement = buffer.slice(start, i + 1);
        start = i + 1;
        if (statement.trim()) yield statement;
      }
    }
    scanOffset = buffer.length;
    if (droppingOversizedStatement) {
      buffer = "";
      scanOffset = 0;
      return;
    }
    if (start > 0) {
      buffer = buffer.slice(start);
      scanOffset -= start;
    }
  };

  while (true) {
    if (options.signal?.aborted) throw new DOMException("Aborted", "AbortError");
    const { value, done } = await reader.read();
    if (done) break;

    bytesRead += value.byteLength;
    buffer += decoder.decode(value, { stream: true });
    for (const statement of emitBufferedStatements()) yield statement;
    emitProgress();
  }

  const tail = decoder.decode();
  if (tail) buffer += tail;
  for (const statement of emitBufferedStatements()) yield statement;
  if (droppingOversizedStatement) {
    reportOversizedStatement();
  } else if (buffer.trim()) {
    yield buffer;
  }
  emitProgress();
}

export function looksLikeAuditLogMysqlDump(sample: string): boolean {
  return INSERT_OR_CREATE_AUDIT_LOG_RE.test(sample);
}

export function isMysqlDumpNoiseRecord(record: Pick<ParsedAuditLogRecord, "stmtRaw">): boolean {
  const stmt = (record.stmtRaw ?? "").trim().toLowerCase();
  if (!stmt) return false;
  return MYSQL_DUMP_NOISE_STMT_PREFIXES.some((prefix) => stmt.startsWith(prefix));
}

export async function* iterateAuditLogRecordsFromMysqlDump(
  file: Blob,
  options: MysqlDumpReaderOptions = {}
): AsyncGenerator<ParsedAuditLogRecord> {
  const maxStatementChars =
    options.maxStatementChars && options.maxStatementChars > 0
      ? options.maxStatementChars
      : DEFAULT_MAX_SQL_STATEMENT_CHARS;
  let bytesRead = 0;
  let statementsScanned = 0;
  let insertStatementsMatched = 0;
  let tuplesParsed = 0;
  let badStatements = 0;

  const emitProgress = () => {
    options.onProgress?.({
      bytesRead,
      statementsScanned,
      insertStatementsMatched,
      tuplesParsed,
      badStatements,
    });
  };

  for await (const statement of iterateSqlStatements(file, {
    signal: options.signal,
    maxStatementChars,
    onBytesRead: (nextBytesRead) => {
      bytesRead = nextBytesRead;
      emitProgress();
    },
    onOversizedStatement: ({ statementChars, snippet }) => {
      statementsScanned++;
      if (!INSERT_AUDIT_LOG_RE.test(snippet)) {
        emitProgress();
        return;
      }
      insertStatementsMatched++;
      badStatements++;
      options.onBadStatement?.({
        statementIndex: statementsScanned,
        reason: `statement exceeds max size (${statementChars} chars > ${maxStatementChars})`,
        snippet,
      });
      emitProgress();
    },
  })) {
    if (options.signal?.aborted) throw new DOMException("Aborted", "AbortError");
    statementsScanned++;
    emitProgress();
    if (!INSERT_AUDIT_LOG_RE.test(statement)) continue;

    insertStatementsMatched++;
    emitProgress();
    try {
      for (const record of iterateRecordsFromInsertStatement(statement)) {
        if (options.signal?.aborted) throw new DOMException("Aborted", "AbortError");
        tuplesParsed++;
        emitProgress();
        yield record;
      }
    } catch (err) {
      badStatements++;
      options.onBadStatement?.({
        statementIndex: statementsScanned,
        reason: err instanceof Error ? err.message : String(err),
        snippet: normalizeStatementSnippet(statement),
      });
      emitProgress();
      // Skip malformed statements and continue scanning.
    }
  }
  emitProgress();
}
