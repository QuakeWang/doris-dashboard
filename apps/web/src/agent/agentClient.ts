export type DorisConnectionInfo = {
  host: string;
  port: number;
  user: string;
  database?: string;
};

export type DorisConnectionInput = DorisConnectionInfo & {
  password: string;
};

export type SchemaAuditFindingSummary = {
  ruleId: string;
  severity: string;
  summary: string;
};

export type SchemaAuditScanItem = {
  database: string;
  table: string;
  partitionCount: number;
  emptyPartitionCount: number;
  emptyPartitionRatio: number;
  dynamicPartitionEnabled: boolean;
  score: number;
  findingCount: number;
  findings: SchemaAuditFindingSummary[];
};

export type SchemaAuditInventory = {
  databaseCount: number;
  tableCount: number;
  partitionedTableCount: number;
  totalPartitionCount: number;
  emptyPartitionCount: number;
  emptyPartitionRatio: number;
  dynamicPartitionTableCount: number;
};

export type SchemaAuditScanResult = {
  inventory: SchemaAuditInventory;
  items: SchemaAuditScanItem[];
  page: number;
  pageSize: number;
  totalItems: number;
  truncated: boolean;
  scanLimit: number;
  warning?: string;
};

export type SchemaAuditFinding = {
  ruleId: string;
  severity: string;
  confidence: number;
  summary: string;
  evidence: Record<string, unknown>;
  recommendation?: string;
};

export type SchemaAuditPartition = {
  name: string;
  rows: number;
  dataSizeBytes: number;
  buckets: number;
  empty: boolean;
};

export type SchemaAuditIndex = {
  name: string;
  indexType: string;
  columns: string[];
};

export type SchemaAuditTableDetailResult = {
  database: string;
  table: string;
  createTableSql: string;
  dynamicProperties: Record<string, string>;
  partitions: SchemaAuditPartition[];
  indexes: SchemaAuditIndex[];
  findings: SchemaAuditFinding[];
};

type ApiSuccess<T> = {
  ok: true;
  data: T;
  traceId?: string;
};

type ApiFailure = {
  ok: false;
  error?: { message?: string };
  traceId?: string;
};

type DataParser<T> = (data: unknown) => T;

function toTraceSuffix(traceId: unknown): string {
  return typeof traceId === "string" && traceId.trim() ? ` (traceId=${traceId.trim()})` : "";
}

function parseErrMessage(text: string): string | null {
  let v: unknown;
  try {
    v = JSON.parse(text) as unknown;
  } catch {
    return null;
  }
  if (isApiFailure(v) && typeof v.error?.message === "string") {
    return `${v.error.message}${toTraceSuffix(v.traceId)}`;
  }
  return null;
}

function isApiSuccess<T>(v: unknown): v is ApiSuccess<T> {
  const obj = asObject(v);
  return !!obj && obj.ok === true && "data" in obj;
}

function isApiFailure(v: unknown): v is ApiFailure {
  const obj = asObject(v);
  return !!obj && obj.ok === false;
}

function toApiErrorMessage(failure: ApiFailure): string {
  const message =
    typeof failure.error?.message === "string" && failure.error.message
      ? failure.error.message
      : "Request failed";
  return `${message}${toTraceSuffix(failure.traceId)}`;
}

function toInvalidEnvelopeMessage(body: unknown): string {
  return `Invalid API response envelope: ${toJsonPreview(body)}`;
}

function toInvalidDataMessage(message: string, data: unknown): string {
  return `Invalid API response data (${message}): ${toJsonPreview(data)}`;
}

function toJsonPreview(value: unknown): string {
  let preview = "";
  try {
    const json = JSON.stringify(value);
    preview = json && json.length > 256 ? `${json.slice(0, 256)}...` : (json ?? "");
  } catch {
    preview = String(value);
  }
  return preview || "(empty)";
}

function asObject(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

function parseConnectionTestData(data: unknown): { connected: true } {
  const obj = asObject(data);
  if (!obj || obj.connected !== true) {
    throw new Error(toInvalidDataMessage("expected { connected: true }", data));
  }
  return { connected: true };
}

function parseDatabasesData(data: unknown): { databases: string[] } {
  const obj = asObject(data);
  const values = obj?.databases;
  if (!Array.isArray(values) || values.some((v) => typeof v !== "string")) {
    throw new Error(toInvalidDataMessage("expected { databases: string[] }", data));
  }
  return { databases: values };
}

function parseExplainData(data: unknown): { rawText: string } {
  const obj = asObject(data);
  if (!obj || typeof obj.rawText !== "string") {
    throw new Error(toInvalidDataMessage("expected { rawText: string }", data));
  }
  return { rawText: obj.rawText };
}

function toFiniteNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function toStringArray(value: unknown): string[] | null {
  if (!Array.isArray(value) || value.some((v) => typeof v !== "string")) {
    return null;
  }
  return value;
}

function parseSchemaAuditFindingSummary(value: unknown, data: unknown): SchemaAuditFindingSummary {
  const obj = asObject(value);
  if (!obj) {
    throw new Error(toInvalidDataMessage("invalid finding summary", data));
  }
  if (
    typeof obj.ruleId !== "string" ||
    typeof obj.severity !== "string" ||
    typeof obj.summary !== "string"
  ) {
    throw new Error(toInvalidDataMessage("invalid finding summary fields", data));
  }
  return {
    ruleId: obj.ruleId,
    severity: obj.severity,
    summary: obj.summary,
  };
}

function parseSchemaAuditScanItem(value: unknown, data: unknown): SchemaAuditScanItem {
  const obj = asObject(value);
  if (!obj) {
    throw new Error(toInvalidDataMessage("invalid schema audit item", data));
  }
  const partitionCount = toFiniteNumber(obj.partitionCount);
  const emptyPartitionCount = toFiniteNumber(obj.emptyPartitionCount);
  const emptyPartitionRatio = toFiniteNumber(obj.emptyPartitionRatio);
  const score = toFiniteNumber(obj.score);
  const findingCount = toFiniteNumber(obj.findingCount);
  const findings = Array.isArray(obj.findings)
    ? obj.findings.map((finding) => parseSchemaAuditFindingSummary(finding, data))
    : null;
  if (
    typeof obj.database !== "string" ||
    typeof obj.table !== "string" ||
    partitionCount == null ||
    emptyPartitionCount == null ||
    emptyPartitionRatio == null ||
    typeof obj.dynamicPartitionEnabled !== "boolean" ||
    score == null ||
    findingCount == null ||
    findings == null
  ) {
    throw new Error(toInvalidDataMessage("invalid schema audit scan item fields", data));
  }
  return {
    database: obj.database,
    table: obj.table,
    partitionCount,
    emptyPartitionCount,
    emptyPartitionRatio,
    dynamicPartitionEnabled: obj.dynamicPartitionEnabled,
    score,
    findingCount,
    findings,
  };
}

function parseSchemaAuditInventory(value: unknown, data: unknown): SchemaAuditInventory {
  const obj = asObject(value);
  const databaseCount = toFiniteNumber(obj?.databaseCount);
  const tableCount = toFiniteNumber(obj?.tableCount);
  const partitionedTableCount = toFiniteNumber(obj?.partitionedTableCount);
  const totalPartitionCount = toFiniteNumber(obj?.totalPartitionCount);
  const emptyPartitionCount = toFiniteNumber(obj?.emptyPartitionCount);
  const emptyPartitionRatio = toFiniteNumber(obj?.emptyPartitionRatio);
  const dynamicPartitionTableCount = toFiniteNumber(obj?.dynamicPartitionTableCount);
  if (
    !obj ||
    databaseCount == null ||
    tableCount == null ||
    partitionedTableCount == null ||
    totalPartitionCount == null ||
    emptyPartitionCount == null ||
    emptyPartitionRatio == null ||
    dynamicPartitionTableCount == null
  ) {
    throw new Error(toInvalidDataMessage("invalid schema audit inventory", data));
  }
  return {
    databaseCount,
    tableCount,
    partitionedTableCount,
    totalPartitionCount,
    emptyPartitionCount,
    emptyPartitionRatio,
    dynamicPartitionTableCount,
  };
}

function parseSchemaAuditScanData(data: unknown): SchemaAuditScanResult {
  const obj = asObject(data);
  if (!obj) {
    throw new Error(toInvalidDataMessage("expected scan result shape", data));
  }
  if (!Array.isArray(obj.items)) {
    throw new Error(toInvalidDataMessage("expected scan items array", data));
  }
  const page = toFiniteNumber(obj.page);
  const pageSize = toFiniteNumber(obj.pageSize);
  const totalItems = toFiniteNumber(obj.totalItems);
  const scanLimit = obj.scanLimit == null ? 0 : toFiniteNumber(obj.scanLimit);
  const items = obj.items.map((item) => parseSchemaAuditScanItem(item, data));
  const inventory = parseSchemaAuditInventory(obj.inventory, data);
  if (page == null || pageSize == null || totalItems == null) {
    throw new Error(toInvalidDataMessage("invalid scan paging fields", data));
  }
  if (obj.truncated != null && typeof obj.truncated !== "boolean") {
    throw new Error(toInvalidDataMessage("invalid scan truncated field", data));
  }
  if (scanLimit == null) {
    throw new Error(toInvalidDataMessage("invalid scanLimit field", data));
  }
  if (obj.warning != null && typeof obj.warning !== "string") {
    throw new Error(toInvalidDataMessage("invalid scan warning field", data));
  }
  return {
    inventory,
    items,
    page,
    pageSize,
    totalItems,
    truncated: obj.truncated === true,
    scanLimit,
    warning: typeof obj.warning === "string" ? obj.warning : undefined,
  };
}

function parseSchemaAuditFinding(value: unknown, data: unknown): SchemaAuditFinding {
  const obj = asObject(value);
  const evidence = asObject(obj?.evidence);
  const confidence = toFiniteNumber(obj?.confidence);
  if (
    !obj ||
    typeof obj.ruleId !== "string" ||
    typeof obj.severity !== "string" ||
    confidence == null ||
    typeof obj.summary !== "string" ||
    !evidence
  ) {
    throw new Error(toInvalidDataMessage("invalid finding", data));
  }
  if (obj.recommendation != null && typeof obj.recommendation !== "string") {
    throw new Error(toInvalidDataMessage("invalid finding recommendation", data));
  }
  return {
    ruleId: obj.ruleId,
    severity: obj.severity,
    confidence,
    summary: obj.summary,
    evidence,
    recommendation: typeof obj.recommendation === "string" ? obj.recommendation : undefined,
  };
}

function parseSchemaAuditPartition(value: unknown, data: unknown): SchemaAuditPartition {
  const obj = asObject(value);
  const rows = toFiniteNumber(obj?.rows);
  const dataSizeBytes = toFiniteNumber(obj?.dataSizeBytes);
  const buckets = toFiniteNumber(obj?.buckets);
  if (
    !obj ||
    typeof obj.name !== "string" ||
    rows == null ||
    dataSizeBytes == null ||
    buckets == null ||
    typeof obj.empty !== "boolean"
  ) {
    throw new Error(toInvalidDataMessage("invalid partition", data));
  }
  return {
    name: obj.name,
    rows,
    dataSizeBytes,
    buckets,
    empty: obj.empty,
  };
}

function parseSchemaAuditIndex(value: unknown, data: unknown): SchemaAuditIndex {
  const obj = asObject(value);
  const columns = toStringArray(obj?.columns);
  if (!obj || typeof obj.name !== "string" || typeof obj.indexType !== "string" || !columns) {
    throw new Error(toInvalidDataMessage("invalid index", data));
  }
  return {
    name: obj.name,
    indexType: obj.indexType,
    columns,
  };
}

function parseSchemaAuditDynamicProperties(value: unknown, data: unknown): Record<string, string> {
  const obj = asObject(value);
  if (!obj) {
    throw new Error(toInvalidDataMessage("invalid dynamicProperties", data));
  }
  const out: Record<string, string> = {};
  for (const [key, rawValue] of Object.entries(obj)) {
    if (typeof rawValue !== "string") {
      throw new Error(toInvalidDataMessage("dynamicProperties values must be string", data));
    }
    out[key] = rawValue;
  }
  return out;
}

function parseSchemaAuditTableDetailData(data: unknown): SchemaAuditTableDetailResult {
  const obj = asObject(data);
  if (
    !obj ||
    !Array.isArray(obj.partitions) ||
    !Array.isArray(obj.indexes) ||
    !Array.isArray(obj.findings)
  ) {
    throw new Error(toInvalidDataMessage("expected table detail shape", data));
  }
  if (
    typeof obj.database !== "string" ||
    typeof obj.table !== "string" ||
    typeof obj.createTableSql !== "string"
  ) {
    throw new Error(toInvalidDataMessage("invalid table detail header fields", data));
  }
  return {
    database: obj.database,
    table: obj.table,
    createTableSql: obj.createTableSql,
    dynamicProperties: parseSchemaAuditDynamicProperties(obj.dynamicProperties, data),
    partitions: obj.partitions.map((partition) => parseSchemaAuditPartition(partition, data)),
    indexes: obj.indexes.map((index) => parseSchemaAuditIndex(index, data)),
    findings: obj.findings.map((finding) => parseSchemaAuditFinding(finding, data)),
  };
}

async function toResponseError(res: Response): Promise<Error> {
  const text = await res.text();
  return new Error(parseErrMessage(text) ?? `Request failed (status=${res.status})`);
}

export class AgentClient {
  private baseUrl: string;

  constructor(baseUrl?: string) {
    const raw = baseUrl ?? import.meta.env.VITE_AGENT_BASE_URL ?? "";
    this.baseUrl = raw.replace(/\/+$/, "");
  }

  private url(path: string): string {
    return `${this.baseUrl}${path}`;
  }

  private async post(path: string, payload: unknown, signal?: AbortSignal): Promise<Response> {
    try {
      return await fetch(this.url(path), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal,
      });
    } catch (e) {
      if (e instanceof DOMException && e.name === "AbortError") throw e;
      const requestUrl = this.url(path);
      const base = this.baseUrl || "(same-origin / dev proxy)";
      throw new Error(
        [
          `agentd request failed: ${requestUrl}`,
          "Possible causes:",
          "- agentd is not running or the port is not reachable",
          "- VITE_AGENT_BASE_URL / dev proxy is misconfigured",
          "- blocked by the browser (CORS, mixed content, TLS certificate)",
          "- CORS note: agentd allows localhost origins only by design",
          `Base URL: ${base}`,
        ].join("\n"),
        { cause: e as unknown }
      );
    }
  }

  private async postBlob(path: string, payload: unknown, signal?: AbortSignal): Promise<Blob> {
    const res = await this.post(path, payload, signal);
    if (!res.ok) throw await toResponseError(res);
    try {
      return await res.blob();
    } catch {
      throw new Error("Export interrupted. Please retry.");
    }
  }

  private async postJson<T>(
    path: string,
    payload: unknown,
    parseData: DataParser<T>,
    signal?: AbortSignal
  ): Promise<T> {
    const res = await this.post(path, payload, signal);
    if (!res.ok) throw await toResponseError(res);
    let body: unknown;
    try {
      body = (await res.json()) as unknown;
    } catch {
      throw new Error("Request interrupted. Please retry.");
    }
    if (isApiSuccess<unknown>(body)) return parseData(body.data);
    if (isApiFailure(body)) throw new Error(toApiErrorMessage(body));
    throw new Error(toInvalidEnvelopeMessage(body));
  }

  async testDorisConnection(
    params: { connection: DorisConnectionInput },
    signal?: AbortSignal
  ): Promise<void> {
    await this.postJson("/api/v1/doris/connection/test", params, parseConnectionTestData, signal);
  }

  async listDorisDatabases(
    params: { connection: DorisConnectionInput },
    signal?: AbortSignal
  ): Promise<string[]> {
    const res = await this.postJson("/api/v1/doris/databases", params, parseDatabasesData, signal);
    return res.databases;
  }

  async exportAuditLogOutfileTsv(
    params: {
      connection: DorisConnectionInput;
      lookbackSeconds: number;
      limit: number;
    },
    signal?: AbortSignal
  ): Promise<Blob> {
    return this.postBlob("/api/v1/doris/audit-log/export", params, signal);
  }

  async explain(
    params: { connection: DorisConnectionInput; sql: string; mode?: "tree" | "plan" },
    signal?: AbortSignal
  ): Promise<string> {
    const res = await this.postJson("/api/v1/doris/explain", params, parseExplainData, signal);
    return res.rawText;
  }

  async schemaAuditScan(
    params: {
      connection: DorisConnectionInput;
      database?: string;
      tableLike?: string;
      page?: number;
      pageSize?: number;
    },
    signal?: AbortSignal
  ): Promise<SchemaAuditScanResult> {
    return this.postJson(
      "/api/v1/doris/schema-audit/scan",
      params,
      parseSchemaAuditScanData,
      signal
    );
  }

  async schemaAuditTableDetail(
    params: {
      connection: DorisConnectionInput;
      database: string;
      table: string;
    },
    signal?: AbortSignal
  ): Promise<SchemaAuditTableDetailResult> {
    return this.postJson(
      "/api/v1/doris/schema-audit/table-detail",
      params,
      parseSchemaAuditTableDetailData,
      signal
    );
  }
}

export function isDorisConnectionConfigured(
  conn: DorisConnectionInput | null
): conn is DorisConnectionInput {
  return !!conn?.host && conn.port > 0 && !!conn.user && !!conn.password;
}
