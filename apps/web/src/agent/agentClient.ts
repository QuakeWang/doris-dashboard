export type DorisConnectionInfo = {
  host: string;
  port: number;
  user: string;
  database?: string;
};

export type DorisConnectionInput = DorisConnectionInfo & {
  password: string;
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

function parseVersionData(data: unknown): { version: string } {
  const obj = asObject(data);
  if (!obj || typeof obj.version !== "string") {
    throw new Error(toInvalidDataMessage("expected { version: string }", data));
  }
  return { version: obj.version };
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
  ): Promise<string> {
    const res = await this.postJson(
      "/api/v1/doris/connection/test",
      params,
      parseVersionData,
      signal
    );
    return res.version;
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
    return await this.postBlob("/api/v1/doris/audit-log/export", params, signal);
  }

  async explain(
    params: { connection: DorisConnectionInput; sql: string; mode?: "tree" | "plan" },
    signal?: AbortSignal
  ): Promise<string> {
    const res = await this.postJson("/api/v1/doris/explain", params, parseExplainData, signal);
    return res.rawText;
  }
}

export function isDorisConnectionConfigured(
  conn: DorisConnectionInput | null
): conn is DorisConnectionInput {
  return !!conn?.host && conn.port > 0 && !!conn.user && !!conn.password;
}
