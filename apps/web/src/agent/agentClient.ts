export type DorisConnectionInfo = {
  host: string;
  port: number;
  user: string;
};

export type DorisConnectionInput = DorisConnectionInfo & {
  password: string;
};

function parseErrMessage(text: string): string | null {
  let v: unknown;
  try {
    v = JSON.parse(text) as unknown;
  } catch {
    return null;
  }
  const x = v as any;
  return x && x.ok === false && typeof x?.error?.message === "string"
    ? (x.error.message as string)
    : null;
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
    if (res.ok) {
      try {
        return await res.blob();
      } catch {
        throw new Error("Export interrupted. Please retry.");
      }
    }
    const text = await res.text();
    throw new Error(parseErrMessage(text) ?? `Request failed (status=${res.status})`);
  }

  private async postJson<T>(path: string, payload: unknown, signal?: AbortSignal): Promise<T> {
    const res = await this.post(path, payload, signal);
    if (res.ok) {
      try {
        return (await res.json()) as T;
      } catch {
        throw new Error("Request interrupted. Please retry.");
      }
    }
    const text = await res.text();
    throw new Error(parseErrMessage(text) ?? `Request failed (status=${res.status})`);
  }

  async testDorisConnection(
    params: { connection: DorisConnectionInput },
    signal?: AbortSignal
  ): Promise<string> {
    const res = await this.postJson<{ version: string }>(
      "/api/v1/doris/connection/test",
      params,
      signal
    );
    return res.version;
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
}

export function isDorisConnectionConfigured(
  conn: DorisConnectionInput | null
): conn is DorisConnectionInput {
  return !!conn?.host && conn.port > 0 && !!conn.user && !!conn.password;
}
