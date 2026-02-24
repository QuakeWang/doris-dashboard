import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentClient, type DorisConnectionInput } from "./agentClient";

const TEST_CONNECTION: DorisConnectionInput = {
  host: "127.0.0.1",
  port: 9030,
  user: "root",
  password: "pwd",
};
const TEST_BASE_URL = "http://127.0.0.1:12306";

function createClient(): AgentClient {
  return new AgentClient(TEST_BASE_URL);
}

function mockJsonResponse(body: unknown, status = 200): void {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(
      new Response(JSON.stringify(body), {
        status,
        headers: { "Content-Type": "application/json" },
      })
    )
  );
}

describe("AgentClient.postJson envelope handling", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("accepts success envelope", async () => {
    mockJsonResponse({ ok: true, data: { version: "2.1.0" } });

    const client = createClient();
    const version = await client.testDorisConnection({ connection: TEST_CONNECTION });

    expect(version).toBe("2.1.0");
  });

  it("fails fast for non-envelope JSON body", async () => {
    mockJsonResponse({ version: "2.1.0" });

    const client = createClient();
    await expect(client.testDorisConnection({ connection: TEST_CONNECTION })).rejects.toThrow(
      "Invalid API response envelope"
    );
  });

  it("fails fast for invalid success data shape", async () => {
    mockJsonResponse({ ok: true, data: { version: 2100 } });

    const client = createClient();
    await expect(client.testDorisConnection({ connection: TEST_CONNECTION })).rejects.toThrow(
      "Invalid API response data"
    );
  });

  it("validates array element types for databases response", async () => {
    mockJsonResponse({ ok: true, data: { databases: ["db1", 2] } });

    const client = createClient();
    await expect(client.listDorisDatabases({ connection: TEST_CONNECTION })).rejects.toThrow(
      "Invalid API response data"
    );
  });

  it("surfaces traceId from non-2xx envelope error", async () => {
    mockJsonResponse({ ok: false, error: { message: "bad request" }, traceId: "t-1" }, 400);

    const client = createClient();
    await expect(client.testDorisConnection({ connection: TEST_CONNECTION })).rejects.toThrow(
      "bad request (traceId=t-1)"
    );
  });

  it("validates explain response data shape", async () => {
    mockJsonResponse({ ok: true, data: { rawText: 123 } });

    const client = createClient();
    await expect(
      client.explain({
        connection: TEST_CONNECTION,
        sql: "select 1",
      })
    ).rejects.toThrow("Invalid API response data");
  });
});
