import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentClient, type DorisConnectionInput } from "./agentClient";

const TEST_CONNECTION: DorisConnectionInput = {
  host: "127.0.0.1",
  port: 9030,
  user: "root",
  password: "pwd",
};
const TEST_BASE_URL = "http://127.0.0.1:12306";
const INVALID_DATA_ERROR = "Invalid API response data";

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

async function expectInvalidData(action: () => Promise<unknown>): Promise<void> {
  await expect(action()).rejects.toThrow(INVALID_DATA_ERROR);
}

describe("AgentClient.postJson envelope handling", () => {
  let client: AgentClient;

  beforeEach(() => {
    client = new AgentClient(TEST_BASE_URL);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("accepts success envelope", async () => {
    mockJsonResponse({ ok: true, data: { connected: true } });

    await expect(
      client.testDorisConnection({ connection: TEST_CONNECTION })
    ).resolves.toBeUndefined();
  });

  it("fails fast for non-envelope JSON body", async () => {
    mockJsonResponse({ connected: true });

    await expect(client.testDorisConnection({ connection: TEST_CONNECTION })).rejects.toThrow(
      "Invalid API response envelope"
    );
  });

  it("fails fast for invalid success data shape", async () => {
    mockJsonResponse({ ok: true, data: { connected: "true" } });

    await expectInvalidData(() => client.testDorisConnection({ connection: TEST_CONNECTION }));
  });

  it("validates array element types for databases response", async () => {
    mockJsonResponse({ ok: true, data: { databases: ["db1", 2] } });

    await expectInvalidData(() => client.listDorisDatabases({ connection: TEST_CONNECTION }));
  });

  it("surfaces traceId from non-2xx envelope error", async () => {
    mockJsonResponse({ ok: false, error: { message: "bad request" }, traceId: "t-1" }, 400);

    await expect(client.testDorisConnection({ connection: TEST_CONNECTION })).rejects.toThrow(
      "bad request (traceId=t-1)"
    );
  });

  it("validates explain response data shape", async () => {
    mockJsonResponse({ ok: true, data: { rawText: 123 } });

    await expectInvalidData(() =>
      client.explain({
        connection: TEST_CONNECTION,
        sql: "select 1",
      })
    );
  });

  it("parses schema audit scan envelope", async () => {
    mockJsonResponse({
      ok: true,
      data: {
        inventory: {
          databaseCount: 1,
          tableCount: 1,
          partitionedTableCount: 1,
          totalPartitionCount: 10,
          emptyPartitionCount: 5,
          emptyPartitionRatio: 0.5,
          dynamicPartitionTableCount: 1,
        },
        items: [
          {
            database: "db1",
            table: "tbl1",
            partitionCount: 10,
            emptyPartitionCount: 5,
            emptyPartitionRatio: 0.5,
            dynamicPartitionEnabled: true,
            score: 50,
            findingCount: 1,
            findings: [{ ruleId: "SA-E001", severity: "warn", summary: "x" }],
          },
        ],
        page: 1,
        pageSize: 20,
        totalItems: 1,
        truncated: true,
        scanLimit: 5000,
        warning: "truncated for testing",
      },
    });

    const result = await client.schemaAuditScan({ connection: TEST_CONNECTION });
    expect(result.inventory.tableCount).toBe(1);
    expect(result.items[0]?.database).toBe("db1");
    expect(result.items[0]?.findings[0]?.ruleId).toBe("SA-E001");
    expect(result.truncated).toBe(true);
    expect(result.scanLimit).toBe(5000);
    expect(result.warning).toContain("truncated");
  });

  it("rejects schema audit scan with null items", async () => {
    mockJsonResponse({
      ok: true,
      data: {
        inventory: {
          databaseCount: 0,
          tableCount: 0,
          partitionedTableCount: 0,
          totalPartitionCount: 0,
          emptyPartitionCount: 0,
          emptyPartitionRatio: 0,
          dynamicPartitionTableCount: 0,
        },
        items: null,
        page: 99,
        pageSize: 20,
        totalItems: 0,
        truncated: false,
        scanLimit: 5000,
      },
    });

    await expectInvalidData(() => client.schemaAuditScan({ connection: TEST_CONNECTION }));
  });

  it("validates schema audit table detail data shape", async () => {
    mockJsonResponse({
      ok: true,
      data: {
        database: "db1",
        table: "tbl1",
        createTableSql: "CREATE TABLE ...",
        dynamicProperties: { "dynamic_partition.enable": true },
        partitions: [],
        indexes: [],
        findings: [],
      },
    });

    await expectInvalidData(() =>
      client.schemaAuditTableDetail({ connection: TEST_CONNECTION, database: "db1", table: "tbl1" })
    );
  });
});
