import { afterEach, describe, expect, it, vi } from "vitest";

const {
  ensureDbMock,
  dropAuditLogIndexesMock,
  createAuditLogIndexesMock,
  queryWithParamsMock,
  insertArrowViaStagingMock,
  logMock,
  replyMock,
} = vi.hoisted(() => ({
  ensureDbMock: vi.fn(),
  dropAuditLogIndexesMock: vi.fn(),
  createAuditLogIndexesMock: vi.fn(),
  queryWithParamsMock: vi.fn(),
  insertArrowViaStagingMock: vi.fn(),
  logMock: vi.fn(),
  replyMock: vi.fn(),
}));

vi.mock("./engine", () => ({ ensureDb: ensureDbMock }));
vi.mock("./schema", () => ({
  dropAuditLogIndexes: dropAuditLogIndexesMock,
  createAuditLogIndexes: createAuditLogIndexesMock,
}));
vi.mock("./sql", () => ({
  queryWithParams: queryWithParamsMock,
  insertArrowViaStaging: insertArrowViaStagingMock,
}));
vi.mock("./messaging", () => ({ log: logMock, reply: replyMock }));

import { handleImportAuditLog } from "./importAuditLog";

function mockFile(size: number): File {
  return {
    size,
    slice: () =>
      ({
        text: async () => "",
      }) as Blob,
  } as unknown as File;
}

function mockFileWithSampleReadError(size: number, message: string): File {
  return {
    size,
    slice: () =>
      ({
        text: async () => {
          throw new Error(message);
        },
      }) as unknown as Blob,
  } as unknown as File;
}

function mockStreamFile(content: string): File {
  const blob = new Blob([content], { type: "text/plain" });
  return {
    size: blob.size,
    slice: (start?: number, end?: number) => blob.slice(start, end),
    stream: () => blob.stream(),
  } as unknown as File;
}

const SAMPLE_BYTES = 256 * 1024;

function mockSampledFile(
  sampleContentsByStart: Record<number, string>,
  streamContent: string,
  options?: { size?: number; onStreamRead?: () => void }
): File {
  const size = options?.size ?? SAMPLE_BYTES * 2;
  const slices = new Map<string, Blob>(
    Object.entries(sampleContentsByStart).map(([start, content]) => {
      const s = Number(start);
      return [`${s}:${s + SAMPLE_BYTES}`, new Blob([content], { type: "text/plain" })];
    })
  );
  const streamBlob = new Blob([streamContent], { type: "text/plain" });
  return {
    size,
    slice: (start?: number, end?: number) =>
      slices.get(`${start ?? 0}:${end ?? size}`) ?? new Blob([""], { type: "text/plain" }),
    stream: () => {
      options?.onStreamRead?.();
      return streamBlob.stream();
    },
  } as unknown as File;
}

function mockFileWithHeadTailSamples(
  headSample: string,
  tailSample: string,
  streamContent: string
): File {
  const fakeSize = SAMPLE_BYTES * 2;
  return mockSampledFile({ 0: headSample, [fakeSize - SAMPLE_BYTES]: tailSample }, streamContent, {
    size: fakeSize,
  });
}

function mockFileWithHeadMiddleTailSamples(
  headSample: string,
  middleSample: string,
  tailSample: string,
  streamContent: string
): File {
  const fakeSize = SAMPLE_BYTES * 3;
  return mockSampledFile(
    { 0: headSample, [SAMPLE_BYTES]: middleSample, [SAMPLE_BYTES * 2]: tailSample },
    streamContent,
    { size: fakeSize }
  );
}

function mockLargeFileWithHeadMiddleTailSamples(
  headSample: string,
  middleSample: string,
  tailSample: string,
  streamContent: string,
  onStreamRead?: () => void
): File {
  const fakeSize = 128 * 1024 * 1024;
  const maxSampleStart = fakeSize - SAMPLE_BYTES;
  const middleStart = Math.floor(maxSampleStart / 2);
  const tailStart = maxSampleStart;
  return mockSampledFile(
    { 0: headSample, [middleStart]: middleSample, [tailStart]: tailSample },
    streamContent,
    { size: fakeSize, onStreamRead }
  );
}

function setupSuccessfulImportMocks(): void {
  const recordStmt = { query: vi.fn(async () => undefined), close: vi.fn(async () => undefined) };
  const templateStmt = {
    query: vi.fn(async () => undefined),
    close: vi.fn(async () => undefined),
  };
  const query = vi.fn(async () => undefined);
  const prepare = vi.fn().mockResolvedValueOnce(recordStmt).mockResolvedValueOnce(templateStmt);

  ensureDbMock.mockResolvedValue({ query, prepare });
  dropAuditLogIndexesMock.mockResolvedValue(undefined);
  createAuditLogIndexesMock.mockResolvedValue(undefined);
  queryWithParamsMock.mockResolvedValue(undefined);
  insertArrowViaStagingMock.mockResolvedValue(undefined);
}

function expectImportSuccess(requestId: string, result: Record<string, unknown>): void {
  expect(replyMock).toHaveBeenCalledWith(
    expect.objectContaining({
      type: "response",
      requestId,
      ok: true,
      result: expect.objectContaining(result),
    })
  );
}

function buildAuditLogRow(
  fieldValuesByIndex: Record<number, string>,
  options?: { fill?: string }
): string {
  const row = new Array(29).fill(options?.fill ?? "");
  for (const [index, value] of Object.entries(fieldValuesByIndex)) row[Number(index)] = value;
  return row.join(",");
}

describe("handleImportAuditLog index lifecycle", () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it("rebuilds indexes when single-transaction import fails after dropping indexes", async () => {
    const recordStmt = { query: vi.fn(async () => undefined), close: vi.fn(async () => undefined) };
    const templateStmt = {
      query: vi.fn(async () => undefined),
      close: vi.fn(async () => undefined),
    };
    const query = vi.fn(async (sql: string) => {
      if (sql === "BEGIN TRANSACTION") throw new Error("begin failed");
      return undefined;
    });
    const prepare = vi.fn().mockResolvedValueOnce(recordStmt).mockResolvedValueOnce(templateStmt);

    ensureDbMock.mockResolvedValue({ query, prepare });
    dropAuditLogIndexesMock.mockResolvedValue(undefined);
    createAuditLogIndexesMock.mockResolvedValue(undefined);
    queryWithParamsMock.mockResolvedValue(undefined);

    await expect(handleImportAuditLog("req-1", "dataset-1", mockFile(16))).rejects.toThrow(
      "begin failed"
    );

    expect(dropAuditLogIndexesMock).toHaveBeenCalledTimes(1);
    expect(createAuditLogIndexesMock).toHaveBeenCalledTimes(1);
    expect(prepare).toHaveBeenCalledTimes(2);
    expect(recordStmt.close).toHaveBeenCalledTimes(1);
    expect(templateStmt.close).toHaveBeenCalledTimes(1);
  });

  it("rebuilds indexes when failure happens before transaction starts", async () => {
    const query = vi.fn(async () => undefined);
    const prepare = vi.fn();

    ensureDbMock.mockResolvedValue({ query, prepare });
    dropAuditLogIndexesMock.mockResolvedValue(undefined);
    createAuditLogIndexesMock.mockResolvedValue(undefined);
    queryWithParamsMock.mockRejectedValueOnce(new Error("delete failed"));

    await expect(handleImportAuditLog("req-2", "dataset-2", mockFile(16))).rejects.toThrow(
      "delete failed"
    );

    expect(createAuditLogIndexesMock).toHaveBeenCalledTimes(1);
    expect(prepare).not.toHaveBeenCalled();
    expect(query).not.toHaveBeenCalledWith("ROLLBACK");
  });

  it("keeps original import error when rollback also fails", async () => {
    const recordStmt = { query: vi.fn(async () => undefined), close: vi.fn(async () => undefined) };
    const templateStmt = {
      query: vi.fn(async () => undefined),
      close: vi.fn(async () => undefined),
    };
    const query = vi.fn(async (sql: string) => {
      if (sql === "ROLLBACK") throw new Error("rollback failed");
      return undefined;
    });
    const prepare = vi.fn().mockResolvedValueOnce(recordStmt).mockResolvedValueOnce(templateStmt);

    ensureDbMock.mockResolvedValue({ query, prepare });
    dropAuditLogIndexesMock.mockResolvedValue(undefined);
    createAuditLogIndexesMock.mockResolvedValue(undefined);
    queryWithParamsMock.mockResolvedValue(undefined);

    await expect(
      handleImportAuditLog(
        "req-3",
        "dataset-3",
        mockFileWithSampleReadError(16, "sample read failed")
      )
    ).rejects.toThrow("sample read failed");

    expect(query).toHaveBeenCalledWith("ROLLBACK");
    expect(logMock).toHaveBeenCalledWith(
      "Single transaction rollback failed: rollback failed (original error: sample read failed)"
    );
    expect(createAuditLogIndexesMock).toHaveBeenCalledTimes(1);
    expect(recordStmt.close).toHaveBeenCalledTimes(1);
    expect(templateStmt.close).toHaveBeenCalledTimes(1);
  });

  it("exposes finalize failure when index rebuild fails after import error", async () => {
    const recordStmt = { query: vi.fn(async () => undefined), close: vi.fn(async () => undefined) };
    const templateStmt = {
      query: vi.fn(async () => undefined),
      close: vi.fn(async () => undefined),
    };
    const query = vi.fn(async () => undefined);
    const prepare = vi.fn().mockResolvedValueOnce(recordStmt).mockResolvedValueOnce(templateStmt);

    ensureDbMock.mockResolvedValue({ query, prepare });
    dropAuditLogIndexesMock.mockResolvedValue(undefined);
    createAuditLogIndexesMock.mockRejectedValueOnce(new Error("rebuild failed"));
    queryWithParamsMock.mockResolvedValue(undefined);

    let thrown: unknown;
    try {
      await handleImportAuditLog(
        "req-4",
        "dataset-4",
        mockFileWithSampleReadError(16, "sample read failed")
      );
    } catch (e) {
      thrown = e;
    }

    expect(thrown).toBeInstanceOf(Error);
    const message = (thrown as Error).message;
    expect(message).toContain("sample read failed");
    expect(message).toContain("Finalize failure: Rebuilding audit log indexes: rebuild failed");

    expect(createAuditLogIndexesMock).toHaveBeenCalled();
    expect(logMock).toHaveBeenCalledWith(
      "Rebuilding audit log indexes after failed import: rebuild failed"
    );
  });

  it("imports MySQL dump SQL via the shared ingestion pipeline", async () => {
    setupSuccessfulImportMocks();

    const sql =
      "INSERT INTO `audit_log` (`query_id`,`time`,`client_ip`,`user`,`db`,`state`,`error_code`,`error_message`,`query_time`,`scan_bytes`,`scan_rows`,`return_rows`,`frontend_ip`,`cpu_time_ms`,`peak_memory_bytes`,`workload_group`,`compute_group`,`stmt`) VALUES ('q-1','2026-01-05 06:44:17.002000','203.0.113.10:51178','test_user','test_db','EOF',0,'',13,10,11,12,'10.0.0.1',19,20,'wg1','cg1','select * from t1 where k = 1');";

    await handleImportAuditLog("req-dump-1", "dataset-dump-1", mockStreamFile(sql));

    expect(insertArrowViaStagingMock).toHaveBeenCalledWith(
      expect.anything(),
      "audit_log_records",
      expect.anything(),
      "insert"
    );
    expectImportSuccess("req-dump-1", {
      recordsInserted: 1,
      badRecords: 0,
      formatDetected: "auditLogMysqlDump",
    });
  });

  it("imports INSERT IGNORE INTO audit_log via worker pipeline", async () => {
    setupSuccessfulImportMocks();

    const sql =
      "INSERT IGNORE INTO `audit_log` (`query_id`,`time`,`client_ip`,`stmt`) VALUES ('q-ig-worker','2026-01-05 06:44:17.002000','203.0.113.10:51178','select 1');";

    await handleImportAuditLog(
      "req-dump-insert-ignore",
      "dataset-dump-insert-ignore",
      mockStreamFile(sql)
    );

    expectImportSuccess("req-dump-insert-ignore", {
      recordsInserted: 1,
      badStatements: 0,
      formatDetected: "auditLogMysqlDump",
    });
  });

  it("prefers MySQL dump detection over CSV delimiter inference", async () => {
    setupSuccessfulImportMocks();
    const sql = `INSERT INTO \`audit_log\` VALUES (${buildAuditLogRow(
      {
        0: "'q-csv-ambiguous'",
        1: "'2026-01-05 06:44:17.002000'",
        2: "'203.0.113.10:51178'",
        3: "'test_user'",
        5: "'test_db'",
        6: "'EOF'",
        7: "0",
        8: "''",
        9: "13",
        10: "10",
        11: "11",
        12: "12",
        21: "'10.0.0.1'",
        22: "19",
        25: "20",
        26: "'wg1'",
        27: "'cg1'",
        28: "'select 42'",
      },
      { fill: "NULL" }
    )});`;

    await handleImportAuditLog(
      "req-dump-csv-ambiguous",
      "dataset-dump-csv-ambiguous",
      mockStreamFile(sql)
    );

    expectImportSuccess("req-dump-csv-ambiguous", {
      recordsInserted: 1,
      badRecords: 0,
      formatDetected: "auditLogMysqlDump",
    });
  });

  it("keeps CSV format when stmt field only contains mysql marker substring", async () => {
    setupSuccessfulImportMocks();
    const csv = buildAuditLogRow({
      0: "q-csv-marker",
      1: "2026-01-05 06:44:17.002000",
      2: "203.0.113.10:51178",
      3: "test_user",
      5: "test_db",
      6: "EOF",
      7: "0",
      9: "13",
      10: "10",
      11: "11",
      12: "12",
      21: "10.0.0.1",
      22: "19",
      25: "20",
      26: "wg1",
      27: "cg1",
      28: "select 42; insert into audit_log values (1)",
    });

    await handleImportAuditLog("req-csv-marker", "dataset-csv-marker", mockStreamFile(csv));

    expectImportSuccess("req-csv-marker", {
      recordsInserted: 1,
      badRecords: 0,
      formatDetected: "auditLogOutfileCsv",
    });
  });

  it("detects MySQL dump by full scan when probe samples miss markers", async () => {
    setupSuccessfulImportMocks();
    const filler = new Array(29).fill("NULL");
    const firstLine = `INSERT INTO \`orders\` VALUES (${filler.join(",")});`;
    const dumpInsert =
      "INSERT INTO `audit_log` (`query_id`,`time`,`client_ip`,`user`,`db`,`state`,`error_code`,`error_message`,`query_time`,`scan_bytes`,`scan_rows`,`return_rows`,`frontend_ip`,`cpu_time_ms`,`peak_memory_bytes`,`workload_group`,`compute_group`,`stmt`) VALUES ('q-scan-fallback','2026-01-05 06:44:17.002000','203.0.113.10:51178','test_user','test_db','EOF',0,'',13,10,11,12,'10.0.0.1',19,20,'wg1','cg1','select 1');";
    const file = mockFileWithHeadMiddleTailSamples(
      `${firstLine}\n`,
      "-- middle sample without audit_log markers\n",
      "-- tail sample without audit_log markers\n",
      `${firstLine}\n${dumpInsert}\n`
    );

    await handleImportAuditLog("req-dump-scan-fallback", "dataset-dump-scan-fallback", file);

    expectImportSuccess("req-dump-scan-fallback", {
      recordsInserted: 1,
      badRecords: 0,
      formatDetected: "auditLogMysqlDump",
    });
  });

  it("full-scans SQL-like file when first line has no CSV delimiter", async () => {
    setupSuccessfulImportMocks();
    const dumpInsert =
      "INSERT INTO `audit_log` (`query_id`,`time`,`client_ip`,`user`,`db`,`state`,`error_code`,`error_message`,`query_time`,`scan_bytes`,`scan_rows`,`return_rows`,`frontend_ip`,`cpu_time_ms`,`peak_memory_bytes`,`workload_group`,`compute_group`,`stmt`) VALUES ('q-no-delim','2026-01-05 06:44:17.002000','203.0.113.10:51178','test_user','test_db','EOF',0,'',13,10,11,12,'10.0.0.1',19,20,'wg1','cg1','select 1');";
    const file = mockFileWithHeadMiddleTailSamples(
      "-- mysql dump preface without csv delimiter\n",
      "-- middle sample without audit_log markers\n",
      "-- tail sample without audit_log markers\n",
      `${dumpInsert}\n`
    );

    await handleImportAuditLog("req-dump-no-delim", "dataset-dump-no-delim", file);

    expectImportSuccess("req-dump-no-delim", {
      recordsInserted: 1,
      badRecords: 0,
      formatDetected: "auditLogMysqlDump",
    });
  });

  it("detects MySQL dump by full scan for large files when probe samples miss markers", async () => {
    setupSuccessfulImportMocks();
    const filler = new Array(29).fill("NULL");
    const firstLine = `INSERT INTO \`orders\` VALUES (${filler.join(",")});`;
    const dumpInsert =
      "INSERT INTO `audit_log` (`query_id`,`time`,`client_ip`,`user`,`db`,`state`,`error_code`,`error_message`,`query_time`,`scan_bytes`,`scan_rows`,`return_rows`,`frontend_ip`,`cpu_time_ms`,`peak_memory_bytes`,`workload_group`,`compute_group`,`stmt`) VALUES ('q-large-fallback','2026-01-05 06:44:17.002000','203.0.113.10:51178','test_user','test_db','EOF',0,'',13,10,11,12,'10.0.0.1',19,20,'wg1','cg1','select 1');";
    const file = mockLargeFileWithHeadMiddleTailSamples(
      `${firstLine}\n`,
      "-- middle sample without audit_log markers\n",
      "-- tail sample without audit_log markers\n",
      `${firstLine}\n${dumpInsert}\n`
    );

    await handleImportAuditLog(
      "req-dump-large-scan-fallback",
      "dataset-dump-large-scan-fallback",
      file
    );

    expectImportSuccess("req-dump-large-scan-fallback", {
      recordsInserted: 1,
      badRecords: 0,
      formatDetected: "auditLogMysqlDump",
    });
  });

  it("avoids full dump marker scan for large CSV-like input", async () => {
    setupSuccessfulImportMocks();
    const csv = buildAuditLogRow({
      0: "q-large-csv",
      1: "2026-01-05 06:44:17.002000",
      2: "203.0.113.10:51178",
      3: "test_user",
      5: "test_db",
      6: "EOF",
      7: "0",
      9: "13",
      10: "10",
      11: "11",
      12: "12",
      21: "10.0.0.1",
      22: "19",
      25: "20",
      26: "wg1",
      27: "cg1",
      28: "select 42",
    });
    let streamReadCount = 0;
    const file = mockLargeFileWithHeadMiddleTailSamples(
      `${csv}\n`,
      "q-large-csv-middle,2026-01-05 06:44:17.002000,203.0.113.11:51178,test_user,,test_db,EOF,0,,13,10,11,12,,,,,,,,,10.0.0.2,19,,,20,wg1,cg1,select 43\n",
      "q-large-csv-tail,2026-01-05 06:44:17.002000,203.0.113.12:51178,test_user,,test_db,EOF,0,,13,10,11,12,,,,,,,,,10.0.0.3,19,,,20,wg1,cg1,select 44\n",
      `${csv}\n`,
      () => {
        streamReadCount++;
      }
    );

    await handleImportAuditLog("req-large-csv-no-prescan", "dataset-large-csv-no-prescan", file);

    expect(streamReadCount).toBe(1);
    expectImportSuccess("req-large-csv-no-prescan", {
      recordsInserted: 1,
      badRecords: 0,
      formatDetected: "auditLogOutfileCsv",
    });
  });

  it("does not classify as MySQL dump when probe slice starts in the middle of CSV stmt", async () => {
    setupSuccessfulImportMocks();
    const csv = buildAuditLogRow({
      0: "q-csv-probe-boundary",
      1: "2026-01-05 06:44:17.002000",
      2: "203.0.113.10:51178",
      3: "test_user",
      5: "test_db",
      6: "EOF",
      7: "0",
      9: "13",
      10: "10",
      11: "11",
      12: "12",
      21: "10.0.0.1",
      22: "19",
      25: "20",
      26: "wg1",
      27: "cg1",
      28: "select 42",
    });
    const file = mockLargeFileWithHeadMiddleTailSamples(
      `${csv}\n`,
      " insert into audit_log values (1) this is sliced from inside csv stmt\n",
      "-- tail sample without dump markers\n",
      `${csv}\n`
    );

    await handleImportAuditLog("req-csv-probe-boundary", "dataset-csv-probe-boundary", file);

    expectImportSuccess("req-csv-probe-boundary", {
      recordsInserted: 1,
      badRecords: 0,
      formatDetected: "auditLogOutfileCsv",
    });
  });

  it("filters known MySQL dump noise statements", async () => {
    setupSuccessfulImportMocks();

    const sql =
      "INSERT INTO `audit_log` (`query_id`,`time`,`client_ip`,`stmt`) VALUES ('q-noise','2026-01-05 06:44:17.002000','203.0.113.10:51178','select @@version_comment limit 1'),('q-user','2026-01-05 06:44:18.002000','203.0.113.10:51178','select * from orders where o_orderkey = 1');";

    await handleImportAuditLog("req-dump-noise", "dataset-dump-noise", mockStreamFile(sql));

    expectImportSuccess("req-dump-noise", {
      recordsInserted: 1,
      badRecords: 0,
      filteredRecords: 1,
    });
    expect(logMock).toHaveBeenCalledWith("mysql-dump-import: skipped 1 known noise records.");
  });

  it("detects MySQL dump from tail sample when head sample has no insert markers", async () => {
    setupSuccessfulImportMocks();
    const streamSql =
      "INSERT INTO `audit_log` (`query_id`,`time`,`client_ip`,`stmt`) VALUES ('q-tail','2026-01-05 06:44:17.002000','203.0.113.10:51178','select 1');";
    const file = mockFileWithHeadTailSamples(
      "-- long preface without insert markers\n",
      "INSERT INTO `audit_log` VALUES (1);\n",
      streamSql
    );

    await handleImportAuditLog("req-dump-tail", "dataset-dump-tail", file);

    expectImportSuccess("req-dump-tail", { recordsInserted: 1, badRecords: 0 });
  });

  it("detects MySQL dump from middle sample when head and tail have no markers", async () => {
    setupSuccessfulImportMocks();
    const streamSql =
      "INSERT INTO `audit_log` (`query_id`,`time`,`client_ip`,`stmt`) VALUES ('q-middle','2026-01-05 06:44:17.002000','203.0.113.10:51178','select 1');";
    const file = mockFileWithHeadMiddleTailSamples(
      "-- head without markers\n",
      "CREATE TABLE `__internal_schema`.`audit_log` (`id` int);\n",
      "-- tail without markers\n",
      streamSql
    );

    await handleImportAuditLog("req-dump-middle", "dataset-dump-middle", file);

    expectImportSuccess("req-dump-middle", { recordsInserted: 1, badRecords: 0 });
  });

  it("continues import when MySQL dump contains malformed INSERT statements", async () => {
    setupSuccessfulImportMocks();
    const sql = [
      "INSERT INTO `audit_log` (`query_id`,`time`,`client_ip`,`stmt`) VALUES ('q-good','2026-01-05 06:44:17.002000','203.0.113.10:51178','select 1');",
      "INSERT INTO `audit_log` (`query_id`,`time`,`client_ip`,`stmt`) VALUES ('q-bad','2026-01-05 06:44:17.002000','203.0.113.10:51178','select 1';",
    ].join("\n");

    await handleImportAuditLog("req-dump-badstmt", "dataset-dump-badstmt", mockStreamFile(sql));

    expect(logMock).toHaveBeenCalledWith(expect.stringContaining("skipped malformed statement #2"));
    expect(logMock).toHaveBeenCalledWith(
      "mysql-dump-import: skipped 1 malformed or oversized INSERT statements and continued import."
    );
    expectImportSuccess("req-dump-badstmt", {
      recordsInserted: 1,
      badStatements: 1,
      formatDetected: "auditLogMysqlDump",
    });
    expect(queryWithParamsMock).toHaveBeenCalledTimes(2);
  });

  it("does not fail when all matched MySQL dump INSERT statements are malformed", async () => {
    setupSuccessfulImportMocks();
    const sql =
      "INSERT INTO `audit_log` (`query_id`,`time`,`client_ip`,`stmt`) VALUES ('q-bad','2026-01-05 06:44:17.002000','203.0.113.10:51178','select 1';";

    await handleImportAuditLog(
      "req-dump-all-badstmt",
      "dataset-dump-all-badstmt",
      mockStreamFile(sql)
    );

    expect(logMock).toHaveBeenCalledWith(expect.stringContaining("skipped malformed statement #1"));
    expect(logMock).toHaveBeenCalledWith(
      "mysql-dump-import: skipped 1 malformed or oversized INSERT statements and continued import."
    );
    expect(logMock).not.toHaveBeenCalledWith(
      "Import failed, cleaning up partially committed data for this dataset."
    );
    expectImportSuccess("req-dump-all-badstmt", {
      recordsInserted: 0,
      badStatements: 1,
      formatDetected: "auditLogMysqlDump",
    });
    expect(queryWithParamsMock).toHaveBeenCalledTimes(2);
  });
});
