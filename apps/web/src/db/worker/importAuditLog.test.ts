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
});
