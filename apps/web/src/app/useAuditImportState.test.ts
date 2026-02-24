import { describe, expect, it, vi } from "vitest";
import type { DbClient } from "../db/client/dbClient";
import type { OverviewResult } from "../db/client/protocol";
import { normalizeAuditImportError, runAuditImportFlow } from "./useAuditImportState";

describe("normalizeAuditImportError", () => {
  it("returns null for aborted import", () => {
    expect(normalizeAuditImportError("Aborted")).toBeNull();
  });

  it("maps wasm out-of-bounds to actionable message", () => {
    const message = normalizeAuditImportError("RuntimeError: memory access out of bounds");
    expect(message).toContain("local analysis engine crashed");
    expect(message).toContain("browser memory limits");
  });

  it("passes through regular error message", () => {
    expect(normalizeAuditImportError("import failed")).toBe("import failed");
  });
});

describe("runAuditImportFlow", () => {
  it("runs import, refreshes overview, and updates time filters", async () => {
    const importProgress = {
      bytesRead: 10,
      bytesTotal: 100,
      recordsParsed: 8,
      recordsInserted: 8,
      badRecords: 0,
    };
    const overview: OverviewResult = {
      records: 8,
      totalCpuMs: 120,
      totalTimeMs: 240,
      failedCount: 0,
      p95TimeMs: 50,
      p99TimeMs: 80,
      minTimeMs: 1_700_000_000_000,
      maxTimeMs: 1_700_000_001_000,
      approxDistinctUsers: 1,
      approxDistinctDbs: 1,
      approxDistinctClientIps: 1,
      approxDistinctStrippedTemplates: 1,
      topDbsByCpu: [],
      topTablesByCpu: [],
      topUsersByCpu: [],
      topClientIpsByCpu: [],
    };

    const client = {
      importAuditLog: vi.fn(async (_datasetId, _file, onProgress) => {
        onProgress(importProgress);
      }),
      queryOverview: vi.fn(async () => overview),
    } as unknown as DbClient;

    const setImporting = vi.fn();
    const setImportProgress = vi.fn();
    const setFiltersBoth = vi.fn();
    const setImportError = vi.fn();
    const overviewQuery = { cancel: vi.fn(), setData: vi.fn() };
    const topSqlQuery = { cancel: vi.fn() };
    const shareQuery = { cancel: vi.fn() };

    await runAuditImportFlow({
      client,
      datasetId: "dataset-1",
      file: {} as File,
      filters: { excludeInternal: true, dbName: "db_a", startMs: 1, endMs: 2 },
      setImporting,
      setImportProgress,
      setFiltersBoth,
      setImportError,
      overviewQuery,
      topSqlQuery,
      shareQuery,
    });

    expect(overviewQuery.cancel).toHaveBeenCalledTimes(1);
    expect(topSqlQuery.cancel).toHaveBeenCalledTimes(1);
    expect(shareQuery.cancel).toHaveBeenCalledTimes(1);
    expect(setImporting).toHaveBeenNthCalledWith(1, true);
    expect(setImporting).toHaveBeenNthCalledWith(2, false);
    expect(setImportProgress).toHaveBeenNthCalledWith(1, null);
    expect(setImportProgress).toHaveBeenNthCalledWith(2, importProgress);
    expect(client.queryOverview).toHaveBeenCalledWith("dataset-1", {
      excludeInternal: true,
      dbName: "db_a",
      startMs: undefined,
      endMs: undefined,
    });
    expect(overviewQuery.setData).toHaveBeenCalledWith(overview);
    expect(setFiltersBoth).toHaveBeenCalledWith({
      excludeInternal: true,
      dbName: "db_a",
      startMs: 1_700_000_000_000,
      endMs: 1_700_000_001_001,
    });
    expect(setImportError).toHaveBeenCalledTimes(1);
    expect(setImportError).toHaveBeenCalledWith(null);
  });

  it("ignores aborted import errors", async () => {
    const client = {
      importAuditLog: vi.fn(async () => {
        throw new Error("Aborted");
      }),
      queryOverview: vi.fn(),
    } as unknown as DbClient;

    const setImporting = vi.fn();
    const setImportProgress = vi.fn();
    const setFiltersBoth = vi.fn();
    const setImportError = vi.fn();
    const overviewQuery = { cancel: vi.fn(), setData: vi.fn() };
    const topSqlQuery = { cancel: vi.fn() };
    const shareQuery = { cancel: vi.fn() };

    await runAuditImportFlow({
      client,
      datasetId: "dataset-1",
      file: {} as File,
      filters: { excludeInternal: true },
      setImporting,
      setImportProgress,
      setFiltersBoth,
      setImportError,
      overviewQuery,
      topSqlQuery,
      shareQuery,
    });

    expect(setImportError).toHaveBeenCalledTimes(1);
    expect(setImportError).toHaveBeenCalledWith(null);
    expect(setImporting).toHaveBeenNthCalledWith(1, true);
    expect(setImporting).toHaveBeenNthCalledWith(2, false);
  });

  it("no-ops when dataset is missing", async () => {
    const client = {
      importAuditLog: vi.fn(),
      queryOverview: vi.fn(),
    } as unknown as DbClient;

    const setImporting = vi.fn();
    const setImportProgress = vi.fn();
    const setFiltersBoth = vi.fn();
    const setImportError = vi.fn();
    const overviewQuery = { cancel: vi.fn(), setData: vi.fn() };
    const topSqlQuery = { cancel: vi.fn() };
    const shareQuery = { cancel: vi.fn() };

    await runAuditImportFlow({
      client,
      datasetId: null,
      file: {} as File,
      filters: { excludeInternal: true },
      setImporting,
      setImportProgress,
      setFiltersBoth,
      setImportError,
      overviewQuery,
      topSqlQuery,
      shareQuery,
    });

    expect(client.importAuditLog).not.toHaveBeenCalled();
    expect(setImporting).not.toHaveBeenCalled();
    expect(overviewQuery.cancel).not.toHaveBeenCalled();
  });
});
