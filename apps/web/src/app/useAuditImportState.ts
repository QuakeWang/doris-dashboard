import { useCallback } from "react";
import type { DbClient } from "../db/client/dbClient";
import type {
  ImportProgress,
  OverviewResult,
  QueryFilters,
  ShareRow,
  TopSqlRow,
} from "../db/client/protocol";
import type { AsyncData } from "../utils/useAsync";
import { toErrorMessage } from "./hooks";

const DUCKDB_WASM_OOB_ERROR =
  "The local analysis engine crashed due to browser memory limits.\nTry: reload the page and re-import, or switch to a Chromium-based browser.\nIf it persists, please share a small log snippet that reproduces the issue.";

export interface AuditImportState {
  importing: boolean;
  importProgress: ImportProgress | null;
  runImport: (file: File) => void;
  clearImportError: () => void;
}

interface UseAuditImportStateParams {
  client: DbClient;
  datasetId: string | null;
  importing: boolean;
  setImporting: (value: boolean) => void;
  importProgress: ImportProgress | null;
  setImportProgress: (value: ImportProgress | null) => void;
  filters: QueryFilters;
  setFiltersBoth: (next: QueryFilters) => void;
  setImportError: (value: string | null) => void;
  overviewQuery: Pick<AsyncData<OverviewResult | null>, "cancel" | "setData">;
  topSqlQuery: Pick<AsyncData<TopSqlRow[]>, "cancel">;
  shareQuery: Pick<AsyncData<ShareRow[]>, "cancel">;
}

export interface RunAuditImportFlowParams {
  client: DbClient;
  datasetId: string | null;
  file: File;
  filters: QueryFilters;
  setImporting: (value: boolean) => void;
  setImportProgress: (value: ImportProgress | null) => void;
  setFiltersBoth: (next: QueryFilters) => void;
  setImportError: (value: string | null) => void;
  overviewQuery: Pick<AsyncData<OverviewResult | null>, "cancel" | "setData">;
  topSqlQuery: Pick<AsyncData<TopSqlRow[]>, "cancel">;
  shareQuery: Pick<AsyncData<ShareRow[]>, "cancel">;
}

export function normalizeAuditImportError(message: string): string | null {
  if (message === "Aborted") return null;
  if (message.toLowerCase().includes("memory access out of bounds")) {
    return DUCKDB_WASM_OOB_ERROR;
  }
  return message;
}

export async function runAuditImportFlow(params: RunAuditImportFlowParams): Promise<void> {
  const {
    client,
    datasetId,
    file,
    filters,
    setImporting,
    setImportProgress,
    setFiltersBoth,
    setImportError,
    overviewQuery,
    topSqlQuery,
    shareQuery,
  } = params;

  if (!datasetId) return;
  setImportError(null);
  setImporting(true);
  [overviewQuery, topSqlQuery, shareQuery].forEach((q) => q.cancel());
  setImportProgress(null);
  try {
    await client.importAuditLog(datasetId, file, (p) => setImportProgress(p));

    const baseFilters: QueryFilters = { ...filters, startMs: undefined, endMs: undefined };
    const ov = await client.queryOverview(datasetId, baseFilters);
    overviewQuery.setData(ov);
    if (ov.minTimeMs != null && ov.maxTimeMs != null) {
      setFiltersBoth({ ...filters, startMs: ov.minTimeMs, endMs: ov.maxTimeMs + 1 });
    }
  } catch (e) {
    const message = normalizeAuditImportError(toErrorMessage(e));
    if (message) setImportError(message);
  } finally {
    setImporting(false);
  }
}

export function useAuditImportState(params: UseAuditImportStateParams): AuditImportState {
  const {
    client,
    datasetId,
    importing,
    setImporting,
    importProgress,
    setImportProgress,
    filters,
    setFiltersBoth,
    setImportError,
    overviewQuery,
    topSqlQuery,
    shareQuery,
  } = params;

  const runImport = useCallback(
    (file: File) => {
      void runAuditImportFlow({
        client,
        datasetId,
        file,
        filters,
        setImporting,
        setImportProgress,
        setFiltersBoth,
        setImportError,
        overviewQuery,
        topSqlQuery,
        shareQuery,
      });
    },
    [
      client,
      datasetId,
      filters,
      overviewQuery,
      setImportError,
      setFiltersBoth,
      setImporting,
      setImportProgress,
      shareQuery,
      topSqlQuery,
    ]
  );

  const clearImportError = useCallback(() => setImportError(null), [setImportError]);

  return {
    importing,
    importProgress,
    runImport,
    clearImportError,
  };
}
