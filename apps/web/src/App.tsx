import { useCallback, useMemo, useState } from "react";
import {
  AgentClient,
  type DorisConnectionInfo,
  type DorisConnectionInput,
  isDorisConnectionConfigured,
} from "./agent/agentClient";
import { useDiagnosticsNavigation } from "./app/diagnosticsNavigation";
import { toErrorMessage, useDatasetQueries, useDuckDbSession } from "./app/hooks";
import AuditWorkspace from "./components/AuditWorkspace";
import DiagnosticsShell from "./components/DiagnosticsShell";
import DorisAuditLogImportModal from "./components/DorisAuditLogImportModal";
import DorisConnectionModal from "./components/DorisConnectionModal";
import ExplainWorkspace from "./components/ExplainWorkspace";
import TemplateDetailDrawer, { type TemplateRef } from "./components/TemplateDetailDrawer";
import { DbClient } from "./db/client/dbClient";
import type {
  ImportProgress,
  QueryFilters,
  ShareRankBy,
  ShareRow,
  TopSqlRow,
} from "./db/client/protocol";

const DEFAULT_FILTERS: QueryFilters = { excludeInternal: true };
const DUCKDB_WASM_OOB_ERROR =
  "The local analysis engine crashed due to browser memory limits.\nTry: reload the page and re-import, or switch to a Chromium-based browser.\nIf it persists, please share a small log snippet that reproduces the issue.";

const DORIS_SESSION_KEY = "doris.connection.info.v1";

function readDorisSessionInfo(): DorisConnectionInfo | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = window.sessionStorage.getItem(DORIS_SESSION_KEY);
    if (!raw) return null;
    const v = JSON.parse(raw) as any;
    if (!v || typeof v.host !== "string") return null;
    const port = Number(v.port ?? 0);
    if (!Number.isFinite(port) || port <= 0 || port > 65535) return null;
    const user = String(v.user ?? "");
    if (!user) return null;
    return { host: v.host, port, user };
  } catch {
    return null;
  }
}

function writeDorisSessionInfo(info: DorisConnectionInfo | null): void {
  if (typeof window === "undefined") return;
  try {
    if (!info) window.sessionStorage.removeItem(DORIS_SESSION_KEY);
    else window.sessionStorage.setItem(DORIS_SESSION_KEY, JSON.stringify(info));
  } catch {
    // Ignore storage quota / access errors.
  }
}

export default function App(): JSX.Element {
  const client = useMemo(() => new DbClient(), []);
  const agent = useMemo(() => new AgentClient(), []);
  const isCoi = typeof window !== "undefined" && window.crossOriginIsolated === true;

  const [error, setError] = useState<string | null>(null);
  const { ready, datasetId, retryInit, retryCreateDataset } = useDuckDbSession(client, setError);

  const [importing, setImporting] = useState(false);
  const [importProgress, setImportProgress] = useState<ImportProgress | null>(null);

  const [filtersDraft, setFiltersDraft] = useState<QueryFilters>(DEFAULT_FILTERS);
  const [filters, setFilters] = useState<QueryFilters>(DEFAULT_FILTERS);
  const { activeModule, activeAuditTab, switchModule, switchAuditTab } = useDiagnosticsNavigation();

  const [topSqlSearch, setTopSqlSearch] = useState("");
  const [templateDrawer, setTemplateDrawer] = useState<TemplateRef | null>(null);

  const [explainSql, setExplainSql] = useState("");

  const [shareMetric, setShareMetric] = useState<"cpu" | "time" | "memory">("cpu");
  const [shareRankBy, setShareRankBy] = useState<ShareRankBy>("totalCpuMs");
  const [shareChartType, setShareChartType] = useState<"bar" | "pie">("bar");
  const [shareTopN, setShareTopN] = useState(12);

  const [dorisModalOpen, setDorisModalOpen] = useState(false);
  const [dorisImportOpen, setDorisImportOpen] = useState(false);
  const [dorisConn, setDorisConn] = useState<DorisConnectionInput | null>(() => {
    const info = readDorisSessionInfo();
    return info ? { ...info, password: "" } : null;
  });
  const openDorisModal = useCallback(() => setDorisModalOpen(true), []);
  const closeDorisModal = useCallback(() => setDorisModalOpen(false), []);
  const openDorisImport = useCallback(() => setDorisImportOpen(true), []);
  const closeDorisImport = useCallback(() => setDorisImportOpen(false), []);

  const setFiltersBoth = (next: QueryFilters) => {
    setFiltersDraft(next);
    setFilters(next);
  };

  const patchFiltersDraft = (patch: Partial<QueryFilters>) => {
    setFiltersDraft((prev) => ({ ...prev, ...patch }));
  };

  const patchFiltersBoth = (patch: Partial<QueryFilters>) => {
    setFiltersDraft((prev) => {
      const next = { ...prev, ...patch };
      setFilters(next);
      return next;
    });
  };

  const { overviewQuery, topSqlQuery, shareQuery } = useDatasetQueries({
    client,
    datasetId,
    importing,
    activeTab: activeModule === "audit" ? activeAuditTab : null,
    filters,
    shareTopN,
    shareRankBy,
    setError,
  });

  const runImport = async (file: File) => {
    if (!datasetId) return;
    setError(null);
    setImporting(true);
    [overviewQuery, topSqlQuery, shareQuery].forEach((q) => q.cancel());
    setImportProgress(null);
    try {
      await client.importAuditLog(datasetId, file, (p) => setImportProgress(p));

      const baseFilters: QueryFilters = { ...filters, startMs: undefined, endMs: undefined };
      const ov = await client.queryOverview(datasetId, baseFilters);
      overviewQuery.setData(ov);
      if (ov.minTimeMs != null && ov.maxTimeMs != null) {
        const next: QueryFilters = {
          ...filters,
          startMs: ov.minTimeMs,
          endMs: ov.maxTimeMs + 1,
        };
        setFiltersBoth(next);
      }
    } catch (e) {
      const message = toErrorMessage(e);
      if (message === "Aborted") return;
      if (message.toLowerCase().includes("memory access out of bounds")) {
        setError(DUCKDB_WASM_OOB_ERROR);
        return;
      }
      setError(message);
    } finally {
      setImporting(false);
    }
  };

  const openTemplateFromTopSql = (row: TopSqlRow) =>
    setTemplateDrawer({
      templateHash: row.templateHash,
      template: row.template,
      tableGuess: row.tableGuess,
      execCount: row.execCount,
      totalCpuMs: row.totalCpuMs,
      totalTimeMs: row.totalTimeMs,
      p95TimeMs: row.p95TimeMs,
      maxPeakMemBytes: row.maxPeakMemBytes,
    });

  const openTemplateFromShare = (row: ShareRow) =>
    setTemplateDrawer({
      templateHash: row.templateHash,
      template: row.template,
      execCount: row.execCount,
      totalCpuMs: row.totalCpuMs,
      totalTimeMs: row.totalTimeMs,
      maxPeakMemBytes: row.maxPeakMemBytes,
    });

  const dorisLabel = dorisConn ? `${dorisConn.user}@${dorisConn.host}:${dorisConn.port}` : "-";
  const dorisConfigured = isDorisConnectionConfigured(dorisConn);

  return (
    <>
      <DiagnosticsShell
        ready={ready}
        isCoi={isCoi}
        dorisLabel={dorisLabel}
        activeModule={activeModule}
        onSwitchModule={switchModule}
        onOpenDoris={openDorisModal}
        error={error}
        onRetryInit={retryInit}
        canRetrySession={ready && !datasetId}
        onRetrySession={retryCreateDataset}
        onDismissError={() => setError(null)}
      >
        {activeModule === "audit" ? (
          <AuditWorkspace
            ready={ready}
            datasetId={datasetId}
            importing={importing}
            importProgress={importProgress}
            dorisConfigured={dorisConfigured}
            onOpenDoris={openDorisModal}
            onOpenDorisImport={openDorisImport}
            onImport={runImport}
            onCancel={() => void client.cancelCurrentTask().catch(() => undefined)}
            overview={overviewQuery.data}
            filtersDraft={filtersDraft}
            onPatchFiltersDraft={patchFiltersDraft}
            onApplyFilters={() => setFilters(filtersDraft)}
            onSetFiltersBoth={setFiltersBoth}
            activeAuditTab={activeAuditTab}
            onSwitchAuditTab={switchAuditTab}
            overviewLoading={overviewQuery.loading}
            onRefreshOverview={overviewQuery.reload}
            onPatchFiltersBoth={patchFiltersBoth}
            onJumpToTopSqlByTable={(tableName) => {
              setTopSqlSearch(tableName);
              switchAuditTab("topSql");
            }}
            topSqlLoading={topSqlQuery.loading}
            topSqlRows={topSqlQuery.data}
            topSqlSearch={topSqlSearch}
            onChangeTopSqlSearch={setTopSqlSearch}
            onRefreshTopSql={topSqlQuery.reload}
            onOpenTopSqlTemplate={openTemplateFromTopSql}
            shareLoading={shareQuery.loading}
            shareRows={shareQuery.data}
            shareMetric={shareMetric}
            shareRankBy={shareRankBy}
            shareChartType={shareChartType}
            shareTopN={shareTopN}
            onChangeShareMetric={(m) => {
              setShareMetric(m);
              if (m === "memory") setShareRankBy("maxPeakMemBytes");
              else if (shareRankBy === "maxPeakMemBytes") {
                setShareRankBy(m === "time" ? "totalTimeMs" : "totalCpuMs");
              }
            }}
            onChangeShareRankBy={setShareRankBy}
            onChangeShareChartType={setShareChartType}
            onChangeShareTopN={setShareTopN}
            onRefreshShare={shareQuery.reload}
            onOpenShareTemplate={openTemplateFromShare}
          />
        ) : (
          <ExplainWorkspace
            agent={agent}
            dorisConn={dorisConn}
            dorisConfigured={dorisConfigured}
            onOpenDoris={openDorisModal}
            sql={explainSql}
            onChangeSql={setExplainSql}
          />
        )}
      </DiagnosticsShell>

      <TemplateDetailDrawer
        open={templateDrawer != null}
        onClose={() => setTemplateDrawer(null)}
        client={client}
        datasetId={datasetId}
        template={templateDrawer}
        filters={filters}
      />

      <DorisConnectionModal
        open={dorisModalOpen}
        onClose={closeDorisModal}
        agent={agent}
        current={dorisConn}
        onSave={(conn, rememberInfo) => {
          setDorisConn(conn);
          writeDorisSessionInfo(
            rememberInfo ? { host: conn.host, port: conn.port, user: conn.user } : null
          );
        }}
      />
      <DorisAuditLogImportModal
        open={dorisImportOpen}
        onClose={closeDorisImport}
        agent={agent}
        connection={dorisConn}
        onOpenDoris={openDorisModal}
        importing={importing}
        onImport={runImport}
      />
    </>
  );
}
