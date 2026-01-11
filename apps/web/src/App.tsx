import { Alert, Button, Card, Layout, Space, Spin, Tabs, Typography } from "antd";
import { useCallback, useMemo, useState } from "react";
import {
  AgentClient,
  type DorisConnectionInfo,
  type DorisConnectionInput,
  isDorisConnectionConfigured,
} from "./agent/agentClient";
import {
  type TabKey,
  toErrorMessage,
  useDatasetQueries,
  useDuckDbSession,
  useTabState,
} from "./app/hooks";
import AppHeader from "./components/AppHeader";
import DorisAuditLogImportModal from "./components/DorisAuditLogImportModal";
import DorisConnectionModal from "./components/DorisConnectionModal";
import FiltersCard from "./components/FiltersCard";
import ImportCard from "./components/ImportCard";
import OverviewTab from "./components/OverviewTab";
import ShareTab from "./components/ShareTab";
import TemplateDetailDrawer, { type TemplateRef } from "./components/TemplateDetailDrawer";
import TopSqlTab from "./components/TopSqlTab";
import { DbClient } from "./db/client/dbClient";
import type {
  ImportProgress,
  QueryFilters,
  ShareRankBy,
  ShareRow,
  TopSqlRow,
} from "./db/client/protocol";

const { Content } = Layout;
const { Text } = Typography;

const DEFAULT_FILTERS: QueryFilters = { excludeInternal: true };
const DUCKDB_WASM_OOB_ERROR =
  "DuckDB wasm crashed with 'memory access out of bounds'.\nThis is usually triggered by large inserts or a wasm runtime limitation.\nTry: reload the page and re-import; or switch to a Chromium-based browser.\nIf it persists, please share a small log snippet that reproduces the crash.";

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
  const { ready, datasetId, createDataset, retryInit } = useDuckDbSession(client, setError);

  const [importing, setImporting] = useState(false);
  const [importProgress, setImportProgress] = useState<ImportProgress | null>(null);

  const [filtersDraft, setFiltersDraft] = useState<QueryFilters>(DEFAULT_FILTERS);
  const [filters, setFilters] = useState<QueryFilters>(DEFAULT_FILTERS);

  const { activeTab, setTab } = useTabState();

  const [topSqlSearch, setTopSqlSearch] = useState("");
  const [templateDrawer, setTemplateDrawer] = useState<TemplateRef | null>(null);

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
    activeTab,
    filters,
    shareTopN,
    shareRankBy,
    setError,
  });

  const createNewDataset = async () => {
    setError(null);
    [overviewQuery, topSqlQuery, shareQuery].forEach((q) => q.reset());
    setTemplateDrawer(null);
    setImportProgress(null);
    try {
      await createDataset(`session-${Date.now()}`);
    } catch (e) {
      setError(toErrorMessage(e));
    }
  };

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
    <Layout style={{ minHeight: "100vh" }}>
      <AppHeader
        ready={ready}
        importing={importing}
        datasetId={datasetId}
        isCoi={isCoi}
        dorisLabel={dorisLabel}
        onNewDataset={createNewDataset}
        onOpenDoris={openDorisModal}
      />

      <Content style={{ padding: 24, maxWidth: 1360, width: "100%", margin: "0 auto" }}>
        {!ready && !error && (
          <Card>
            <Spin /> <Text>Initializing DuckDB...</Text>
          </Card>
        )}
        {error && (
          <Alert
            type="error"
            message="Error"
            description={<Text style={{ whiteSpace: "pre-wrap" }}>{error}</Text>}
            showIcon
            closable={ready}
            action={
              !ready ? (
                <Space>
                  <Button size="small" onClick={retryInit}>
                    Retry
                  </Button>
                  <Button
                    size="small"
                    onClick={() => {
                      if (typeof window !== "undefined") window.location.reload();
                    }}
                  >
                    Reload
                  </Button>
                </Space>
              ) : null
            }
            style={{ marginBottom: 12 }}
            onClose={() => setError(null)}
          />
        )}
        {ready && !isCoi && (
          <Alert
            type="warning"
            message="Performance warning"
            description={
              <Text style={{ whiteSpace: "pre-wrap" }}>
                crossOriginIsolated is disabled. DuckDB threads may be unavailable and import can be
                slower.
                {"\n"}For local dev, restart `npm run dev`.
                {"\n"}For production, serve with COOP/COEP headers.
              </Text>
            }
            showIcon
            style={{ marginBottom: 12 }}
          />
        )}

        <ImportCard
          ready={ready}
          datasetId={datasetId}
          importing={importing}
          importProgress={importProgress}
          dorisConfigured={dorisConfigured}
          onOpenDoris={openDorisModal}
          onOpenDorisImport={openDorisImport}
          onImport={runImport}
          onCancel={() => void client.cancelCurrentTask().catch(() => undefined)}
        />

        <FiltersCard
          datasetId={datasetId}
          importing={importing}
          overview={overviewQuery.data}
          draft={filtersDraft}
          onPatchDraft={patchFiltersDraft}
          onApply={() => setFilters(filtersDraft)}
          onSetBoth={setFiltersBoth}
        />

        <Tabs
          centered
          activeKey={activeTab}
          onChange={(k) => setTab(k as TabKey)}
          items={[
            {
              key: "overview",
              label: "Overview",
              children: (
                <OverviewTab
                  datasetId={datasetId}
                  importing={importing}
                  loading={overviewQuery.loading}
                  overview={overviewQuery.data}
                  onRefresh={overviewQuery.reload}
                  onPatchFilters={patchFiltersBoth}
                  onJumpToTopSqlByTable={(tableName) => {
                    setTopSqlSearch(tableName);
                    setTab("topSql");
                  }}
                />
              ),
            },
            {
              key: "topSql",
              label: "TopSQL",
              children: (
                <TopSqlTab
                  datasetId={datasetId}
                  importing={importing}
                  loading={topSqlQuery.loading}
                  rows={topSqlQuery.data}
                  search={topSqlSearch}
                  onChangeSearch={setTopSqlSearch}
                  onRefresh={topSqlQuery.reload}
                  onOpenTemplate={openTemplateFromTopSql}
                />
              ),
            },
            {
              key: "share",
              label: "Share",
              children: (
                <ShareTab
                  datasetId={datasetId}
                  importing={importing}
                  loading={shareQuery.loading}
                  rows={shareQuery.data}
                  metric={shareMetric}
                  rankBy={shareRankBy}
                  chartType={shareChartType}
                  topN={shareTopN}
                  onMetricChange={(m) => {
                    setShareMetric(m);
                    if (m === "memory") setShareRankBy("maxPeakMemBytes");
                    else if (shareRankBy === "maxPeakMemBytes")
                      setShareRankBy(m === "time" ? "totalTimeMs" : "totalCpuMs");
                  }}
                  onRankByChange={setShareRankBy}
                  onChartTypeChange={setShareChartType}
                  onTopNChange={setShareTopN}
                  onRefresh={shareQuery.reload}
                  onOpenTemplate={openTemplateFromShare}
                />
              ),
            },
          ]}
        />
      </Content>

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
    </Layout>
  );
}
