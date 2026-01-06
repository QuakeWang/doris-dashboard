import { Alert, Button, Card, Layout, Space, Spin, Tabs, Typography } from "antd";
import { useMemo, useState } from "react";
import {
  type TabKey,
  toErrorMessage,
  useDatasetQueries,
  useDuckDbSession,
  useTabState,
} from "./app/hooks";
import AppHeader from "./components/AppHeader";
import FiltersCard from "./components/FiltersCard";
import ImportCard from "./components/ImportCard";
import OverviewTab from "./components/OverviewTab";
import ShareTab from "./components/ShareTab";
import TemplateDetailDrawer, { type TemplateRef } from "./components/TemplateDetailDrawer";
import TopSqlTab from "./components/TopSqlTab";
import { DbClient } from "./db/client/dbClient";
import type { ImportProgress, QueryFilters, ShareRow, TopSqlRow } from "./db/client/protocol";

const { Content } = Layout;
const { Text } = Typography;

const DEFAULT_FILTERS: QueryFilters = { excludeInternal: true };
const DUCKDB_WASM_OOB_ERROR =
  "DuckDB wasm crashed with 'memory access out of bounds'.\nThis is usually triggered by large inserts or a wasm runtime limitation.\nTry: reload the page and re-import; or switch to a Chromium-based browser.\nIf it persists, please share a small log snippet that reproduces the crash.";

export default function App(): JSX.Element {
  const client = useMemo(() => new DbClient(), []);
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

  const [shareMetric, setShareMetric] = useState<"cpu" | "time">("cpu");
  const [shareRankBy, setShareRankBy] = useState<"totalCpuMs" | "totalTimeMs">("totalCpuMs");
  const [shareChartType, setShareChartType] = useState<"bar" | "pie">("bar");
  const [shareTopN, setShareTopN] = useState(12);

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
      maxTimeMs: row.maxTimeMs,
    });

  const openTemplateFromShare = (row: ShareRow) =>
    setTemplateDrawer({
      templateHash: row.templateHash,
      template: row.template,
      execCount: row.execCount,
      totalCpuMs: row.totalCpuMs,
      totalTimeMs: row.totalTimeMs,
    });

  return (
    <Layout style={{ minHeight: "100vh" }}>
      <AppHeader
        ready={ready}
        importing={importing}
        datasetId={datasetId}
        isCoi={isCoi}
        onNewDataset={createNewDataset}
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
          onImport={runImport}
          onCancel={() => void client.cancelCurrentTask().catch(() => {})}
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
                  onMetricChange={setShareMetric}
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
    </Layout>
  );
}
