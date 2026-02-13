import { Tabs } from "antd";
import type { AuditTabKey } from "../app/diagnosticsNavigation";
import type {
  ImportProgress,
  OverviewResult,
  QueryFilters,
  ShareRankBy,
  ShareRow,
  TopSqlRow,
} from "../db/client/protocol";
import FiltersCard from "./FiltersCard";
import ImportCard from "./ImportCard";
import OverviewTab from "./OverviewTab";
import ShareTab from "./ShareTab";
import TopSqlTab from "./TopSqlTab";

export interface AuditWorkspaceProps {
  ready: boolean;
  datasetId: string | null;
  importing: boolean;
  importProgress: ImportProgress | null;
  dorisConfigured: boolean;
  onOpenDoris: () => void;
  onOpenDorisImport: () => void;
  onImport: (file: File) => void;
  onCancel: () => void;
  overview: OverviewResult | null;
  filtersDraft: QueryFilters;
  onPatchFiltersDraft: (patch: Partial<QueryFilters>) => void;
  onApplyFilters: () => void;
  onSetFiltersBoth: (next: QueryFilters) => void;
  activeAuditTab: AuditTabKey;
  onSwitchAuditTab: (tab: AuditTabKey) => void;
  overviewLoading: boolean;
  onRefreshOverview: () => void;
  onPatchFiltersBoth: (patch: Partial<QueryFilters>) => void;
  onJumpToTopSqlByTable: (tableName: string) => void;
  topSqlLoading: boolean;
  topSqlRows: TopSqlRow[];
  topSqlSearch: string;
  onChangeTopSqlSearch: (value: string) => void;
  onRefreshTopSql: () => void;
  onOpenTopSqlTemplate: (row: TopSqlRow) => void;
  shareLoading: boolean;
  shareRows: ShareRow[];
  shareMetric: "cpu" | "time" | "memory";
  shareRankBy: ShareRankBy;
  shareChartType: "bar" | "pie";
  shareTopN: number;
  onChangeShareMetric: (metric: "cpu" | "time" | "memory") => void;
  onChangeShareRankBy: (rankBy: ShareRankBy) => void;
  onChangeShareChartType: (chartType: "bar" | "pie") => void;
  onChangeShareTopN: (topN: number) => void;
  onRefreshShare: () => void;
  onOpenShareTemplate: (row: ShareRow) => void;
}

export default function AuditWorkspace(props: AuditWorkspaceProps): JSX.Element {
  const {
    ready,
    datasetId,
    importing,
    importProgress,
    dorisConfigured,
    onOpenDoris,
    onOpenDorisImport,
    onImport,
    onCancel,
    overview,
    filtersDraft,
    onPatchFiltersDraft,
    onApplyFilters,
    onSetFiltersBoth,
    activeAuditTab,
    onSwitchAuditTab,
    overviewLoading,
    onRefreshOverview,
    onPatchFiltersBoth,
    onJumpToTopSqlByTable,
    topSqlLoading,
    topSqlRows,
    topSqlSearch,
    onChangeTopSqlSearch,
    onRefreshTopSql,
    onOpenTopSqlTemplate,
    shareLoading,
    shareRows,
    shareMetric,
    shareRankBy,
    shareChartType,
    shareTopN,
    onChangeShareMetric,
    onChangeShareRankBy,
    onChangeShareChartType,
    onChangeShareTopN,
    onRefreshShare,
    onOpenShareTemplate,
  } = props;

  return (
    <>
      <ImportCard
        ready={ready}
        datasetId={datasetId}
        importing={importing}
        importProgress={importProgress}
        dorisConfigured={dorisConfigured}
        onOpenDoris={onOpenDoris}
        onOpenDorisImport={onOpenDorisImport}
        onImport={onImport}
        onCancel={onCancel}
      />

      <FiltersCard
        datasetId={datasetId}
        importing={importing}
        overview={overview}
        draft={filtersDraft}
        onPatchDraft={onPatchFiltersDraft}
        onApply={onApplyFilters}
        onSetBoth={onSetFiltersBoth}
      />

      <Tabs
        centered
        activeKey={activeAuditTab}
        onChange={(k) => onSwitchAuditTab(k as AuditTabKey)}
        items={[
          {
            key: "overview",
            label: "Overview",
            children: (
              <OverviewTab
                datasetId={datasetId}
                importing={importing}
                loading={overviewLoading}
                overview={overview}
                onRefresh={onRefreshOverview}
                onPatchFilters={onPatchFiltersBoth}
                onJumpToTopSqlByTable={onJumpToTopSqlByTable}
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
                loading={topSqlLoading}
                rows={topSqlRows}
                search={topSqlSearch}
                onChangeSearch={onChangeTopSqlSearch}
                onRefresh={onRefreshTopSql}
                onOpenTemplate={onOpenTopSqlTemplate}
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
                loading={shareLoading}
                rows={shareRows}
                metric={shareMetric}
                rankBy={shareRankBy}
                chartType={shareChartType}
                topN={shareTopN}
                onMetricChange={onChangeShareMetric}
                onRankByChange={onChangeShareRankBy}
                onChartTypeChange={onChangeShareChartType}
                onTopNChange={onChangeShareTopN}
                onRefresh={onRefreshShare}
                onOpenTemplate={onOpenShareTemplate}
              />
            ),
          },
        ]}
      />
    </>
  );
}
