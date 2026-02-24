import { Alert, Tabs, Typography } from "antd";
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

export interface AuditWorkspaceStatusModel {
  ready: boolean;
  datasetId: string | null;
  importing: boolean;
  importProgress: ImportProgress | null;
  dorisConfigured: boolean;
  importError: string | null;
}

export interface AuditWorkspaceImportModel {
  onDismissImportError: () => void;
  onOpenDoris: () => void;
  onOpenDorisImport: () => void;
  onImport: (file: File) => void;
  onCancel: () => void;
}

export interface AuditWorkspaceFiltersModel {
  overview: OverviewResult | null;
  draft: QueryFilters;
  onPatchDraft: (patch: Partial<QueryFilters>) => void;
  onApply: () => void;
  onSetBoth: (next: QueryFilters) => void;
}

export interface AuditWorkspaceTabsModel {
  activeAuditTab: AuditTabKey;
  onSwitchAuditTab: (tab: AuditTabKey) => void;
  overview: {
    loading: boolean;
    error: string | null;
    onRefresh: () => void;
    onPatchFilters: (patch: Partial<QueryFilters>) => void;
    onJumpToTopSqlByTable: (tableName: string) => void;
  };
  topSql: {
    loading: boolean;
    error: string | null;
    rows: TopSqlRow[];
    search: string;
    onChangeSearch: (value: string) => void;
    onRefresh: () => void;
    onOpenTemplate: (row: TopSqlRow) => void;
  };
  share: {
    loading: boolean;
    error: string | null;
    rows: ShareRow[];
    metric: "cpu" | "time" | "memory";
    rankBy: ShareRankBy;
    chartType: "bar" | "pie";
    topN: number;
    onChangeMetric: (metric: "cpu" | "time" | "memory") => void;
    onChangeRankBy: (rankBy: ShareRankBy) => void;
    onChangeChartType: (chartType: "bar" | "pie") => void;
    onChangeTopN: (topN: number) => void;
    onRefresh: () => void;
    onOpenTemplate: (row: ShareRow) => void;
  };
}

export interface AuditWorkspaceProps {
  status: AuditWorkspaceStatusModel;
  importModel: AuditWorkspaceImportModel;
  filtersModel: AuditWorkspaceFiltersModel;
  tabsModel: AuditWorkspaceTabsModel;
}

export default function AuditWorkspace(props: AuditWorkspaceProps): JSX.Element {
  const { status, importModel, filtersModel, tabsModel } = props;

  return (
    <>
      {status.importError ? (
        <Alert
          type="error"
          message="Audit import error"
          description={
            <Typography.Text style={{ whiteSpace: "pre-wrap" }}>
              {status.importError}
            </Typography.Text>
          }
          showIcon
          closable
          onClose={importModel.onDismissImportError}
          style={{ marginBottom: 12 }}
        />
      ) : null}

      <ImportCard
        ready={status.ready}
        datasetId={status.datasetId}
        importing={status.importing}
        importProgress={status.importProgress}
        dorisConfigured={status.dorisConfigured}
        onOpenDoris={importModel.onOpenDoris}
        onOpenDorisImport={importModel.onOpenDorisImport}
        onImport={importModel.onImport}
        onCancel={importModel.onCancel}
      />

      <FiltersCard
        datasetId={status.datasetId}
        importing={status.importing}
        overview={filtersModel.overview}
        draft={filtersModel.draft}
        onPatchDraft={filtersModel.onPatchDraft}
        onApply={filtersModel.onApply}
        onSetBoth={filtersModel.onSetBoth}
      />

      <Tabs
        centered
        activeKey={tabsModel.activeAuditTab}
        onChange={(k) => tabsModel.onSwitchAuditTab(k as AuditTabKey)}
        items={[
          {
            key: "overview",
            label: "Overview",
            children: (
              <OverviewTab
                datasetId={status.datasetId}
                importing={status.importing}
                loading={tabsModel.overview.loading}
                error={tabsModel.overview.error}
                overview={filtersModel.overview}
                onRefresh={tabsModel.overview.onRefresh}
                onPatchFilters={tabsModel.overview.onPatchFilters}
                onJumpToTopSqlByTable={tabsModel.overview.onJumpToTopSqlByTable}
              />
            ),
          },
          {
            key: "topSql",
            label: "TopSQL",
            children: (
              <TopSqlTab
                datasetId={status.datasetId}
                importing={status.importing}
                loading={tabsModel.topSql.loading}
                error={tabsModel.topSql.error}
                rows={tabsModel.topSql.rows}
                search={tabsModel.topSql.search}
                onChangeSearch={tabsModel.topSql.onChangeSearch}
                onRefresh={tabsModel.topSql.onRefresh}
                onOpenTemplate={tabsModel.topSql.onOpenTemplate}
              />
            ),
          },
          {
            key: "share",
            label: "Share",
            children: (
              <ShareTab
                datasetId={status.datasetId}
                importing={status.importing}
                loading={tabsModel.share.loading}
                error={tabsModel.share.error}
                rows={tabsModel.share.rows}
                metric={tabsModel.share.metric}
                rankBy={tabsModel.share.rankBy}
                chartType={tabsModel.share.chartType}
                topN={tabsModel.share.topN}
                onMetricChange={tabsModel.share.onChangeMetric}
                onRankByChange={tabsModel.share.onChangeRankBy}
                onChartTypeChange={tabsModel.share.onChangeChartType}
                onTopNChange={tabsModel.share.onChangeTopN}
                onRefresh={tabsModel.share.onRefresh}
                onOpenTemplate={tabsModel.share.onOpenTemplate}
              />
            ),
          },
        ]}
      />
    </>
  );
}
