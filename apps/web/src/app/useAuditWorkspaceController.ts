import { useCallback, useState } from "react";
import type { AuditWorkspaceProps } from "../components/AuditWorkspace";
import type { TemplateRef } from "../components/TemplateDetailDrawer";
import type { DbClient } from "../db/client/dbClient";
import type { ImportProgress, OverviewResult, ShareRow, TopSqlRow } from "../db/client/protocol";
import type { AsyncData } from "../utils/useAsync";
import type { AuditTabKey, DiagnosticsModule } from "./diagnosticsNavigation";
import { useDatasetQueries } from "./hooks";
import { type AuditFiltersState, useAuditFiltersState } from "./useAuditFiltersState";
import { type AuditImportState, useAuditImportState } from "./useAuditImportState";
import { type AuditShareState, useAuditShareState } from "./useAuditShareState";

function toTemplateRef(row: TopSqlRow | ShareRow): TemplateRef {
  return {
    templateHash: row.templateHash,
    template: row.template,
    tableGuess: "tableGuess" in row ? row.tableGuess : undefined,
    execCount: row.execCount,
    totalCpuMs: row.totalCpuMs,
    totalTimeMs: row.totalTimeMs,
    p95TimeMs: "p95TimeMs" in row ? row.p95TimeMs : undefined,
    maxPeakMemBytes: row.maxPeakMemBytes,
  };
}

type QueryState<T> = Pick<AsyncData<T>, "data" | "loading" | "error" | "reload">;

interface BuildAuditWorkspacePropsParams {
  ready: boolean;
  datasetId: string | null;
  dorisConfigured: boolean;
  importError: string | null;
  activeAuditTab: AuditTabKey;
  onOpenDoris: () => void;
  onOpenDorisImport: () => void;
  onCancel: () => void;
  switchAuditTab: (tab: AuditTabKey) => void;
  setTopSqlSearch: (value: string) => void;
  setTemplateDrawer: (value: TemplateRef | null) => void;
  topSqlSearch: string;
  filtersState: AuditFiltersState;
  shareState: AuditShareState;
  importState: AuditImportState;
  overviewQuery: QueryState<OverviewResult | null>;
  topSqlQuery: QueryState<TopSqlRow[]>;
  shareQuery: QueryState<ShareRow[]>;
}

export function buildAuditWorkspaceProps(
  params: BuildAuditWorkspacePropsParams
): AuditWorkspaceProps {
  const {
    ready,
    datasetId,
    dorisConfigured,
    importError,
    activeAuditTab,
    onOpenDoris,
    onOpenDorisImport,
    onCancel,
    switchAuditTab,
    setTopSqlSearch,
    setTemplateDrawer,
    topSqlSearch,
    filtersState,
    shareState,
    importState,
    overviewQuery,
    topSqlQuery,
    shareQuery,
  } = params;

  const onJumpToTopSqlByTable = (tableName: string) => {
    setTopSqlSearch(tableName);
    switchAuditTab("topSql");
  };
  const onOpenTemplate = (row: TopSqlRow | ShareRow) => setTemplateDrawer(toTemplateRef(row));

  return {
    status: {
      ready,
      datasetId,
      importing: importState.importing,
      importProgress: importState.importProgress,
      dorisConfigured,
      importError,
    },
    importModel: {
      onDismissImportError: importState.clearImportError,
      onOpenDoris,
      onOpenDorisImport,
      onImport: importState.runImport,
      onCancel,
    },
    filtersModel: {
      overview: overviewQuery.data,
      draft: filtersState.filtersDraft,
      onPatchDraft: filtersState.patchFiltersDraft,
      onApply: filtersState.applyFilters,
      onSetBoth: filtersState.setFiltersBoth,
    },
    tabsModel: {
      activeAuditTab,
      onSwitchAuditTab: switchAuditTab,
      overview: {
        loading: overviewQuery.loading,
        error: overviewQuery.error,
        onRefresh: overviewQuery.reload,
        onPatchFilters: filtersState.patchFiltersBoth,
        onJumpToTopSqlByTable,
      },
      topSql: {
        loading: topSqlQuery.loading,
        error: topSqlQuery.error,
        rows: topSqlQuery.data,
        search: topSqlSearch,
        onChangeSearch: setTopSqlSearch,
        onRefresh: topSqlQuery.reload,
        onOpenTemplate,
      },
      share: {
        loading: shareQuery.loading,
        error: shareQuery.error,
        rows: shareQuery.data,
        metric: shareState.shareMetric,
        rankBy: shareState.shareRankBy,
        chartType: shareState.shareChartType,
        topN: shareState.shareTopN,
        onChangeMetric: shareState.onChangeShareMetric,
        onChangeRankBy: shareState.onChangeShareRankBy,
        onChangeChartType: shareState.onChangeShareChartType,
        onChangeTopN: shareState.onChangeShareTopN,
        onRefresh: shareQuery.reload,
        onOpenTemplate,
      },
    },
  };
}

export function useAuditWorkspaceController(params: {
  client: DbClient;
  ready: boolean;
  datasetId: string | null;
  activeModule: DiagnosticsModule;
  activeAuditTab: AuditTabKey;
  switchAuditTab: (tab: AuditTabKey) => void;
  dorisConfigured: boolean;
  onOpenDoris: () => void;
  onOpenDorisImport: () => void;
}) {
  const {
    client,
    ready,
    datasetId,
    activeModule,
    activeAuditTab,
    switchAuditTab,
    dorisConfigured,
    onOpenDoris,
    onOpenDorisImport,
  } = params;

  const filtersState = useAuditFiltersState();
  const shareState = useAuditShareState();
  const [importError, setImportError] = useState<string | null>(null);
  const [importing, setImporting] = useState(false);
  const [importProgress, setImportProgress] = useState<ImportProgress | null>(null);
  const [topSqlSearch, setTopSqlSearch] = useState("");
  const [templateDrawer, setTemplateDrawer] = useState<TemplateRef | null>(null);

  const { overviewQuery, topSqlQuery, shareQuery } = useDatasetQueries({
    client,
    datasetId,
    importing,
    activeTab: activeModule === "audit" ? activeAuditTab : null,
    filters: filtersState.filters,
    shareTopN: shareState.shareTopN,
    shareRankBy: shareState.shareRankBy,
  });

  const importState = useAuditImportState({
    client,
    datasetId,
    importing,
    setImporting,
    importProgress,
    setImportProgress,
    filters: filtersState.filters,
    setFiltersBoth: filtersState.setFiltersBoth,
    setImportError,
    overviewQuery,
    topSqlQuery,
    shareQuery,
  });

  const onCancel = useCallback(() => {
    void client.cancelCurrentTask().catch(() => undefined);
  }, [client]);

  const closeTemplateDrawer = useCallback(() => setTemplateDrawer(null), []);

  const workspaceProps = buildAuditWorkspaceProps({
    ready,
    datasetId,
    dorisConfigured,
    importError,
    activeAuditTab,
    onOpenDoris,
    onOpenDorisImport,
    onCancel,
    switchAuditTab,
    setTopSqlSearch,
    setTemplateDrawer,
    topSqlSearch,
    filtersState,
    shareState,
    importState,
    overviewQuery,
    topSqlQuery,
    shareQuery,
  });

  return {
    workspaceProps,
    filters: filtersState.filters,
    importing: importState.importing,
    runImport: importState.runImport,
    templateDrawer,
    closeTemplateDrawer,
  };
}
