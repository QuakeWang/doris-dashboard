import { describe, expect, it, vi } from "vitest";
import type { ShareRow, TopSqlRow } from "../db/client/protocol";
import { buildAuditWorkspaceProps } from "./useAuditWorkspaceController";

type BuildParams = Parameters<typeof buildAuditWorkspaceProps>[0];
const queryState = <T>(data: T, error: string | null = null) => ({
  data,
  loading: false,
  error,
  reload: vi.fn(),
});

function createBuildParams(overrides: Partial<BuildParams> = {}): BuildParams {
  return {
    ready: true,
    datasetId: "dataset-1",
    dorisConfigured: true,
    importError: null,
    activeAuditTab: "overview",
    onOpenDoris: vi.fn(),
    onOpenDorisImport: vi.fn(),
    onCancel: vi.fn(),
    switchAuditTab: vi.fn(),
    setTopSqlSearch: vi.fn(),
    setTemplateDrawer: vi.fn(),
    topSqlSearch: "",
    filtersState: {
      filters: { excludeInternal: true },
      filtersDraft: { excludeInternal: true },
      patchFiltersDraft: vi.fn(),
      applyFilters: vi.fn(),
      setFiltersBoth: vi.fn(),
      patchFiltersBoth: vi.fn(),
    },
    shareState: {
      shareMetric: "cpu",
      shareRankBy: "totalCpuMs",
      shareChartType: "bar",
      shareTopN: 12,
      onChangeShareMetric: vi.fn(),
      onChangeShareRankBy: vi.fn(),
      onChangeShareChartType: vi.fn(),
      onChangeShareTopN: vi.fn(),
    },
    importState: {
      importing: false,
      importProgress: null,
      runImport: vi.fn(),
      clearImportError: vi.fn(),
    },
    overviewQuery: queryState<null>(null),
    topSqlQuery: queryState([] as TopSqlRow[]),
    shareQuery: queryState([] as ShareRow[]),
    ...overrides,
  };
}

describe("buildAuditWorkspaceProps", () => {
  it("wires per-tab errors and jump behavior", () => {
    const switchAuditTab = vi.fn();
    const setTopSqlSearch = vi.fn();

    const props = buildAuditWorkspaceProps(
      createBuildParams({
        switchAuditTab,
        setTopSqlSearch,
        overviewQuery: queryState<null>(null, "overview-error"),
        topSqlQuery: queryState([] as TopSqlRow[], "topsql-error"),
        shareQuery: queryState([] as ShareRow[], "share-error"),
      })
    );

    expect(props.tabsModel.overview.error).toBe("overview-error");
    expect(props.tabsModel.topSql.error).toBe("topsql-error");
    expect(props.tabsModel.share.error).toBe("share-error");

    props.tabsModel.overview.onJumpToTopSqlByTable("tbl_a");
    expect(setTopSqlSearch).toHaveBeenCalledWith("tbl_a");
    expect(switchAuditTab).toHaveBeenCalledWith("topSql");
  });

  it("maps template rows from topSql/share into drawer shape", () => {
    const setTemplateDrawer = vi.fn();
    const baseProps = buildAuditWorkspaceProps(
      createBuildParams({
        activeAuditTab: "topSql",
        setTemplateDrawer,
      })
    );

    const topRow: TopSqlRow = {
      templateHash: "tpl-1",
      template: "select 1",
      tableGuess: "db.tbl",
      execCount: 10,
      totalCpuMs: 100,
      totalTimeMs: 200,
      avgTimeMs: 20,
      maxTimeMs: 40,
      p95TimeMs: 35,
      maxPeakMemBytes: 1024,
    };
    baseProps.tabsModel.topSql.onOpenTemplate(topRow);
    expect(setTemplateDrawer).toHaveBeenLastCalledWith(
      expect.objectContaining({
        templateHash: "tpl-1",
        tableGuess: "db.tbl",
        p95TimeMs: 35,
      })
    );

    const shareRow: ShareRow = {
      templateHash: "tpl-2",
      template: "select 2",
      execCount: 8,
      totalCpuMs: 80,
      totalTimeMs: 160,
      cpuShare: 0.2,
      timeShare: 0.3,
      maxPeakMemBytes: 512,
      isOthers: false,
    };
    baseProps.tabsModel.share.onOpenTemplate(shareRow);
    expect(setTemplateDrawer).toHaveBeenLastCalledWith(
      expect.objectContaining({
        templateHash: "tpl-2",
        tableGuess: undefined,
        p95TimeMs: undefined,
      })
    );
  });
});
