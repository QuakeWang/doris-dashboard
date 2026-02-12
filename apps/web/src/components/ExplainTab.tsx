import { ReloadOutlined } from "@ant-design/icons";
import { Alert, Button, Card, Drawer, Input, Segmented, Select, Space, Typography } from "antd";
import { useCallback, useEffect, useMemo, useState } from "react";
import type { AgentClient, DorisConnectionInput } from "../agent/agentClient";
import { buildFragmentGraph } from "../explain/fragmentGraph";
import {
  type ExplainNodeOptimizationSignals,
  buildFragmentOptimizationSignals,
  buildNodeOptimizationSignals,
  parseMaterializationSummary,
} from "../explain/optimizationSignals";
import { parseExplain, selectNodesByFragment } from "../explain/parseExplainTree";
import type { ExplainParseResult } from "../explain/types";
import CopyIconButton from "./CopyIconButton";
import ExplainFragmentDagViewer from "./ExplainFragmentDagViewer";
import ExplainFragmentSummaryBanner from "./ExplainFragmentSummaryBanner";
import ExplainNodeDetailPanel from "./ExplainNodeDetailPanel";
import ExplainOperatorList from "./ExplainOperatorList";
import useExplainInputState from "./useExplainInputState";

const { Text } = Typography;
const { TextArea } = Input;

type ExplainViewMode = "dag" | "raw";
type FragmentFlowStats = {
  upstreamFragments: number;
  downstreamFragments: number;
};

const EXPLAIN_VIEW_OPTIONS: Array<{ label: string; value: ExplainViewMode }> = [
  { label: "Fragment DAG", value: "dag" },
  { label: "Raw", value: "raw" },
];

export interface ExplainTabProps {
  agent: AgentClient;
  dorisConn: DorisConnectionInput | null;
  dorisConfigured: boolean;
  onOpenDoris: () => void;
  sql: string;
  onChangeSql: (sql: string) => void;
}

export default function ExplainTab(props: ExplainTabProps): JSX.Element {
  const { agent, dorisConn, dorisConfigured, onOpenDoris, sql, onChangeSql } = props;

  const [parseResult, setParseResult] = useState<ExplainParseResult | null>(null);
  const [selectedNodeKey, setSelectedNodeKey] = useState<string | null>(null);
  const [selectedFragmentId, setSelectedFragmentId] = useState<number | null>(null);
  const [activeView, setActiveView] = useState<ExplainViewMode>("dag");
  const [collapsedFragmentIds, setCollapsedFragmentIds] = useState<Set<number>>(() => new Set());

  const resetFragmentState = useCallback(() => {
    setSelectedFragmentId(null);
    setCollapsedFragmentIds(new Set());
  }, []);

  const clearNodeSelection = useCallback(() => {
    setSelectedNodeKey(null);
  }, []);

  const resetInspectorSelection = useCallback(() => {
    setSelectedFragmentId(null);
    clearNodeSelection();
  }, [clearNodeSelection]);

  const runParse = useCallback(
    (text: string) => {
      const res = parseExplain(text);
      setParseResult(res);
      clearNodeSelection();
      resetFragmentState();
      setActiveView(res.ok ? "dag" : "raw");
    },
    [clearNodeSelection, resetFragmentState]
  );

  const {
    database,
    setDatabase,
    databaseList,
    databaseLoading,
    loading,
    error,
    onClearSql,
    refreshDatabases,
    onRunExplain,
  } = useExplainInputState({
    agent,
    dorisConn,
    dorisConfigured,
    onOpenDoris,
    sql,
    onChangeSql,
    onParseResult: runParse,
  });

  const okRes = parseResult?.ok ? parseResult : null;
  const baseNodes = useMemo(() => okRes?.nodes ?? [], [okRes]);
  const fragmentGraph = useMemo(() => buildFragmentGraph(baseNodes), [baseNodes]);

  const filteredNodes = useMemo(() => {
    return selectNodesByFragment(baseNodes, selectedFragmentId);
  }, [baseNodes, selectedFragmentId]);

  const nodeByKey = useMemo(
    () => new Map(baseNodes.map((node) => [node.key, node] as const)),
    [baseNodes]
  );
  const fragmentSummaryById = useMemo(
    () => new Map(fragmentGraph.nodes.map((node) => [node.fragmentId, node] as const)),
    [fragmentGraph.nodes]
  );

  const fragmentFlowStatsById = useMemo(() => {
    const upstreamById = new Map<number, Set<number>>();
    const downstreamById = new Map<number, Set<number>>();

    for (const node of fragmentGraph.nodes) {
      upstreamById.set(node.fragmentId, new Set<number>());
      downstreamById.set(node.fragmentId, new Set<number>());
    }

    for (const edge of fragmentGraph.edges) {
      downstreamById.get(edge.fromFragmentId)?.add(edge.toFragmentId);
      upstreamById.get(edge.toFragmentId)?.add(edge.fromFragmentId);
    }

    const stats = new Map<number, FragmentFlowStats>();
    for (const node of fragmentGraph.nodes) {
      stats.set(node.fragmentId, {
        upstreamFragments: upstreamById.get(node.fragmentId)?.size ?? 0,
        downstreamFragments: downstreamById.get(node.fragmentId)?.size ?? 0,
      });
    }
    return stats;
  }, [fragmentGraph.edges, fragmentGraph.nodes]);

  const detailNode = useMemo(
    () => (selectedNodeKey ? (nodeByKey.get(selectedNodeKey) ?? null) : null),
    [nodeByKey, selectedNodeKey]
  );

  const detailFragmentId = detailNode?.fragmentId ?? null;

  const inspectorFragmentSummary = useMemo(() => {
    const fragmentId = detailNode?.fragmentId ?? selectedFragmentId;
    if (fragmentId == null) return null;
    return fragmentSummaryById.get(fragmentId) ?? null;
  }, [detailNode?.fragmentId, fragmentSummaryById, selectedFragmentId]);

  useEffect(() => {
    if (selectedNodeKey && !nodeByKey.has(selectedNodeKey)) {
      setSelectedNodeKey(null);
    }
  }, [nodeByKey, selectedNodeKey]);

  useEffect(() => {
    if (selectedFragmentId == null) return;
    if (!fragmentSummaryById.has(selectedFragmentId)) {
      setSelectedFragmentId(null);
    }
  }, [fragmentSummaryById, selectedFragmentId]);

  const onSelectNode = (key: string) => {
    if (selectedNodeKey === key) {
      resetInspectorSelection();
      return;
    }

    setSelectedNodeKey(key);
    const node = nodeByKey.get(key) ?? null;
    setSelectedFragmentId(node?.fragmentId ?? null);
  };

  const onSelectFragment = (fragmentId: number) => {
    if (selectedFragmentId === fragmentId && selectedNodeKey == null) {
      resetInspectorSelection();
      return;
    }

    setSelectedFragmentId(fragmentId);
    clearNodeSelection();
  };

  const onClearFragmentFilter = () => {
    resetInspectorSelection();
  };
  const showParseAlert = !!parseResult && !parseResult.ok;
  const explainRawText = okRes?.rawText ?? "";
  const dagDetailDrawerOpen =
    activeView === "dag" && (selectedFragmentId != null || detailNode != null);

  useEffect(() => {
    if (!dagDetailDrawerOpen) return;

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      event.preventDefault();
      resetInspectorSelection();
    };

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [dagDetailDrawerOpen, resetInspectorSelection]);
  const hasUnknownColumnStats = useMemo(
    () => /planned with unknown column statistics/i.test(explainRawText),
    [explainRawText]
  );
  const materializationSummary = useMemo(
    () => parseMaterializationSummary(explainRawText),
    [explainRawText]
  );
  const nodeSignalsByKey = useMemo(() => {
    const map = new Map<string, ExplainNodeOptimizationSignals>();
    for (const node of baseNodes) {
      map.set(node.key, buildNodeOptimizationSignals(node, materializationSummary));
    }
    return map;
  }, [baseNodes, materializationSummary]);
  const fragmentOptimizationSignals = useMemo(
    () => buildFragmentOptimizationSignals(baseNodes, nodeSignalsByKey),
    [baseNodes, nodeSignalsByKey]
  );

  const onResetWorkspace = () => {
    clearNodeSelection();
    resetFragmentState();
  };

  const renderDagInspectorContent = () => (
    <>
      <div className="dd-explain-drawer-summary">
        {inspectorFragmentSummary ? (
          <ExplainFragmentSummaryBanner
            summary={inspectorFragmentSummary}
            flow={fragmentFlowStatsById.get(inspectorFragmentSummary.fragmentId) ?? null}
            hasUnknownColumnStats={hasUnknownColumnStats}
            optimizationSignals={
              fragmentOptimizationSignals.get(inspectorFragmentSummary.fragmentId) ?? null
            }
          />
        ) : (
          <Text type="secondary">Select a fragment or plan node to inspect details.</Text>
        )}
      </div>

      <section className="dd-explain-drawer-pane dd-explain-drawer-pane-operators">
        <div className="dd-explain-drawer-pane-body">
          {detailNode ? (
            <ExplainNodeDetailPanel
              node={detailNode}
              nodeSignalsByKey={nodeSignalsByKey}
              onBack={clearNodeSelection}
            />
          ) : (
            <ExplainOperatorList
              nodes={filteredNodes}
              selectedNodeKey={selectedNodeKey}
              onSelectNodeKey={onSelectNode}
              nodeSignalsByKey={nodeSignalsByKey}
            />
          )}
        </div>
      </section>
    </>
  );

  return (
    <Space direction="vertical" style={{ width: "100%" }} size="middle">
      <Card
        size="small"
        title="Explain Input"
        extra={
          <Space>
            <CopyIconButton text={sql} tooltip="Copy SQL" />
            <Button icon={<ReloadOutlined />} onClick={onClearSql} disabled={loading || !sql}>
              Clear
            </Button>
          </Space>
        }
      >
        <Space direction="vertical" style={{ width: "100%" }} size={12}>
          <Space direction="vertical" style={{ width: "100%" }} size={8}>
            <TextArea
              value={sql}
              onChange={(e) => onChangeSql(e.target.value)}
              placeholder="SQL to explain (auto mode: EXPLAIN TREE -> tree, others -> plan)"
              autoSize={{ minRows: 5, maxRows: 10 }}
            />
            <Space wrap>
              <Button type="primary" onClick={() => void onRunExplain()} loading={loading}>
                Run EXPLAIN
              </Button>
              {!dorisConfigured || !dorisConn ? (
                <Text type="secondary">no doris connection (use header button)</Text>
              ) : null}
              <Select
                value={database.trim() ? database.trim() : undefined}
                onChange={(v) => setDatabase(String(v ?? ""))}
                placeholder="Database (optional)"
                allowClear
                showSearch
                loading={databaseLoading}
                options={databaseList.map((d) => ({ label: d, value: d }))}
                style={{ minWidth: 220 }}
                disabled={loading || !dorisConfigured}
                onDropdownVisibleChange={(open) => {
                  if (!open) return;
                  if (!dorisConfigured || !dorisConn) return;
                  if (databaseLoading) return;
                  if (databaseList.length > 0) return;
                  void refreshDatabases();
                }}
              />
              <Button
                icon={<ReloadOutlined />}
                title="Refresh databases"
                onClick={() => void refreshDatabases()}
                loading={databaseLoading}
                disabled={loading || databaseLoading}
              />
            </Space>
          </Space>

          {error ? (
            <Alert
              type="error"
              message="Error"
              description={<Text style={{ whiteSpace: "pre-wrap" }}>{error}</Text>}
              showIcon
            />
          ) : null}

          {showParseAlert ? (
            <Alert
              type="warning"
              message="Parse failed"
              description={
                <Text style={{ whiteSpace: "pre-wrap" }}>
                  {parseResult && !parseResult.ok ? parseResult.error : ""}
                </Text>
              }
              showIcon
            />
          ) : null}
        </Space>
      </Card>

      {okRes ? (
        <div className="dd-explain-layout dd-explain-layout-dag">
          <div className="dd-explain-shell dd-explain-shell-main">
            <div className="dd-explain-shell-header dd-explain-main-header">
              <Space size={8} wrap>
                <Segmented
                  value={activeView}
                  onChange={(value) => setActiveView(String(value) as ExplainViewMode)}
                  options={EXPLAIN_VIEW_OPTIONS}
                />
                {selectedFragmentId != null ? (
                  <>
                    <Text type="secondary">
                      fragment: <Text code>{selectedFragmentId}</Text>
                    </Text>
                    <Button size="small" type="link" onClick={onClearFragmentFilter}>
                      clear fragment filter
                    </Button>
                  </>
                ) : null}
              </Space>
              <Button size="small" type="text" onClick={onResetWorkspace}>
                Reset
              </Button>
            </div>

            <div className="dd-explain-shell-body dd-explain-main-body">
              {activeView === "dag" ? (
                <ExplainFragmentDagViewer
                  graph={fragmentGraph}
                  selectedFragmentId={selectedFragmentId}
                  detailFragmentId={detailFragmentId}
                  onSelectFragment={onSelectFragment}
                  onBackgroundClick={resetInspectorSelection}
                  collapsedFragmentIds={collapsedFragmentIds}
                  setCollapsedFragmentIds={setCollapsedFragmentIds}
                />
              ) : null}

              {activeView === "raw" ? (
                <pre className="dd-sql-block dd-explain-raw-view">
                  <code>{parseResult?.rawText ?? ""}</code>
                </pre>
              ) : null}
            </div>
          </div>

          <Drawer
            title="Inspector"
            placement="right"
            width={420}
            open={dagDetailDrawerOpen}
            mask={false}
            push={false}
            destroyOnClose={false}
            onClose={resetInspectorSelection}
          >
            <div className="dd-explain-drawer-body">{renderDagInspectorContent()}</div>
          </Drawer>
        </div>
      ) : (
        <div className="dd-explain-workspace-empty">
          <Text type="secondary">Run EXPLAIN to build workbench.</Text>
          {parseResult?.rawText ? (
            <pre className="dd-sql-block dd-explain-empty-raw">
              <code>{parseResult.rawText}</code>
            </pre>
          ) : null}
        </div>
      )}
    </Space>
  );
}
