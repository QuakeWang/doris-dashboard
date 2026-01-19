import { ReloadOutlined } from "@ant-design/icons";
import { Alert, Button, Card, Input, Select, Space, Tabs, Typography } from "antd";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { AgentClient, DorisConnectionInput } from "../agent/agentClient";
import { parseExplainTree, selectNodesByFragment } from "../explain/parseExplainTree";
import type { ExplainParseResult } from "../explain/types";
import { buildParentByKey, parseNumberLike } from "../explain/utils";
import CopyIconButton from "./CopyIconButton";
import ExplainDiagramTree from "./ExplainDiagramTree";
import ExplainNodeDrawer from "./ExplainNodeDrawer";
import ExplainOutlineTree from "./ExplainOutlineTree";

const { Text } = Typography;
const { TextArea } = Input;

export interface ExplainTabProps {
  agent: AgentClient;
  dorisConn: DorisConnectionInput | null;
  dorisConfigured: boolean;
  onOpenDoris: () => void;

  sql: string;
  onChangeSql: (sql: string) => void;
}

type ViewTabKey = "diagram" | "outline" | "raw";

const DORIS_EXPLAIN_DATABASE_KEY = "doris.explain.database.v1";

function readExplainDatabaseFromSession(): string {
  if (typeof window === "undefined") return "";
  try {
    return window.sessionStorage.getItem(DORIS_EXPLAIN_DATABASE_KEY) ?? "";
  } catch {
    return "";
  }
}

function writeExplainDatabaseToSession(db: string): void {
  if (typeof window === "undefined") return;
  const trimmed = db.trim();
  try {
    if (!trimmed) window.sessionStorage.removeItem(DORIS_EXPLAIN_DATABASE_KEY);
    else window.sessionStorage.setItem(DORIS_EXPLAIN_DATABASE_KEY, trimmed);
  } catch {
    // Ignore storage quota / access errors.
  }
}

export default function ExplainTab(props: ExplainTabProps): JSX.Element {
  const { agent, dorisConn, dorisConfigured, onOpenDoris, sql, onChangeSql } = props;

  const [database, setDatabase] = useState(() => readExplainDatabaseFromSession());
  const [databaseList, setDatabaseList] = useState<string[]>([]);
  const [databaseLoading, setDatabaseLoading] = useState(false);

  const [parseResult, setParseResult] = useState<ExplainParseResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const [selectedFragment, setSelectedFragment] = useState<number | null>(null);
  const [selectedNodeKey, setSelectedNodeKey] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<ViewTabKey>("outline");
  const [diagramFocusToken, setDiagramFocusToken] = useState(0);

  const [collapsedChildrenKeys, setCollapsedChildrenKeys] = useState<Set<string>>(() => new Set());
  const [expandedDetailKeys, setExpandedDetailKeys] = useState<Set<string>>(() => new Set());
  const [detailDrawerOpen, setDetailDrawerOpen] = useState(false);

  const runParse = useCallback((text: string) => {
    setError(null);
    const res = parseExplainTree(text);
    setParseResult(res);
    setSelectedFragment(null);
    setSelectedNodeKey(null);
    setActiveView(res.ok ? "outline" : "raw");
  }, []);

  const onClearSql = () => {
    setError(null);
    onChangeSql("");
  };

  useEffect(() => {
    setDatabaseLoading(false);
    setDatabaseList([]);
  }, [dorisConn]);

  useEffect(() => {
    writeExplainDatabaseToSession(database);
  }, [database]);

  const refreshDatabases = async () => {
    setError(null);
    if (!dorisConfigured || !dorisConn) {
      setError("Doris connection is not configured.");
      onOpenDoris();
      return;
    }
    setDatabaseLoading(true);
    try {
      const dbs = await agent.listDorisDatabases({ connection: dorisConn });
      setDatabaseList(dbs);
      const currentDb = database.trim();
      if (!currentDb) {
        setDatabase(dbs[0] ?? "");
      } else if (dbs.length > 0 && !dbs.includes(currentDb)) {
        setDatabase(dbs[0] ?? "");
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setDatabaseLoading(false);
    }
  };

  const onRunExplain = async () => {
    setError(null);
    if (!dorisConfigured || !dorisConn) {
      setError("Doris connection is not configured.");
      onOpenDoris();
      return;
    }
    const sqlText = sql.trim();
    if (!sqlText) {
      setError("SQL is required.");
      return;
    }

    setLoading(true);
    try {
      const dbName = database.trim();
      const conn = dbName ? { ...dorisConn, database: dbName } : dorisConn;
      const text = await agent.explainTree({ connection: conn, sql: sqlText });
      runParse(text);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  const okRes = parseResult?.ok ? parseResult : null;
  const fragments = okRes?.fragments ?? [];

  const viewNodes = useMemo(() => {
    if (!okRes) return [];
    return selectNodesByFragment(okRes.nodes, selectedFragment);
  }, [okRes, selectedFragment]);

  useEffect(() => {
    setCollapsedChildrenKeys(new Set());
    setExpandedDetailKeys(new Set());
  }, [viewNodes]);

  const parentByKey = useMemo(() => buildParentByKey(viewNodes), [viewNodes]);

  useEffect(() => {
    if (!selectedNodeKey) return;
    let p = parentByKey.get(selectedNodeKey) ?? null;
    if (!p) return;
    setCollapsedChildrenKeys((prev) => {
      const next = new Set(prev);
      while (p) {
        next.delete(p);
        p = parentByKey.get(p) ?? null;
      }
      return next;
    });
  }, [parentByKey, selectedNodeKey]);

  useEffect(() => {
    if (!selectedNodeKey) setDetailDrawerOpen(false);
  }, [selectedNodeKey]);

  const maxCardinality = useMemo(() => {
    let max = 0;
    for (const n of viewNodes) {
      const c = parseNumberLike(n.cardinality);
      if (c != null) max = Math.max(max, c);
    }
    return max;
  }, [viewNodes]);

  const selectedNode = useMemo(
    () => (selectedNodeKey ? (viewNodes.find((n) => n.key === selectedNodeKey) ?? null) : null),
    [selectedNodeKey, viewNodes]
  );

  useEffect(() => {
    if (selectedNodeKey && !selectedNode) setSelectedNodeKey(null);
  }, [selectedNode, selectedNodeKey]);

  const openNodeDetail = (key: string) => {
    setSelectedNodeKey(key);
    setDetailDrawerOpen(true);
  };

  const DIAGRAM_MAX_NODES = 2000;
  const diagramDisabled = viewNodes.length > DIAGRAM_MAX_NODES;
  const lastViewRef = useRef<ViewTabKey>(activeView);

  useEffect(() => {
    if (activeView !== "diagram") return;
    if (!diagramDisabled) return;
    setActiveView("outline");
  }, [activeView, diagramDisabled]);

  useEffect(() => {
    const prev = lastViewRef.current;
    lastViewRef.current = activeView;
    if (prev === "diagram" || activeView !== "diagram") return;
    if (!selectedNodeKey) return;
    setDiagramFocusToken((t) => t + 1);
  }, [activeView, selectedNodeKey]);

  const showParseAlert = !!parseResult && !parseResult.ok;
  const warningCount = okRes?.warnings.length ?? 0;

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
              placeholder="SQL to explain (no EXPLAIN prefix required)"
              autoSize={{ minRows: 5, maxRows: 10 }}
            />
            <Space wrap>
              <Button type="primary" onClick={() => void onRunExplain()} loading={loading}>
                Run EXPLAIN TREE
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

          {error && (
            <Alert
              type="error"
              message="Error"
              description={<Text style={{ whiteSpace: "pre-wrap" }}>{error}</Text>}
              showIcon
            />
          )}

          {showParseAlert && (
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
          )}
        </Space>
      </Card>

      <Card
        size="small"
        title="Explain Viewer"
        extra={
          <Space>
            {okRes ? (
              <>
                <Text type="secondary">
                  nodes: <Text code>{okRes.nodes.length}</Text>
                </Text>
                {warningCount > 0 ? (
                  <Text type="secondary">
                    warnings: <Text code>{warningCount}</Text>
                  </Text>
                ) : null}
              </>
            ) : (
              <Text type="secondary">Run EXPLAIN TREE to view</Text>
            )}
          </Space>
        }
      >
        <Space direction="vertical" style={{ width: "100%" }} size={12}>
          <Space
            wrap
            size={8}
            style={{ width: "100%", justifyContent: "space-between", display: "flex" }}
          >
            <Space wrap size={8}>
              <Text type="secondary">Fragment:</Text>
              <Select
                value={selectedFragment ?? "__all__"}
                onChange={(v) => {
                  setSelectedNodeKey(null);
                  setSelectedFragment(v === "__all__" ? null : Number(v));
                }}
                style={{ width: 220 }}
                disabled={!okRes}
                options={[
                  { value: "__all__", label: "All" },
                  ...fragments.map((f) => ({ value: String(f), label: `Fragment ${f}` })),
                ]}
              />
            </Space>
          </Space>

          <Tabs
            activeKey={activeView}
            onChange={(k) => setActiveView(k as ViewTabKey)}
            items={[
              {
                key: "diagram",
                label: "Diagram",
                disabled: !okRes || diagramDisabled,
                children:
                  activeView === "diagram" ? (
                    <ExplainDiagramTree
                      nodes={viewNodes}
                      selectedNodeKey={selectedNodeKey}
                      onOpenNodeKey={openNodeDetail}
                      focusToken={diagramFocusToken}
                      maxCardinality={maxCardinality}
                      collapsedChildrenKeys={collapsedChildrenKeys}
                      setCollapsedChildrenKeys={setCollapsedChildrenKeys}
                    />
                  ) : null,
              },
              {
                key: "outline",
                label: "Outline",
                disabled: !okRes,
                children: (
                  <ExplainOutlineTree
                    nodes={viewNodes}
                    selectedNodeKey={selectedNodeKey}
                    onOpenNodeKey={openNodeDetail}
                    maxCardinality={maxCardinality}
                    collapsedChildrenKeys={collapsedChildrenKeys}
                    setCollapsedChildrenKeys={setCollapsedChildrenKeys}
                    expandedDetailKeys={expandedDetailKeys}
                    setExpandedDetailKeys={setExpandedDetailKeys}
                  />
                ),
              },
              {
                key: "raw",
                label: "Raw",
                children: (
                  <pre
                    className="dd-sql-block"
                    style={{ margin: 0, maxHeight: 520, overflow: "auto" }}
                  >
                    <code>{parseResult?.rawText ?? ""}</code>
                  </pre>
                ),
              },
            ]}
          />
        </Space>
      </Card>

      <ExplainNodeDrawer
        open={detailDrawerOpen}
        node={selectedNode}
        onClose={() => setDetailDrawerOpen(false)}
      />
    </Space>
  );
}
