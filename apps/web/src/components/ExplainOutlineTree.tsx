import {
  DownOutlined,
  MinusSquareOutlined,
  MoreOutlined,
  PlusSquareOutlined,
  RightOutlined,
} from "@ant-design/icons";
import { Button, Dropdown, Space, Switch, Tag, Typography } from "antd";
import type { MenuProps } from "antd";
import type { Dispatch, SetStateAction } from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import type { ExplainNode } from "../explain/types";
import { computeCardinalityStyle, lookupKv, toggleKey } from "../explain/utils";
import { CATPPUCCIN_MOCHA } from "../theme/catppuccin";

const { Text } = Typography;

type OutlineRow = {
  node: ExplainNode;
  depth: number;
  hasChildren: boolean;
  descendantCount: number;
  paramText: string;
};

type DetailItem = {
  label: string;
  value: string;
};

type DetailInfo = {
  items: DetailItem[];
  segmentsText: string;
  hasDetail: boolean;
};

const SIGNAL_LINE_RE =
  /(PREDICATES|partitions=|tablets=|PREAGGREGATION|afterFilter|cardinality=|runtime filters|pushAggOp|PARTITION:|REWRITE|MATERIALIZED VIEW)/i;

function isSignalText(text: string): boolean {
  return SIGNAL_LINE_RE.test(text);
}

function isExchangeNode(node: ExplainNode): boolean {
  const op = node.operator.toUpperCase();
  if (op.includes("EXCHANGE")) return true;
  if (op.includes("STREAM DATA SINK")) return true;
  if (op.endsWith("SINK")) return true;
  return false;
}

function filterExchangeNodes(nodes: ExplainNode[]): ExplainNode[] {
  const out: ExplainNode[] = [];
  const hiddenDepths: number[] = [];

  for (const node of nodes) {
    const depth = Math.max(0, node.depth);
    while (hiddenDepths.length > 0) {
      const top = hiddenDepths[hiddenDepths.length - 1];
      if (depth > top) break;
      hiddenDepths.pop();
    }
    const adjustedDepth = Math.max(0, depth - hiddenDepths.length);
    if (isExchangeNode(node)) {
      hiddenDepths.push(depth);
      continue;
    }
    if (adjustedDepth === node.depth) out.push(node);
    else out.push({ ...node, depth: adjustedDepth });
  }

  return out;
}

function isFragmentHeader(node: ExplainNode): boolean {
  return node.operator.toUpperCase().startsWith("PLAN FRAGMENT");
}

function extractOutputExprs(segments: string[]): string[] {
  const out: string[] = [];
  let collecting = false;
  for (const seg of segments) {
    const line = seg.trim();
    if (!line) continue;
    const upper = line.toUpperCase();
    if (upper.startsWith("OUTPUT EXPRS")) {
      collecting = true;
      continue;
    }
    if (!collecting) continue;
    if (/^[A-Z_ ]+:\s*/.test(line)) break;
    out.push(line);
  }
  return out;
}

function buildParamText(node: ExplainNode): string {
  const parts: string[] = [];
  for (const seg of node.segments.slice(1)) {
    if (seg.startsWith("[Fragment:")) continue;
    if (seg.startsWith("TABLE:")) continue;
    if (/^cardinality=/i.test(seg)) continue;
    if (/^afterFilter=/i.test(seg)) continue;
    if (/^PREDICATES:/i.test(seg)) continue;
    parts.push(seg);
  }
  return parts.join("  ").trim();
}

function buildDetailInfo(node: ExplainNode, focusMode: boolean): DetailInfo {
  const items: DetailItem[] = [];
  const usedKeys = new Set<string>();

  const addUsedKey = (key: string) => usedKeys.add(key.toLowerCase());
  const addItem = (label: string, value: string | number | null | undefined, key?: string) => {
    if (value == null) return;
    const text = String(value);
    if (!text.trim()) return;
    items.push({ label, value: text });
    if (key) addUsedKey(key);
  };

  if (focusMode) {
    const outputExprs = extractOutputExprs(node.segments);
    if (outputExprs.length > 0) addItem("Output Exprs", outputExprs.join(", "));
    addItem("Table", node.table, "table");
    addItem("Predicates", node.predicates, "predicates");
    addItem("Partitions", lookupKv(node, "partitions"), "partitions");
    addItem("Tablets", lookupKv(node, "tablets"), "tablets");
    addItem("Preaggregation", lookupKv(node, "PREAGGREGATION"), "preaggregation");
    addItem("After Filter", lookupKv(node, "afterFilter"), "afterfilter");
    addItem("Cardinality", node.cardinality, "cardinality");
    addItem("Runtime Filters", lookupKv(node, "RUNTIME_FILTERS"), "runtime filters");
    addItem("Push Agg", lookupKv(node, "pushAggOp"), "pushaggop");
    addItem("Partition", lookupKv(node, "PARTITION"), "partition");
    addItem("Colocate", lookupKv(node, "HAS_COLO_PLAN_NODE"), "has_colo_plan_node");
  } else {
    addItem("Node ID", node.nodeId != null ? String(node.nodeId) : null);
    addItem("Fragment", node.fragmentId != null ? String(node.fragmentId) : null);
    addItem("IDs", node.idsRaw || null);
    addItem("Table", node.table, "table");
    addItem("Cardinality", node.cardinality, "cardinality");
    addItem("Predicates", node.predicates, "predicates");

    const kvFields: Array<[string, string]> = [
      ["Partitions", "partitions"],
      ["Tablets", "tablets"],
      ["Runtime Filters", "RUNTIME_FILTERS"],
      ["Join Op", "JOIN_OP"],
      ["Equal Join Conjunct", "EQUAL_JOIN_CONJUNCT"],
      ["Distribute Expr Lists", "DISTRIBUTE_EXPR_LISTS"],
      ["Final Projections", "FINAL_PROJECTIONS"],
      ["Partition", "PARTITION"],
      ["Has Colo Plan Node", "HAS_COLO_PLAN_NODE"],
    ];

    for (const [label, key] of kvFields) {
      const value = lookupKv(node, key);
      if (!value) continue;
      items.push({ label, value });
      addUsedKey(key);
    }

    for (const [key, value] of Object.entries(node.kv)) {
      if (!value) continue;
      const keyLower = key.toLowerCase();
      if (usedKeys.has(keyLower)) continue;
      items.push({ label: key, value: String(value) });
    }
  }

  const segmentsText = focusMode
    ? ""
    : node.segments.length > 0
      ? node.segments.join("\n")
      : "";
  const hasDetail = items.length > 0 || !!segmentsText.trim();
  return { items, segmentsText, hasDetail };
}

function buildOutlineIndex(nodes: ExplainNode[]): {
  rows: OutlineRow[];
  allParentKeys: Set<string>;
  allNodeKeys: Set<string>;
} {
  const rows: OutlineRow[] = [];
  const stack: Array<{ key: string }> = [];

  for (const node of nodes) {
    const depth = Math.max(0, node.depth);
    while (stack.length > depth) stack.pop();

    const effectiveDepth = Math.min(depth, stack.length);
    rows.push({
      node,
      depth: effectiveDepth,
      hasChildren: false,
      descendantCount: 0,
      paramText: buildParamText(node),
    });

    stack[effectiveDepth] = { key: node.key };
    stack.length = effectiveDepth + 1;
  }

  const allParentKeys = new Set<string>();
  const allNodeKeys = new Set<string>();
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    allNodeKeys.add(r.node.key);
    const next = rows[i + 1] ?? null;
    r.hasChildren = !!next && next.depth > r.depth;
    if (r.hasChildren) allParentKeys.add(r.node.key);
  }

  const indexStack: number[] = [];
  for (let i = 0; i < rows.length; i++) {
    const d = rows[i].depth;
    while (indexStack.length > 0) {
      const top = indexStack[indexStack.length - 1];
      if (d > rows[top].depth) break;
      rows[top].descendantCount = i - top - 1;
      indexStack.pop();
    }
    indexStack.push(i);
  }
  while (indexStack.length > 0) {
    const top = indexStack.pop()!;
    rows[top].descendantCount = rows.length - top - 1;
  }

  return { rows, allParentKeys, allNodeKeys };
}

export interface ExplainOutlineTreeProps {
  nodes: ExplainNode[];

  selectedNodeKey: string | null;
  onOpenNodeKey: (key: string) => void;

  maxCardinality: number;

  collapsedChildrenKeys: Set<string>;
  setCollapsedChildrenKeys: Dispatch<SetStateAction<Set<string>>>;

  expandedDetailKeys: Set<string>;
  setExpandedDetailKeys: Dispatch<SetStateAction<Set<string>>>;
}

export default function ExplainOutlineTree(props: ExplainOutlineTreeProps): JSX.Element {
  const {
    nodes,
    selectedNodeKey,
    onOpenNodeKey,
    maxCardinality,
    collapsedChildrenKeys,
    setCollapsedChildrenKeys,
    expandedDetailKeys,
    setExpandedDetailKeys,
  } = props;

  const [focusMode, setFocusMode] = useState(true);
  const [hideExchangeNodes, setHideExchangeNodes] = useState(true);

  const listRef = useRef<HTMLDivElement | null>(null);

  const viewNodes = useMemo(
    () => (hideExchangeNodes ? filterExchangeNodes(nodes) : nodes),
    [hideExchangeNodes, nodes]
  );
  const outline = useMemo(() => buildOutlineIndex(viewNodes), [viewNodes]);
  const detailByKey = useMemo(() => {
    const map = new Map<string, DetailInfo>();
    for (const row of outline.rows) {
      map.set(row.node.key, buildDetailInfo(row.node, focusMode));
    }
    return map;
  }, [focusMode, outline.rows]);
  const detailKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const [key, detail] of detailByKey) {
      if (detail.hasDetail) keys.add(key);
    }
    return keys;
  }, [detailByKey]);

  const visibleRows = useMemo(() => {
    const out: OutlineRow[] = [];
    let skipDepth: number | null = null;
    for (const row of outline.rows) {
      if (skipDepth != null) {
        if (row.depth > skipDepth) continue;
        skipDepth = null;
      }
      out.push(row);
      if (row.hasChildren && collapsedChildrenKeys.has(row.node.key)) {
        skipDepth = row.depth;
      }
    }
    return out;
  }, [collapsedChildrenKeys, outline.rows]);

  useEffect(() => {
    if (!selectedNodeKey) return;
    const container = listRef.current;
    if (!container) return;
    const el = container.querySelector(
      `[data-node-key="${selectedNodeKey}"]`
    ) as HTMLElement | null;
    if (!el) return;
    try {
      el.scrollIntoView({ block: "nearest" });
    } catch {
      // Ignore scroll errors.
    }
  }, [selectedNodeKey, visibleRows]);

  const onCollapseAll = () => setCollapsedChildrenKeys(new Set(outline.allParentKeys));
  const onExpandAll = () => setCollapsedChildrenKeys(new Set());
  const onExpandAllDetails = () => setExpandedDetailKeys(new Set(detailKeys));
  const onCollapseAllDetails = () => setExpandedDetailKeys(new Set());

  const treeMenuItems: MenuProps["items"] = useMemo(
    () => [
      {
        key: "expandAll",
        label: "Expand all children",
        disabled: collapsedChildrenKeys.size === 0,
      },
      {
        key: "collapseAll",
        label: "Collapse all children",
        disabled: outline.allParentKeys.size === 0,
      },
    ],
    [collapsedChildrenKeys.size, outline.allParentKeys.size]
  );

  const onTreeMenuClick: MenuProps["onClick"] = ({ key }) => {
    if (key === "expandAll") onExpandAll();
    else if (key === "collapseAll") onCollapseAll();
  };

  const renderRow = (row: OutlineRow): JSX.Element => {
    const n = row.node;
    const isSelected = selectedNodeKey === n.key;
    const isFragment = isFragmentHeader(n);
    const partitions = lookupKv(n, "partitions");
    const fragmentPartition = isFragment ? lookupKv(n, "PARTITION") : null;
    const fragmentColo = isFragment ? lookupKv(n, "HAS_COLO_PLAN_NODE") : null;
    const fragmentOutputs = isFragment ? extractOutputExprs(n.segments) : [];
    const detail = detailByKey.get(n.key);
    const detailAvailable = detail?.hasDetail ?? false;
    const paramText = focusMode ? "" : row.paramText;

    const hasChildren = row.hasChildren;
    const childrenCollapsed = hasChildren && collapsedChildrenKeys.has(n.key);
    const detailExpanded = detailAvailable && expandedDetailKeys.has(n.key);
    const showMeta = !focusMode || !detailExpanded;

    const metric = computeCardinalityStyle(n.cardinality, maxCardinality);
    const accentColor = isSelected
      ? CATPPUCCIN_MOCHA.mauve
      : (metric.color ?? CATPPUCCIN_MOCHA.surface2);
    const rowClass = `dd-outline-row${isSelected ? " dd-outline-row-selected" : ""}${isFragment ? " dd-outline-row-fragment" : ""}`;

    return (
      <div
        key={n.key}
        className={rowClass}
        style={{ ["--dd-row-accent" as any]: accentColor } as any}
        data-node-key={n.key}
      >
        <div className="dd-outline-plan">
          <div className="dd-outline-plan-header" style={{ paddingLeft: row.depth * 14 }}>
            <span className="dd-outline-actions">
              {hasChildren ? (
                <Button
                  type="text"
                  size="small"
                  icon={childrenCollapsed ? <PlusSquareOutlined /> : <MinusSquareOutlined />}
                  onClick={(e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    setCollapsedChildrenKeys((prev) => toggleKey(prev, n.key));
                  }}
                  aria-label={childrenCollapsed ? "Expand children" : "Collapse children"}
                />
              ) : (
                <span className="dd-outline-icon-placeholder" />
              )}

              {detailAvailable ? (
                <Button
                  type="text"
                  size="small"
                  icon={detailExpanded ? <DownOutlined /> : <RightOutlined />}
                  className="dd-outline-detail-toggle"
                  onClick={(e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    setExpandedDetailKeys((prev) => toggleKey(prev, n.key));
                  }}
                  aria-label={detailExpanded ? "Collapse detail" : "Expand detail"}
                />
              ) : (
                <span className="dd-outline-icon-placeholder" />
              )}
            </span>

            <button
              type="button"
              className="dd-outline-open-btn dd-outline-open-plan"
              onClick={() => onOpenNodeKey(n.key)}
            >
              {isFragment ? (
                <>
                  <span className="dd-outline-fragment-title">
                    <Text>{`Fragment ${n.fragmentId ?? "-"}`}</Text>
                  </span>
                  <div className="dd-outline-fragment-meta">
                    {fragmentPartition ? (
                      <span className="dd-code-ellipsis">
                        <Text type="secondary">PARTITION: </Text>
                        <Text code>{fragmentPartition}</Text>
                      </span>
                    ) : null}
                    {fragmentColo ? (
                      <span className="dd-code-ellipsis">
                        <Text type="secondary">COLO: </Text>
                        <Text code>{fragmentColo}</Text>
                      </span>
                    ) : null}
                    {fragmentOutputs.length > 0 ? (
                      <span className="dd-code-ellipsis">
                        <Text type="secondary">OUTPUT: </Text>
                        <Text code>{fragmentOutputs.join(", ")}</Text>
                      </span>
                    ) : null}
                  </div>
                </>
              ) : (
                <>
                  <span className="dd-outline-title">
                    <Text code className="dd-code-ellipsis">
                      {n.operator}
                    </Text>
                    {n.fragmentId != null ? (
                      <Tag className="dd-outline-tag" color="geekblue">
                        Fragment {n.fragmentId}
                      </Tag>
                    ) : null}
                  </span>

                  {showMeta ? (
                    <div className="dd-outline-meta">
                      {n.table ? (
                        <span className="dd-code-ellipsis">
                          <Text type="secondary">TABLE: </Text>
                          <Text code>{n.table}</Text>
                        </span>
                      ) : null}
                      {n.cardinality ? (
                        <span className="dd-code-ellipsis">
                          <Text type="secondary">card: </Text>
                          <Text code>{n.cardinality}</Text>
                        </span>
                      ) : null}
                      {partitions ? (
                        <span className="dd-code-ellipsis">
                          <Text type="secondary">partitions: </Text>
                          <Text code>{partitions}</Text>
                        </span>
                      ) : null}
                    </div>
                  ) : null}
                </>
              )}

              {hasChildren && childrenCollapsed ? (
                <div className="dd-outline-hint">
                  <Text type="secondary">
                    collapsed <Text code>{row.descendantCount}</Text> nodes
                  </Text>
                </div>
              ) : null}

              {detailExpanded && paramText ? (
                <div className="dd-outline-param">
                  <span className="dd-code-ellipsis">
                    <Text type="secondary">Param: </Text>
                    <Text code>{paramText}</Text>
                  </span>
                </div>
              ) : null}

              {detailExpanded && detail ? (
                <div className="dd-outline-detail">
                  {detail.items.length > 0 ? (
                    <div className="dd-outline-detail-grid">
                      {detail.items.map((item, idx) => (
                        <div key={`${n.key}-detail-${idx}`} className="dd-outline-detail-row">
                          <Text type="secondary" className="dd-outline-detail-label">
                            {item.label}
                          </Text>
                          <Text code className="dd-outline-detail-value">
                            {item.value}
                          </Text>
                        </div>
                      ))}
                    </div>
                  ) : null}
                  {detail.segmentsText ? (
                    <div className="dd-outline-detail-block">
                      <Text type="secondary">{focusMode ? "Signals" : "Segments"}</Text>
                      <pre className="dd-sql-block dd-outline-detail-raw">
                        <code>{detail.segmentsText}</code>
                      </pre>
                    </div>
                  ) : null}
                </div>
              ) : null}
            </button>
          </div>
        </div>

        <button
          type="button"
          className="dd-outline-open-btn dd-outline-metric"
          onClick={() => onOpenNodeKey(n.key)}
        >
          <div className="dd-outline-metric-header">
            <Text type="secondary">Node</Text>{" "}
            <Text code>{n.nodeId != null ? String(n.nodeId) : "-"}</Text>
          </div>

          <div className="dd-outline-metric-label">
            <Text type="secondary">Cardinality:</Text> <Text code>{n.cardinality ?? "-"}</Text>
          </div>
          <div className="dd-outline-metric-bar">
            <div
              className="dd-outline-metric-bar-fill"
              style={{
                width: `${Math.round(metric.pct * 100)}%`,
                backgroundColor: accentColor,
              }}
            />
          </div>
        </button>
      </div>
    );
  };

  if (outline.rows.length === 0) {
    return <Text type="secondary">No plan tree</Text>;
  }

  return (
    <Space direction="vertical" style={{ width: "100%" }} size={8}>
      <Space
        style={{ justifyContent: "space-between", display: "flex", width: "100%" }}
        size={12}
        wrap
      >
        <Space size={6} wrap>
          <Text type="secondary">Focus signals</Text>
          <Switch size="small" checked={focusMode} onChange={setFocusMode} />
          <Text type="secondary">Hide exchange/sink</Text>
          <Switch size="small" checked={hideExchangeNodes} onChange={setHideExchangeNodes} />
        </Space>
        <Space>
          <Button
            type="text"
            size="small"
            onClick={onExpandAllDetails}
            disabled={detailKeys.size === 0}
            aria-label="Expand details"
          >
            Expand details
          </Button>
          <Button
            type="text"
            size="small"
            onClick={onCollapseAllDetails}
            disabled={expandedDetailKeys.size === 0}
            aria-label="Collapse details"
          >
            Collapse details
          </Button>
          <Dropdown menu={{ items: treeMenuItems, onClick: onTreeMenuClick }} trigger={["click"]}>
            <Button
              type="text"
              size="small"
              icon={<MoreOutlined />}
              aria-label="Outline actions"
            />
          </Dropdown>
        </Space>
      </Space>
      <div ref={listRef} className="dd-explain-outline">
        {visibleRows.map((r) => renderRow(r))}
      </div>
    </Space>
  );
}
