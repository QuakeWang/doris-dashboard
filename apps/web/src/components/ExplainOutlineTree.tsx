import {
  DownOutlined,
  MinusSquareOutlined,
  MoreOutlined,
  PlusSquareOutlined,
  RightOutlined,
} from "@ant-design/icons";
import { Button, Dropdown, Space, Tag, Typography } from "antd";
import type { MenuProps } from "antd";
import type { Dispatch, SetStateAction } from "react";
import { useEffect, useMemo, useRef } from "react";
import type { ExplainNode } from "../explain/types";
import { computeCardinalityStyle, toggleKey } from "../explain/utils";
import { CATPPUCCIN_MOCHA } from "../theme/catppuccin";

const { Text } = Typography;

type OutlineRow = {
  node: ExplainNode;
  depth: number;
  hasChildren: boolean;
  descendantCount: number;
  paramText: string;
};

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

  const listRef = useRef<HTMLDivElement | null>(null);

  const outline = useMemo(() => buildOutlineIndex(nodes), [nodes]);

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
  const onExpandAllDetails = () => setExpandedDetailKeys(new Set(outline.allNodeKeys));
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

    const hasChildren = row.hasChildren;
    const childrenCollapsed = hasChildren && collapsedChildrenKeys.has(n.key);
    const detailExpanded = expandedDetailKeys.has(n.key);

    const metric = computeCardinalityStyle(n.cardinality, maxCardinality);
    const accentColor = isSelected
      ? CATPPUCCIN_MOCHA.mauve
      : (metric.color ?? CATPPUCCIN_MOCHA.surface2);
    const rowClass = `dd-outline-row${isSelected ? " dd-outline-row-selected" : ""}`;

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

              {row.paramText ? (
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
              </div>

              {hasChildren && childrenCollapsed ? (
                <div className="dd-outline-hint">
                  <Text type="secondary">
                    collapsed <Text code>{row.descendantCount}</Text> nodes
                  </Text>
                </div>
              ) : null}

              {detailExpanded && row.paramText ? (
                <div className="dd-outline-param">
                  <span className="dd-code-ellipsis">
                    <Text type="secondary">Param: </Text>
                    <Text code>{row.paramText}</Text>
                  </span>
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
      <Space style={{ justifyContent: "flex-end", display: "flex" }}>
        <Button type="text" size="small" onClick={onExpandAllDetails} aria-label="Expand details">
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
          <Button type="text" size="small" icon={<MoreOutlined />} aria-label="Outline actions" />
        </Dropdown>
      </Space>
      <div ref={listRef} className="dd-explain-outline">
        {visibleRows.map((r) => renderRow(r))}
      </div>
    </Space>
  );
}
