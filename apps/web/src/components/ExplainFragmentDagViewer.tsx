import { FullscreenOutlined, MinusOutlined, PlusOutlined } from "@ant-design/icons";
import { Button, Tag, Typography } from "antd";
import type { CSSProperties, Dispatch, MouseEvent as ReactMouseEvent, SetStateAction } from "react";
import { useEffect, useId, useMemo, useRef, useState } from "react";
import type { FragmentGraph, FragmentGraphEdge, FragmentGraphNode } from "../explain/fragmentGraph";
import { toggleKey } from "../explain/utils";

const { Text } = Typography;

const LAYOUT_SWEEPS = 2;
const MIN_ZOOM_SCALE = 0.3;
const MAX_ZOOM_SCALE = 1.8;
const ZOOM_STEP = 0.1;
const ZOOM_PADDING = 18;

type LayoutMetrics = {
  nodeWidth: number;
  nodeHeightExpanded: number;
  nodeHeightCollapsed: number;
  columnGap: number;
  rowGap: number;
  canvasPaddingX: number;
  canvasPaddingY: number;
};

const DAG_LAYOUT_METRICS_COMFY: LayoutMetrics = {
  nodeWidth: 336,
  nodeHeightExpanded: 208,
  nodeHeightCollapsed: 102,
  columnGap: 144,
  rowGap: 36,
  canvasPaddingX: 38,
  canvasPaddingY: 44,
};

const DAG_LAYOUT_METRICS_BALANCED: LayoutMetrics = {
  nodeWidth: 304,
  nodeHeightExpanded: 188,
  nodeHeightCollapsed: 96,
  columnGap: 124,
  rowGap: 32,
  canvasPaddingX: 34,
  canvasPaddingY: 40,
};

const DAG_LAYOUT_METRICS_COMPACT: LayoutMetrics = {
  nodeWidth: 272,
  nodeHeightExpanded: 172,
  nodeHeightCollapsed: 90,
  columnGap: 104,
  rowGap: 28,
  canvasPaddingX: 28,
  canvasPaddingY: 34,
};

const LEVEL_ACCENTS = [
  "rgba(137, 180, 250, 0.92)",
  "rgba(116, 199, 236, 0.92)",
  "rgba(166, 227, 161, 0.92)",
  "rgba(249, 226, 175, 0.92)",
  "rgba(245, 194, 231, 0.92)",
  "rgba(203, 166, 247, 0.92)",
];

type FragmentNodeStyle = CSSProperties & {
  "--dd-fragment-accent": string;
  left: number;
  top: number;
  width: number;
  height: number;
};

type FragmentColumn = {
  level: number;
  x: number;
  nodes: FragmentGraphNode[];
};

type FragmentNodePosition = {
  x: number;
  y: number;
  height: number;
};

type FragmentDagLayout = {
  width: number;
  height: number;
  columns: FragmentColumn[];
  positions: Map<number, FragmentNodePosition>;
};

type VisualEdge = FragmentGraphEdge & {
  key: string;
  path: string;
  labelX: number;
  labelY: number;
};

export interface ExplainFragmentDagViewerProps {
  graph: FragmentGraph;
  selectedFragmentId: number | null;
  detailFragmentId: number | null;
  onSelectFragment: (fragmentId: number) => void;
  onBackgroundClick?: () => void;
  collapsedFragmentIds: Set<number>;
  setCollapsedFragmentIds: Dispatch<SetStateAction<Set<number>>>;
}

function accentForLevel(level: number): string {
  const index = Math.abs(level) % LEVEL_ACCENTS.length;
  return LEVEL_ACCENTS[index] ?? LEVEL_ACCENTS[0];
}

function formatCardinality(value: number | null): string {
  if (value == null) return "-";
  return value.toLocaleString("en-US");
}

function summarizeTables(tables: string[]): string {
  if (tables.length === 0) return "-";
  if (tables.length <= 2) return tables.join(", ");
  return `${tables.slice(0, 2).join(", ")} +${tables.length - 2}`;
}

function clampZoomScale(scale: number): number {
  return Math.max(MIN_ZOOM_SCALE, Math.min(MAX_ZOOM_SCALE, Number(scale.toFixed(2))));
}

function clampFitZoomScale(scale: number): number {
  if (!Number.isFinite(scale)) return 1;
  const floored = Math.floor(scale * 100) / 100;
  return Math.max(MIN_ZOOM_SCALE, Math.min(MAX_ZOOM_SCALE, floored));
}

function pickLayoutMetrics(viewportWidth: number): LayoutMetrics {
  if (viewportWidth > 0 && viewportWidth < 980) {
    return DAG_LAYOUT_METRICS_COMPACT;
  }
  if (viewportWidth > 0 && viewportWidth < 1360) {
    return DAG_LAYOUT_METRICS_BALANCED;
  }
  return DAG_LAYOUT_METRICS_COMFY;
}

function averageNeighborRank(
  neighborFragmentIds: number[],
  rankByFragment: Map<number, number>
): number | null {
  let sum = 0;
  let count = 0;

  for (const fragmentId of neighborFragmentIds) {
    const rank = rankByFragment.get(fragmentId);
    if (rank == null) continue;
    sum += rank;
    count += 1;
  }

  if (count === 0) return null;
  return sum / count;
}

function rebuildRankByFragment(columns: FragmentColumn[]): Map<number, number> {
  const rankByFragment = new Map<number, number>();
  for (const column of columns) {
    for (const [rowIndex, node] of column.nodes.entries()) {
      rankByFragment.set(node.fragmentId, rowIndex);
    }
  }
  return rankByFragment;
}

function sortedByNeighborRank(
  nodes: FragmentGraphNode[],
  neighborByFragment: Map<number, number[]>,
  rankByFragment: Map<number, number>
): FragmentGraphNode[] {
  const output = [...nodes];

  output.sort((left, right) => {
    const leftRank = averageNeighborRank(
      neighborByFragment.get(left.fragmentId) ?? [],
      rankByFragment
    );
    const rightRank = averageNeighborRank(
      neighborByFragment.get(right.fragmentId) ?? [],
      rankByFragment
    );

    if (leftRank != null && rightRank != null && leftRank !== rightRank) {
      return leftRank - rightRank;
    }
    if (leftRank != null && rightRank == null) return -1;
    if (leftRank == null && rightRank != null) return 1;
    return left.fragmentId - right.fragmentId;
  });

  return output;
}

function buildAdjacency(
  edges: FragmentGraphEdge[],
  fragmentIdSet: Set<number>
): {
  incomingByFragment: Map<number, number[]>;
  outgoingByFragment: Map<number, number[]>;
} {
  const incomingByFragment = new Map<number, number[]>();
  const outgoingByFragment = new Map<number, number[]>();

  for (const edge of edges) {
    if (!fragmentIdSet.has(edge.fromFragmentId)) continue;
    if (!fragmentIdSet.has(edge.toFragmentId)) continue;

    const incoming = incomingByFragment.get(edge.toFragmentId) ?? [];
    incoming.push(edge.fromFragmentId);
    incomingByFragment.set(edge.toFragmentId, incoming);

    const outgoing = outgoingByFragment.get(edge.fromFragmentId) ?? [];
    outgoing.push(edge.toFragmentId);
    outgoingByFragment.set(edge.fromFragmentId, outgoing);
  }

  return { incomingByFragment, outgoingByFragment };
}

function buildLayout(
  nodes: FragmentGraphNode[],
  edges: FragmentGraphEdge[],
  metrics: LayoutMetrics,
  collapsedFragmentIds: Set<number>
): FragmentDagLayout {
  const {
    nodeWidth,
    nodeHeightExpanded,
    nodeHeightCollapsed,
    columnGap,
    rowGap,
    canvasPaddingX,
    canvasPaddingY,
  } = metrics;

  if (nodes.length === 0) {
    return {
      width: canvasPaddingX * 2 + nodeWidth,
      height: canvasPaddingY * 2 + nodeHeightCollapsed,
      columns: [],
      positions: new Map<number, FragmentNodePosition>(),
    };
  }

  const levelList = [...new Set(nodes.map((node) => node.level))].sort((a, b) => a - b);
  const fragmentIdSet = new Set(nodes.map((node) => node.fragmentId));
  const { incomingByFragment, outgoingByFragment } = buildAdjacency(edges, fragmentIdSet);

  const columns: FragmentColumn[] = levelList.map((level, index) => ({
    level,
    x: canvasPaddingX + index * (nodeWidth + columnGap),
    nodes: nodes
      .filter((node) => node.level === level)
      .sort((left, right) => left.fragmentId - right.fragmentId),
  }));

  for (let sweep = 0; sweep < LAYOUT_SWEEPS; sweep += 1) {
    let rankByFragment = rebuildRankByFragment(columns);

    for (let index = 1; index < columns.length; index += 1) {
      const column = columns[index];
      if (!column) continue;
      column.nodes = sortedByNeighborRank(column.nodes, incomingByFragment, rankByFragment);
      rankByFragment = rebuildRankByFragment(columns);
    }

    rankByFragment = rebuildRankByFragment(columns);
    for (let index = columns.length - 2; index >= 0; index -= 1) {
      const column = columns[index];
      if (!column) continue;
      column.nodes = sortedByNeighborRank(column.nodes, outgoingByFragment, rankByFragment);
      rankByFragment = rebuildRankByFragment(columns);
    }
  }

  const positions = new Map<number, FragmentNodePosition>();
  let maxColumnBottom = canvasPaddingY + nodeHeightCollapsed;

  for (const column of columns) {
    let yCursor = canvasPaddingY;

    for (const node of column.nodes) {
      const collapsed = collapsedFragmentIds.has(node.fragmentId);
      const height = collapsed ? nodeHeightCollapsed : nodeHeightExpanded;
      positions.set(node.fragmentId, {
        x: column.x,
        y: yCursor,
        height,
      });
      yCursor += height + rowGap;
    }

    const columnBottom =
      column.nodes.length > 0 ? yCursor - rowGap : canvasPaddingY + nodeHeightCollapsed;
    maxColumnBottom = Math.max(maxColumnBottom, columnBottom);
  }

  const width =
    canvasPaddingX * 2 + nodeWidth + Math.max(0, columns.length - 1) * (nodeWidth + columnGap);
  const height = maxColumnBottom + canvasPaddingY;

  return { width, height, columns, positions };
}

function buildVisualEdges(
  edges: FragmentGraphEdge[],
  positions: Map<number, FragmentNodePosition>,
  nodeWidth: number
): VisualEdge[] {
  const rows: VisualEdge[] = [];

  for (const edge of edges) {
    const fromPos = positions.get(edge.fromFragmentId);
    const toPos = positions.get(edge.toFragmentId);
    if (!fromPos || !toPos) continue;

    const fromX = fromPos.x + nodeWidth;
    const fromY = fromPos.y + fromPos.height / 2;
    const toX = toPos.x;
    const toY = toPos.y + toPos.height / 2;

    const span = Math.max(80, Math.abs(toX - fromX));
    const curve = Math.max(56, span * 0.36);
    const path = `M ${fromX} ${fromY} C ${fromX + curve} ${fromY}, ${toX - curve} ${toY}, ${toX} ${toY}`;

    rows.push({
      ...edge,
      key: `${edge.fromFragmentId}->${edge.toFragmentId}`,
      path,
      labelX: (fromX + toX) / 2,
      labelY: (fromY + toY) / 2,
    });
  }

  return rows;
}

export default function ExplainFragmentDagViewer(
  props: ExplainFragmentDagViewerProps
): JSX.Element {
  const {
    graph,
    selectedFragmentId,
    detailFragmentId,
    onSelectFragment,
    onBackgroundClick,
    collapsedFragmentIds,
    setCollapsedFragmentIds,
  } = props;

  const [activeExchangeId, setActiveExchangeId] = useState<string | null>(null);
  const [zoomMode, setZoomMode] = useState<"fit" | "manual">("fit");
  const [manualZoomScale, setManualZoomScale] = useState(1);
  const [viewportSize, setViewportSize] = useState({ width: 0, height: 0 });
  const layoutMetrics = useMemo(() => pickLayoutMetrics(viewportSize.width), [viewportSize.width]);
  const nodeWidth = layoutMetrics.nodeWidth;
  const markerId = useId().replace(/:/g, "_");
  const scrollContainerRef = useRef<HTMLDivElement | null>(null);

  const layout = useMemo(
    () => buildLayout(graph.nodes, graph.edges, layoutMetrics, collapsedFragmentIds),
    [collapsedFragmentIds, graph.edges, graph.nodes, layoutMetrics]
  );

  const visualEdges = useMemo(
    () => buildVisualEdges(graph.edges, layout.positions, nodeWidth),
    [graph.edges, layout.positions, nodeWidth]
  );

  const activePathState = useMemo(() => {
    if (!activeExchangeId) {
      return {
        edgeKeys: new Set<string>(),
        fragmentIds: new Set<number>(),
      };
    }

    const edgeKeys = new Set<string>();
    const fragmentIds = new Set<number>();
    for (const edge of visualEdges) {
      if (!edge.exchangeIds.includes(activeExchangeId)) continue;
      edgeKeys.add(edge.key);
      fragmentIds.add(edge.fromFragmentId);
      fragmentIds.add(edge.toFragmentId);
    }

    return { edgeKeys, fragmentIds };
  }, [activeExchangeId, visualEdges]);

  const fitZoomScale = useMemo(() => {
    if (viewportSize.width <= 0 || viewportSize.height <= 0) return 1;
    const widthScale = (viewportSize.width - ZOOM_PADDING * 2) / layout.width;
    const heightScale = (viewportSize.height - ZOOM_PADDING * 2) / layout.height;
    return clampFitZoomScale(Math.min(widthScale, heightScale));
  }, [layout.height, layout.width, viewportSize.height, viewportSize.width]);

  const zoomScale = zoomMode === "fit" ? fitZoomScale : manualZoomScale;
  const scaledWidth = layout.width * zoomScale;
  const scaledHeight = layout.height * zoomScale;
  const scaleShellWidth = Math.max(scaledWidth + ZOOM_PADDING * 2, viewportSize.width);
  const scaleShellHeight = Math.max(scaledHeight + ZOOM_PADDING * 2, viewportSize.height);
  const canvasOffsetX = Math.max(ZOOM_PADDING, (scaleShellWidth - scaledWidth) / 2);
  const canvasOffsetY = Math.max(ZOOM_PADDING, (scaleShellHeight - scaledHeight) / 2);

  useEffect(() => {
    const container = scrollContainerRef.current;
    if (!container) return;

    const updateViewportSize = () => {
      const nextWidth = container.clientWidth;
      const nextHeight = container.clientHeight;
      setViewportSize((prev) => {
        if (prev.width === nextWidth && prev.height === nextHeight) return prev;
        return {
          width: nextWidth,
          height: nextHeight,
        };
      });
    };

    updateViewportSize();

    if (typeof ResizeObserver !== "undefined") {
      const observer = new ResizeObserver(() => updateViewportSize());
      observer.observe(container);
      return () => observer.disconnect();
    }

    window.addEventListener("resize", updateViewportSize);
    return () => window.removeEventListener("resize", updateViewportSize);
  }, []);

  useEffect(() => {
    if (!activeExchangeId) return;
    const exists = graph.edges.some((edge) => edge.exchangeIds.includes(activeExchangeId));
    if (!exists) setActiveExchangeId(null);
  }, [activeExchangeId, graph.edges]);

  useEffect(() => {
    if (selectedFragmentId == null) return;
    const container = scrollContainerRef.current;
    if (!container) return;
    const pos = layout.positions.get(selectedFragmentId);
    if (!pos) return;

    const targetCenterX = (pos.x + nodeWidth / 2) * zoomScale + canvasOffsetX;
    const targetCenterY = (pos.y + pos.height / 2) * zoomScale + canvasOffsetY;
    const scrollLeft = Math.max(0, targetCenterX - container.clientWidth / 2);
    const scrollTop = Math.max(0, targetCenterY - container.clientHeight / 2);
    if (
      Math.abs(container.scrollLeft - scrollLeft) < 1 &&
      Math.abs(container.scrollTop - scrollTop) < 1
    ) {
      return;
    }

    container.scrollTo({
      left: scrollLeft,
      top: scrollTop,
      behavior: "auto",
    });
  }, [canvasOffsetX, canvasOffsetY, layout.positions, nodeWidth, selectedFragmentId, zoomScale]);

  const onToggleCollapse = (fragmentId: number) => {
    setCollapsedFragmentIds((prev) => toggleKey(prev, fragmentId));
  };

  const onPickExchange = (exchangeId: string, preferredFragmentId: number) => {
    setActiveExchangeId((prev) => (prev === exchangeId ? null : exchangeId));
    onSelectFragment(preferredFragmentId);
  };

  const onSetManualZoom = (scale: number) => {
    setZoomMode("manual");
    setManualZoomScale(clampZoomScale(scale));
  };

  const onZoomIn = () => onSetManualZoom(zoomScale + ZOOM_STEP);
  const onZoomOut = () => onSetManualZoom(zoomScale - ZOOM_STEP);
  const onFitZoom = () => setZoomMode("fit");

  const onCanvasBackgroundClick = (event: ReactMouseEvent<HTMLDivElement>) => {
    if (!onBackgroundClick) return;
    const target = event.target as HTMLElement | null;
    if (!target) return;
    if (target.closest("[data-dd-interactive='true']")) return;
    onBackgroundClick();
  };

  if (graph.nodes.length === 0) {
    return (
      <div className="dd-fragment-dag-empty">
        <Text type="secondary">No fragment nodes available.</Text>
      </div>
    );
  }

  return (
    <div className="dd-fragment-dag-root">
      {activeExchangeId ? (
        <div className="dd-fragment-dag-active-exchange">
          <Button
            size="small"
            type="link"
            data-dd-interactive="true"
            onClick={() => setActiveExchangeId(null)}
          >
            clear highlighted path (EX {activeExchangeId})
          </Button>
        </div>
      ) : null}

      <div
        className="dd-fragment-dag-scroll"
        ref={scrollContainerRef}
        onMouseDown={onCanvasBackgroundClick}
      >
        <div
          className="dd-fragment-dag-scale-shell"
          style={{ width: scaleShellWidth, height: scaleShellHeight }}
        >
          <div
            className="dd-fragment-dag-canvas"
            style={{
              width: layout.width,
              height: layout.height,
              left: canvasOffsetX,
              top: canvasOffsetY,
              transform: `scale(${zoomScale})`,
            }}
          >
            {layout.columns.map((column, index) => (
              <div
                key={`level-${column.level}`}
                className="dd-fragment-level-chip"
                style={{ left: column.x + nodeWidth / 2 }}
              >
                Stage {index + 1} · L{column.level} · {column.nodes.length} fragments
              </div>
            ))}

            <svg
              className="dd-fragment-dag-svg"
              width={layout.width}
              height={layout.height}
              viewBox={`0 0 ${layout.width} ${layout.height}`}
              aria-hidden="true"
            >
              <defs>
                <marker
                  id={markerId}
                  viewBox="0 0 8 8"
                  refX="7"
                  refY="4"
                  markerWidth="7"
                  markerHeight="7"
                  orient="auto"
                >
                  <path d="M 0 0 L 8 4 L 0 8 z" fill="rgba(137, 180, 250, 0.85)" />
                </marker>
              </defs>

              {visualEdges.map((edge) => {
                const highlighted = activePathState.edgeKeys.has(edge.key);
                const dimmed = activeExchangeId != null && !highlighted;
                const className = [
                  "dd-fragment-edge-path",
                  highlighted ? "dd-fragment-edge-path-active" : "",
                  dimmed ? "dd-fragment-edge-path-dim" : "",
                ]
                  .filter(Boolean)
                  .join(" ");

                return (
                  <path
                    key={edge.key}
                    className={className}
                    d={edge.path}
                    markerEnd={`url(#${markerId})`}
                  />
                );
              })}
            </svg>

            <div className="dd-fragment-edge-label-layer">
              {visualEdges.map((edge) => {
                const edgeHighlighted = activePathState.edgeKeys.has(edge.key);
                const edgeDimmed = activeExchangeId != null && !edgeHighlighted;

                return edge.exchangeIds.map((exchangeId, index) => {
                  const active = activeExchangeId === exchangeId;
                  const offset = (index - (edge.exchangeIds.length - 1) / 2) * 19;
                  const className = [
                    "dd-fragment-edge-badge",
                    active ? "dd-fragment-edge-badge-active" : "",
                    edgeDimmed ? "dd-fragment-edge-badge-dim" : "",
                  ]
                    .filter(Boolean)
                    .join(" ");

                  return (
                    <button
                      key={`${edge.key}-${exchangeId}`}
                      type="button"
                      className={className}
                      data-dd-interactive="true"
                      style={{ left: edge.labelX, top: edge.labelY + offset }}
                      onClick={() => onPickExchange(exchangeId, edge.fromFragmentId)}
                    >
                      EX {exchangeId}
                    </button>
                  );
                });
              })}
            </div>

            {graph.nodes.map((node) => {
              const pos = layout.positions.get(node.fragmentId);
              if (!pos) return null;

              const collapsed = collapsedFragmentIds.has(node.fragmentId);
              const selected = selectedFragmentId === node.fragmentId;
              const detailFocused = detailFragmentId === node.fragmentId;
              const onPath = activePathState.fragmentIds.has(node.fragmentId);
              const dimmed = activeExchangeId != null && !onPath && !selected;

              const className = [
                "dd-fragment-node",
                selected ? "dd-fragment-node-selected" : "",
                detailFocused ? "dd-fragment-node-detail" : "",
                onPath ? "dd-fragment-node-path" : "",
                dimmed ? "dd-fragment-node-dim" : "",
              ]
                .filter(Boolean)
                .join(" ");

              const style: FragmentNodeStyle = {
                "--dd-fragment-accent": accentForLevel(node.level),
                left: pos.x,
                top: pos.y,
                width: nodeWidth,
                height: pos.height,
              };

              return (
                <div
                  key={node.fragmentId}
                  className={className}
                  style={style}
                  data-dd-interactive="true"
                  data-fragment-id={node.fragmentId}
                >
                  <div className="dd-fragment-node-head">
                    <button
                      type="button"
                      className="dd-fragment-node-open"
                      data-dd-interactive="true"
                      onClick={() => onSelectFragment(node.fragmentId)}
                    >
                      <span className="dd-fragment-node-title">Fragment {node.fragmentId}</span>
                      <span className="dd-fragment-node-subtitle">{node.rootOperator ?? "-"}</span>
                    </button>

                    <Button
                      size="small"
                      type="text"
                      data-dd-interactive="true"
                      title={collapsed ? "Expand" : "Collapse"}
                      onClick={() => onToggleCollapse(node.fragmentId)}
                    >
                      {collapsed ? "+" : "−"}
                    </Button>
                  </div>

                  <div className="dd-fragment-node-kv-grid">
                    <span>
                      nodes <code>{node.nodeCount}</code>
                    </span>
                    <span>
                      joins <code>{node.joinCount}</code>
                    </span>
                    <span>
                      scans <code>{node.scanCount}</code>
                    </span>
                    <span>
                      rf <code>{node.runtimeFilterCount}</code>
                    </span>
                    <span>
                      max card <code>{formatCardinality(node.maxCardinality)}</code>
                    </span>
                    <span>
                      ex in/out{" "}
                      <code>
                        {node.consumerExchangeIds.length}/{node.producerExchangeIds.length}
                      </code>
                    </span>
                  </div>

                  {!collapsed ? (
                    <div className="dd-fragment-node-tail">
                      <Tag bordered={false} color="geekblue" className="dd-fragment-node-tag">
                        partition: {node.partition ?? "-"}
                      </Tag>
                      <Text type="secondary" className="dd-fragment-node-table">
                        tables: <Text code>{summarizeTables(node.tables)}</Text>
                      </Text>
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        </div>
      </div>

      <div className="dd-fragment-dag-zoom-controls">
        <Button
          size="small"
          className="dd-fragment-dag-zoom-btn"
          icon={<FullscreenOutlined />}
          data-dd-interactive="true"
          type={zoomMode === "fit" ? "primary" : "default"}
          title="Fit"
          onClick={onFitZoom}
        />
        <Button
          size="small"
          className="dd-fragment-dag-zoom-btn"
          icon={<MinusOutlined />}
          data-dd-interactive="true"
          title="Zoom out"
          onClick={onZoomOut}
          disabled={zoomScale <= MIN_ZOOM_SCALE}
        />
        <Button
          size="small"
          className="dd-fragment-dag-zoom-btn"
          icon={<PlusOutlined />}
          data-dd-interactive="true"
          title="Zoom in"
          onClick={onZoomIn}
          disabled={zoomScale >= MAX_ZOOM_SCALE}
        />
      </div>
    </div>
  );
}
