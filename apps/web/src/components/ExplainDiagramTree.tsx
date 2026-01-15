import { MinusSquareOutlined, PlusSquareOutlined } from "@ant-design/icons";
import { Button, Tag, Typography } from "antd";
import { useEffect, useMemo, useRef, useState } from "react";
import type { Dispatch, SetStateAction } from "react";
import type { ExplainNode } from "../explain/types";
import { buildParentByKey, computeCardinalityStyle, toggleKey } from "../explain/utils";
import { CATPPUCCIN_MOCHA } from "../theme/catppuccin";
import ExplainDiagramControls from "./ExplainDiagramControls";

const { Text } = Typography;

type D3Module = typeof import("d3");
type FlextreeFactory = (options: any) => (root: any) => any;

type DiagramLib = {
  d3: D3Module;
  flextree: FlextreeFactory;
};

type TreeNode = { node: ExplainNode | null; children: TreeNode[] };

type LayoutNode = {
  key: string;
  x: number;
  y: number;
  node: ExplainNode;
  hasChildren: boolean;
  collapsedChildCount: number;
};

type LayoutLink = {
  key: string;
  sourceX: number;
  sourceY: number;
  targetX: number;
  targetY: number;
};

type LayoutResult = {
  nodes: LayoutNode[];
  links: LayoutLink[];
  bbox: { minX: number; minY: number; maxX: number; maxY: number };
  byKey: Map<string, { x: number; y: number; width: number; height: number }>;
};

const CARD_WIDTH = 300;
const CARD_HEIGHT = 88;
const H_SPACING = 70;
const V_SPACING = 44;
const VIEW_PADDING = 20;

function buildTree(nodes: ExplainNode[]): TreeNode {
  const root: TreeNode = { node: null, children: [] };
  const stack: TreeNode[] = [];

  for (const node of nodes) {
    const depth = Math.max(0, node.depth);
    while (stack.length > depth) stack.pop();

    const effectiveDepth = Math.min(depth, stack.length);
    const parentIndex = Math.max(0, effectiveDepth - 1);
    const parent = effectiveDepth === 0 ? root : (stack[parentIndex] ?? root);

    const wrapper: TreeNode = { node, children: [] };
    parent.children.push(wrapper);

    stack[effectiveDepth] = wrapper;
    stack.length = effectiveDepth + 1;
  }

  return root;
}

function computeLayout(
  lib: DiagramLib,
  root: TreeNode,
  collapsedChildrenKeys: Set<string>
): LayoutResult {
  const { d3, flextree } = lib;

  const hierarchy = d3.hierarchy<TreeNode>(root, (d) => {
    if (!d.node) return d.children;
    if (collapsedChildrenKeys.has(d.node.key)) return [];
    return d.children;
  });

  const layout = flextree({
    nodeSize: () => [CARD_WIDTH + H_SPACING, CARD_HEIGHT + V_SPACING],
  });

  const layoutRoot = layout(hierarchy) as any;

  const nodes = (layoutRoot.descendants() as any[]).filter((d) => !!d.data?.node) as any[];
  const links = (layoutRoot.links() as any[]).filter((l) => !!l.source?.data?.node) as any[];

  let minX = Number.POSITIVE_INFINITY;
  let maxX = Number.NEGATIVE_INFINITY;
  let minY = Number.POSITIVE_INFINITY;
  let maxY = Number.NEGATIVE_INFINITY;

  for (const d of nodes) {
    const x0 = d.x - CARD_WIDTH / 2;
    const x1 = d.x + CARD_WIDTH / 2;
    const y0 = d.y;
    const y1 = d.y + CARD_HEIGHT;
    minX = Math.min(minX, x0);
    maxX = Math.max(maxX, x1);
    minY = Math.min(minY, y0);
    maxY = Math.max(maxY, y1);
  }

  if (!Number.isFinite(minX) || !Number.isFinite(minY)) {
    minX = 0;
    minY = 0;
    maxX = 0;
    maxY = 0;
  }

  const dx = -minX + VIEW_PADDING;
  const dy = -minY + VIEW_PADDING;

  const layoutNodes: LayoutNode[] = [];
  const byKey = new Map<string, { x: number; y: number; width: number; height: number }>();
  for (const d of nodes) {
    const n = d.data.node as ExplainNode;
    const x = d.x + dx;
    const y = d.y + dy;
    const hasChildren = (d.data.children?.length ?? 0) > 0;
    const isCollapsed = collapsedChildrenKeys.has(n.key);
    const collapsedChildCount = isCollapsed ? d.data.children.length : 0;

    layoutNodes.push({ key: n.key, x, y, node: n, hasChildren, collapsedChildCount });
    byKey.set(n.key, { x: x - CARD_WIDTH / 2, y, width: CARD_WIDTH, height: CARD_HEIGHT });
  }

  const layoutLinks: LayoutLink[] = links.map((l) => {
    const source = l.source;
    const target = l.target;
    const sourceKey = String(source.data.node.key);
    const targetKey = String(target.data.node.key);
    return {
      key: `${sourceKey}->${targetKey}`,
      sourceX: source.x + dx,
      sourceY: source.y + dy + CARD_HEIGHT,
      targetX: target.x + dx,
      targetY: target.y + dy,
    };
  });

  const bbox = {
    minX: minX + dx,
    minY: minY + dy,
    maxX: maxX + dx,
    maxY: maxY + dy,
  };

  return { nodes: layoutNodes, links: layoutLinks, bbox, byKey };
}

function linkPath(l: LayoutLink): string {
  const midY = (l.sourceY + l.targetY) / 2;
  return `M${l.sourceX},${l.sourceY} C${l.sourceX},${midY} ${l.targetX},${midY} ${l.targetX},${l.targetY}`;
}

export interface ExplainDiagramTreeProps {
  nodes: ExplainNode[];

  selectedNodeKey: string | null;
  onOpenNodeKey: (key: string) => void;
  focusToken?: number;

  maxCardinality: number;

  collapsedChildrenKeys: Set<string>;
  setCollapsedChildrenKeys: Dispatch<SetStateAction<Set<string>>>;
}

export default function ExplainDiagramTree(props: ExplainDiagramTreeProps): JSX.Element {
  const {
    nodes,
    selectedNodeKey,
    onOpenNodeKey,
    focusToken = 0,
    maxCardinality,
    collapsedChildrenKeys,
    setCollapsedChildrenKeys,
  } = props;

  const containerRef = useRef<HTMLDivElement | null>(null);
  const svgRef = useRef<SVGSVGElement | null>(null);
  const viewportRef = useRef<SVGGElement | null>(null);

  const zoomRef = useRef<any>(null);
  const didInitialFitRef = useRef(false);

  const [lib, setLib] = useState<DiagramLib | null>(null);
  const [libError, setLibError] = useState<string | null>(null);
  const [zoomReady, setZoomReady] = useState(false);

  useEffect(() => {
    didInitialFitRef.current = false;
  }, [nodes]);

  useEffect(() => {
    let canceled = false;
    setLib(null);
    setLibError(null);

    const load = async () => {
      try {
        const [d3, flextreeModule] = await Promise.all([import("d3"), import("d3-flextree")]);
        const d3Mod = d3 as unknown as D3Module;
        const flextreeFn = (flextreeModule as any).flextree as FlextreeFactory | undefined;
        if (!flextreeFn) throw new Error("d3-flextree import returned no flextree export");
        if (canceled) return;
        setLib({ d3: d3Mod, flextree: flextreeFn });
      } catch (e) {
        if (canceled) return;
        setLibError(e instanceof Error ? e.message : String(e));
      }
    };

    void load();

    return () => {
      canceled = true;
    };
  }, []);

  const tree = useMemo(() => buildTree(nodes), [nodes]);

  const layout = useMemo(() => {
    if (!lib) return null;
    return computeLayout(lib, tree, collapsedChildrenKeys);
  }, [collapsedChildrenKeys, lib, tree]);

  const parentByKey = useMemo(() => buildParentByKey(nodes), [nodes]);

  const selectedPathLinkKeys = useMemo(() => {
    if (!selectedNodeKey) return null;
    const links = new Set<string>();
    let child = selectedNodeKey;
    let parent = parentByKey.get(child) ?? null;
    while (parent) {
      links.add(`${parent}->${child}`);
      child = parent;
      parent = parentByKey.get(child) ?? null;
    }
    return links;
  }, [parentByKey, selectedNodeKey]);

  useEffect(() => {
    if (!lib || !svgRef.current || !viewportRef.current) return;

    const { d3 } = lib;
    const svg = d3.select(svgRef.current);

    const zoom = d3
      .zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.15, 2.5])
      .on("zoom", (event) => {
        const viewport = viewportRef.current;
        if (!viewport) return;
        d3.select(viewport).attr("transform", event.transform.toString());
        if (event.sourceEvent) didInitialFitRef.current = true;
      });

    svg.call(zoom as any);
    zoomRef.current = zoom;
    setZoomReady(true);

    svg.call((zoom as any).transform, d3.zoomIdentity);

    return () => {
      svg.on(".zoom", null);
      setZoomReady(false);
    };
  }, [lib]);

  useEffect(() => {
    const container = containerRef.current;
    const svg = svgRef.current;
    if (!container || !svg) return;

    const applySize = () => {
      const w = container.clientWidth || 800;
      const h = container.clientHeight || 520;
      svg.setAttribute("width", String(w));
      svg.setAttribute("height", String(h));
    };

    applySize();
    const ro = new ResizeObserver(() => applySize());
    ro.observe(container);
    return () => ro.disconnect();
  }, []);

  const applyTransform = (t: any, animateMs: number) => {
    if (!lib || !svgRef.current || !zoomRef.current) return;
    const svg = lib.d3.select(svgRef.current);
    if (animateMs > 0) {
      (svg.transition().duration(animateMs) as any).call((zoomRef.current as any).transform, t);
    } else {
      svg.call((zoomRef.current as any).transform, t);
    }
  };

  const onZoomIn = () => {
    if (!lib || !svgRef.current || !zoomRef.current) return;
    const svg = lib.d3.select(svgRef.current);
    (svg.transition().duration(220) as any).call((zoomRef.current as any).scaleBy, 1.25);
  };

  const onZoomOut = () => {
    if (!lib || !svgRef.current || !zoomRef.current) return;
    const svg = lib.d3.select(svgRef.current);
    (svg.transition().duration(220) as any).call((zoomRef.current as any).scaleBy, 1 / 1.25);
  };

  const onResetZoom = () => {
    if (!lib) return;
    applyTransform(lib.d3.zoomIdentity.translate(0, 0).scale(1), 220);
  };

  const fitToView = (animateMs: number) => {
    if (!lib || !layout || !containerRef.current) return;
    const { width, height } = containerRef.current.getBoundingClientRect();
    if (width <= 0 || height <= 0) return;

    const bboxW = Math.max(1, layout.bbox.maxX - layout.bbox.minX);
    const bboxH = Math.max(1, layout.bbox.maxY - layout.bbox.minY);

    const scale = Math.min((width - VIEW_PADDING * 2) / bboxW, (height - VIEW_PADDING * 2) / bboxH);
    const clamped = Math.min(2.5, Math.max(0.15, scale));

    const tx = (width - bboxW * clamped) / 2 - layout.bbox.minX * clamped;
    const ty = (height - bboxH * clamped) / 2 - layout.bbox.minY * clamped;

    applyTransform(lib.d3.zoomIdentity.translate(tx, ty).scale(clamped), animateMs);
  };

  const onFitToView = () => fitToView(320);

  const onFocusSelected = () => {
    if (!lib || !layout || !containerRef.current || !selectedNodeKey || !svgRef.current) return;
    const rect = layout.byKey.get(selectedNodeKey);
    if (!rect) return;

    const { width, height } = containerRef.current.getBoundingClientRect();
    if (width <= 0 || height <= 0) return;

    const k = lib.d3.zoomTransform(svgRef.current).k || 1;

    const cx = rect.x + rect.width / 2;
    const cy = rect.y + rect.height / 2;

    const tx = width / 2 - cx * k;
    const ty = height / 2 - cy * k;
    applyTransform(lib.d3.zoomIdentity.translate(tx, ty).scale(k), 280);
  };

  useEffect(() => {
    if (!lib || !layout || !zoomReady) return;
    if (didInitialFitRef.current) return;
    didInitialFitRef.current = true;
    fitToView(0);
  }, [layout, lib, zoomReady]);

  const lastFocusTokenRef = useRef(0);
  useEffect(() => {
    if (!focusToken) return;
    if (focusToken <= lastFocusTokenRef.current) return;
    if (!lib || !layout || !containerRef.current || !zoomReady || !selectedNodeKey) return;
    if (!layout.byKey.get(selectedNodeKey)) return;
    lastFocusTokenRef.current = focusToken;
    onFocusSelected();
  }, [focusToken, layout, lib, selectedNodeKey, zoomReady]);

  if (nodes.length === 0) {
    return <Text type="secondary">No plan tree</Text>;
  }

  return (
    <div ref={containerRef} className="dd-explain-diagram">
      {libError ? <Text type="secondary">Diagram unavailable: {libError}</Text> : null}

      <svg ref={svgRef} className="dd-diagram-svg">
        <title>Explain Diagram</title>
        <g ref={viewportRef}>
          <g className="dd-diagram-links">
            {layout
              ? layout.links.map((l) => {
                  const onSelectedPath = !!selectedPathLinkKeys?.has(l.key);
                  const linkClass = `dd-diagram-link${onSelectedPath ? " dd-diagram-link-selected" : ""}`;
                  return <path key={l.key} className={linkClass} d={linkPath(l)} />;
                })
              : null}
          </g>
          <g className="dd-diagram-nodes">
            {layout
              ? layout.nodes.map((n) => {
                  const isSelected = selectedNodeKey === n.key;

                  const metric = computeCardinalityStyle(n.node.cardinality, maxCardinality);
                  const accentColor = isSelected
                    ? CATPPUCCIN_MOCHA.mauve
                    : (metric.color ?? CATPPUCCIN_MOCHA.surface2);
                  const cardClass = `dd-diagram-card${isSelected ? " dd-diagram-card-selected" : ""}`;

                  return (
                    <foreignObject
                      key={n.key}
                      x={n.x - CARD_WIDTH / 2}
                      y={n.y}
                      width={CARD_WIDTH}
                      height={CARD_HEIGHT}
                      style={{ overflow: "visible" }}
                    >
                      <div
                        {...({ xmlns: "http://www.w3.org/1999/xhtml" } as any)}
                        className={cardClass}
                        style={{ ["--dd-diagram-accent" as any]: accentColor } as any}
                      >
                        <div className="dd-diagram-card-header">
                          <button
                            type="button"
                            className="dd-diagram-card-open"
                            onPointerDown={(e) => e.stopPropagation()}
                            onClick={() => onOpenNodeKey(n.key)}
                          >
                            <span className="dd-diagram-title">
                              <Text code className="dd-code-ellipsis">
                                {n.node.operator}
                              </Text>
                              {n.node.fragmentId != null ? (
                                <Tag className="dd-diagram-tag" color="geekblue">
                                  Fragment {n.node.fragmentId}
                                </Tag>
                              ) : null}
                            </span>
                            {n.node.table ? (
                              <span className="dd-code-ellipsis">
                                <Text type="secondary">TABLE: </Text>
                                <Text code>{n.node.table}</Text>
                              </span>
                            ) : null}
                          </button>

                          {n.hasChildren ? (
                            <Button
                              type="text"
                              size="small"
                              className="dd-diagram-collapse-btn"
                              onPointerDown={(e) => e.stopPropagation()}
                              icon={
                                collapsedChildrenKeys.has(n.key) ? (
                                  <PlusSquareOutlined />
                                ) : (
                                  <MinusSquareOutlined />
                                )
                              }
                              onClick={(e) => {
                                e.preventDefault();
                                e.stopPropagation();
                                setCollapsedChildrenKeys((prev) => toggleKey(prev, n.key));
                              }}
                              aria-label={
                                collapsedChildrenKeys.has(n.key)
                                  ? "Expand children"
                                  : "Collapse children"
                              }
                            />
                          ) : null}
                        </div>

                        {metric.color ? (
                          <div className="dd-diagram-metric">
                            <div
                              className="dd-diagram-metric-bar"
                              style={{
                                width: `${Math.round(metric.pct * 100)}%`,
                                backgroundColor: accentColor,
                              }}
                            />
                          </div>
                        ) : null}

                        {n.hasChildren && collapsedChildrenKeys.has(n.key) ? (
                          <div className="dd-diagram-collapsed-hint">
                            <Text type="secondary">
                              collapsed <Text code>{n.collapsedChildCount}</Text> children
                            </Text>
                          </div>
                        ) : null}
                      </div>
                    </foreignObject>
                  );
                })
              : null}
          </g>
        </g>
      </svg>

      <ExplainDiagramControls
        onZoomIn={onZoomIn}
        onZoomOut={onZoomOut}
        onResetZoom={onResetZoom}
        onFitToView={onFitToView}
        onFocusSelected={onFocusSelected}
        canFocusSelected={!!selectedNodeKey && !!layout?.byKey.get(selectedNodeKey)}
      />
    </div>
  );
}
