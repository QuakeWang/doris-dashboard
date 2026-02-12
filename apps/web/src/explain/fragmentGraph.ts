import type { ExplainNode } from "./types";
import { lookupKv, parseNumberLike } from "./utils";

export type FragmentGraphNode = {
  fragmentId: number;
  level: number;
  partition: string | null;
  hasColocatePlanNode: boolean | null;
  rootOperator: string | null;
  nodeCount: number;
  joinCount: number;
  scanCount: number;
  runtimeFilterCount: number;
  maxCardinality: number | null;
  tables: string[];
  producerExchangeIds: string[];
  consumerExchangeIds: string[];
};

export type FragmentGraphEdge = {
  fromFragmentId: number;
  toFragmentId: number;
  exchangeIds: string[];
};

export type FragmentGraph = {
  nodes: FragmentGraphNode[];
  edges: FragmentGraphEdge[];
};

function normalizeExchangeId(raw: string | number | null | undefined): string | null {
  if (raw == null) return null;
  const text = String(raw).trim();
  if (!text) return null;
  if (!/^\d+$/.test(text)) return text;
  const n = Number(text);
  if (!Number.isFinite(n)) return text;
  return String(n);
}

function extractExchangeIdFromText(text: string): string | null {
  const m = text.match(/EXCHANGE\s+ID\s*:\s*([0-9]+)/i);
  if (!m) return null;
  return normalizeExchangeId(m[1]);
}

function extractExchangeIdFromNode(node: ExplainNode): string | null {
  const kvId = lookupKv(node, "EXCHANGE ID");
  if (kvId) {
    const normalized = normalizeExchangeId(kvId);
    if (normalized) return normalized;
  }

  for (const seg of node.segments) {
    const id = extractExchangeIdFromText(seg);
    if (id) return id;
  }

  if (node.rawLine) {
    const id = extractExchangeIdFromText(node.rawLine);
    if (id) return id;
  }

  return null;
}

function extractConsumerExchangeId(node: ExplainNode): string | null {
  const fromSegments = extractExchangeIdFromNode(node);
  if (fromSegments) return fromSegments;
  if (node.nodeId != null) return normalizeExchangeId(node.nodeId);
  if (node.idsRaw) return normalizeExchangeId(node.idsRaw);
  return null;
}

function isFragmentHeader(node: ExplainNode): boolean {
  return node.operator.toUpperCase().startsWith("PLAN FRAGMENT");
}

function isExchangeConsumer(node: ExplainNode): boolean {
  const op = node.operator.toUpperCase();
  if (!op.includes("EXCHANGE")) return false;
  if (op.includes("STREAM DATA SINK")) return false;
  if (op.endsWith("SINK")) return false;
  return true;
}

function isStreamDataSink(node: ExplainNode): boolean {
  if (/STREAM\s+DATA\s+SINK/i.test(node.operator)) return true;
  if (/DATASTREAMSINK/i.test(node.operator)) return true;
  if (node.segments.some((seg) => /STREAM\s+DATA\s+SINK/i.test(seg))) return true;
  if (node.rawLine && /STREAM\s+DATA\s+SINK/i.test(node.rawLine)) return true;
  return false;
}

function withSortedUnique(values: Iterable<string>): string[] {
  return [...new Set(values)].sort((a, b) => {
    const na = Number(a);
    const nb = Number(b);
    if (Number.isFinite(na) && Number.isFinite(nb)) return na - nb;
    return a.localeCompare(b);
  });
}

function computeLevels(fragmentIds: number[], edges: FragmentGraphEdge[]): Map<number, number> {
  const indegree = new Map<number, number>();
  const outgoing = new Map<number, number[]>();
  const level = new Map<number, number>();

  for (const fragmentId of fragmentIds) {
    indegree.set(fragmentId, 0);
    outgoing.set(fragmentId, []);
    level.set(fragmentId, 0);
  }

  for (const edge of edges) {
    if (!indegree.has(edge.fromFragmentId)) continue;
    if (!indegree.has(edge.toFragmentId)) continue;
    indegree.set(edge.toFragmentId, (indegree.get(edge.toFragmentId) ?? 0) + 1);
    outgoing.get(edge.fromFragmentId)?.push(edge.toFragmentId);
  }

  const queue: number[] = fragmentIds
    .filter((fragmentId) => (indegree.get(fragmentId) ?? 0) === 0)
    .sort((a, b) => a - b);
  const visited = new Set<number>();

  while (queue.length > 0) {
    const fragmentId = queue.shift();
    if (fragmentId == null) continue;
    visited.add(fragmentId);
    const nextLevel = (level.get(fragmentId) ?? 0) + 1;
    for (const child of outgoing.get(fragmentId) ?? []) {
      if ((level.get(child) ?? 0) < nextLevel) level.set(child, nextLevel);
      const remaining = (indegree.get(child) ?? 0) - 1;
      indegree.set(child, remaining);
      if (remaining === 0) queue.push(child);
    }
    queue.sort((a, b) => a - b);
  }

  if (visited.size === fragmentIds.length) return level;

  for (const fragmentId of fragmentIds) {
    if (visited.has(fragmentId)) continue;
    level.set(fragmentId, Math.max(0, level.get(fragmentId) ?? 0));
  }
  return level;
}

export function buildFragmentGraph(nodes: ExplainNode[]): FragmentGraph {
  const byFragment = new Map<number, ExplainNode[]>();
  const fragmentIds = new Set<number>();
  const consumerFragmentsByExchangeId = new Map<string, Set<number>>();
  const producerFragmentsByExchangeId = new Map<string, Set<number>>();
  const consumerExchangeIdsByFragment = new Map<number, Set<string>>();
  const producerExchangeIdsByFragment = new Map<number, Set<string>>();

  for (const node of nodes) {
    if (node.fragmentId == null) continue;
    fragmentIds.add(node.fragmentId);
    const arr = byFragment.get(node.fragmentId) ?? [];
    arr.push(node);
    byFragment.set(node.fragmentId, arr);

    if (isExchangeConsumer(node)) {
      const exchangeId = extractConsumerExchangeId(node);
      if (!exchangeId) continue;
      const fragments = consumerFragmentsByExchangeId.get(exchangeId) ?? new Set<number>();
      fragments.add(node.fragmentId);
      consumerFragmentsByExchangeId.set(exchangeId, fragments);

      const fragmentExchangeIds =
        consumerExchangeIdsByFragment.get(node.fragmentId) ?? new Set<string>();
      fragmentExchangeIds.add(exchangeId);
      consumerExchangeIdsByFragment.set(node.fragmentId, fragmentExchangeIds);
      continue;
    }

    if (!isStreamDataSink(node)) continue;
    const exchangeId = extractExchangeIdFromNode(node);
    if (!exchangeId) continue;

    const fragments = producerFragmentsByExchangeId.get(exchangeId) ?? new Set<number>();
    fragments.add(node.fragmentId);
    producerFragmentsByExchangeId.set(exchangeId, fragments);

    const fragmentExchangeIds =
      producerExchangeIdsByFragment.get(node.fragmentId) ?? new Set<string>();
    fragmentExchangeIds.add(exchangeId);
    producerExchangeIdsByFragment.set(node.fragmentId, fragmentExchangeIds);
  }

  const edgeExchangeMap = new Map<string, Set<string>>();
  for (const [exchangeId, producers] of producerFragmentsByExchangeId) {
    const consumers = consumerFragmentsByExchangeId.get(exchangeId);
    if (!consumers) continue;

    for (const producer of producers) {
      for (const consumer of consumers) {
        if (producer === consumer) continue;
        const key = `${producer}->${consumer}`;
        const exchangeIds = edgeExchangeMap.get(key) ?? new Set<string>();
        exchangeIds.add(exchangeId);
        edgeExchangeMap.set(key, exchangeIds);
      }
    }
  }

  const edges: FragmentGraphEdge[] = [...edgeExchangeMap.entries()]
    .map(([key, exchangeIds]) => {
      const [fromText, toText] = key.split("->");
      return {
        fromFragmentId: Number(fromText),
        toFragmentId: Number(toText),
        exchangeIds: withSortedUnique(exchangeIds),
      };
    })
    .sort((a, b) => {
      if (a.fromFragmentId !== b.fromFragmentId) {
        return a.fromFragmentId - b.fromFragmentId;
      }
      return a.toFragmentId - b.toFragmentId;
    });

  const sortedFragmentIds = [...fragmentIds].sort((a, b) => a - b);
  const levelByFragment = computeLevels(sortedFragmentIds, edges);

  const graphNodes: FragmentGraphNode[] = sortedFragmentIds.map((fragmentId) => {
    const fragmentNodes = byFragment.get(fragmentId) ?? [];
    const header = fragmentNodes.find(isFragmentHeader) ?? null;
    const payloadNodes = fragmentNodes.filter((node) => !isFragmentHeader(node));
    const rootOperator = payloadNodes[0]?.operator ?? null;
    const joinCount = payloadNodes.filter((node) => /\bJOIN\b/i.test(node.operator)).length;
    const scanCount = payloadNodes.filter((node) => /\bSCAN\b/i.test(node.operator)).length;
    const runtimeFilterCount = payloadNodes.filter(
      (node) => !!lookupKv(node, "RUNTIME_FILTERS")
    ).length;
    const maxCardinality = payloadNodes.reduce<number | null>((max, node) => {
      const value = parseNumberLike(node.cardinality);
      if (value == null) return max;
      if (max == null) return value;
      return Math.max(max, value);
    }, null);
    const tables = withSortedUnique(
      payloadNodes.map((node) => node.table).filter((value): value is string => !!value?.trim())
    );
    const partition = header ? lookupKv(header, "PARTITION") : null;
    const hasColoRaw = header ? lookupKv(header, "HAS_COLO_PLAN_NODE") : null;
    const hasColocatePlanNode = hasColoRaw == null ? null : /^true$/i.test(hasColoRaw.trim());

    return {
      fragmentId,
      level: levelByFragment.get(fragmentId) ?? 0,
      partition,
      hasColocatePlanNode,
      rootOperator,
      nodeCount: payloadNodes.length,
      joinCount,
      scanCount,
      runtimeFilterCount,
      maxCardinality,
      tables,
      producerExchangeIds: withSortedUnique(producerExchangeIdsByFragment.get(fragmentId) ?? []),
      consumerExchangeIds: withSortedUnique(consumerExchangeIdsByFragment.get(fragmentId) ?? []),
    };
  });

  return {
    nodes: graphNodes,
    edges,
  };
}
