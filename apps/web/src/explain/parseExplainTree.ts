import type { ExplainNode, ExplainParseResult } from "./types";

const headerRe = /^\[(?<ids>[^\]]*)\]:\[(?<id>\d+)\s*:\s*(?<op>.+)\]$/;
const fragmentRe = /^\[Fragment:\s*(?<id>\d+)\]$/;
const planFragmentRe = /^PLAN FRAGMENT\s+(?<id>\d+)\b/i;
const planNodeRe = /^(?<prefix>[|\s-]*?)(?<id>\d+):(?<op>.+)$/;
const planSplitRe = /,\s+(?=[A-Za-z_][A-Za-z0-9_ ]*\s*[:=])/;

function normalizeExplainText(rawText: string): { text: string; warnings: string[] } {
  const warnings: string[] = [];
  const lines = rawText.replace(/\r\n/g, "\n").replace(/\r/g, "\n").split("\n");

  const normalized: string[] = [];
  let strippedTableLines = 0;

  for (const line of lines) {
    const v = line.trimEnd();
    if (!v) {
      normalized.push("");
      continue;
    }
    if (v.startsWith("+") && v.endsWith("+")) {
      const mid = v.slice(1, -1).trim();
      if (mid && /^-+$/.test(mid.replace(/\+/g, ""))) {
        strippedTableLines++;
        continue;
      }
      if (/^-+$/.test(mid)) {
        strippedTableLines++;
        continue;
      }
    }
    if (v.startsWith("|") && v.endsWith("|")) {
      strippedTableLines++;
      normalized.push(v.slice(1, -1).trim());
      continue;
    }
    normalized.push(v);
  }

  if (strippedTableLines > 0) warnings.push("mysql table formatting detected and normalized");
  return { text: normalized.join("\n"), warnings };
}

function countLeadingDashes(line: string): number {
  let i = 0;
  while (i < line.length && line[i] === "-") i++;
  return i;
}

function normalizeKeyFromEq(key: string): string {
  return key.trim().toLowerCase();
}

function normalizeKeyFromColon(key: string): string {
  return key.trim().toUpperCase().replace(/\s+/g, "_");
}

function extractKv(segments: string[]): Record<string, string> {
  const kv: Record<string, string> = {};
  for (const seg of segments) {
    const eq = seg.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.+)$/);
    if (eq) {
      kv[normalizeKeyFromEq(eq[1])] = eq[2].trim();
      continue;
    }
    const colon = seg.match(/^([A-Za-z_][A-Za-z0-9_ ]*):\s*(.+)$/);
    if (colon) {
      kv[normalizeKeyFromColon(colon[1])] = colon[2].trim();
    }
  }
  return kv;
}

function mergeKv(dst: Record<string, string>, next: Record<string, string>): void {
  for (const [k, v] of Object.entries(next)) {
    if (!v) continue;
    const prev = dst[k];
    if (!prev) {
      dst[k] = v;
      continue;
    }
    if (prev === v) continue;
    if (prev.split("; ").includes(v)) continue;
    dst[k] = `${prev}; ${v}`;
  }
}

function splitPlanSegment(seg: string): string[] {
  const trimmed = seg.trim();
  if (!trimmed) return [];
  if (!planSplitRe.test(trimmed)) return [trimmed];
  return trimmed
    .split(planSplitRe)
    .map((s) => s.trim())
    .filter(Boolean);
}

function isPlanMetaLine(line: string): boolean {
  const v = line.trim();
  if (!v) return true;
  if (/^Explain String/i.test(v)) return true;
  if (/^=+/.test(v)) return true;
  if (/^planed /i.test(v)) return true;
  return false;
}

function isPlanSinkLine(line: string): boolean {
  const v = line.trim();
  if (!v) return false;
  if (!v.toUpperCase().endsWith("SINK")) return false;
  if (v.includes(":")) return false;
  return /^[A-Za-z0-9_ ]+SINK$/i.test(v);
}

function cleanPlanSegment(line: string): string {
  let v = line.trimEnd();
  v = v.replace(/^\s*\|/, "");
  v = v.trim();
  return v;
}

function planDepthFromPrefix(prefix: string): number {
  const v = prefix.replace(/\s+/g, "");
  const pipeCount = (v.match(/\|/g) ?? []).length;
  const dashCount = (v.match(/-/g) ?? []).length;
  return pipeCount + Math.max(0, Math.floor(dashCount / 4));
}

function parsePlanNodeLine(
  line: string
): { depth: number; nodeId: number; operator: string; rawLine: string } | null {
  const m = line.match(planNodeRe);
  if (!m || !m.groups) return null;
  const nodeId = Number(m.groups.id);
  if (!Number.isFinite(nodeId)) return null;
  const operator = m.groups.op.trim();
  const depth = planDepthFromPrefix(m.groups.prefix ?? "");
  return { depth, nodeId, operator, rawLine: line };
}

function isTreeNodeLine(line: string): boolean {
  const rest = line.replace(/^-+/, "").trimStart();
  return rest.startsWith("[") && rest.includes("]||");
}

function parseHeader(header: string): { idsRaw: string; nodeId: number; operator: string } | null {
  const m = header.match(headerRe);
  if (!m || !m.groups) return null;
  const nodeId = Number(m.groups.id);
  if (!Number.isFinite(nodeId)) return null;
  return { idsRaw: m.groups.ids, nodeId, operator: m.groups.op.trim() };
}

type ExplainNodeInput = {
  depth: number;
  fragmentId: number | null;
  idsRaw: string;
  nodeId: number | null;
  operator: string;
  segments: string[];
  rawLine: string;
  kv?: Record<string, string>;
};

function buildExplainNode(key: string, input: ExplainNodeInput): ExplainNode {
  const kv = input.kv ?? extractKv(input.segments);
  return {
    key,
    depth: input.depth,
    fragmentId: input.fragmentId,
    idsRaw: input.idsRaw,
    nodeId: input.nodeId,
    operator: input.operator,
    segments: input.segments,
    kv,
    table: kv["TABLE"] ?? null,
    cardinality: kv["cardinality"] ?? null,
    predicates: kv["PREDICATES"] ?? null,
    rawLine: input.rawLine,
  };
}

function appendExplainNode(nodes: ExplainNode[], input: ExplainNodeInput): number {
  const node = buildExplainNode(`n${nodes.length}`, input);
  nodes.push(node);
  return nodes.length - 1;
}

function toSortedFragments(fragments: Set<number>): number[] {
  return [...fragments].sort((a, b) => a - b);
}

function buildParseErrorResult(rawText: string, error: string): ExplainParseResult {
  return {
    ok: false,
    rawText,
    error,
  };
}

function buildParseOkResult(
  format: "tree" | "plan",
  rawText: string,
  fragments: Set<number>,
  nodes: ExplainNode[],
  warnings: string[]
): ExplainParseResult {
  return {
    ok: true,
    format,
    rawText,
    fragments: toSortedFragments(fragments),
    nodes,
    warnings,
  };
}

export function selectNodesByFragment(
  nodes: ExplainNode[],
  fragmentId: number | null
): ExplainNode[] {
  if (fragmentId == null) return nodes;
  const filtered = nodes.filter((n) => n.fragmentId === fragmentId);
  if (filtered.length === 0) return [];
  const baseDepth = Math.min(...filtered.map((n) => n.depth));
  return filtered.map((n) => ({ ...n, depth: Math.max(0, n.depth - baseDepth) }));
}

export function parseExplainTree(rawText: string): ExplainParseResult {
  const normalized = normalizeExplainText(rawText);
  const lines = normalized.text.split("\n");

  const nodes: ExplainNode[] = [];
  const fragments = new Set<number>();
  const warnings = [...normalized.warnings];

  for (const line of lines) {
    if (!line.trim()) continue;
    if (!isTreeNodeLine(line)) continue;

    const leadingDashes = countLeadingDashes(line);
    const depth = Math.max(0, Math.floor(leadingDashes / 2));
    const rest = line.slice(leadingDashes).trimStart();
    const segments = rest
      .split("||")
      .map((s) => s.trim())
      .filter((s) => s.length > 0);
    if (segments.length === 0) continue;

    const header = segments[0];
    const headerInfo = parseHeader(header);
    if (!headerInfo) {
      warnings.push(`unrecognized header: ${header}`);
      continue;
    }

    let fragmentId: number | null = null;
    for (const seg of segments) {
      const fm = seg.match(fragmentRe);
      if (!fm || !fm.groups) continue;
      const id = Number(fm.groups.id);
      if (!Number.isFinite(id)) continue;
      fragmentId = id;
      fragments.add(id);
      break;
    }

    appendExplainNode(nodes, {
      depth,
      fragmentId,
      idsRaw: headerInfo.idsRaw,
      nodeId: headerInfo.nodeId,
      operator: headerInfo.operator,
      segments,
      kv: extractKv(segments),
      rawLine: line,
    });
  }

  if (nodes.length === 0) {
    return buildParseErrorResult(normalized.text, "No EXPLAIN TREE nodes found in input.");
  }

  return buildParseOkResult("tree", normalized.text, fragments, nodes, warnings);
}

export function parseExplainPlan(rawText: string): ExplainParseResult {
  const normalized = normalizeExplainText(rawText);
  const lines = normalized.text.split("\n");

  const nodes: ExplainNode[] = [];
  const fragments = new Set<number>();
  const warnings = [...normalized.warnings];

  let currentFragmentId: number | null = null;
  let currentNodeIndex: number | null = null;
  let fragmentNodeIndex: number | null = null;

  const appendSegment = (node: ExplainNode, seg: string) => {
    if (!seg) return;
    node.segments.push(seg);
    for (const part of splitPlanSegment(seg)) {
      mergeKv(node.kv, extractKv([part]));
    }
    if (node.table == null && node.kv["TABLE"]) node.table = node.kv["TABLE"];
    if (node.cardinality == null && node.kv["cardinality"]) {
      node.cardinality = node.kv["cardinality"];
    }
    if (node.predicates == null && node.kv["PREDICATES"]) {
      node.predicates = node.kv["PREDICATES"];
    }
  };

  for (const rawLine of lines) {
    const trimmed = rawLine.trim();
    if (!trimmed) continue;
    if (isPlanMetaLine(trimmed)) continue;

    const fragMatch = trimmed.match(planFragmentRe);
    if (fragMatch?.groups) {
      const id = Number(fragMatch.groups.id);
      if (Number.isFinite(id)) {
        currentFragmentId = id;
        fragments.add(id);
        currentNodeIndex = appendExplainNode(nodes, {
          depth: 0,
          fragmentId: currentFragmentId,
          idsRaw: "",
          nodeId: null,
          operator: `PLAN FRAGMENT ${id}`,
          segments: [`PLAN FRAGMENT ${id}`],
          kv: {},
          rawLine,
        });
        fragmentNodeIndex = currentNodeIndex;
      }
      continue;
    }

    const planNode = parsePlanNodeLine(rawLine);
    if (planNode) {
      const baseDepth = fragmentNodeIndex != null ? 1 : 0;
      const depth = baseDepth + planNode.depth;
      const header = `${planNode.nodeId}:${planNode.operator}`;
      currentNodeIndex = appendExplainNode(nodes, {
        depth,
        fragmentId: currentFragmentId,
        idsRaw: String(planNode.nodeId),
        nodeId: planNode.nodeId,
        operator: planNode.operator,
        segments: [header],
        kv: {},
        rawLine: planNode.rawLine,
      });
      continue;
    }

    if (isPlanSinkLine(trimmed)) {
      const baseDepth = fragmentNodeIndex != null ? 1 : 0;
      currentNodeIndex = appendExplainNode(nodes, {
        depth: baseDepth,
        fragmentId: currentFragmentId,
        idsRaw: "",
        nodeId: null,
        operator: trimmed,
        segments: [trimmed],
        kv: {},
        rawLine,
      });
      continue;
    }

    if (currentNodeIndex != null) {
      const seg = cleanPlanSegment(rawLine);
      appendSegment(nodes[currentNodeIndex], seg);
    }
  }

  if (nodes.length === 0) {
    return buildParseErrorResult(normalized.text, "No EXPLAIN PLAN nodes found in input.");
  }

  return buildParseOkResult("plan", normalized.text, fragments, nodes, warnings);
}

export function parseExplain(rawText: string): ExplainParseResult {
  const tree = parseExplainTree(rawText);
  if (tree.ok) return tree;
  const plan = parseExplainPlan(rawText);
  if (plan.ok) return plan;
  return buildParseErrorResult(tree.rawText, `${tree.error} | ${plan.error}`);
}
