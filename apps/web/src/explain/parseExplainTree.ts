import type { ExplainNode, ExplainParseResult } from "./types";

const headerRe = /^\[(?<ids>[^\]]*)\]:\[(?<id>\d+)\s*:\s*(?<op>.+)\]$/;
const fragmentRe = /^\[Fragment:\s*(?<id>\d+)\]$/;

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

    const kv = extractKv(segments);
    const node: ExplainNode = {
      key: `n${nodes.length}`,
      depth,
      fragmentId,
      idsRaw: headerInfo.idsRaw,
      nodeId: headerInfo.nodeId,
      operator: headerInfo.operator,
      segments,
      kv,
      table: kv["TABLE"] ?? null,
      cardinality: kv["cardinality"] ?? null,
      predicates: kv["PREDICATES"] ?? null,
      rawLine: line,
    };
    nodes.push(node);
  }

  if (nodes.length === 0) {
    return {
      ok: false,
      rawText: normalized.text,
      error: "No EXPLAIN TREE nodes found in input.",
    };
  }

  return {
    ok: true,
    format: "tree",
    rawText: normalized.text,
    fragments: [...fragments].sort((a, b) => a - b),
    nodes,
    warnings,
  };
}
