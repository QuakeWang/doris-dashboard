import { CATPPUCCIN_MOCHA } from "../theme/catppuccin";
import type { ExplainNode } from "./types";

export function parseNumberLike(v: string | null): number | null {
  if (!v) return null;
  const cleaned = v.replace(/[, _]/g, "").trim();
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function colorForPercent(p: number): string {
  if (p < 0.3) return CATPPUCCIN_MOCHA.green;
  if (p < 0.7) return CATPPUCCIN_MOCHA.peach;
  return CATPPUCCIN_MOCHA.red;
}

export function computeCardinalityStyle(
  cardinality: string | null,
  maxCardinality: number
): { pct: number; color: string | null } {
  const raw = parseNumberLike(cardinality);
  if (raw == null || maxCardinality <= 0) return { pct: 0, color: null };
  const pct = Math.min(1, Math.max(0, raw / maxCardinality));
  return { pct, color: colorForPercent(pct) };
}

export function toggleKey(prev: Set<string>, key: string): Set<string> {
  const next = new Set(prev);
  if (next.has(key)) next.delete(key);
  else next.add(key);
  return next;
}

export function buildParentByKey(nodes: ExplainNode[]): Map<string, string | null> {
  const parent = new Map<string, string | null>();
  const stack: Array<{ key: string }> = [];

  for (const n of nodes) {
    const depth = Math.max(0, n.depth);
    while (stack.length > depth) stack.pop();

    const effectiveDepth = Math.min(depth, stack.length);
    const parentKey =
      effectiveDepth === 0 ? null : (stack[Math.max(0, effectiveDepth - 1)]?.key ?? null);

    parent.set(n.key, parentKey);
    stack[effectiveDepth] = { key: n.key };
    stack.length = effectiveDepth + 1;
  }

  return parent;
}
