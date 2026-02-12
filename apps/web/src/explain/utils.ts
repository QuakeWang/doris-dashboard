import type { ExplainNode } from "./types";

export function lookupKv(node: ExplainNode, key: string): string | null {
  return node.kv[key] ?? node.kv[key.toLowerCase()] ?? node.kv[key.toUpperCase()] ?? null;
}

export function parseNumberLike(v: string | null): number | null {
  if (!v) return null;
  const cleaned = v.replace(/[, _]/g, "").trim();
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

export function toggleKey<T>(prev: Set<T>, key: T): Set<T> {
  const next = new Set(prev);
  if (next.has(key)) next.delete(key);
  else next.add(key);
  return next;
}
