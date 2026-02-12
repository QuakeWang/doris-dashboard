import type { ExplainNode } from "./types";
import { lookupKv } from "./utils";

export type ExplainMaterializationFailure = {
  name: string;
  reason: string | null;
};

export type ExplainMaterializationSummary = {
  chosen: string[];
  successButNotChosen: string[];
  failed: ExplainMaterializationFailure[];
};

export type ExplainPruningSignal = {
  active: boolean;
  selected: number | null;
  total: number | null;
  ratio: number | null;
  evidence: string | null;
};

export type ExplainTransparentRewriteSignal = {
  level: "hit" | "candidate" | "none";
  indexName: string | null;
  chosenMaterializations: string[];
};

export type ExplainNodeOptimizationSignals = {
  predicatePushdown: {
    active: boolean;
    evidence: string | null;
  };
  partitionPruning: ExplainPruningSignal;
  tabletPruning: ExplainPruningSignal;
  transparentRewrite: ExplainTransparentRewriteSignal;
  runtimeFilters: string | null;
};

export type ExplainFragmentOptimizationSignals = {
  predicatePushdownCount: number;
  pruningCount: number;
  rewriteCount: number;
  scanCount: number;
};

function normalizeMaterializationName(raw: string): string {
  return raw.trim().replace(/\s+/g, "");
}

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values.map(normalizeMaterializationName).filter(Boolean))];
}

function parsePruningSignal(value: string | null): ExplainPruningSignal {
  if (!value) {
    return {
      active: false,
      selected: null,
      total: null,
      ratio: null,
      evidence: null,
    };
  }

  const match = value.match(/(\d+)\s*\/\s*(\d+)/);
  if (!match) {
    return {
      active: false,
      selected: null,
      total: null,
      ratio: null,
      evidence: value,
    };
  }

  const selected = Number(match[1]);
  const total = Number(match[2]);
  const hasNumbers = Number.isFinite(selected) && Number.isFinite(total) && total > 0;
  if (!hasNumbers) {
    return {
      active: false,
      selected: null,
      total: null,
      ratio: null,
      evidence: value,
    };
  }

  return {
    active: selected < total,
    selected,
    total,
    ratio: selected / total,
    evidence: value,
  };
}

function parseIndexName(tableText: string | null): string | null {
  if (!tableText) return null;
  const match = tableText.match(/\(([^()]+)\)\s*$/);
  if (!match) return null;
  const indexName = match[1]?.trim() ?? "";
  return indexName || null;
}

function looksLikeTransparentRewriteIndex(indexName: string | null): boolean {
  if (!indexName) return false;
  return /(mv|rollup|materialized|agg)/i.test(indexName);
}

function isScanLikeOperator(operator: string): boolean {
  return /SCAN/i.test(operator);
}

function isFragmentHeaderNode(operator: string): boolean {
  return operator.toUpperCase().startsWith("PLAN FRAGMENT");
}

function parsePredicateEvidence(node: ExplainNode): string | null {
  if (node.predicates?.trim()) return node.predicates.trim();
  const predicates = lookupKv(node, "PREDICATES");
  if (predicates?.trim()) return predicates.trim();
  const frontendPredicates = lookupKv(node, "FRONTEND_PREDICATES");
  if (frontendPredicates?.trim()) return frontendPredicates.trim();
  return null;
}

export function buildNodeOptimizationSignals(
  node: ExplainNode,
  materializationSummary: ExplainMaterializationSummary | null
): ExplainNodeOptimizationSignals {
  const predicateEvidence = parsePredicateEvidence(node);
  const partitionSignal = parsePruningSignal(lookupKv(node, "partitions"));
  const tabletSignal = parsePruningSignal(lookupKv(node, "tablets"));
  const runtimeFilters = lookupKv(node, "RUNTIME_FILTERS");
  const scanLike = isScanLikeOperator(node.operator);

  const chosenMaterializations = materializationSummary?.chosen ?? [];
  const indexName = parseIndexName(node.table ?? lookupKv(node, "TABLE"));
  const transparentRewriteLevel: ExplainTransparentRewriteSignal["level"] =
    scanLike && chosenMaterializations.length > 0
      ? "hit"
      : scanLike && looksLikeTransparentRewriteIndex(indexName)
        ? "candidate"
        : "none";

  return {
    predicatePushdown: {
      active: !!predicateEvidence,
      evidence: predicateEvidence,
    },
    partitionPruning: partitionSignal,
    tabletPruning: tabletSignal,
    transparentRewrite: {
      level: transparentRewriteLevel,
      indexName,
      chosenMaterializations,
    },
    runtimeFilters: runtimeFilters?.trim() ? runtimeFilters.trim() : null,
  };
}

export function parseMaterializationSummary(rawText: string): ExplainMaterializationSummary | null {
  if (!/MATERIALIZATION/i.test(rawText)) return null;

  const chosen: string[] = [];
  const successButNotChosen: string[] = [];
  const failed: ExplainMaterializationFailure[] = [];

  const lines = rawText.split("\n");
  let lastFailure: ExplainMaterializationFailure | null = null;

  for (const line of lines) {
    const text = line.trim();
    if (!text) {
      lastFailure = null;
      continue;
    }

    const chooseMatch = text.match(/^(?:RBO|CBO)\.([^\s]+)\s+chose$/i);
    if (chooseMatch) {
      chosen.push(chooseMatch[1]);
      lastFailure = null;
      continue;
    }

    const notChooseMatch = text.match(/^(?:RBO|CBO)\.([^\s]+)\s+not chose$/i);
    if (notChooseMatch) {
      successButNotChosen.push(notChooseMatch[1]);
      lastFailure = null;
      continue;
    }

    const failMatch = text.match(/^(?:RBO|CBO)\.([^\s]+)\s+fail$/i);
    if (failMatch) {
      lastFailure = { name: failMatch[1], reason: null };
      failed.push(lastFailure);
      continue;
    }

    const failInfoMatch = text.match(/^FailInfo:\s*(.+)$/i);
    if (failInfoMatch && lastFailure) {
      lastFailure.reason = failInfoMatch[1].trim();
      continue;
    }

    lastFailure = null;
  }

  const normalizedChosen = uniqueStrings(chosen);
  const normalizedSuccessButNotChosen = uniqueStrings(successButNotChosen);

  const hasContent =
    normalizedChosen.length > 0 || normalizedSuccessButNotChosen.length > 0 || failed.length > 0;
  if (!hasContent) return null;

  return {
    chosen: normalizedChosen,
    successButNotChosen: normalizedSuccessButNotChosen,
    failed,
  };
}

export function buildFragmentOptimizationSignals(
  nodes: ExplainNode[],
  nodeSignalsByKey: Map<string, ExplainNodeOptimizationSignals>
): Map<number, ExplainFragmentOptimizationSignals> {
  const byFragment = new Map<number, ExplainFragmentOptimizationSignals>();

  for (const node of nodes) {
    if (node.fragmentId == null) continue;
    if (isFragmentHeaderNode(node.operator)) continue;

    const current =
      byFragment.get(node.fragmentId) ??
      ({
        predicatePushdownCount: 0,
        pruningCount: 0,
        rewriteCount: 0,
        scanCount: 0,
      } as ExplainFragmentOptimizationSignals);

    const signals = nodeSignalsByKey.get(node.key);
    if (!signals) continue;
    if (signals.predicatePushdown.active) current.predicatePushdownCount += 1;
    if (signals.partitionPruning.active || signals.tabletPruning.active) {
      current.pruningCount += 1;
    }
    if (signals.transparentRewrite.level !== "none") current.rewriteCount += 1;
    if (isScanLikeOperator(node.operator)) current.scanCount += 1;

    byFragment.set(node.fragmentId, current);
  }

  return byFragment;
}
