export type ExplainParseResult =
  | {
      ok: true;
      format: "tree";
      rawText: string;
      fragments: number[];
      nodes: ExplainNode[];
      warnings: string[];
    }
  | { ok: false; rawText: string; error: string };

export interface ExplainNode {
  key: string;
  depth: number;
  fragmentId: number | null;

  idsRaw: string;
  nodeId: number | null;
  operator: string;

  segments: string[];
  kv: Record<string, string>;

  table: string | null;
  cardinality: string | null;
  predicates: string | null;

  rawLine: string;
}
