import { describe, expect, it } from "vitest";
import { EXPLAIN_TREE_FIXTURE_MULTI_FRAGMENT, EXPLAIN_TREE_FIXTURE_SIMPLE } from "./fixtures";
import { parseExplainTree, selectNodesByFragment } from "./parseExplainTree";

describe("parseExplainTree", () => {
  it("parses basic EXPLAIN TREE output", () => {
    const res = parseExplainTree(EXPLAIN_TREE_FIXTURE_SIMPLE);
    expect(res.ok).toBe(true);
    if (!res.ok) return;
    expect(res.fragments).toEqual([0]);
    expect(res.nodes).toHaveLength(2);
    expect(res.nodes[0].operator).toBe("ResultSink");
    expect(res.nodes[1].operator).toBe("VUNION");
    expect(res.nodes[1].depth).toBe(1);
  });

  it("extracts fragments and kv fields", () => {
    const res = parseExplainTree(EXPLAIN_TREE_FIXTURE_MULTI_FRAGMENT);
    expect(res.ok).toBe(true);
    if (!res.ok) return;
    expect(res.fragments).toEqual([0, 1, 2]);
    const scan = res.nodes.find((n) => n.operator === "VOlapScanNode");
    expect(scan).toBeTruthy();
    expect(scan?.table).toBe("tpch.lineitem(lineitem)");
    expect(scan?.cardinality).toBe("149,996,355");
    expect(scan?.kv["afterfilter"]).toBe("1,841,539");
    expect(scan?.predicates).toBe("2");
  });

  it("normalizes fragment depth to start from 0", () => {
    const res = parseExplainTree(EXPLAIN_TREE_FIXTURE_MULTI_FRAGMENT);
    expect(res.ok).toBe(true);
    if (!res.ok) return;
    const f2 = selectNodesByFragment(res.nodes, 2);
    expect(f2.length).toBeGreaterThan(0);
    const minDepth = Math.min(...f2.map((n) => n.depth));
    expect(minDepth).toBe(0);
  });

  it("returns ok=false for non-tree text", () => {
    const res = parseExplainTree("hello");
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.error).toContain("No EXPLAIN TREE nodes");
  });
});
