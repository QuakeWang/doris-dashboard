import { describe, expect, it } from "vitest";
import {
  EXPLAIN_PLAN_FIXTURE_SIMPLE,
  EXPLAIN_TREE_FIXTURE_MULTI_FRAGMENT,
  EXPLAIN_TREE_FIXTURE_SIMPLE,
} from "./fixtures";
import { buildFragmentGraph } from "./fragmentGraph";
import {
  parseExplain,
  parseExplainPlan,
  parseExplainTree,
  selectNodesByFragment,
} from "./parseExplainTree";

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
    expect(scan?.kv.afterfilter).toBe("1,841,539");
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

  it("parses basic EXPLAIN PLAN output", () => {
    const res = parseExplainPlan(EXPLAIN_PLAN_FIXTURE_SIMPLE);
    expect(res.ok).toBe(true);
    if (!res.ok) return;
    expect(res.format).toBe("plan");
    expect(res.fragments).toEqual([0, 1]);
    const scan = res.nodes.find((n) => n.operator.startsWith("VOlapScanNode"));
    expect(scan).toBeTruthy();
    expect(scan?.table).toBe("test_db.t(t)");
    expect(scan?.predicates).toBe("((k1[#0] >= '2024-01-10'))");
    expect(scan?.kv.partitions).toContain("p202401");
  });

  it("auto-detects plan output", () => {
    const res = parseExplain(EXPLAIN_PLAN_FIXTURE_SIMPLE);
    expect(res.ok).toBe(true);
    if (!res.ok) return;
    expect(res.format).toBe("plan");
  });

  it("returns combined error when both parsers fail", () => {
    const res = parseExplain("hello");
    expect(res.ok).toBe(false);
    if (res.ok) return;
    expect(res.error).toContain("EXPLAIN TREE");
    expect(res.error).toContain("EXPLAIN PLAN");
  });

  it("merges repeated kv keys and splits multi-kv segments", () => {
    const text = `PLAN FRAGMENT 0
  1:VHASH JOIN
  |  equal join conjunct: (a=b)
  |  equal join conjunct: (c=d)
  0:VOlapScanNode(1)
     cardinality=1, avgRowSize=2.0, numNodes=3
`;
    const res = parseExplainPlan(text);
    expect(res.ok).toBe(true);
    if (!res.ok) return;
    const join = res.nodes.find((n) => n.operator.includes("JOIN"));
    expect(join?.kv.EQUAL_JOIN_CONJUNCT).toContain("(a=b)");
    expect(join?.kv.EQUAL_JOIN_CONJUNCT).toContain("(c=d)");
    const scan = res.nodes.find((n) => n.operator.startsWith("VOlapScanNode"));
    expect(scan?.kv.avgrowsize).toBe("2.0");
  });

  it("builds fragment graph edges from producer and consumer exchanges", () => {
    const parsed = parseExplain(EXPLAIN_TREE_FIXTURE_MULTI_FRAGMENT);
    expect(parsed.ok).toBe(true);
    if (!parsed.ok) return;

    const graph = buildFragmentGraph(parsed.nodes);
    expect(graph.nodes.map((node) => node.fragmentId)).toEqual([0, 1, 2]);
    expect(graph.edges).toEqual([
      { fromFragmentId: 1, toFragmentId: 0, exchangeIds: ["5"] },
      { fromFragmentId: 2, toFragmentId: 1, exchangeIds: ["2"] },
    ]);
  });
});
