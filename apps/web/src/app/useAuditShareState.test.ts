import { describe, expect, it } from "vitest";
import {
  createInitialAuditShareSnapshot,
  deriveShareRankBy,
  reduceAuditShareSnapshot,
} from "./useAuditShareState";

describe("deriveShareRankBy", () => {
  it("forces memory rank for memory metric", () => {
    expect(deriveShareRankBy("memory", "totalCpuMs")).toBe("maxPeakMemBytes");
    expect(deriveShareRankBy("memory", "totalTimeMs")).toBe("maxPeakMemBytes");
  });

  it("switches memory rank back to metric-compatible rank", () => {
    expect(deriveShareRankBy("time", "maxPeakMemBytes")).toBe("totalTimeMs");
    expect(deriveShareRankBy("cpu", "maxPeakMemBytes")).toBe("totalCpuMs");
  });
});

describe("useAuditShareState reducer", () => {
  it("initializes with cpu metric defaults", () => {
    expect(createInitialAuditShareSnapshot()).toEqual({
      shareMetric: "cpu",
      shareRankBy: "totalCpuMs",
      shareChartType: "bar",
      shareTopN: 12,
    });
  });

  it("changeMetric updates metric and derives rankBy", () => {
    const state = reduceAuditShareSnapshot(createInitialAuditShareSnapshot(), {
      type: "changeMetric",
      metric: "memory",
    });
    expect(state.shareMetric).toBe("memory");
    expect(state.shareRankBy).toBe("maxPeakMemBytes");
  });

  it("keeps independent updates for rank/chart/topN", () => {
    const rankUpdated = reduceAuditShareSnapshot(createInitialAuditShareSnapshot(), {
      type: "changeRankBy",
      rankBy: "totalTimeMs",
    });
    const chartUpdated = reduceAuditShareSnapshot(rankUpdated, {
      type: "changeChartType",
      chartType: "pie",
    });
    const topNUpdated = reduceAuditShareSnapshot(chartUpdated, { type: "changeTopN", topN: 20 });

    expect(topNUpdated).toEqual({
      shareMetric: "cpu",
      shareRankBy: "totalTimeMs",
      shareChartType: "pie",
      shareTopN: 20,
    });
  });
});
