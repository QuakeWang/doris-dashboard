import { useCallback, useReducer } from "react";
import type { ShareRankBy } from "../db/client/protocol";

type ShareMetric = "cpu" | "time" | "memory";

export interface AuditShareSnapshot {
  shareMetric: ShareMetric;
  shareRankBy: ShareRankBy;
  shareChartType: "bar" | "pie";
  shareTopN: number;
}

type AuditShareAction =
  | { type: "changeMetric"; metric: ShareMetric }
  | { type: "changeRankBy"; rankBy: ShareRankBy }
  | { type: "changeChartType"; chartType: "bar" | "pie" }
  | { type: "changeTopN"; topN: number };

export interface AuditShareState {
  shareMetric: ShareMetric;
  shareRankBy: ShareRankBy;
  shareChartType: "bar" | "pie";
  shareTopN: number;
  onChangeShareMetric: (metric: ShareMetric) => void;
  onChangeShareRankBy: (rankBy: ShareRankBy) => void;
  onChangeShareChartType: (chartType: "bar" | "pie") => void;
  onChangeShareTopN: (topN: number) => void;
}

export function deriveShareRankBy(metric: ShareMetric, previous: ShareRankBy): ShareRankBy {
  if (metric === "memory") return "maxPeakMemBytes";
  if (previous !== "maxPeakMemBytes") return previous;
  return metric === "time" ? "totalTimeMs" : "totalCpuMs";
}

export function createInitialAuditShareSnapshot(): AuditShareSnapshot {
  return {
    shareMetric: "cpu",
    shareRankBy: "totalCpuMs",
    shareChartType: "bar",
    shareTopN: 12,
  };
}

export function reduceAuditShareSnapshot(
  state: AuditShareSnapshot,
  action: AuditShareAction
): AuditShareSnapshot {
  switch (action.type) {
    case "changeMetric":
      return {
        ...state,
        shareMetric: action.metric,
        shareRankBy: deriveShareRankBy(action.metric, state.shareRankBy),
      };
    case "changeRankBy":
      return { ...state, shareRankBy: action.rankBy };
    case "changeChartType":
      return { ...state, shareChartType: action.chartType };
    case "changeTopN":
      return { ...state, shareTopN: action.topN };
    default:
      return state;
  }
}

export function useAuditShareState(): AuditShareState {
  const [state, dispatch] = useReducer(
    reduceAuditShareSnapshot,
    undefined,
    createInitialAuditShareSnapshot
  );

  const onChangeShareMetric = useCallback((metric: ShareMetric) => {
    dispatch({ type: "changeMetric", metric });
  }, []);

  return {
    shareMetric: state.shareMetric,
    shareRankBy: state.shareRankBy,
    shareChartType: state.shareChartType,
    shareTopN: state.shareTopN,
    onChangeShareMetric,
    onChangeShareRankBy: (rankBy) => dispatch({ type: "changeRankBy", rankBy }),
    onChangeShareChartType: (chartType) => dispatch({ type: "changeChartType", chartType }),
    onChangeShareTopN: (topN) => dispatch({ type: "changeTopN", topN }),
  };
}
