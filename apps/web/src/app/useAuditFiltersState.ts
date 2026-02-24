import { useCallback, useReducer } from "react";
import type { QueryFilters } from "../db/client/protocol";

const DEFAULT_FILTERS: QueryFilters = { excludeInternal: true };

export interface AuditFiltersSnapshot {
  filters: QueryFilters;
  filtersDraft: QueryFilters;
}

type AuditFiltersAction =
  | { type: "setBoth"; next: QueryFilters }
  | { type: "patchDraft"; patch: Partial<QueryFilters> }
  | { type: "patchBoth"; patch: Partial<QueryFilters> }
  | { type: "applyFilters" };

export function createInitialAuditFiltersSnapshot(): AuditFiltersSnapshot {
  return {
    filters: { ...DEFAULT_FILTERS },
    filtersDraft: { ...DEFAULT_FILTERS },
  };
}

export function reduceAuditFiltersSnapshot(
  state: AuditFiltersSnapshot,
  action: AuditFiltersAction
): AuditFiltersSnapshot {
  switch (action.type) {
    case "setBoth":
      return { filters: action.next, filtersDraft: action.next };
    case "patchDraft":
      return { ...state, filtersDraft: { ...state.filtersDraft, ...action.patch } };
    case "patchBoth": {
      const next = { ...state.filtersDraft, ...action.patch };
      return { filters: next, filtersDraft: next };
    }
    case "applyFilters":
      return { ...state, filters: state.filtersDraft };
    default:
      return state;
  }
}

export interface AuditFiltersState {
  filters: QueryFilters;
  filtersDraft: QueryFilters;
  setFiltersBoth: (next: QueryFilters) => void;
  patchFiltersDraft: (patch: Partial<QueryFilters>) => void;
  patchFiltersBoth: (patch: Partial<QueryFilters>) => void;
  applyFilters: () => void;
}

export function useAuditFiltersState(): AuditFiltersState {
  const [state, dispatch] = useReducer(
    reduceAuditFiltersSnapshot,
    undefined,
    createInitialAuditFiltersSnapshot
  );

  const setFiltersBoth = useCallback((next: QueryFilters) => {
    dispatch({ type: "setBoth", next });
  }, []);

  const patchFiltersDraft = useCallback((patch: Partial<QueryFilters>) => {
    dispatch({ type: "patchDraft", patch });
  }, []);

  const patchFiltersBoth = useCallback((patch: Partial<QueryFilters>) => {
    dispatch({ type: "patchBoth", patch });
  }, []);

  const applyFilters = useCallback(() => {
    dispatch({ type: "applyFilters" });
  }, []);

  return {
    filters: state.filters,
    filtersDraft: state.filtersDraft,
    setFiltersBoth,
    patchFiltersDraft,
    patchFiltersBoth,
    applyFilters,
  };
}
