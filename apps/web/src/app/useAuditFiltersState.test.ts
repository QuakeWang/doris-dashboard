import { describe, expect, it } from "vitest";
import {
  createInitialAuditFiltersSnapshot,
  reduceAuditFiltersSnapshot,
} from "./useAuditFiltersState";

describe("useAuditFiltersState reducer", () => {
  it("initializes filters and draft with default excludeInternal flag", () => {
    expect(createInitialAuditFiltersSnapshot()).toEqual({
      filters: { excludeInternal: true },
      filtersDraft: { excludeInternal: true },
    });
  });

  it("patchDraft only updates draft", () => {
    const state = reduceAuditFiltersSnapshot(createInitialAuditFiltersSnapshot(), {
      type: "patchDraft",
      patch: { dbName: "db_a" },
    });
    expect(state.filters).toEqual({ excludeInternal: true });
    expect(state.filtersDraft).toEqual({ excludeInternal: true, dbName: "db_a" });
  });

  it("patchBoth updates filters and draft from current draft", () => {
    const draftPatched = reduceAuditFiltersSnapshot(createInitialAuditFiltersSnapshot(), {
      type: "patchDraft",
      patch: { userName: "alice" },
    });
    const state = reduceAuditFiltersSnapshot(draftPatched, {
      type: "patchBoth",
      patch: { dbName: "db_a" },
    });
    expect(state.filters).toEqual({ excludeInternal: true, userName: "alice", dbName: "db_a" });
    expect(state.filtersDraft).toEqual({
      excludeInternal: true,
      userName: "alice",
      dbName: "db_a",
    });
  });

  it("applyFilters synchronizes committed filters from draft", () => {
    const draftOnly = reduceAuditFiltersSnapshot(createInitialAuditFiltersSnapshot(), {
      type: "patchDraft",
      patch: { clientIp: "10.1.1.7" },
    });
    const state = reduceAuditFiltersSnapshot(draftOnly, { type: "applyFilters" });
    expect(state.filters).toEqual({ excludeInternal: true, clientIp: "10.1.1.7" });
    expect(state.filtersDraft).toEqual({ excludeInternal: true, clientIp: "10.1.1.7" });
  });
});
