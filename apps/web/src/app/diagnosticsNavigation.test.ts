import { afterEach, describe, expect, it, vi } from "vitest";
import {
  resolveNavigationFromSearch,
  resolveSwitchModuleState,
  syncModuleStateToUrl,
} from "./diagnosticsNavigation";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("resolveNavigationFromSearch", () => {
  it("supports audit tab deep link without explicit module", () => {
    expect(resolveNavigationFromSearch("?auditTab=share")).toEqual({
      module: "audit",
      moduleView: "share",
    });
  });

  it("parses explicit module and audit tab", () => {
    expect(resolveNavigationFromSearch("?module=audit&auditTab=share")).toEqual({
      module: "audit",
      moduleView: "share",
    });
  });

  it("keeps explain module without module-specific view", () => {
    expect(resolveNavigationFromSearch("?module=explain")).toEqual({
      module: "explain",
      moduleView: null,
    });
  });

  it("keeps schemaAudit module without module-specific view", () => {
    expect(resolveNavigationFromSearch("?module=schemaAudit")).toEqual({
      module: "schemaAudit",
      moduleView: null,
    });
  });

  it("supports legacy overview/topSql/share tab URLs", () => {
    expect(resolveNavigationFromSearch("?tab=topSql")).toEqual({
      module: "audit",
      moduleView: "topSql",
    });
  });

  it("prefers explicit auditTab over legacy tab when both exist", () => {
    expect(resolveNavigationFromSearch("?tab=topSql&auditTab=share")).toEqual({
      module: "audit",
      moduleView: "share",
    });
  });

  it("supports legacy explain tab URLs", () => {
    expect(resolveNavigationFromSearch("?tab=explain")).toEqual({
      module: "explain",
      moduleView: null,
    });
  });

  it("supports legacy schemaAudit tab URLs", () => {
    expect(resolveNavigationFromSearch("?tab=schemaAudit")).toEqual({
      module: "schemaAudit",
      moduleView: null,
    });
  });

  it("falls back to defaults for invalid values", () => {
    expect(resolveNavigationFromSearch("?module=invalid&auditTab=unknown")).toEqual({
      module: "audit",
      moduleView: "overview",
    });
  });
});

describe("resolveSwitchModuleState", () => {
  it("remembers previous module view when switching back", () => {
    const memory = {};
    const toExplain = resolveSwitchModuleState(
      { module: "audit", moduleView: "topSql" },
      "explain",
      memory
    );
    expect(toExplain).toEqual({ module: "explain", moduleView: null });

    const backToAudit = resolveSwitchModuleState(toExplain, "audit", memory);
    expect(backToAudit).toEqual({ module: "audit", moduleView: "topSql" });
  });
});

describe("navigation side effects", () => {
  it("syncModuleStateToUrl writes module view and clears legacy params", () => {
    const pushState = vi.fn();
    vi.stubGlobal("window", {
      location: { href: "http://localhost:12305/?module=audit&auditTab=overview&tab=topSql" },
      history: { pushState },
    });

    syncModuleStateToUrl("audit", "share");
    expect(pushState).toHaveBeenCalledTimes(1);
    const nextHref = String(pushState.mock.calls[0]?.[2] ?? "");
    const nextUrl = new URL(nextHref);
    expect(nextUrl.searchParams.get("module")).toBe("audit");
    expect(nextUrl.searchParams.get("auditTab")).toBe("share");
    expect(nextUrl.searchParams.get("tab")).toBeNull();
  });

  it("syncModuleStateToUrl writes schemaAudit module and clears legacy params", () => {
    const pushState = vi.fn();
    vi.stubGlobal("window", {
      location: { href: "http://localhost:12305/?tab=schemaAudit&auditTab=share" },
      history: { pushState },
    });

    syncModuleStateToUrl("schemaAudit", null);
    expect(pushState).toHaveBeenCalledTimes(1);
    const nextHref = String(pushState.mock.calls[0]?.[2] ?? "");
    const nextUrl = new URL(nextHref);
    expect(nextUrl.searchParams.get("module")).toBe("schemaAudit");
    expect(nextUrl.searchParams.get("tab")).toBeNull();
    expect(nextUrl.searchParams.get("auditTab")).toBeNull();
  });
});
