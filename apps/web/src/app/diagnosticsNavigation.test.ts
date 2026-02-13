import { describe, expect, it } from "vitest";
import { resolveNavigationFromSearch } from "./diagnosticsNavigation";

describe("resolveNavigationFromSearch", () => {
  it("parses explicit module and audit tab", () => {
    expect(resolveNavigationFromSearch("?module=audit&auditTab=share")).toEqual({
      module: "audit",
      auditTab: "share",
    });
  });

  it("keeps explain module and falls back to default audit tab", () => {
    expect(resolveNavigationFromSearch("?module=explain")).toEqual({
      module: "explain",
      auditTab: "overview",
    });
  });

  it("supports legacy overview/topSql/share tab URLs", () => {
    expect(resolveNavigationFromSearch("?tab=topSql")).toEqual({
      module: "audit",
      auditTab: "topSql",
    });
  });

  it("supports legacy explain tab URLs", () => {
    expect(resolveNavigationFromSearch("?tab=explain")).toEqual({
      module: "explain",
      auditTab: "overview",
    });
  });

  it("falls back to defaults for invalid values", () => {
    expect(resolveNavigationFromSearch("?module=invalid&auditTab=unknown")).toEqual({
      module: "audit",
      auditTab: "overview",
    });
  });
});
