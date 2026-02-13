import { useCallback, useEffect, useState } from "react";

export type DiagnosticsModule = "audit" | "explain";
export type AuditTabKey = "overview" | "topSql" | "share";

export interface DiagnosticsNavigationState {
  module: DiagnosticsModule;
  auditTab: AuditTabKey;
}

const MODULE_PARAM_KEY = "module";
const AUDIT_TAB_PARAM_KEY = "auditTab";

export function parseModuleFromSearch(search: string): DiagnosticsModule | null {
  const params = new URLSearchParams(search);
  const module = params.get(MODULE_PARAM_KEY);
  if (module === "audit" || module === "explain") return module;

  const legacyTab = params.get("tab");
  if (legacyTab === "explain") return "explain";
  if (legacyTab === "overview" || legacyTab === "topSql" || legacyTab === "share") return "audit";
  return null;
}

export function parseAuditTabFromSearch(search: string): AuditTabKey | null {
  const params = new URLSearchParams(search);
  const tab = params.get(AUDIT_TAB_PARAM_KEY);
  if (tab === "overview" || tab === "topSql" || tab === "share") return tab;

  const legacyTab = params.get("tab");
  if (legacyTab === "overview" || legacyTab === "topSql" || legacyTab === "share") {
    return legacyTab;
  }
  return null;
}

export function resolveNavigationFromSearch(search: string): DiagnosticsNavigationState {
  return {
    module: parseModuleFromSearch(search) ?? "audit",
    auditTab: parseAuditTabFromSearch(search) ?? "overview",
  };
}

export function syncModuleStateToUrl(module: DiagnosticsModule, auditTab: AuditTabKey): void {
  if (typeof window === "undefined") return;
  const currentHref = window.location.href;
  const url = new URL(currentHref);
  url.searchParams.set(MODULE_PARAM_KEY, module);
  if (module === "audit") url.searchParams.set(AUDIT_TAB_PARAM_KEY, auditTab);
  else url.searchParams.delete(AUDIT_TAB_PARAM_KEY);
  url.searchParams.delete("tab");
  const nextHref = url.toString();
  if (nextHref === currentHref) return;
  window.history.pushState({}, "", nextHref);
}

export function useDiagnosticsNavigation(): {
  activeModule: DiagnosticsModule;
  activeAuditTab: AuditTabKey;
  switchModule: (module: DiagnosticsModule) => void;
  switchAuditTab: (tab: AuditTabKey) => void;
} {
  const [state, setState] = useState<DiagnosticsNavigationState>(() =>
    typeof window === "undefined"
      ? { module: "audit", auditTab: "overview" }
      : resolveNavigationFromSearch(window.location.search)
  );

  const { module: activeModule, auditTab: activeAuditTab } = state;

  const switchModule = useCallback(
    (module: DiagnosticsModule) => {
      setState((prev) => ({ ...prev, module }));
      syncModuleStateToUrl(module, activeAuditTab);
    },
    [activeAuditTab]
  );

  const switchAuditTab = useCallback((tab: AuditTabKey) => {
    setState({ module: "audit", auditTab: tab });
    syncModuleStateToUrl("audit", tab);
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const syncFromUrl = () => {
      setState(resolveNavigationFromSearch(window.location.search));
    };
    window.addEventListener("popstate", syncFromUrl);
    return () => window.removeEventListener("popstate", syncFromUrl);
  }, []);

  return { activeModule, activeAuditTab, switchModule, switchAuditTab };
}
