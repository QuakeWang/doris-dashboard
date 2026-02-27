export const AUDIT_TABS = ["overview", "topSql", "share"] as const;
export type AuditTabKey = (typeof AUDIT_TABS)[number];

interface ModuleViewConfig {
  paramKey: string;
  defaultView: string;
  isValidView: (value: string) => boolean;
}

interface DiagnosticsModuleRegistration {
  id: string;
  label: string;
  legacyTabs?: readonly string[];
  view?: ModuleViewConfig;
}

const AUDIT_TAB_SET = new Set<string>(AUDIT_TABS);

export function isAuditTab(value: string | null): value is AuditTabKey {
  return !!value && AUDIT_TAB_SET.has(value);
}

export const DIAGNOSTICS_MODULES = [
  {
    id: "audit",
    label: "Audit",
    legacyTabs: AUDIT_TABS,
    view: {
      paramKey: "auditTab",
      defaultView: "overview",
      isValidView: isAuditTab,
    },
  },
  {
    id: "explain",
    label: "Explain",
    legacyTabs: ["explain"],
  },
  {
    id: "schemaAudit",
    label: "Schema Audit",
    legacyTabs: ["schemaAudit"],
  },
] as const satisfies readonly DiagnosticsModuleRegistration[];

export type DiagnosticsModule = (typeof DIAGNOSTICS_MODULES)[number]["id"];
const MODULE_REGISTRATIONS: readonly DiagnosticsModuleRegistration[] = DIAGNOSTICS_MODULES;
const DEFAULT_DIAGNOSTICS_MODULE: DiagnosticsModule = DIAGNOSTICS_MODULES[0].id;
const DEFAULT_AUDIT_TAB: AuditTabKey = AUDIT_TABS[0];

const DIAGNOSTICS_MODULE_SET = new Set<string>(MODULE_REGISTRATIONS.map((module) => module.id));
const DIAGNOSTICS_MODULE_MAP = new Map<DiagnosticsModule, DiagnosticsModuleRegistration>(
  MODULE_REGISTRATIONS.map((module) => [module.id as DiagnosticsModule, module])
);
const MODULE_VIEW_PARAM_KEYS = Array.from(
  new Set(
    MODULE_REGISTRATIONS.map((module) => module.view?.paramKey).filter(
      (value): value is string => !!value
    )
  )
);

export function isDiagnosticsModule(value: string | null): value is DiagnosticsModule {
  return !!value && DIAGNOSTICS_MODULE_SET.has(value);
}

export function getModuleViewParamKey(module: DiagnosticsModule): string | null {
  return DIAGNOSTICS_MODULE_MAP.get(module)?.view?.paramKey ?? null;
}

export function getModuleViewParamKeys(): readonly string[] {
  return MODULE_VIEW_PARAM_KEYS;
}

export function getDefaultModuleView(module: DiagnosticsModule): string | null {
  return DIAGNOSTICS_MODULE_MAP.get(module)?.view?.defaultView ?? null;
}

export function getDefaultDiagnosticsModule(): DiagnosticsModule {
  return DEFAULT_DIAGNOSTICS_MODULE;
}

export function getDefaultAuditTab(): AuditTabKey {
  return DEFAULT_AUDIT_TAB;
}

export function normalizeModuleView(
  module: DiagnosticsModule,
  rawView: string | null
): string | null {
  const config = DIAGNOSTICS_MODULE_MAP.get(module)?.view;
  if (!config) return null;
  if (rawView && config.isValidView(rawView)) return rawView;
  return config.defaultView;
}

export function parseLegacyTabToModuleState(
  legacyTab: string | null
): { module: DiagnosticsModule; moduleView: string | null } | null {
  if (!legacyTab) return null;
  for (const module of MODULE_REGISTRATIONS) {
    if (!module.legacyTabs?.includes(legacyTab)) continue;
    const moduleId = module.id as DiagnosticsModule;
    return {
      module: moduleId,
      moduleView: normalizeModuleView(moduleId, legacyTab),
    };
  }
  return null;
}

export type DiagnosticsModuleRenderer = () => JSX.Element;

export type DiagnosticsModuleRendererMap = Record<DiagnosticsModule, DiagnosticsModuleRenderer>;

export function renderDiagnosticsModule(
  module: DiagnosticsModule,
  renderers: DiagnosticsModuleRendererMap
): JSX.Element {
  return renderers[module]();
}
