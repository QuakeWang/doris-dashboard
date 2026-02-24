import { useCallback, useEffect, useRef, useState } from "react";
import {
  type DiagnosticsModule,
  getDefaultDiagnosticsModule,
  getDefaultModuleView,
  getModuleViewParamKey,
  getModuleViewParamKeys,
  isDiagnosticsModule,
  normalizeModuleView,
  parseLegacyTabToModuleState,
} from "./diagnosticsModules";

export type { AuditTabKey, DiagnosticsModule } from "./diagnosticsModules";

export interface DiagnosticsNavigationState {
  module: DiagnosticsModule;
  moduleView: string | null;
}

type ModuleViewMemory = Partial<Record<DiagnosticsModule, string | null>>;

const MODULE_PARAM_KEY = "module";
const LEGACY_TAB_PARAM_KEY = "tab";

export function parseModuleFromSearch(search: string): DiagnosticsModule | null {
  const params = new URLSearchParams(search);
  const module = params.get(MODULE_PARAM_KEY);
  if (isDiagnosticsModule(module)) return module;
  return null;
}

export function parseModuleViewFromSearch(
  search: string,
  module: DiagnosticsModule
): string | null {
  const viewParamKey = getModuleViewParamKey(module);
  if (!viewParamKey) return null;
  const params = new URLSearchParams(search);
  const rawView = params.get(viewParamKey);
  if (rawView == null) return null;
  const normalized = normalizeModuleView(module, rawView);
  return normalized === rawView ? normalized : null;
}

function parseLegacyNavigationFromSearch(search: string): DiagnosticsNavigationState | null {
  const params = new URLSearchParams(search);
  return parseLegacyTabToModuleState(params.get(LEGACY_TAB_PARAM_KEY));
}

function getDefaultNavigationState(): DiagnosticsNavigationState {
  const module = getDefaultDiagnosticsModule();
  return { module, moduleView: getDefaultModuleView(module) };
}

function rememberModuleView(
  memory: ModuleViewMemory,
  module: DiagnosticsModule,
  moduleView: string | null
): void {
  memory[module] = normalizeModuleView(module, moduleView);
}

export function resolveSwitchModuleState(
  prev: DiagnosticsNavigationState,
  nextModule: DiagnosticsModule,
  memory: ModuleViewMemory
): DiagnosticsNavigationState {
  rememberModuleView(memory, prev.module, prev.moduleView);
  const rememberedView = memory[nextModule] ?? getDefaultModuleView(nextModule);
  const nextModuleView = normalizeModuleView(nextModule, rememberedView);
  rememberModuleView(memory, nextModule, nextModuleView);
  return { module: nextModule, moduleView: nextModuleView };
}

export function resolveNavigationFromSearch(search: string): DiagnosticsNavigationState {
  const module = parseModuleFromSearch(search);
  const legacy = parseLegacyNavigationFromSearch(search);
  if (module) {
    const parsedModuleView = parseModuleViewFromSearch(search, module);
    const legacyModuleView = legacy?.module === module ? legacy.moduleView : null;
    return {
      module,
      moduleView: parsedModuleView ?? legacyModuleView ?? getDefaultModuleView(module),
    };
  }

  if (legacy) {
    return {
      module: legacy.module,
      moduleView: parseModuleViewFromSearch(search, legacy.module) ?? legacy.moduleView,
    };
  }

  const defaults = getDefaultNavigationState();
  return {
    module: defaults.module,
    moduleView: parseModuleViewFromSearch(search, defaults.module) ?? defaults.moduleView,
  };
}

export function syncModuleStateToUrl(module: DiagnosticsModule, moduleView: string | null): void {
  if (typeof window === "undefined") return;
  const currentHref = window.location.href;
  const url = new URL(currentHref);
  url.searchParams.set(MODULE_PARAM_KEY, module);
  for (const key of getModuleViewParamKeys()) {
    url.searchParams.delete(key);
  }
  const viewParamKey = getModuleViewParamKey(module);
  const normalizedView = normalizeModuleView(module, moduleView);
  if (viewParamKey && normalizedView) {
    url.searchParams.set(viewParamKey, normalizedView);
  }
  url.searchParams.delete(LEGACY_TAB_PARAM_KEY);
  const nextHref = url.toString();
  if (nextHref === currentHref) return;
  window.history.pushState({}, "", nextHref);
}

export function useDiagnosticsNavigation(): {
  activeModule: DiagnosticsModule;
  activeModuleView: string | null;
  switchModule: (module: DiagnosticsModule) => void;
  switchModuleView: (module: DiagnosticsModule, moduleView: string | null) => void;
} {
  const [state, setState] = useState<DiagnosticsNavigationState>(() =>
    typeof window === "undefined"
      ? getDefaultNavigationState()
      : resolveNavigationFromSearch(window.location.search)
  );
  const moduleViewMemoryRef = useRef<ModuleViewMemory>({
    [state.module]: normalizeModuleView(state.module, state.moduleView),
  });
  const stateRef = useRef(state);

  const { module: activeModule, moduleView: activeModuleView } = state;

  const switchModule = useCallback((module: DiagnosticsModule) => {
    const nextState = resolveSwitchModuleState(
      stateRef.current,
      module,
      moduleViewMemoryRef.current
    );
    stateRef.current = nextState;
    setState(nextState);
    syncModuleStateToUrl(nextState.module, nextState.moduleView);
  }, []);

  const switchModuleView = useCallback((module: DiagnosticsModule, moduleView: string | null) => {
    const normalized = normalizeModuleView(module, moduleView);
    moduleViewMemoryRef.current[module] = normalized;
    const nextState = { module, moduleView: normalized };
    stateRef.current = nextState;
    setState(nextState);
    syncModuleStateToUrl(module, normalized);
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const onPopstate = () => {
      const nextState = resolveNavigationFromSearch(window.location.search);
      moduleViewMemoryRef.current[nextState.module] = normalizeModuleView(
        nextState.module,
        nextState.moduleView
      );
      stateRef.current = nextState;
      setState(nextState);
    };
    window.addEventListener("popstate", onPopstate);
    return () => window.removeEventListener("popstate", onPopstate);
  }, []);

  return { activeModule, activeModuleView, switchModule, switchModuleView };
}
