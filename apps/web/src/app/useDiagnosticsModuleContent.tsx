import { useCallback, useState } from "react";
import type { AgentClient, DorisConnectionInput } from "../agent/agentClient";
import AuditWorkspace from "../components/AuditWorkspace";
import ExplainWorkspace from "../components/ExplainWorkspace";
import SchemaAuditWorkspace from "../components/SchemaAuditWorkspace";
import type { DbClient } from "../db/client/dbClient";
import {
  type AuditTabKey,
  getDefaultAuditTab,
  isAuditTab,
  renderDiagnosticsModule,
} from "./diagnosticsModules";
import type { DiagnosticsModule } from "./diagnosticsNavigation";
import { useAuditWorkspaceController } from "./useAuditWorkspaceController";

interface UseDiagnosticsModuleContentParams {
  client: DbClient;
  agent: AgentClient;
  ready: boolean;
  datasetId: string | null;
  activeModule: DiagnosticsModule;
  activeModuleView: string | null;
  switchModuleView: (module: DiagnosticsModule, moduleView: string | null) => void;
  dorisConn: DorisConnectionInput | null;
  dorisConfigured: boolean;
  onOpenDoris: () => void;
  onOpenDorisImport: () => void;
}

export function useDiagnosticsModuleContent(params: UseDiagnosticsModuleContentParams): {
  moduleContent: JSX.Element;
  audit: ReturnType<typeof useAuditWorkspaceController>;
} {
  const {
    client,
    agent,
    ready,
    datasetId,
    activeModule,
    activeModuleView,
    switchModuleView,
    dorisConn,
    dorisConfigured,
    onOpenDoris,
    onOpenDorisImport,
  } = params;

  const activeAuditTab: AuditTabKey =
    activeModule === "audit" && isAuditTab(activeModuleView)
      ? activeModuleView
      : getDefaultAuditTab();
  const switchAuditTab = useCallback(
    (tab: AuditTabKey) => switchModuleView("audit", tab),
    [switchModuleView]
  );

  const audit = useAuditWorkspaceController({
    client,
    ready,
    datasetId,
    activeModule,
    activeAuditTab,
    switchAuditTab,
    dorisConfigured,
    onOpenDoris,
    onOpenDorisImport,
  });
  const [explainSql, setExplainSql] = useState("");

  const moduleContent = renderDiagnosticsModule(activeModule, {
    audit: () => <AuditWorkspace {...audit.workspaceProps} />,
    explain: () => (
      <ExplainWorkspace
        agent={agent}
        dorisConn={dorisConn}
        dorisConfigured={dorisConfigured}
        onOpenDoris={onOpenDoris}
        sql={explainSql}
        onChangeSql={setExplainSql}
      />
    ),
    schemaAudit: () => (
      <SchemaAuditWorkspace
        agent={agent}
        dorisConn={dorisConn}
        dorisConfigured={dorisConfigured}
        onOpenDoris={onOpenDoris}
      />
    ),
  });

  return { moduleContent, audit };
}
