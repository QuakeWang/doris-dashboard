import { useEffect, useMemo, useRef, useState } from "react";
import { AgentClient } from "./agent/agentClient";
import { useDiagnosticsNavigation } from "./app/diagnosticsNavigation";
import { useDuckDbSession } from "./app/hooks";
import { useDiagnosticsModuleContent } from "./app/useDiagnosticsModuleContent";
import { useDorisConnectionState } from "./app/useDorisConnectionState";
import DiagnosticsShell from "./components/DiagnosticsShell";
import DorisAuditLogImportModal from "./components/DorisAuditLogImportModal";
import DorisConnectionModal from "./components/DorisConnectionModal";
import TemplateDetailDrawer from "./components/TemplateDetailDrawer";
import { DbClient } from "./db/client/dbClient";

export default function App(): JSX.Element {
  const client = useMemo(() => new DbClient(), []);
  const agent = useMemo(() => new AgentClient(), []);
  const disposeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => {
    if (disposeTimerRef.current != null) {
      clearTimeout(disposeTimerRef.current);
      disposeTimerRef.current = null;
    }
    return () => {
      disposeTimerRef.current = setTimeout(() => {
        client.dispose();
        disposeTimerRef.current = null;
      }, 0);
    };
  }, [client]);
  const isCoi = typeof window !== "undefined" && window.crossOriginIsolated === true;
  const doris = useDorisConnectionState();

  const [shellError, setShellError] = useState<string | null>(null);
  const { ready, datasetId, retryInit, retryCreateDataset } = useDuckDbSession(
    client,
    setShellError
  );
  const { activeModule, activeModuleView, switchModule, switchModuleView } =
    useDiagnosticsNavigation();
  const { moduleContent, audit } = useDiagnosticsModuleContent({
    client,
    agent,
    ready,
    datasetId,
    activeModule,
    activeModuleView,
    switchModuleView,
    dorisConn: doris.dorisConn,
    dorisConfigured: doris.dorisConfigured,
    onOpenDoris: doris.openDorisModal,
    onOpenDorisImport: doris.openDorisImport,
  });

  return (
    <>
      <DiagnosticsShell
        ready={ready}
        isCoi={isCoi}
        dorisLabel={doris.dorisLabel}
        activeModule={activeModule}
        onSwitchModule={switchModule}
        onOpenDoris={doris.openDorisModal}
        error={shellError}
        onRetryInit={retryInit}
        canRetrySession={ready && !datasetId}
        onRetrySession={retryCreateDataset}
        onDismissError={() => setShellError(null)}
      >
        {moduleContent}
      </DiagnosticsShell>

      <TemplateDetailDrawer
        open={audit.templateDrawer != null}
        onClose={audit.closeTemplateDrawer}
        client={client}
        datasetId={datasetId}
        template={audit.templateDrawer}
        filters={audit.filters}
      />

      <DorisConnectionModal
        open={doris.dorisModalOpen}
        onClose={doris.closeDorisModal}
        agent={agent}
        current={doris.dorisConn}
        onSave={doris.saveDorisConnection}
      />
      <DorisAuditLogImportModal
        open={doris.dorisImportOpen}
        onClose={doris.closeDorisImport}
        agent={agent}
        connection={doris.dorisConn}
        onOpenDoris={doris.openDorisModal}
        importing={audit.importing}
        onImport={audit.runImport}
      />
    </>
  );
}
