import type { AgentClient, DorisConnectionInput } from "../agent/agentClient";
import ExplainTab from "./ExplainTab";

export interface ExplainWorkspaceProps {
  agent: AgentClient;
  dorisConn: DorisConnectionInput | null;
  dorisConfigured: boolean;
  onOpenDoris: () => void;
  sql: string;
  onChangeSql: (sql: string) => void;
}

export default function ExplainWorkspace(props: ExplainWorkspaceProps): JSX.Element {
  const { agent, dorisConn, dorisConfigured, onOpenDoris, sql, onChangeSql } = props;
  return (
    <ExplainTab
      agent={agent}
      dorisConn={dorisConn}
      dorisConfigured={dorisConfigured}
      onOpenDoris={onOpenDoris}
      sql={sql}
      onChangeSql={onChangeSql}
    />
  );
}
