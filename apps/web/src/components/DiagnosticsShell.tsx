import { Alert, Button, Card, Layout, Space, Spin, Typography } from "antd";
import type { ReactNode } from "react";
import type { DiagnosticsModule } from "../app/diagnosticsNavigation";
import AppHeader from "./AppHeader";

const { Content } = Layout;
const { Text } = Typography;

export interface DiagnosticsShellProps {
  ready: boolean;
  isCoi: boolean;
  dorisLabel: string;
  activeModule: DiagnosticsModule;
  onSwitchModule: (module: DiagnosticsModule) => void;
  onOpenDoris: () => void;
  error: string | null;
  onRetryInit: () => void;
  canRetrySession?: boolean;
  onRetrySession?: () => void;
  onDismissError: () => void;
  children: ReactNode;
}

export default function DiagnosticsShell(props: DiagnosticsShellProps): JSX.Element {
  const {
    ready,
    isCoi,
    dorisLabel,
    activeModule,
    onSwitchModule,
    onOpenDoris,
    error,
    onRetryInit,
    canRetrySession = false,
    onRetrySession,
    onDismissError,
    children,
  } = props;

  return (
    <Layout style={{ minHeight: "100vh" }}>
      <AppHeader
        dorisLabel={dorisLabel}
        activeModule={activeModule}
        onSwitchModule={onSwitchModule}
        onOpenDoris={onOpenDoris}
      />

      <Content style={{ padding: 24, maxWidth: 1360, width: "100%", margin: "0 auto" }}>
        {!ready && !error ? (
          <Card>
            <Spin /> <Text>Initializing local analysis engine...</Text>
          </Card>
        ) : null}

        {error ? (
          <Alert
            type="error"
            message="Error"
            description={<Text style={{ whiteSpace: "pre-wrap" }}>{error}</Text>}
            showIcon
            closable={ready}
            action={
              !ready ? (
                <Space>
                  <Button size="small" onClick={onRetryInit}>
                    Retry
                  </Button>
                  <Button
                    size="small"
                    onClick={() => {
                      if (typeof window !== "undefined") window.location.reload();
                    }}
                  >
                    Reload
                  </Button>
                </Space>
              ) : canRetrySession && onRetrySession ? (
                <Space>
                  <Button size="small" onClick={onRetrySession}>
                    Retry Session
                  </Button>
                </Space>
              ) : null
            }
            style={{ marginBottom: 12 }}
            onClose={onDismissError}
          />
        ) : null}

        {ready && !isCoi ? (
          <Alert
            type="warning"
            message="Performance warning"
            description={
              <Text style={{ whiteSpace: "pre-wrap" }}>
                Browser isolation is disabled. Local analysis may run with reduced performance.
                {"\n"}For local dev, restart `npm run dev`.
                {"\n"}For production, enable required isolation headers.
              </Text>
            }
            showIcon
            style={{ marginBottom: 12 }}
          />
        ) : null}

        {children}
      </Content>
    </Layout>
  );
}
