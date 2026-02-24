import { LinkOutlined } from "@ant-design/icons";
import { Button, Layout, Space, Typography } from "antd";
import { DIAGNOSTICS_MODULES } from "../app/diagnosticsModules";
import type { DiagnosticsModule } from "../app/diagnosticsNavigation";

const { Header } = Layout;
const { Title, Text } = Typography;

export interface AppHeaderProps {
  dorisLabel: string;
  activeModule: DiagnosticsModule;
  onSwitchModule: (module: DiagnosticsModule) => void;
  onOpenDoris: () => void;
}

export default function AppHeader(props: AppHeaderProps): JSX.Element {
  const { dorisLabel, activeModule, onSwitchModule, onOpenDoris } = props;
  return (
    <Header
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        position: "sticky",
        top: 0,
        zIndex: 10,
        background: "rgba(30, 30, 46, 0.86)",
        backdropFilter: "blur(10px)",
        borderBottom: "1px solid rgba(88, 91, 112, 0.65)",
      }}
    >
      <Title level={3} style={{ margin: 0, color: "white" }}>
        Doris Dashboard
      </Title>
      <Space>
        {DIAGNOSTICS_MODULES.map((module) => (
          <Button
            key={module.id}
            type={activeModule === module.id ? "primary" : "default"}
            onClick={() => onSwitchModule(module.id)}
          >
            {module.label}
          </Button>
        ))}
        <Button icon={<LinkOutlined />} onClick={onOpenDoris}>
          Doris
        </Button>
        <Text style={{ color: "rgba(255,255,255,0.65)" }}>Doris: {dorisLabel}</Text>
      </Space>
    </Header>
  );
}
