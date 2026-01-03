import { PlusOutlined } from "@ant-design/icons";
import { Button, Layout, Space, Typography } from "antd";

const { Header } = Layout;
const { Title, Text } = Typography;

export interface AppHeaderProps {
  ready: boolean;
  importing: boolean;
  datasetId: string | null;
  isCoi: boolean;
  onNewDataset: () => void;
}

export default function AppHeader(props: AppHeaderProps): JSX.Element {
  const { ready, importing, datasetId, isCoi, onNewDataset } = props;
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
      <Space>
        <Title level={3} style={{ margin: 0, color: "white" }}>
          Doris Dashboard
        </Title>
      </Space>
      <Space>
        <Button icon={<PlusOutlined />} onClick={onNewDataset} disabled={!ready || importing}>
          New Dataset
        </Button>
        <Text style={{ color: isCoi ? "rgba(166, 227, 161, 0.9)" : "rgba(250, 179, 135, 0.9)" }}>
          COI: {isCoi ? "on" : "off"}
        </Text>
        <Text style={{ color: "rgba(255,255,255,0.65)" }}>Dataset: {datasetId ?? "-"}</Text>
      </Space>
    </Header>
  );
}
