import { Divider, Drawer, Space, Tag, Typography } from "antd";
import type { ExplainNode } from "../explain/types";
import CopyIconButton from "./CopyIconButton";

const { Text, Title } = Typography;

export interface ExplainNodeDrawerProps {
  open: boolean;
  node: ExplainNode | null;
  onClose: () => void;
}

function MetaRow(props: { label: string; value: string }): JSX.Element {
  const { label, value } = props;
  return (
    <span className="dd-code-ellipsis">
      <Text type="secondary">{label}</Text> <Text code>{value}</Text>
    </span>
  );
}

export default function ExplainNodeDrawer(props: ExplainNodeDrawerProps): JSX.Element {
  const { open, node, onClose } = props;

  return (
    <Drawer
      title="Plan Node"
      placement="right"
      width={560}
      open={open}
      onClose={onClose}
      destroyOnClose
    >
      {!node ? <Text type="secondary">No node selected.</Text> : null}

      {node ? (
        <Space direction="vertical" size={14} style={{ width: "100%" }}>
          <Space direction="vertical" size={6} style={{ width: "100%" }}>
            <Title level={5} style={{ margin: 0 }}>
              <Text code>{node.operator}</Text>
            </Title>
            <Space wrap size={6}>
              {node.fragmentId != null ? (
                <Tag color="geekblue">Fragment {node.fragmentId}</Tag>
              ) : null}
              {node.nodeId != null ? <Tag color="default">Node {node.nodeId}</Tag> : null}
              {node.idsRaw ? <Tag color="default">ID {node.idsRaw}</Tag> : null}
            </Space>
          </Space>

          <Space direction="vertical" size={6} style={{ width: "100%" }}>
            {node.table ? <MetaRow label="TABLE:" value={node.table} /> : null}
            {node.cardinality ? <MetaRow label="cardinality:" value={node.cardinality} /> : null}
            {node.predicates ? <MetaRow label="predicates:" value={node.predicates} /> : null}
          </Space>

          <Divider style={{ margin: "10px 0" }} />

          <Space direction="vertical" size={8} style={{ width: "100%" }}>
            <Space style={{ justifyContent: "space-between", display: "flex" }}>
              <Text type="secondary">Segments</Text>
              <CopyIconButton text={node.segments.join("\n")} tooltip="Copy segments" />
            </Space>
            <pre className="dd-sql-block" style={{ maxHeight: 360, overflow: "auto" }}>
              <code>{node.segments.join("\n")}</code>
            </pre>
          </Space>

          <Space direction="vertical" size={8} style={{ width: "100%" }}>
            <Space style={{ justifyContent: "space-between", display: "flex" }}>
              <Text type="secondary">Raw</Text>
            </Space>
            <pre className="dd-sql-block" style={{ margin: 0 }}>
              <code>{node.rawLine}</code>
            </pre>
          </Space>
        </Space>
      ) : null}
    </Drawer>
  );
}
