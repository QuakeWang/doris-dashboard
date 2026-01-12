import { Alert, Button, Checkbox, Input, InputNumber, Modal, Space, Typography } from "antd";
import { useEffect, useState } from "react";
import type { AgentClient, DorisConnectionInput } from "../agent/agentClient";

const { Text } = Typography;

export interface DorisConnectionModalProps {
  open: boolean;
  onClose: () => void;
  agent: AgentClient;
  current: DorisConnectionInput | null;
  onSave: (conn: DorisConnectionInput, rememberInfo: boolean) => void;
}

const DEFAULT_PORT = 9030;
const DEFAULT_USER = "root";

export default function DorisConnectionModal(props: DorisConnectionModalProps): JSX.Element {
  const { open, onClose, agent, current, onSave } = props;
  const [host, setHost] = useState("");
  const [port, setPort] = useState<number>(DEFAULT_PORT);
  const [user, setUser] = useState(DEFAULT_USER);
  const [password, setPassword] = useState("");
  const [rememberInfo, setRememberInfo] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [testing, setTesting] = useState(false);

  const currentLabel = current ? `${current.user}@${current.host}:${current.port}` : "-";

  useEffect(() => {
    if (!open) return;
    setError(null);
    setTesting(false);
    setHost(current?.host ?? "");
    setPort(current?.port ?? DEFAULT_PORT);
    setUser(current?.user ?? DEFAULT_USER);
    setPassword(current?.password ?? "");
    setRememberInfo(true);
  }, [open, current]);

  const buildConn = (): DorisConnectionInput | null => {
    setError(null);
    const conn: DorisConnectionInput = {
      host: host.trim(),
      port: Number(port),
      user: user.trim(),
      password,
    };
    if (!conn.host) {
      setError("Host is required");
      return null;
    }
    if (!Number.isFinite(conn.port) || conn.port <= 0 || conn.port > 65535) {
      setError("Port must be in 1..65535");
      return null;
    }
    if (!conn.user) {
      setError("User is required");
      return null;
    }
    if (!conn.password) {
      setError("Password is required");
      return null;
    }
    return conn;
  };

  const saveAndTest = async () => {
    const conn = buildConn();
    if (!conn) return;
    setTesting(true);
    try {
      await agent.testDorisConnection({ connection: conn });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      return;
    } finally {
      setTesting(false);
    }
    onSave(conn, rememberInfo);
    onClose();
  };

  return (
    <Modal
      title="Doris Connection"
      open={open}
      onCancel={onClose}
      closable={!testing}
      maskClosable={!testing}
      keyboard={!testing}
      footer={
        <Space>
          <Button onClick={onClose} disabled={testing}>
            Close
          </Button>
          <Button type="primary" loading={testing} onClick={() => void saveAndTest()}>
            Save & Test
          </Button>
        </Space>
      }
    >
      <Space direction="vertical" style={{ width: "100%" }}>
        <Text type="secondary">
          Current: <Text code>{currentLabel}</Text>
        </Text>
        <Space direction="vertical" size={8} style={{ width: "100%" }}>
          <Input
            value={host}
            onChange={(e) => setHost(e.target.value)}
            placeholder="Host (e.g. 127.0.0.1)"
            disabled={testing}
          />
          <InputNumber
            value={port}
            onChange={(v) => setPort(Number(v ?? 0))}
            placeholder="Port"
            min={1}
            max={65535}
            controls={false}
            style={{ width: "100%" }}
            disabled={testing}
          />
          <Input
            value={user}
            onChange={(e) => setUser(e.target.value)}
            placeholder="User (e.g. root)"
            disabled={testing}
          />
          <Input.Password
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="Password"
            disabled={testing}
          />
        </Space>
        <Checkbox
          checked={rememberInfo}
          onChange={(e) => setRememberInfo(e.target.checked)}
          disabled={testing}
        >
          Remember host/port/user in this tab
        </Checkbox>
        {error && (
          <Alert
            type="error"
            message="Connection error"
            description={<Text style={{ whiteSpace: "pre-wrap" }}>{error}</Text>}
            showIcon
          />
        )}
      </Space>
    </Modal>
  );
}
