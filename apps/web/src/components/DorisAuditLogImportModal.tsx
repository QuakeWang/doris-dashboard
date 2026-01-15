import { Alert, Button, InputNumber, Modal, Select, Space, Typography } from "antd";
import { useEffect, useRef, useState } from "react";
import {
  type AgentClient,
  type DorisConnectionInput,
  isDorisConnectionConfigured,
} from "../agent/agentClient";

const { Text } = Typography;

const MAX_LIMIT = 200_000;
const DEFAULT_LIMIT = 50_000;
const DEFAULT_LOOKBACK_SECONDS = 24 * 60 * 60;
const LOOKBACK_OPTIONS = [
  { value: 60 * 60, label: "Last 1h" },
  { value: 6 * 60 * 60, label: "Last 6h" },
  { value: 24 * 60 * 60, label: "Last 24h" },
  { value: 3 * 24 * 60 * 60, label: "Last 3d" },
];
export interface DorisAuditLogImportModalProps {
  open: boolean;
  onClose: () => void;
  agent: AgentClient;
  connection: DorisConnectionInput | null;
  onOpenDoris: () => void;
  importing: boolean;
  onImport: (file: File) => void;
}

export default function DorisAuditLogImportModal(
  props: DorisAuditLogImportModalProps
): JSX.Element {
  const { open, onClose, agent, connection, onOpenDoris, importing, onImport } = props;

  const [lookbackSeconds, setLookbackSeconds] = useState(DEFAULT_LOOKBACK_SECONDS);
  const [limit, setLimit] = useState(DEFAULT_LIMIT);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    if (!open) return;
    setLookbackSeconds(DEFAULT_LOOKBACK_SECONDS);
    setLimit(DEFAULT_LIMIT);
    setError(null);
    setLoading(false);
    abortRef.current = null;
  }, [open]);

  const canSubmit = !loading && !importing;
  const dorisConfigured = isDorisConnectionConfigured(connection);

  const cancelExport = () => {
    abortRef.current?.abort();
    abortRef.current = null;
  };

  const doImport = async () => {
    if (!isDorisConnectionConfigured(connection)) {
      onOpenDoris();
      return;
    }
    const lb = Number(lookbackSeconds);
    if (!Number.isFinite(lb) || lb <= 0) {
      setError("Lookback must be a positive number");
      return;
    }
    const n = Number(limit);
    if (!Number.isFinite(n) || n <= 0) {
      setError("Limit must be a positive number");
      return;
    }
    if (n > MAX_LIMIT) {
      setError(`Limit too large (max=${MAX_LIMIT})`);
      return;
    }

    setError(null);
    setLoading(true);
    abortRef.current?.abort();
    abortRef.current = new AbortController();
    try {
      const blob = await agent.exportAuditLogOutfileTsv(
        {
          connection,
          lookbackSeconds: lb,
          limit: n,
        },
        abortRef.current.signal
      );
      const file = new File([blob], "audit_log.tsv", {
        type: blob.type || "text/tab-separated-values",
      });
      onImport(file);
      onClose();
    } catch (e) {
      if (e instanceof DOMException && e.name === "AbortError") return;
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      abortRef.current = null;
      setLoading(false);
    }
  };

  return (
    <Modal
      title="Import Audit Log from Doris"
      open={open}
      onCancel={onClose}
      closable={!loading}
      maskClosable={!loading}
      keyboard={!loading}
      footer={
        <Space>
          <Button onClick={onClose} disabled={loading}>
            Close
          </Button>
          {loading && <Button onClick={cancelExport}>Cancel</Button>}
          <Button type="primary" loading={loading} disabled={!canSubmit} onClick={doImport}>
            Import
          </Button>
        </Space>
      }
    >
      <Space direction="vertical" style={{ width: "100%" }}>
        <Text type="secondary">
          Source: <Text code>__internal_schema.audit_log</Text>
        </Text>
        <Space direction="vertical" size={8} style={{ width: "100%" }}>
          <Text type="secondary">Lookback (based on Doris server time):</Text>
          <Select
            value={lookbackSeconds}
            onChange={(v) => setLookbackSeconds(Number(v))}
            options={LOOKBACK_OPTIONS}
            disabled={loading || importing}
            style={{ width: "100%" }}
          />
          <Text type="secondary">
            If you see "no rows", try a larger lookback window or run a few queries to generate
            audit logs.
          </Text>
        </Space>
        <Space>
          <Text type="secondary">Limit:</Text>
          <InputNumber
            min={1}
            max={MAX_LIMIT}
            value={limit}
            onChange={(v) => setLimit(Number(v ?? 0))}
            disabled={loading || importing}
          />
          <Text type="secondary">(max {MAX_LIMIT})</Text>
        </Space>
        {!dorisConfigured && (
          <Alert
            type="info"
            showIcon
            message="Doris connection is not configured"
            action={
              <Button size="small" onClick={onOpenDoris}>
                Configure
              </Button>
            }
          />
        )}
        {importing && <Alert type="warning" showIcon message="Import is running. Please wait." />}
        {error && (
          <Alert
            type="error"
            showIcon
            message="Export error"
            description={<Text style={{ whiteSpace: "pre-wrap" }}>{error}</Text>}
          />
        )}
      </Space>
    </Modal>
  );
}
