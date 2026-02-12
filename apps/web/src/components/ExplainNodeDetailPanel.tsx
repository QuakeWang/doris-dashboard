import { Button, Collapse, Typography } from "antd";
import type { ExplainNodeOptimizationSignals } from "../explain/optimizationSignals";
import type { ExplainNode } from "../explain/types";
import CopyIconButton from "./CopyIconButton";

const { Text } = Typography;

export interface ExplainNodeDetailPanelProps {
  node: ExplainNode | null;
  nodeSignalsByKey: Map<string, ExplainNodeOptimizationSignals>;
  onBack?: () => void;
}

function formatRatio(signal: {
  selected: number | null;
  total: number | null;
  ratio: number | null;
}): string | null {
  if (signal.selected == null || signal.total == null || signal.ratio == null) {
    return null;
  }
  const pct = (signal.ratio * 100).toFixed(1);
  return `${signal.selected}/${signal.total} (${pct}%)`;
}

function buildOptimizationSummaryRows(
  signals: ExplainNodeOptimizationSignals
): Array<{ label: string; value: string; active: boolean }> {
  const rows: Array<{ label: string; value: string; active: boolean }> = [];

  if (signals.predicatePushdown.active) {
    rows.push({
      label: "Predicate Pushdown",
      value: signals.predicatePushdown.evidence ?? "detected",
      active: true,
    });
  }

  const partitionRatio = formatRatio(signals.partitionPruning);
  if (signals.partitionPruning.active) {
    rows.push({
      label: "Partition Pruning",
      value: partitionRatio ?? signals.partitionPruning.evidence ?? "active",
      active: true,
    });
  }

  const tabletRatio = formatRatio(signals.tabletPruning);
  if (signals.tabletPruning.active) {
    rows.push({
      label: "Tablet Pruning",
      value: tabletRatio ?? signals.tabletPruning.evidence ?? "active",
      active: true,
    });
  }

  if (signals.runtimeFilters) {
    rows.push({
      label: "Runtime Filters",
      value: signals.runtimeFilters,
      active: true,
    });
  }

  const rewriteValue =
    signals.transparentRewrite.level === "hit"
      ? signals.transparentRewrite.chosenMaterializations.join(", ") || "chosen"
      : signals.transparentRewrite.level === "candidate"
        ? (signals.transparentRewrite.indexName ?? "candidate")
        : "not detected";

  if (signals.transparentRewrite.level !== "none") {
    rows.push({
      label: "Transparent Rewrite",
      value: rewriteValue,
      active: true,
    });
  }

  if (rows.length === 0) {
    rows.push({
      label: "Status",
      value: "No optimization signals detected",
      active: false,
    });
  }

  return rows;
}

function MetaRow(props: { label: string; value: string }): JSX.Element {
  const { label, value } = props;
  return (
    <div className="dd-node-detail-grid-row">
      <Text type="secondary" className="dd-node-detail-grid-label">
        {label}
      </Text>
      <Text code className="dd-node-detail-grid-value">
        {value}
      </Text>
    </div>
  );
}

function formatNodeField(value: string | number | null | undefined): string {
  if (value == null) return "-";
  const text = String(value).trim();
  return text || "-";
}

function renderEmptyDetail(): JSX.Element {
  return (
    <div className="dd-node-detail dd-node-detail-empty">
      <Text type="secondary">Select an operator to inspect details.</Text>
    </div>
  );
}

export default function ExplainNodeDetailPanel(props: ExplainNodeDetailPanelProps): JSX.Element {
  const { node, nodeSignalsByKey, onBack } = props;

  if (!node) {
    return renderEmptyDetail();
  }

  const signals = nodeSignalsByKey.get(node.key);
  if (!signals) {
    return renderEmptyDetail();
  }
  const optimizationRows = buildOptimizationSummaryRows(signals);
  const basicRows: Array<{ label: string; value: string }> = [
    {
      label: "Fragment",
      value: formatNodeField(node.fragmentId),
    },
    {
      label: "Node ID",
      value: formatNodeField(node.nodeId),
    },
    {
      label: "Depth",
      value: formatNodeField(node.depth),
    },
    {
      label: "Table",
      value: formatNodeField(node.table),
    },
    {
      label: "Cardinality",
      value: formatNodeField(node.cardinality),
    },
  ];

  if (node.predicates?.trim()) {
    basicRows.push({
      label: "Predicates",
      value: node.predicates.trim(),
    });
  }

  const rawLineItems = [
    {
      key: "raw-plan-line",
      label: <Text type="secondary">Raw Plan Line</Text>,
      extra: <CopyIconButton text={node.rawLine} tooltip="Copy raw line" />,
      children: (
        <pre className="dd-sql-block dd-node-detail-code-block">
          <code>{node.rawLine}</code>
        </pre>
      ),
    },
  ];

  return (
    <div className="dd-node-detail">
      <div className="dd-node-detail-header">
        <div className="dd-node-detail-header-main">
          <Text code className="dd-node-detail-title">
            {node.operator}
          </Text>
          {onBack ? (
            <Button type="link" size="small" className="dd-node-detail-back-btn" onClick={onBack}>
              Back to operators
            </Button>
          ) : null}
        </div>
        <Text type="secondary">
          fragment <Text code>{formatNodeField(node.fragmentId)}</Text> · node{" "}
          <Text code>{formatNodeField(node.nodeId)}</Text>
        </Text>
      </div>

      <div className="dd-node-detail-section">
        <Text type="secondary" className="dd-node-detail-section-title">
          Facts
        </Text>
        <div className="dd-node-detail-grid">
          {basicRows.map((row) => (
            <MetaRow key={`${node.key}-basic-${row.label}`} label={row.label} value={row.value} />
          ))}
        </div>
      </div>

      <div className="dd-node-detail-section">
        <Text type="secondary" className="dd-node-detail-section-title">
          Signals
        </Text>
        <div className="dd-node-detail-grid">
          {optimizationRows.map((row) => (
            <div key={row.label} className="dd-node-detail-grid-row">
              <Text type="secondary" className="dd-node-detail-grid-label">
                {row.label}
              </Text>
              <Text
                className={`dd-node-detail-grid-value ${
                  row.active ? "dd-node-detail-grid-value-active" : ""
                }`}
              >
                {row.value}
              </Text>
            </div>
          ))}
        </div>
      </div>

      <div className="dd-node-detail-section dd-node-detail-section-raw">
        <Collapse ghost size="small" className="dd-node-detail-collapse" items={rawLineItems} />
      </div>
    </div>
  );
}
