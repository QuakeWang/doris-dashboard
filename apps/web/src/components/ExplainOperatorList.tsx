import { Tag, Typography } from "antd";
import type { ExplainNodeOptimizationSignals } from "../explain/optimizationSignals";
import type { ExplainNode } from "../explain/types";

const { Text } = Typography;

const DEPTH_INDENT_STEP = 12;
const DEPTH_INDENT_MAX = 56;

type OperatorCategory = "scan" | "join" | "agg" | "exchange" | "sink" | "other";

function classifyOperator(node: ExplainNode): OperatorCategory {
  const op = node.operator.toUpperCase();
  if (op.includes("SCAN")) return "scan";
  if (op.includes("JOIN")) return "join";
  if (op.includes("AGG")) return "agg";
  if (op.includes("EXCHANGE")) return "exchange";
  if (op.endsWith("SINK") || op.includes(" SINK")) return "sink";
  return "other";
}

function isFragmentHeader(node: ExplainNode): boolean {
  return node.operator.toUpperCase().startsWith("PLAN FRAGMENT");
}

function isSystemSink(node: ExplainNode): boolean {
  const op = node.operator.toUpperCase().trim();
  if (op === "VRESULT SINK" || op === "RESULT SINK") return true;
  if (op === "DATASTREAMSINK") return true;
  return /STREAM\s+DATA\s+SINK/.test(op);
}

function formatNodeMeta(node: ExplainNode): string {
  if (node.nodeId != null) return `node ${node.nodeId}`;
  return "node -";
}

export interface ExplainOperatorListProps {
  nodes: ExplainNode[];
  selectedNodeKey: string | null;
  onSelectNodeKey: (key: string) => void;
  nodeSignalsByKey: Map<string, ExplainNodeOptimizationSignals>;
}

export default function ExplainOperatorList(props: ExplainOperatorListProps): JSX.Element {
  const { nodes, selectedNodeKey, onSelectNodeKey, nodeSignalsByKey } = props;

  const visibleNodes = nodes.filter((node) => !isFragmentHeader(node) && !isSystemSink(node));

  if (visibleNodes.length === 0) {
    return <Text type="secondary">No high-signal operators in current scope.</Text>;
  }

  return (
    <div className="dd-operator-list-shell">
      <div className="dd-operator-list" aria-label="Operators">
        {visibleNodes.map((node) => {
          const operatorCategory = classifyOperator(node);

          const signals = nodeSignalsByKey.get(node.key);
          const hasPredicatePushdown = !!signals?.predicatePushdown.active;
          const hasPartitionPruning = !!signals?.partitionPruning.active;
          const hasTransparentRewrite = signals?.transparentRewrite.level !== "none";
          const selected = selectedNodeKey === node.key;
          const rowClassName = [
            "dd-operator-list-row",
            selected ? "dd-operator-list-row-selected" : "",
          ]
            .filter(Boolean)
            .join(" ");

          const indentation = Math.min(node.depth * DEPTH_INDENT_STEP, DEPTH_INDENT_MAX);

          return (
            <button
              key={node.key}
              type="button"
              className={rowClassName}
              onClick={() => onSelectNodeKey(node.key)}
            >
              <div className="dd-operator-list-main" style={{ paddingLeft: indentation }}>
                <Text code className="dd-code-ellipsis">
                  {node.operator}
                </Text>
              </div>
              <div className="dd-operator-list-signals">
                <Tag className="dd-operator-list-signal-tag">{operatorCategory.toUpperCase()}</Tag>
                {hasPredicatePushdown ? (
                  <Tag color="processing" className="dd-operator-list-signal-tag">
                    PPD
                  </Tag>
                ) : null}
                {hasPartitionPruning ? (
                  <Tag color="success" className="dd-operator-list-signal-tag">
                    PRUNE
                  </Tag>
                ) : null}
                {hasTransparentRewrite ? (
                  <Tag color="purple" className="dd-operator-list-signal-tag">
                    REWRITE
                  </Tag>
                ) : null}
              </div>
              <div className="dd-operator-list-meta">
                <span>
                  {formatNodeMeta(node)}
                  {node.cardinality ? (
                    <>
                      , card <code>{node.cardinality}</code>
                    </>
                  ) : null}
                </span>
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}
