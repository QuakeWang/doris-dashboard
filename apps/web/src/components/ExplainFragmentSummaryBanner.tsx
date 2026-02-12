import { Space, Typography } from "antd";
import type { FragmentGraphNode } from "../explain/fragmentGraph";
import type { ExplainFragmentOptimizationSignals } from "../explain/optimizationSignals";

const { Text } = Typography;

function formatNumber(value: number | null): string {
  if (value == null) return "-";
  return value.toLocaleString("en-US");
}

function shortenText(value: string, maxLength: number): string {
  if (value.length <= maxLength) return value;
  return `${value.slice(0, maxLength)}...`;
}

function SummaryItem(props: { label: string; value: string; title?: string }): JSX.Element {
  const { label, value, title } = props;
  return (
    <Text type="secondary" className="dd-fragment-summary-item" title={title ?? value}>
      {label}: <Text code>{value}</Text>
    </Text>
  );
}

export interface ExplainFragmentSummaryBannerProps {
  summary: FragmentGraphNode;
  flow?: {
    upstreamFragments: number;
    downstreamFragments: number;
  } | null;
  hasUnknownColumnStats?: boolean;
  optimizationSignals?: ExplainFragmentOptimizationSignals | null;
}

export default function ExplainFragmentSummaryBanner(
  props: ExplainFragmentSummaryBannerProps
): JSX.Element {
  const { summary, flow = null, hasUnknownColumnStats = false, optimizationSignals = null } = props;

  const rootOperatorRaw = summary.rootOperator ?? "-";
  const rootOperator = shortenText(rootOperatorRaw, 42);
  const flowText = flow ? `u${flow.upstreamFragments}/d${flow.downstreamFragments}` : "u-/d-";
  const exchangeText = `${summary.consumerExchangeIds.length}/${summary.producerExchangeIds.length}`;
  const shapeText = `${summary.nodeCount}/${summary.joinCount}/${summary.scanCount}`;
  const optimizationText = `${optimizationSignals?.predicatePushdownCount ?? 0}/${optimizationSignals?.pruningCount ?? 0}/${optimizationSignals?.rewriteCount ?? 0}`;
  const unknownStatsText = hasUnknownColumnStats ? "unknown" : "normal";

  const tablePreview =
    summary.tables.length === 0
      ? "-"
      : summary.tables.length <= 2
        ? summary.tables.join(", ")
        : `${summary.tables.slice(0, 2).join(", ")} +${summary.tables.length - 2}`;

  const partitionLabel = summary.partition ?? "-";
  const secondaryItems: Array<{ label: string; value: string; title?: string }> = [];

  if (tablePreview !== "-") {
    secondaryItems.push({ label: "tables", value: tablePreview, title: summary.tables.join(", ") });
  }
  if (partitionLabel !== "-") {
    secondaryItems.push({ label: "partition", value: partitionLabel });
  }
  if (summary.hasColocatePlanNode != null) {
    secondaryItems.push({ label: "colo", value: String(summary.hasColocatePlanNode) });
  }

  return (
    <div className="dd-fragment-detail-summary">
      <div className="dd-fragment-summary-primary">
        <Space size={8} wrap>
          <SummaryItem label="fragment" value={String(summary.fragmentId)} />
          <SummaryItem label="root" value={rootOperator} title={rootOperatorRaw} />
          <SummaryItem label="flow" value={flowText} />
          <SummaryItem label="ex in/out" value={exchangeText} />
          <SummaryItem label="shape n/j/s" value={shapeText} />
          <SummaryItem label="opt ppd/prune/rw" value={optimizationText} />
          <SummaryItem label="rf" value={String(summary.runtimeFilterCount)} />
          <SummaryItem label="stats" value={unknownStatsText} />
          <SummaryItem label="max card" value={formatNumber(summary.maxCardinality)} />
        </Space>
      </div>

      {secondaryItems.length > 0 ? (
        <div className="dd-fragment-summary-secondary">
          <Space size={8} wrap>
            {secondaryItems.map((item) => (
              <SummaryItem
                key={item.label}
                label={item.label}
                value={item.value}
                title={item.title}
              />
            ))}
          </Space>
        </div>
      ) : null}
    </div>
  );
}
