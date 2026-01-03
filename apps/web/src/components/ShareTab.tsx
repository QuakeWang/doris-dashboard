import { Button, Card, Col, InputNumber, Row, Select, Space, Table, Typography } from "antd";
import { useMemo } from "react";
import type { ShareRow } from "../db/client/protocol";
import { formatDurationMs, formatNumber, formatPct, formatSqlLabel } from "../utils/format";
import AsyncContent from "./AsyncContent";
import CopyIconButton from "./CopyIconButton";
import EChart from "./EChart";

const { Text } = Typography;

const CHART_TYPE_OPTIONS = [
  { value: "bar", label: "chart: bar" },
  { value: "pie", label: "chart: pie" },
];
const METRIC_OPTIONS = [
  { value: "cpu", label: "cpu" },
  { value: "time", label: "time" },
];
const RANK_BY_OPTIONS = [
  { value: "totalCpuMs", label: "rank: total_cpu_ms" },
  { value: "totalTimeMs", label: "rank: total_time_ms" },
];

export interface ShareTabProps {
  datasetId: string | null;
  importing: boolean;
  loading: boolean;
  rows: ShareRow[];
  metric: "cpu" | "time";
  rankBy: "totalCpuMs" | "totalTimeMs";
  chartType: "bar" | "pie";
  topN: number;
  onMetricChange: (metric: "cpu" | "time") => void;
  onRankByChange: (rankBy: "totalCpuMs" | "totalTimeMs") => void;
  onChartTypeChange: (chartType: "bar" | "pie") => void;
  onTopNChange: (topN: number) => void;
  onRefresh: () => void;
  onOpenTemplate: (row: ShareRow) => void;
}

export default function ShareTab(props: ShareTabProps): JSX.Element {
  const {
    datasetId,
    importing,
    loading,
    rows,
    metric,
    rankBy,
    chartType,
    topN,
    onMetricChange,
    onRankByChange,
    onChartTypeChange,
    onTopNChange,
    onRefresh,
    onOpenTemplate,
  } = props;

  const metricShareLabel = metric === "cpu" ? "CPU Share" : "Time Share";
  const metricTotalLabel = metric === "cpu" ? "Total CPU" : "Total Time";

  const option = useMemo(() => {
    const clampTooltip = (
      _pos: number[] | undefined,
      __: unknown,
      ___: unknown,
      ____: unknown,
      size: any
    ) => {
      const pos = Array.isArray(_pos) ? _pos : [0, 0];
      const viewW = Number(size?.viewSize?.[0] ?? 0);
      const viewH = Number(size?.viewSize?.[1] ?? 0);
      const boxW = Number(size?.contentSize?.[0] ?? 0);
      const boxH = Number(size?.contentSize?.[1] ?? 0);
      const pad = 8;
      return [
        Math.min(Math.max(pos[0], pad), Math.max(pad, viewW - boxW - pad)),
        Math.min(Math.max(pos[1], pad), Math.max(pad, viewH - boxH - pad)),
      ];
    };
    const tooltipCss =
      "max-width: 420px; white-space: normal; word-break: break-word; overflow-wrap: anywhere; line-height: 1.35;";

    const formatShareTooltip = (i: number) => {
      const row = rows[i];
      if (!row) return "";
      const share = metric === "cpu" ? row.cpuShare : row.timeShare;
      const total = metric === "cpu" ? row.totalCpuMs : row.totalTimeMs;
      const title = row.isOthers ? "Others" : formatSqlLabel(row.template, 180);
      return `${title}<br/>share: ${(share * 100).toFixed(2)}%<br/>${metricTotalLabel}: ${formatDurationMs(
        total
      )}<br/>count: ${formatNumber(row.execCount)}`;
    };

    if (chartType === "pie") {
      const data = rows.map((r, i) => ({
        name: r.isOthers ? "Others" : `${i + 1}. ${formatSqlLabel(r.template, 56)}`,
        value: metric === "cpu" ? r.totalCpuMs : r.totalTimeMs,
      }));
      return {
        tooltip: {
          trigger: "item",
          confine: true,
          extraCssText: tooltipCss,
          position: clampTooltip,
          formatter: (p: any) => formatShareTooltip(Number(p?.dataIndex ?? -1)),
        },
        series: [
          {
            type: "pie",
            radius: ["30%", "72%"],
            center: ["50%", "50%"],
            data,
            label: { formatter: "{b}" },
            labelLine: { length: 12, length2: 8 },
          },
        ],
      };
    }

    const yLabels = rows.map((r, i) =>
      r.isOthers ? "Others" : `${i + 1}. ${formatSqlLabel(r.template, 56)}`
    );
    const barValuesMs = rows.map((r) => (metric === "cpu" ? r.totalCpuMs : r.totalTimeMs));

    return {
      grid: { left: 260, right: 28, top: 10, bottom: 20 },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        confine: true,
        extraCssText: tooltipCss,
        position: clampTooltip,
        formatter: (params: any) => {
          const p = Array.isArray(params) ? params[0] : params;
          return formatShareTooltip(Number(p?.dataIndex ?? -1));
        },
      },
      xAxis: {
        type: "value",
        axisLabel: { formatter: (v: unknown) => formatDurationMs(Number(v)) },
      },
      yAxis: {
        type: "category",
        data: yLabels,
        axisLabel: { width: 240, overflow: "truncate" },
      },
      series: [
        {
          type: "bar",
          data: barValuesMs,
          itemStyle: { borderRadius: 6 },
        },
      ],
    };
  }, [chartType, metric, rows]);

  const shareChartClick = (params: unknown) => {
    const p = params as any;
    const i = Number(p?.dataIndex ?? -1);
    if (!Number.isFinite(i) || i < 0) return;
    const row = rows[i];
    if (!row || row.isOthers) return;
    onOpenTemplate(row);
  };

  return (
    <Card
      size="small"
      title={`TopSQL Share (Top ${topN} + Others)`}
      extra={
        <Space>
          <Select
            value={chartType}
            onChange={(v) => onChartTypeChange(v)}
            style={{ width: 120 }}
            options={CHART_TYPE_OPTIONS}
          />
          <Select
            value={metric}
            onChange={(v) => onMetricChange(v)}
            style={{ width: 120 }}
            options={METRIC_OPTIONS}
          />
          <Select
            value={rankBy}
            onChange={(v) => onRankByChange(v)}
            style={{ width: 160 }}
            options={RANK_BY_OPTIONS}
          />
          <InputNumber
            min={1}
            max={200}
            value={topN}
            onChange={(v) => {
              if (typeof v === "number") onTopNChange(v);
            }}
            style={{ width: 96 }}
            addonBefore="top"
          />
          <Button onClick={onRefresh} disabled={!datasetId || importing || loading}>
            Refresh
          </Button>
        </Space>
      }
    >
      <AsyncContent loading={loading} error={null} isEmpty={rows.length === 0}>
        <Row gutter={12}>
          <Col xs={24} lg={12}>
            <EChart option={option as any} height={420} onClick={shareChartClick} />
          </Col>
          <Col xs={24} lg={12}>
            <Table<ShareRow>
              rowKey={(r) => r.templateHash}
              size="small"
              tableLayout="fixed"
              pagination={false}
              columns={[
                {
                  title: "Name",
                  dataIndex: "template",
                  width: 320,
                  render: (v: string, row: ShareRow) =>
                    row.isOthers ? (
                      "Others"
                    ) : (
                      <div className="dd-template-cell" style={{ maxWidth: 320 }}>
                        <Button
                          type="link"
                          className="dd-link-btn dd-template-open"
                          style={{ textAlign: "left" }}
                          title={v}
                          onClick={() => onOpenTemplate(row)}
                        >
                          <Text code className="dd-code-ellipsis">
                            {formatSqlLabel(v, 120)}
                          </Text>
                        </Button>
                        <CopyIconButton
                          className="dd-copy-btn"
                          text={v}
                          tooltip="Copy SQL"
                          ariaLabel="Copy SQL"
                        />
                      </div>
                    ),
                },
                {
                  title: metricShareLabel,
                  render: (_, row: ShareRow) =>
                    formatPct(metric === "cpu" ? row.cpuShare : row.timeShare),
                },
                {
                  title: metricTotalLabel,
                  render: (_, row: ShareRow) =>
                    formatDurationMs(metric === "cpu" ? row.totalCpuMs : row.totalTimeMs),
                },
                {
                  title: "Count",
                  dataIndex: "execCount",
                  render: (v: number) => formatNumber(v),
                },
              ]}
              dataSource={rows}
            />
          </Col>
        </Row>
      </AsyncContent>
    </Card>
  );
}
