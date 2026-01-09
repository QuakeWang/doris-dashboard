import { Button, Card, Col, InputNumber, Row, Select, Space, Table, Typography } from "antd";
import { useMemo } from "react";
import type { ShareRankBy, ShareRow } from "../db/client/protocol";
import {
  formatBytes,
  formatDurationMs,
  formatNumber,
  formatPct,
  formatSqlLabel,
} from "../utils/format";
import AsyncContent from "./AsyncContent";
import CopyIconButton from "./CopyIconButton";
import EChart from "./EChart";

const { Text } = Typography;

type ShareMetric = "cpu" | "time" | "memory";

const TOOLTIP_CSS =
  "max-width: 420px; white-space: normal; word-break: break-word; overflow-wrap: anywhere; line-height: 1.35;";

const clampTooltip = (
  pos: number[] | undefined,
  _params: unknown,
  _dom: unknown,
  _rect: unknown,
  size: any
) => {
  const p = Array.isArray(pos) ? pos : [0, 0];
  const viewW = Number(size?.viewSize?.[0] ?? 0);
  const viewH = Number(size?.viewSize?.[1] ?? 0);
  const boxW = Number(size?.contentSize?.[0] ?? 0);
  const boxH = Number(size?.contentSize?.[1] ?? 0);
  const pad = 8;
  return [
    Math.min(Math.max(p[0], pad), Math.max(pad, viewW - boxW - pad)),
    Math.min(Math.max(p[1], pad), Math.max(pad, viewH - boxH - pad)),
  ];
};

const CHART_TYPE_OPTIONS = [
  { value: "bar", label: "chart: bar" },
  { value: "pie", label: "chart: pie" },
];
const METRIC_OPTIONS = [
  { value: "cpu", label: "cpu" },
  { value: "time", label: "time" },
  { value: "memory", label: "peak_memory_bytes (max)" },
];
const RANK_BY_OPTIONS_CPU_TIME: { value: ShareRankBy; label: string }[] = [
  { value: "totalCpuMs", label: "rank: total_cpu_ms" },
  { value: "totalTimeMs", label: "rank: total_time_ms" },
];
const RANK_BY_OPTIONS_MEMORY: { value: ShareRankBy; label: string }[] = [
  { value: "maxPeakMemBytes", label: "rank: max_peak_mem_bytes" },
];

export interface ShareTabProps {
  datasetId: string | null;
  importing: boolean;
  loading: boolean;
  rows: ShareRow[];
  metric: ShareMetric;
  rankBy: ShareRankBy;
  chartType: "bar" | "pie";
  topN: number;
  onMetricChange: (metric: ShareMetric) => void;
  onRankByChange: (rankBy: ShareRankBy) => void;
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

  const isMemory = metric === "memory";
  const visibleRows = useMemo(
    () => (isMemory ? rows.filter((r) => !r.isOthers) : rows),
    [isMemory, rows]
  );
  const effectiveChartType: "bar" | "pie" = isMemory ? "bar" : chartType;
  const rankByOptions = isMemory ? RANK_BY_OPTIONS_MEMORY : RANK_BY_OPTIONS_CPU_TIME;

  const option = useMemo(() => {
    const formatMetricValue = (v: number) =>
      metric === "memory" ? formatBytes(v) : formatDurationMs(v);

    const formatShareTooltip = (i: number) => {
      const row = visibleRows[i];
      if (!row) return "";
      const title = row.isOthers ? "Others" : formatSqlLabel(row.template, 180);
      if (metric === "memory") {
        return `${title}<br/>max_peak: ${formatBytes(row.maxPeakMemBytes)}<br/>count: ${formatNumber(
          row.execCount
        )}`;
      }

      const share = metric === "cpu" ? row.cpuShare : row.timeShare;
      const total = metric === "cpu" ? row.totalCpuMs : row.totalTimeMs;
      const totalLabel = metric === "cpu" ? "Total CPU" : "Total Time";
      return `${title}<br/>share: ${formatPct(share)}<br/>${totalLabel}: ${formatDurationMs(
        total
      )}<br/>count: ${formatNumber(row.execCount)}`;
    };

    const data = visibleRows.map((r, i) => ({
      name: r.isOthers ? "Others" : `${i + 1}. ${formatSqlLabel(r.template, 56)}`,
      value:
        metric === "cpu"
          ? r.totalCpuMs
          : metric === "time"
            ? r.totalTimeMs
            : (r.maxPeakMemBytes ?? "-"),
    }));

    if (effectiveChartType === "pie") {
      return {
        tooltip: {
          trigger: "item",
          confine: true,
          extraCssText: TOOLTIP_CSS,
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

    return {
      grid: { left: 260, right: 28, top: 10, bottom: 20 },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        confine: true,
        extraCssText: TOOLTIP_CSS,
        position: clampTooltip,
        formatter: (params: any) => {
          const p = Array.isArray(params) ? params[0] : params;
          return formatShareTooltip(Number(p?.dataIndex ?? -1));
        },
      },
      xAxis: {
        type: "value",
        axisLabel: { formatter: (v: unknown) => formatMetricValue(Number(v)) },
      },
      yAxis: {
        type: "category",
        data: data.map((d) => d.name),
        axisLabel: { width: 240, overflow: "truncate" },
      },
      series: [
        {
          type: "bar",
          data: data.map((d) => d.value),
          itemStyle: { borderRadius: 6 },
        },
      ],
    };
  }, [effectiveChartType, metric, visibleRows]);

  const shareChartClick = (params: unknown) => {
    const p = params as any;
    const i = Number(p?.dataIndex ?? -1);
    if (!Number.isFinite(i) || i < 0) return;
    const row = visibleRows[i];
    if (!row || row.isOthers) return;
    onOpenTemplate(row);
  };

  const columns = useMemo(() => {
    const nameWidth = isMemory ? 360 : 320;
    const nameCol = {
      title: "Name",
      dataIndex: "template",
      width: nameWidth,
      render: (v: string, row: ShareRow) =>
        row.isOthers ? (
          "Others"
        ) : (
          <div className="dd-template-cell" style={{ maxWidth: nameWidth }}>
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
    } as const;

    const countCol = {
      title: "Count",
      dataIndex: "execCount",
      width: 100,
      render: (v: number) => formatNumber(v),
    } as const;

    if (isMemory) {
      return [
        nameCol,
        {
          title: "Max Peak Mem",
          width: 140,
          render: (_: unknown, row: ShareRow) => formatBytes(row.maxPeakMemBytes),
        } as const,
        countCol,
      ];
    }

    return [
      nameCol,
      {
        title: metric === "cpu" ? "CPU Share" : "Time Share",
        width: 120,
        render: (_: unknown, row: ShareRow) =>
          formatPct(metric === "cpu" ? row.cpuShare : row.timeShare),
      } as const,
      {
        title: metric === "cpu" ? "Total CPU" : "Total Time",
        width: 130,
        render: (_: unknown, row: ShareRow) =>
          formatDurationMs(metric === "cpu" ? row.totalCpuMs : row.totalTimeMs),
      } as const,
      countCol,
    ];
  }, [isMemory, metric, onOpenTemplate]);

  return (
    <Card
      size="small"
      title={
        metric === "memory"
          ? `TopSQL Peak Mem (Top ${topN})`
          : `TopSQL Share (Top ${topN} + Others)`
      }
      extra={
        <Space>
          <Select
            value={effectiveChartType}
            onChange={(v) => onChartTypeChange(v)}
            style={{ width: 120 }}
            options={CHART_TYPE_OPTIONS}
            disabled={isMemory}
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
            options={rankByOptions}
            disabled={metric === "memory"}
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
      <AsyncContent loading={loading} error={null} isEmpty={visibleRows.length === 0}>
        <Row gutter={12}>
          <Col xs={24} lg={12}>
            <EChart option={option as any} height={420} onClick={shareChartClick} />
          </Col>
          <Col xs={24} lg={12}>
            <Table<ShareRow>
              rowKey={(r) => r.templateHash}
              size="small"
              tableLayout="fixed"
              scroll={{ x: "max-content" }}
              pagination={false}
              columns={columns}
              dataSource={visibleRows}
            />
          </Col>
        </Row>
      </AsyncContent>
    </Card>
  );
}
