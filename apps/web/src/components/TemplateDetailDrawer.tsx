import { ReloadOutlined } from "@ant-design/icons";
import {
  Button,
  Card,
  Col,
  Drawer,
  Row,
  Select,
  Space,
  Statistic,
  Table,
  Tabs,
  Typography,
} from "antd";
import type { ColumnsType } from "antd/es/table";
import dayjs from "dayjs";
import { useEffect, useMemo, useState } from "react";
import type { DbClient } from "../db/client/dbClient";
import type {
  DimensionRankBy,
  DimensionTopRow,
  QueryFilters,
  QuerySampleRow,
  SampleOrderBy,
  TemplateSeriesResult,
} from "../db/client/protocol";
import { formatBytes, formatDurationMs, formatNumber, formatSqlLabel } from "../utils/format";
import { compareNullableNumber } from "../utils/sort";
import { useAsyncData } from "../utils/useAsync";
import AsyncContent from "./AsyncContent";
import CopyIconButton from "./CopyIconButton";
import EChart from "./EChart";

const { Text } = Typography;

export interface TemplateRef {
  templateHash: string;
  template: string;
  tableGuess?: string | null;
  execCount: number;
  totalCpuMs: number;
  totalTimeMs: number;
  p95TimeMs?: number | null;
  maxPeakMemBytes: number | null;
}

export interface TemplateDetailDrawerProps {
  open: boolean;
  onClose: () => void;
  client: DbClient;
  datasetId: string | null;
  template: TemplateRef | null;
  filters: QueryFilters;
  onExplainSql?: (sql: string) => void;
}

type SeriesMetric = "execCount" | "totalCpuMs" | "totalTimeMs";
type DimsData = { users: DimensionTopRow[]; dbs: DimensionTopRow[]; ips: DimensionTopRow[] };

const BUCKET_SECONDS_OPTIONS = [
  60, 300, 900, 1800, 3600, 7200, 21600, 43200, 86400, 172800, 604800,
] as const;

const SAMPLES_ORDER_LABEL: Record<SampleOrderBy, string> = {
  queryTimeMs: "query_time_ms",
  cpuTimeMs: "cpu_time_ms",
  peakMemoryBytes: "peak_memory_bytes",
};

function formatBucketLabel(seconds: number): string {
  if (seconds % 86400 === 0) return `${seconds / 86400}d`;
  if (seconds % 3600 === 0) return `${seconds / 3600}h`;
  if (seconds % 60 === 0) return `${seconds / 60}m`;
  return `${seconds}s`;
}

const renderNullableText = (v: string | null) => v ?? "-";

const SAMPLE_COLUMNS: ColumnsType<QuerySampleRow> = [
  {
    title: "Time",
    dataIndex: "eventTimeMs",
    width: 140,
    render: (v: number | null) => (v == null ? "-" : dayjs(v).format("MM-DD HH:mm:ss")),
  },
  {
    title: "Query Time",
    dataIndex: "queryTimeMs",
    width: 110,
    sorter: (a, b, sortOrder) => compareNullableNumber(a.queryTimeMs, b.queryTimeMs, sortOrder),
    render: formatDurationMs,
  },
  {
    title: "CPU",
    dataIndex: "cpuTimeMs",
    width: 90,
    sorter: (a, b, sortOrder) => compareNullableNumber(a.cpuTimeMs, b.cpuTimeMs, sortOrder),
    render: formatDurationMs,
  },
  {
    title: "Peak Mem",
    dataIndex: "peakMemoryBytes",
    width: 110,
    sorter: (a, b, sortOrder) =>
      compareNullableNumber(a.peakMemoryBytes, b.peakMemoryBytes, sortOrder),
    render: formatBytes,
  },
  {
    title: "Scan",
    dataIndex: "scanBytes",
    width: 110,
    render: formatBytes,
  },
  { title: "User", dataIndex: "userName", width: 120, render: renderNullableText },
  { title: "DB", dataIndex: "dbName", width: 120, render: renderNullableText },
  { title: "Client", dataIndex: "clientIp", width: 120, render: renderNullableText },
  { title: "State", dataIndex: "state", width: 90, render: renderNullableText },
  {
    title: "SQL",
    dataIndex: "stmtRaw",
    render: (v: string | null) => (v ? <Text code>{formatSqlLabel(v, 120)}</Text> : "-"),
  },
];

const DIM_COLUMNS: ColumnsType<DimensionTopRow> = [
  {
    title: "Name",
    dataIndex: "name",
    render: (v: string) => <Text code>{v}</Text>,
  },
  { title: "Count", dataIndex: "execCount", width: 90, render: (v: number) => formatNumber(v) },
  { title: "Total CPU", dataIndex: "totalCpuMs", width: 110, render: formatDurationMs },
  { title: "Total Time", dataIndex: "totalTimeMs", width: 110, render: formatDurationMs },
];

export default function TemplateDetailDrawer(props: TemplateDetailDrawerProps): JSX.Element {
  const { open, onClose, client, datasetId, template, filters, onExplainSql } = props;

  const [bucketSeconds, setBucketSeconds] = useState(300);
  const [seriesMetric, setSeriesMetric] = useState<SeriesMetric>("totalCpuMs");

  const [dimRankBy, setDimRankBy] = useState<DimensionRankBy>("totalCpuMs");
  const [samplesOrderBy, setSamplesOrderBy] = useState<SampleOrderBy>("queryTimeMs");
  const enabled = open && !!datasetId && !!template;

  const seriesQuery = useAsyncData<TemplateSeriesResult | null>(
    enabled,
    () => client.queryTemplateSeries(datasetId!, template!.templateHash, bucketSeconds, filters),
    [bucketSeconds, client, datasetId, filters, template],
    null
  );

  const samplesQuery = useAsyncData<QuerySampleRow[]>(
    enabled,
    () => client.querySamples(datasetId!, template!.templateHash, 50, samplesOrderBy, filters),
    [client, datasetId, filters, samplesOrderBy, template],
    []
  );

  const dimsQuery = useAsyncData<DimsData>(
    enabled,
    () => {
      const id = datasetId!;
      const templateHash = template!.templateHash;
      return Promise.all([
        client.queryDimensionTop(id, templateHash, "userName", 10, dimRankBy, filters),
        client.queryDimensionTop(id, templateHash, "dbName", 10, dimRankBy, filters),
        client.queryDimensionTop(id, templateHash, "clientIp", 10, dimRankBy, filters),
      ]).then(([users, dbs, ips]) => ({ users, dbs, ips }));
    },
    [client, datasetId, dimRankBy, filters, template],
    { users: [], dbs: [], ips: [] }
  );

  const reloadAll = () => [seriesQuery, samplesQuery, dimsQuery].forEach((q) => q.reload());

  const { data: series, loading: seriesLoading, error: seriesError } = seriesQuery;
  const { data: samples, loading: samplesLoading, error: samplesError } = samplesQuery;
  const { data: dimsData, loading: dimsLoading, error: dimsError } = dimsQuery;
  const { users: topUsers, dbs: topDbs, ips: topIps } = dimsData;

  const seriesBucketSeconds = series?.bucketSeconds;
  useEffect(() => {
    if (!enabled) return;
    if (!seriesBucketSeconds || seriesBucketSeconds <= 0) return;
    if (seriesBucketSeconds !== bucketSeconds) setBucketSeconds(seriesBucketSeconds);
  }, [bucketSeconds, enabled, seriesBucketSeconds]);

  const seriesOption = useMemo(() => {
    if (!series || series.bucketStarts.length === 0) return null;
    const xLabels = series.bucketStarts.map((t) => dayjs(t).format("MM-DD HH:mm"));
    const values =
      seriesMetric === "execCount"
        ? series.execCounts
        : seriesMetric === "totalTimeMs"
          ? series.totalTimeMs
          : series.totalCpuMs;

    const interval = xLabels.length <= 16 ? 0 : Math.max(0, Math.ceil(xLabels.length / 12) - 1);
    const yFormatter = (v: number) => {
      if (seriesMetric === "execCount") return formatNumber(Math.round(v));
      return formatDurationMs(Math.round(v));
    };

    return {
      tooltip: {
        trigger: "axis",
        formatter: (params: any) => {
          const p = Array.isArray(params) ? params[0] : params;
          const i = Number(p?.dataIndex ?? -1);
          const bucket = series.bucketStarts[i];
          const v = Number(p?.data ?? 0);
          return `bucket: ${bucket}<br/>value: ${yFormatter(v)}`;
        },
      },
      grid: { left: 60, right: 20, top: 20, bottom: 60 },
      xAxis: { type: "category", data: xLabels, axisLabel: { interval, rotate: 45 } },
      yAxis: { type: "value", axisLabel: { formatter: (v: number) => yFormatter(Number(v)) } },
      series: [
        {
          type: "line",
          data: values,
          smooth: true,
          showSymbol: false,
          areaStyle: { opacity: 0.18 },
        },
      ],
    };
  }, [series, seriesMetric]);

  const drawerTitle = template
    ? `Template Detail · ${formatSqlLabel(template.template, 64)}`
    : "Template Detail";

  const samplesTitleOrderLabel = SAMPLES_ORDER_LABEL[samplesOrderBy];

  const sampleColumns: ColumnsType<QuerySampleRow> = useMemo(
    () => [
      ...SAMPLE_COLUMNS,
      {
        title: "Actions",
        key: "actions",
        width: 120,
        fixed: "right",
        render: (_: unknown, r: QuerySampleRow) => (
          <Space size={4}>
            <CopyIconButton text={r.stmtRaw ?? ""} tooltip="Copy SQL" ariaLabel="Copy SQL" />
            <Button
              type="link"
              size="small"
              className="dd-link-btn"
              disabled={!onExplainSql || !r.stmtRaw}
              onClick={() => {
                if (!r.stmtRaw) return;
                onExplainSql?.(r.stmtRaw);
                onClose();
              }}
            >
              Explain
            </Button>
          </Space>
        ),
      },
    ],
    [onClose, onExplainSql]
  );

  return (
    <Drawer title={drawerTitle} width={920} onClose={onClose} open={open}>
      {!template ? (
        <Text type="secondary">No selection</Text>
      ) : (
        <Space direction="vertical" style={{ width: "100%" }} size="middle">
          <Row gutter={12}>
            {[
              {
                title: "Total CPU",
                value: formatDurationMs(template.totalCpuMs),
              },
              {
                title: "Total Time",
                value: formatDurationMs(template.totalTimeMs),
              },
              {
                title: "Count",
                value: formatNumber(template.execCount),
              },
              { title: "P95 Time", value: formatDurationMs(template.p95TimeMs ?? null) },
            ].map((s) => (
              <Col key={s.title} xs={24} sm={12} md={6}>
                <Statistic title={s.title} value={s.value} />
              </Col>
            ))}
          </Row>
          <Row gutter={12}>
            <Col xs={24} sm={12} md={6}>
              <Statistic
                title="Max Peak Mem"
                value={formatBytes(template.maxPeakMemBytes ?? null)}
              />
            </Col>
          </Row>

          <Card
            size="small"
            title="Template"
            extra={
              <Space size={6}>
                <Text type="secondary">
                  hash: <Text code>{template.templateHash}</Text>
                </Text>
                <CopyIconButton text={template.template} tooltip="Copy SQL" ariaLabel="Copy SQL" />
              </Space>
            }
          >
            <pre className="dd-sql-block">
              <code>{template.template}</code>
            </pre>
            {template.tableGuess && (
              <>
                <div style={{ height: 12 }} />
                <Space direction="vertical" size={0}>
                  <Text type="secondary">
                    table_guess: <Text code>{template.tableGuess}</Text>
                  </Text>
                </Space>
              </>
            )}
          </Card>

          <Tabs
            items={[
              {
                key: "trend",
                label: "Trend",
                children: (
                  <Card
                    size="small"
                    title="Time Series"
                    extra={
                      <Space>
                        <Select
                          value={seriesMetric}
                          onChange={(v) => setSeriesMetric(v)}
                          style={{ width: 160 }}
                          options={[
                            { value: "execCount", label: "metric: count" },
                            { value: "totalCpuMs", label: "metric: total_cpu_ms" },
                            { value: "totalTimeMs", label: "metric: total_time_ms" },
                          ]}
                        />
                        <Select
                          value={bucketSeconds}
                          onChange={(v) => setBucketSeconds(v)}
                          style={{ width: 140 }}
                          options={Array.from(
                            new Set<number>([
                              ...BUCKET_SECONDS_OPTIONS,
                              ...(Number.isFinite(bucketSeconds) ? [bucketSeconds] : []),
                            ])
                          )
                            .sort((a, b) => a - b)
                            .map((v) => ({ value: v, label: `bucket: ${formatBucketLabel(v)}` }))}
                        />
                        <Button
                          icon={<ReloadOutlined />}
                          onClick={reloadAll}
                          disabled={!datasetId || seriesLoading}
                        >
                          Refresh
                        </Button>
                      </Space>
                    }
                  >
                    <AsyncContent
                      loading={seriesLoading}
                      error={seriesError}
                      isEmpty={!seriesOption}
                    >
                      <EChart option={seriesOption as any} height={320} />
                    </AsyncContent>
                  </Card>
                ),
              },
              {
                key: "samples",
                label: "Samples",
                children: (
                  <Card
                    size="small"
                    title={`Top Samples (Top 50 by ${samplesTitleOrderLabel})`}
                    extra={
                      <Space>
                        <Select
                          value={samplesOrderBy}
                          onChange={(v) => setSamplesOrderBy(v)}
                          style={{ width: 220 }}
                          options={[
                            { value: "queryTimeMs", label: "order: query_time_ms" },
                            { value: "cpuTimeMs", label: "order: cpu_time_ms" },
                            { value: "peakMemoryBytes", label: "order: peak_memory_bytes" },
                          ]}
                        />
                        <Button
                          icon={<ReloadOutlined />}
                          onClick={() => samplesQuery.reload()}
                          disabled={!datasetId || samplesLoading}
                        >
                          Refresh
                        </Button>
                      </Space>
                    }
                  >
                    <AsyncContent
                      loading={samplesLoading}
                      error={samplesError}
                      isEmpty={samples.length === 0}
                    >
                      <Table<QuerySampleRow>
                        rowKey={(r) => String(r.recordId)}
                        size="small"
                        pagination={{ pageSize: 10 }}
                        columns={sampleColumns}
                        dataSource={samples}
                        scroll={{ x: 1400 }}
                      />
                    </AsyncContent>
                  </Card>
                ),
              },
              {
                key: "dims",
                label: "Dimensions",
                children: (
                  <Card
                    size="small"
                    title="Top Dimensions"
                    extra={
                      <Space>
                        <Select
                          value={dimRankBy}
                          onChange={(v) => setDimRankBy(v)}
                          style={{ width: 180 }}
                          options={[
                            { value: "totalCpuMs", label: "rank: total_cpu_ms" },
                            { value: "totalTimeMs", label: "rank: total_time_ms" },
                            { value: "execCount", label: "rank: count" },
                          ]}
                        />
                      </Space>
                    }
                  >
                    <AsyncContent loading={dimsLoading} error={dimsError} isEmpty={false}>
                      <Tabs
                        items={[
                          { key: "users", title: "Users", rows: topUsers },
                          { key: "dbs", title: "DBs", rows: topDbs },
                          { key: "ips", title: "Client IPs", rows: topIps },
                        ].map((d) => ({
                          key: d.key,
                          label: `${d.title} (${d.rows.length})`,
                          children: (
                            <Table<DimensionTopRow>
                              rowKey={(r) => r.name}
                              size="small"
                              pagination={false}
                              columns={DIM_COLUMNS}
                              dataSource={d.rows}
                            />
                          ),
                        }))}
                      />
                    </AsyncContent>
                  </Card>
                ),
              },
            ]}
          />
        </Space>
      )}
    </Drawer>
  );
}
