import { ReloadOutlined } from "@ant-design/icons";
import { Alert, Button, Card, Col, Row, Space, Statistic, Table, Tabs, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import dayjs from "dayjs";
import type {
  DbTopRow,
  DimensionTopRow,
  OverviewResult,
  QueryFilters,
  TableTopRow,
} from "../db/client/protocol";
import { formatDurationMs, formatNumber, formatPct, truncateMiddle } from "../utils/format";
import AsyncContent from "./AsyncContent";

const { Text } = Typography;

type WorkloadRow = { execCount: number; totalCpuMs: number };

const defaultTableProps = {
  size: "small" as const,
  pagination: false as const,
  showSorterTooltip: true as const,
  scroll: { x: 760 } as const,
};

function makeWorkloadColumns<T extends WorkloadRow>(
  durationSec: number | null,
  totalCpuMs: number
): ColumnsType<T> {
  return [
    {
      title: "QPS",
      width: 90,
      sorter: (a, b) =>
        (durationSec ? a.execCount / durationSec : a.execCount) -
        (durationSec ? b.execCount / durationSec : b.execCount),
      render: (_, r) => (durationSec ? formatNumber(r.execCount / durationSec, 2) : "-"),
    },
    {
      title: "Count",
      dataIndex: "execCount",
      width: 110,
      sorter: (a, b) => a.execCount - b.execCount,
      render: (v: number) => formatNumber(v),
    },
    {
      title: "Total CPU",
      dataIndex: "totalCpuMs",
      width: 120,
      defaultSortOrder: "descend",
      sorter: (a, b) => a.totalCpuMs - b.totalCpuMs,
      render: (v: number) => formatDurationMs(v),
    },
    {
      title: "CPU Share",
      width: 110,
      render: (_, r) => (totalCpuMs > 0 ? formatPct(r.totalCpuMs / totalCpuMs, 2) : "-"),
    },
  ];
}

export interface OverviewTabProps {
  datasetId: string | null;
  importing: boolean;
  loading: boolean;
  error: string | null;
  overview: OverviewResult | null;
  onRefresh: () => void;
  onPatchFilters: (patch: Partial<QueryFilters>) => void;
  onJumpToTopSqlByTable: (tableName: string) => void;
}

export default function OverviewTab(props: OverviewTabProps): JSX.Element {
  const {
    datasetId,
    importing,
    loading,
    error,
    overview,
    onRefresh,
    onPatchFilters,
    onJumpToTopSqlByTable,
  } = props;

  const refreshButton = (
    <Button
      icon={<ReloadOutlined />}
      onClick={onRefresh}
      disabled={!datasetId || importing || loading}
    >
      Refresh
    </Button>
  );

  let content: JSX.Element | null = null;
  if (overview) {
    const startMs = overview.minTimeMs;
    const endMs = overview.maxTimeMs;
    const durationMs =
      startMs != null && endMs != null && Number.isFinite(startMs) && Number.isFinite(endMs)
        ? Math.max(0, endMs - startMs)
        : null;
    const durationSec = durationMs != null && durationMs > 0 ? durationMs / 1000 : null;

    const rangeText =
      startMs != null && endMs != null
        ? `${dayjs(startMs).format("YYYY-MM-DD HH:mm:ss")} → ${dayjs(endMs).format("YYYY-MM-DD HH:mm:ss")}`
        : "-";

    const qps = durationSec ? overview.records / durationSec : null;
    const avgConcurrency =
      durationMs != null && durationMs > 0 ? overview.totalTimeMs / durationMs : null;
    const avgCpuCores =
      durationMs != null && durationMs > 0 ? overview.totalCpuMs / durationMs : null;

    const workloadColumnsDb = makeWorkloadColumns<DbTopRow>(durationSec, overview.totalCpuMs);
    const workloadColumnsTable = makeWorkloadColumns<TableTopRow>(durationSec, overview.totalCpuMs);
    const workloadColumnsDim = makeWorkloadColumns<DimensionTopRow>(
      durationSec,
      overview.totalCpuMs
    );

    const makeDimColumns = (
      title: string,
      onApply: (value: string) => void
    ): ColumnsType<DimensionTopRow> => [
      {
        title,
        dataIndex: "name",
        width: 220,
        render: (v: string) => (
          <Button type="link" className="dd-link-btn" onClick={() => onApply(v)}>
            <Text code title={v}>
              {truncateMiddle(v, 80)}
            </Text>
          </Button>
        ),
      },
      ...workloadColumnsDim,
    ];

    const applyDbFilter = (dbName: string) => onPatchFilters({ dbName });
    const applyUserFilter = (userName: string) => onPatchFilters({ userName });
    const applyClientIpFilter = (clientIp: string) => onPatchFilters({ clientIp });

    content = (
      <Space direction="vertical" size="middle" style={{ width: "100%" }}>
        <Card size="small" title="Workload Summary" className="dd-summary">
          <Text type="secondary" className="dd-summary-range">
            {rangeText}
          </Text>
          <Row gutter={[8, 8]}>
            {[
              { title: "Records", value: formatNumber(overview.records) },
              { title: "QPS", value: qps != null ? formatNumber(qps, 2) : "-" },
              {
                title: "Failure",
                value:
                  overview.records > 0 ? formatPct(overview.failedCount / overview.records) : "-",
              },
              { title: "Duration", value: durationMs != null ? formatDurationMs(durationMs) : "-" },
              { title: "Total CPU", value: formatDurationMs(overview.totalCpuMs) },
              { title: "Total Time", value: formatDurationMs(overview.totalTimeMs) },
              { title: "P95 Time", value: formatDurationMs(overview.p95TimeMs) },
              { title: "P99 Time", value: formatDurationMs(overview.p99TimeMs) },
              {
                title: "Concurrency",
                value: avgConcurrency != null ? formatNumber(avgConcurrency, 2) : "-",
              },
              {
                title: "CPU Cores",
                value: avgCpuCores != null ? formatNumber(avgCpuCores, 2) : "-",
              },
            ].map((r) => (
              <Col key={r.title} xs={12} sm={8} md={6}>
                <Statistic title={r.title} value={r.value} />
              </Col>
            ))}
          </Row>
        </Card>

        <Card size="small" title="Hotspots">
          <Tabs
            items={[
              {
                key: "dbs",
                label: `DBs (${overview.topDbsByCpu.length})`,
                children: (
                  <Card size="small" title="Top DBs">
                    <Table<DbTopRow>
                      {...defaultTableProps}
                      rowKey={(r) => r.dbName}
                      columns={[
                        {
                          title: "DB",
                          dataIndex: "dbName",
                          width: 220,
                          render: (v: string) => (
                            <Button
                              type="link"
                              className="dd-link-btn"
                              onClick={() => applyDbFilter(v)}
                            >
                              <Text code>{v}</Text>
                            </Button>
                          ),
                        },
                        ...workloadColumnsDb,
                      ]}
                      dataSource={overview.topDbsByCpu}
                    />
                  </Card>
                ),
              },
              {
                key: "tables",
                label: `Tables (${overview.topTablesByCpu.length})`,
                children: (
                  <Card size="small" title="Top Tables">
                    <Table<TableTopRow>
                      {...defaultTableProps}
                      rowKey={(r) => `${r.dbName}::${r.tableName}`}
                      columns={[
                        {
                          title: "Table",
                          key: "table",
                          width: 420,
                          render: (_, r) => (
                            <Button
                              type="link"
                              className="dd-link-btn"
                              style={{ maxWidth: 420 }}
                              onClick={() => onJumpToTopSqlByTable(r.tableName)}
                              title={`${r.dbName}.${r.tableName}`}
                            >
                              <Text code className="dd-code-ellipsis">
                                {r.dbName}.{truncateMiddle(r.tableName, 120)}
                              </Text>
                            </Button>
                          ),
                        },
                        ...workloadColumnsTable,
                      ]}
                      dataSource={overview.topTablesByCpu}
                    />
                  </Card>
                ),
              },
              {
                key: "users",
                label: `Users (${overview.topUsersByCpu.length})`,
                children: (
                  <Card size="small" title="Top Users">
                    <Table<DimensionTopRow>
                      {...defaultTableProps}
                      rowKey={(r) => r.name}
                      columns={makeDimColumns("User", applyUserFilter)}
                      dataSource={overview.topUsersByCpu}
                    />
                  </Card>
                ),
              },
              {
                key: "clientIps",
                label: `Client IPs (${overview.topClientIpsByCpu.length})`,
                children: (
                  <Card size="small" title="Top Client IPs">
                    <Table<DimensionTopRow>
                      {...defaultTableProps}
                      rowKey={(r) => r.name}
                      columns={makeDimColumns("Client IP", applyClientIpFilter)}
                      dataSource={overview.topClientIpsByCpu}
                    />
                  </Card>
                ),
              },
            ]}
          />
        </Card>
      </Space>
    );
  }

  return (
    <Card size="small" title="Overview" extra={refreshButton}>
      {error ? (
        <Alert
          type="error"
          message="Query failed"
          description={error}
          showIcon
          style={{ marginBottom: 12 }}
        />
      ) : null}
      <AsyncContent loading={loading} error={null} isEmpty={!overview}>
        {content}
      </AsyncContent>
    </Card>
  );
}
