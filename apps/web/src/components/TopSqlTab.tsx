import { Alert, Button, Card, Input, Space, Table, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import { useMemo } from "react";
import type { TopSqlRow } from "../db/client/protocol";
import { formatBytes, formatDurationMs, formatNumber } from "../utils/format";
import { compareNullableNumber } from "../utils/sort";
import CopyIconButton from "./CopyIconButton";

const { Text } = Typography;
const TOP_SQL_PAGE_SIZE = 20;

export interface TopSqlTabProps {
  datasetId: string | null;
  importing: boolean;
  loading: boolean;
  error: string | null;
  rows: TopSqlRow[];
  search: string;
  onChangeSearch: (value: string) => void;
  onRefresh: () => void;
  onOpenTemplate: (row: TopSqlRow) => void;
}

export default function TopSqlTab(props: TopSqlTabProps): JSX.Element {
  const {
    datasetId,
    importing,
    loading,
    error,
    rows,
    search,
    onChangeSearch,
    onRefresh,
    onOpenTemplate,
  } = props;

  const columns: ColumnsType<TopSqlRow> = useMemo(
    () => [
      {
        title: "Rank",
        key: "rank",
        width: 70,
        render: (_, __, i) => i + 1,
      },
      {
        title: "Total CPU",
        dataIndex: "totalCpuMs",
        width: 140,
        sorter: (a, b) => a.totalCpuMs - b.totalCpuMs,
        defaultSortOrder: "descend",
        render: (v: number) => formatDurationMs(v),
      },
      {
        title: "Total Time",
        dataIndex: "totalTimeMs",
        width: 140,
        sorter: (a, b) => a.totalTimeMs - b.totalTimeMs,
        render: (v: number) => formatDurationMs(v),
      },
      {
        title: "peak_memory_bytes (max)",
        dataIndex: "maxPeakMemBytes",
        width: 140,
        sorter: (a, b, sortOrder) =>
          compareNullableNumber(a.maxPeakMemBytes, b.maxPeakMemBytes, sortOrder),
        render: (v: number | null) => formatBytes(v),
      },
      {
        title: "Count",
        dataIndex: "execCount",
        width: 100,
        sorter: (a, b) => a.execCount - b.execCount,
        render: (v: number) => formatNumber(v),
      },
      {
        title: "Avg Time",
        dataIndex: "avgTimeMs",
        width: 110,
        sorter: (a, b) => a.avgTimeMs - b.avgTimeMs,
        render: (v: number) => formatDurationMs(v),
      },
      {
        title: "P95 Time",
        dataIndex: "p95TimeMs",
        width: 120,
        sorter: (a, b, sortOrder) => compareNullableNumber(a.p95TimeMs, b.p95TimeMs, sortOrder),
        render: (v: number | null) => formatDurationMs(v),
      },
      {
        title: "Max Time",
        dataIndex: "maxTimeMs",
        width: 120,
        sorter: (a, b) => a.maxTimeMs - b.maxTimeMs,
        render: (v: number) => formatDurationMs(v),
      },
      {
        title: "Table",
        dataIndex: "tableGuess",
        width: 220,
        render: (v: string | null) => v ?? "-",
      },
      {
        title: "Template",
        dataIndex: "template",
        ellipsis: true,
        render: (v: string, row: TopSqlRow) => {
          const compact = v.replace(/\s+/g, " ").trim();
          return (
            <div className="dd-template-cell">
              <Button
                type="link"
                className="dd-link-btn dd-template-open"
                style={{ textAlign: "left" }}
                onClick={() => onOpenTemplate(row)}
              >
                <Text code className="dd-sql-clamp-2" title={compact}>
                  {compact}
                </Text>
              </Button>
              <CopyIconButton
                className="dd-copy-btn"
                text={v}
                tooltip="Copy SQL"
                ariaLabel="Copy SQL"
              />
            </div>
          );
        },
      },
    ],
    [onOpenTemplate]
  );

  const filteredRows = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter((r) => r.template.toLowerCase().includes(q));
  }, [rows, search]);

  return (
    <Card
      size="small"
      title="TopSQL (Top 50, fetched by total_cpu_ms)"
      extra={
        <Space>
          <Input.Search
            allowClear
            placeholder="Search template substring"
            value={search}
            onChange={(e) => onChangeSearch(e.target.value)}
            style={{ width: 320 }}
          />
          <Button onClick={onRefresh} disabled={!datasetId || importing || loading}>
            Refresh
          </Button>
        </Space>
      }
    >
      {error ? (
        <Alert
          type="error"
          message="Query failed"
          description={error}
          showIcon
          style={{ marginBottom: 12 }}
        />
      ) : null}
      <Table<TopSqlRow>
        rowKey={(r) => r.templateHash}
        size="small"
        loading={loading}
        tableLayout="fixed"
        columns={columns}
        dataSource={filteredRows}
        pagination={{ pageSize: TOP_SQL_PAGE_SIZE, showSizeChanger: false }}
      />
    </Card>
  );
}
