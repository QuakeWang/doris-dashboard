import { ReloadOutlined } from "@ant-design/icons";
import {
  Alert,
  Button,
  Card,
  Col,
  Descriptions,
  Drawer,
  Input,
  Row,
  Space,
  Statistic,
  Table,
  Tag,
  Typography,
} from "antd";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
  AgentClient,
  DorisConnectionInput,
  SchemaAuditFinding,
  SchemaAuditScanItem,
  SchemaAuditScanResult,
  SchemaAuditTableDetailResult,
} from "../agent/agentClient";
import {
  buildSchemaAuditConnectionKey,
  formatSchemaAuditEvidence,
  splitSchemaAuditFindings,
} from "./schemaAuditWorkspaceHelpers";

const { Paragraph, Text } = Typography;
const SCAN_DEFAULT_PAGE = 1;
const SCAN_DEFAULT_PAGE_SIZE = 50;
const SCAN_MAX_PAGE_SIZE = 200;
const SCAN_PAGE_SIZE_OPTIONS = ["20", "50", "100", "200"];
const DETAIL_DEFAULT_PAGE_SIZE = 10;
const DETAIL_PAGE_SIZE_OPTIONS = ["10", "20", "50", "100"];
const DETAIL_TABLE_PAGINATION = {
  defaultPageSize: DETAIL_DEFAULT_PAGE_SIZE,
  showSizeChanger: true,
  pageSizeOptions: DETAIL_PAGE_SIZE_OPTIONS,
};

export interface SchemaAuditWorkspaceProps {
  agent: AgentClient;
  dorisConn: DorisConnectionInput | null;
  dorisConfigured: boolean;
  onOpenDoris: () => void;
}

type TableIdentity = {
  database: string;
  table: string;
};

function findingColor(severity: string): "red" | "orange" | "blue" {
  switch (severity.toLowerCase()) {
    case "critical":
      return "red";
    case "warn":
      return "orange";
    default:
      return "blue";
  }
}

function formatPercent(v: number): string {
  return `${(v * 100).toFixed(1)}%`;
}

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let value = bytes;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex++;
  }
  return `${value.toFixed(value >= 10 || unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
}

function renderSchemaAuditFindingCards(
  findings: SchemaAuditFinding[],
  keyPrefix: string,
  options?: {
    emptyText?: string;
    contentSpacing?: number;
  }
): JSX.Element {
  if (findings.length === 0) {
    if (options?.emptyText) {
      return <Text type="secondary">{options.emptyText}</Text>;
    }
    return <></>;
  }

  const contentSpacing = options?.contentSpacing ?? 6;
  return (
    <Space direction="vertical" size={8} style={{ width: "100%" }}>
      {findings.map((finding, index) => (
        <Card key={`${keyPrefix}-${finding.ruleId}-${index}`} size="small">
          <Space direction="vertical" size={contentSpacing} style={{ width: "100%" }}>
            <Space wrap>
              <Tag color={findingColor(finding.severity)}>{finding.severity}</Tag>
              <Text strong>{finding.ruleId}</Text>
            </Space>
            <Text>{finding.summary}</Text>
            {finding.recommendation ? <Text type="secondary">{finding.recommendation}</Text> : null}
            <Paragraph
              copyable
              style={{
                marginBottom: 0,
                whiteSpace: "pre-wrap",
                fontFamily: "Menlo, Consolas, monospace",
              }}
            >
              {formatSchemaAuditEvidence(finding.evidence)}
            </Paragraph>
          </Space>
        </Card>
      ))}
    </Space>
  );
}

export default function SchemaAuditWorkspace(props: SchemaAuditWorkspaceProps): JSX.Element {
  const { agent, dorisConn, dorisConfigured, onOpenDoris } = props;

  const configuredConn = useMemo(() => {
    if (!dorisConfigured || !dorisConn) return null;
    return {
      host: dorisConn.host,
      port: dorisConn.port,
      user: dorisConn.user,
      password: dorisConn.password,
      database: dorisConn.database,
    } satisfies DorisConnectionInput;
  }, [
    dorisConfigured,
    dorisConn?.host,
    dorisConn?.port,
    dorisConn?.user,
    dorisConn?.password,
    dorisConn?.database,
  ]);
  const [databaseFilter, setDatabaseFilter] = useState("");
  const [tableLike, setTableLike] = useState("");
  const [scanLoading, setScanLoading] = useState(false);
  const [scanError, setScanError] = useState<string | null>(null);
  const [scan, setScan] = useState<SchemaAuditScanResult | null>(null);

  const [selected, setSelected] = useState<TableIdentity | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [detail, setDetail] = useState<SchemaAuditTableDetailResult | null>(null);
  const scanRequestIDRef = useRef(0);
  const detailRequestIDRef = useRef(0);
  const bootstrappedConnectionKeyRef = useRef("");
  const scanAbortRef = useRef<AbortController | null>(null);
  const detailAbortRef = useRef<AbortController | null>(null);

  const connectionKey = useMemo(() => {
    return buildSchemaAuditConnectionKey(configuredConn);
  }, [configuredConn]);

  const runScan = useCallback(
    async (
      database: string,
      tablePattern: string,
      page = SCAN_DEFAULT_PAGE,
      pageSize = SCAN_DEFAULT_PAGE_SIZE
    ) => {
      if (!configuredConn) return;
      scanAbortRef.current?.abort();
      const controller = new AbortController();
      scanAbortRef.current = controller;
      const requestID = scanRequestIDRef.current + 1;
      scanRequestIDRef.current = requestID;
      const normalizedPage =
        Number.isFinite(page) && page > 0 ? Math.floor(page) : SCAN_DEFAULT_PAGE;
      const normalizedPageSize =
        Number.isFinite(pageSize) && pageSize > 0
          ? Math.min(Math.floor(pageSize), SCAN_MAX_PAGE_SIZE)
          : SCAN_DEFAULT_PAGE_SIZE;
      setScanLoading(true);
      setScanError(null);
      try {
        const result = await agent.schemaAuditScan(
          {
            connection: configuredConn,
            database: database.trim() || undefined,
            tableLike: tablePattern.trim() || undefined,
            page: normalizedPage,
            pageSize: normalizedPageSize,
          },
          controller.signal
        );
        if (requestID !== scanRequestIDRef.current) return;
        setScan(result);
      } catch (e) {
        if (controller.signal.aborted) return;
        if (requestID !== scanRequestIDRef.current) return;
        setScanError(e instanceof Error ? e.message : String(e));
      } finally {
        if (scanAbortRef.current === controller) {
          scanAbortRef.current = null;
        }
        if (requestID === scanRequestIDRef.current) {
          setScanLoading(false);
        }
      }
    },
    [agent, configuredConn]
  );

  const loadTableDetail = useCallback(
    async (target: TableIdentity) => {
      if (!configuredConn) return;
      detailAbortRef.current?.abort();
      const controller = new AbortController();
      detailAbortRef.current = controller;
      const requestID = detailRequestIDRef.current + 1;
      detailRequestIDRef.current = requestID;
      setSelected(target);
      setDetail(null);
      setDetailError(null);
      setDetailLoading(true);
      try {
        const result = await agent.schemaAuditTableDetail(
          {
            connection: configuredConn,
            database: target.database,
            table: target.table,
          },
          controller.signal
        );
        if (requestID !== detailRequestIDRef.current) return;
        setDetail(result);
      } catch (e) {
        if (controller.signal.aborted) return;
        if (requestID !== detailRequestIDRef.current) return;
        setDetailError(e instanceof Error ? e.message : String(e));
      } finally {
        if (detailAbortRef.current === controller) {
          detailAbortRef.current = null;
        }
        if (requestID === detailRequestIDRef.current) {
          setDetailLoading(false);
        }
      }
    },
    [agent, configuredConn]
  );

  useEffect(() => {
    if (!configuredConn) {
      scanAbortRef.current?.abort();
      detailAbortRef.current?.abort();
      bootstrappedConnectionKeyRef.current = "";
      setScan(null);
      setSelected(null);
      setDetail(null);
      setScanError(null);
      setDetailError(null);
      return;
    }

    if (!connectionKey || bootstrappedConnectionKeyRef.current === connectionKey) {
      return;
    }
    bootstrappedConnectionKeyRef.current = connectionKey;

    const initialDatabase = configuredConn.database?.trim() ?? "";
    detailAbortRef.current?.abort();
    setSelected(null);
    setDetail(null);
    setDetailError(null);
    setDatabaseFilter(initialDatabase);
    setTableLike("");
    void runScan(initialDatabase, "", SCAN_DEFAULT_PAGE, SCAN_DEFAULT_PAGE_SIZE);
  }, [connectionKey, configuredConn, runScan]);

  useEffect(
    () => () => {
      scanAbortRef.current?.abort();
      detailAbortRef.current?.abort();
    },
    []
  );

  if (!configuredConn) {
    return (
      <Card title="Schema Audit">
        <Space direction="vertical" size={12}>
          <Text type="secondary">
            Schema audit requires Doris connection. Configure connection first, then run scan.
          </Text>
          <Button type="primary" onClick={onOpenDoris}>
            Configure Doris Connection
          </Button>
        </Space>
      </Card>
    );
  }

  const rows: SchemaAuditScanItem[] = scan?.items ?? [];
  const currentScanPage = scan?.page ?? SCAN_DEFAULT_PAGE;
  const currentScanPageSize = scan?.pageSize ?? SCAN_DEFAULT_PAGE_SIZE;
  const currentScanTotalItems = scan?.totalItems ?? rows.length;
  const { bucket: bucketFindings, nonBucket: nonBucketFindings } = splitSchemaAuditFindings(
    detail?.findings
  );

  return (
    <Space direction="vertical" size="middle" style={{ width: "100%" }}>
      <Card title="Schema Audit Filters" size="small">
        <Space wrap>
          <Input
            value={databaseFilter}
            onChange={(e) => setDatabaseFilter(e.target.value)}
            placeholder="Database (optional)"
            style={{ width: 220 }}
          />
          <Input
            value={tableLike}
            onChange={(e) => setTableLike(e.target.value)}
            placeholder="Table like (optional)"
            style={{ width: 260 }}
          />
          <Button
            type="primary"
            onClick={() =>
              void runScan(databaseFilter, tableLike, SCAN_DEFAULT_PAGE, currentScanPageSize)
            }
            loading={scanLoading}
          >
            Run Scan
          </Button>
          <Button
            icon={<ReloadOutlined />}
            onClick={() => {
              setDatabaseFilter("");
              setTableLike("");
              void runScan("", "", SCAN_DEFAULT_PAGE, currentScanPageSize);
            }}
            disabled={scanLoading}
          >
            Reset
          </Button>
        </Space>
      </Card>

      {scanError ? (
        <Alert
          type="error"
          message="Schema Audit Scan Error"
          description={<Text style={{ whiteSpace: "pre-wrap" }}>{scanError}</Text>}
          showIcon
        />
      ) : null}

      {scan?.truncated ? (
        <Alert
          type="warning"
          message="Schema Audit Result Truncated"
          description={
            <Text style={{ whiteSpace: "pre-wrap" }}>
              {scan.warning ??
                `Result is truncated to first ${scan.scanLimit} tables for this request.`}
            </Text>
          }
          showIcon
        />
      ) : null}

      {scan?.inventory ? (
        <Card size="small" title="Inventory">
          <Row gutter={[16, 16]}>
            <Col xs={24} sm={12} md={8} lg={6}>
              <Statistic title="Databases" value={scan.inventory.databaseCount} />
            </Col>
            <Col xs={24} sm={12} md={8} lg={6}>
              <Statistic title="Tables" value={scan.inventory.tableCount} />
            </Col>
            <Col xs={24} sm={12} md={8} lg={6}>
              <Statistic title="Total Partitions" value={scan.inventory.totalPartitionCount} />
            </Col>
            <Col xs={24} sm={12} md={8} lg={6}>
              <Statistic
                title="Empty Partition Ratio"
                value={formatPercent(scan.inventory.emptyPartitionRatio)}
              />
            </Col>
          </Row>
        </Card>
      ) : null}

      <Card size="small" title="Tables">
        <Table<SchemaAuditScanItem>
          rowKey={(row) => `${row.database}.${row.table}`}
          dataSource={rows}
          loading={scanLoading}
          pagination={{
            current: currentScanPage,
            pageSize: currentScanPageSize,
            total: currentScanTotalItems,
            showSizeChanger: true,
            pageSizeOptions: SCAN_PAGE_SIZE_OPTIONS,
            showTotal: (total, range) => `${range[0]}-${range[1]} of ${total}`,
            onChange: (page, pageSize) => {
              const requestedPageSize =
                typeof pageSize === "number" ? pageSize : currentScanPageSize;
              const nextPage = requestedPageSize !== currentScanPageSize ? SCAN_DEFAULT_PAGE : page;
              void runScan(databaseFilter, tableLike, nextPage, requestedPageSize);
            },
          }}
          onRow={(row) => ({
            onClick: () => void loadTableDetail({ database: row.database, table: row.table }),
            style: { cursor: "pointer" },
          })}
          columns={[
            { title: "Database", dataIndex: "database", width: 200 },
            { title: "Table", dataIndex: "table", width: 280 },
            {
              title: "Partitions",
              key: "partitions",
              render: (_, row) =>
                `${row.emptyPartitionCount}/${row.partitionCount} (${formatPercent(row.emptyPartitionRatio)})`,
            },
            {
              title: "Dynamic",
              key: "dynamic",
              width: 110,
              render: (_, row) =>
                row.dynamicPartitionEnabled ? <Tag color="blue">Enabled</Tag> : <Tag>Disabled</Tag>,
            },
            {
              title: "Findings",
              key: "findings",
              render: (_, row) =>
                row.findings.length === 0 ? (
                  <Text type="secondary">None</Text>
                ) : (
                  <Space size={[4, 4]} wrap>
                    {row.findings.slice(0, 3).map((finding) => (
                      <Tag
                        key={`${row.database}.${row.table}.${finding.ruleId}`}
                        color={findingColor(finding.severity)}
                      >
                        {finding.ruleId}
                      </Tag>
                    ))}
                    {row.findings.length > 3 ? (
                      <Text type="secondary">+{row.findings.length - 3}</Text>
                    ) : null}
                  </Space>
                ),
            },
            {
              title: "Risk Score",
              dataIndex: "score",
              width: 100,
            },
          ]}
        />
      </Card>

      <Drawer
        open={selected != null}
        onClose={() => {
          detailAbortRef.current?.abort();
          setSelected(null);
          setDetail(null);
          setDetailError(null);
        }}
        width={960}
        title={selected ? `Table Detail: ${selected.database}.${selected.table}` : "Table Detail"}
      >
        {detailError ? (
          <Alert
            type="error"
            message="Schema Audit Detail Error"
            description={<Text style={{ whiteSpace: "pre-wrap" }}>{detailError}</Text>}
            showIcon
            style={{ marginBottom: 12 }}
          />
        ) : null}

        {detailLoading ? <Text type="secondary">Loading table detail...</Text> : null}

        {!detailLoading && detail ? (
          <Space direction="vertical" size="middle" style={{ width: "100%" }}>
            {bucketFindings.length > 0 ? (
              <Card size="small" title="Bucket Findings">
                {renderSchemaAuditFindingCards(bucketFindings, "bucket", { contentSpacing: 6 })}
              </Card>
            ) : null}

            <Card size="small" title="Summary">
              <Descriptions bordered column={2} size="small">
                <Descriptions.Item label="Database">{detail.database}</Descriptions.Item>
                <Descriptions.Item label="Table">{detail.table}</Descriptions.Item>
                <Descriptions.Item label="Partitions">{detail.partitions.length}</Descriptions.Item>
                <Descriptions.Item label="Indexes">{detail.indexes.length}</Descriptions.Item>
                <Descriptions.Item label="Findings">{detail.findings.length}</Descriptions.Item>
                <Descriptions.Item label="Dynamic Properties">
                  {Object.keys(detail.dynamicProperties).length}
                </Descriptions.Item>
              </Descriptions>
            </Card>

            <Card size="small" title="Findings">
              {renderSchemaAuditFindingCards(nonBucketFindings, "non-bucket", {
                emptyText: "No non-bucket finding detected for current rules.",
                contentSpacing: 4,
              })}
            </Card>

            <Card size="small" title="Partitions">
              <Table
                rowKey={(row) => row.name}
                size="small"
                pagination={DETAIL_TABLE_PAGINATION}
                dataSource={detail.partitions}
                columns={[
                  { title: "Name", dataIndex: "name" },
                  { title: "Rows", dataIndex: "rows", width: 140 },
                  {
                    title: "Data Size",
                    key: "dataSizeBytes",
                    width: 140,
                    render: (_, row) => formatBytes(row.dataSizeBytes),
                  },
                  { title: "Buckets", dataIndex: "buckets", width: 120 },
                  {
                    title: "Empty",
                    dataIndex: "empty",
                    width: 100,
                    render: (value: boolean) =>
                      value ? <Tag color="orange">Yes</Tag> : <Tag>No</Tag>,
                  },
                ]}
              />
            </Card>

            <Card size="small" title="Indexes">
              <Table
                rowKey={(row) => `${row.name}-${row.columns.join(",")}`}
                size="small"
                pagination={DETAIL_TABLE_PAGINATION}
                dataSource={detail.indexes}
                columns={[
                  { title: "Name", dataIndex: "name" },
                  { title: "Type", dataIndex: "indexType", width: 160 },
                  {
                    title: "Columns",
                    key: "columns",
                    render: (_, row) => row.columns.join(", "),
                  },
                ]}
              />
            </Card>

            <Card size="small" title="Create Table">
              <Paragraph copyable style={{ marginBottom: 0, whiteSpace: "pre-wrap" }}>
                {detail.createTableSql}
              </Paragraph>
            </Card>
          </Space>
        ) : null}
      </Drawer>
    </Space>
  );
}
