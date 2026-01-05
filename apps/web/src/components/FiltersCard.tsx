import { ReloadOutlined } from "@ant-design/icons";
import { Button, Card, Col, DatePicker, Form, Input, Row, Space, Switch, Typography } from "antd";
import type { Dayjs } from "dayjs";
import dayjs from "dayjs";
import type { OverviewResult, QueryFilters } from "../db/client/protocol";

const { Text } = Typography;

type RangeValue = [Dayjs | null, Dayjs | null] | null;

type TextFilterKey = "userName" | "dbName" | "clientIp" | "cloudClusterName" | "state";

const TEXT_FILTERS: Array<[label: string, key: TextFilterKey, placeholder?: string]> = [
  ["User", "userName"],
  ["DB", "dbName"],
  ["Client IP", "clientIp"],
  ["Cluster", "cloudClusterName"],
  ["State", "state", "EOF"],
];

function rangeToFilterValue(range: RangeValue): Pick<QueryFilters, "startMs" | "endMs"> {
  if (!range || !range[0] || !range[1]) return { startMs: undefined, endMs: undefined };
  const startMs = range[0].valueOf();
  const endMs = range[1].valueOf() + 1;
  return { startMs, endMs };
}

function filtersToRangeValue(filters: QueryFilters): RangeValue {
  if (filters.startMs == null || filters.endMs == null) return null;
  const start = dayjs(filters.startMs);
  const end = dayjs(filters.endMs - 1);
  return [start, end];
}

export interface FiltersCardProps {
  datasetId: string | null;
  importing: boolean;
  overview: OverviewResult | null;
  draft: QueryFilters;
  onPatchDraft: (patch: Partial<QueryFilters>) => void;
  onApply: () => void;
  onSetBoth: (next: QueryFilters) => void;
}

export default function FiltersCard(props: FiltersCardProps): JSX.Element {
  const { datasetId, importing, overview, draft, onPatchDraft, onApply, onSetBoth } = props;
  const canResetRange =
    !!datasetId && !importing && overview?.minTimeMs != null && overview?.maxTimeMs != null;

  return (
    <Card
      size="small"
      title="Filters"
      className="dd-filters"
      extra={
        <Space>
          <Space size={6} align="center">
            <Text type="secondary" style={{ whiteSpace: "nowrap" }}>
              Exclude Internal
            </Text>
            <Switch
              checked={draft.excludeInternal ?? true}
              onChange={(checked) => onPatchDraft({ excludeInternal: checked })}
              disabled={importing}
            />
          </Space>
          <Button icon={<ReloadOutlined />} onClick={onApply} disabled={!datasetId || importing}>
            Apply
          </Button>
          <Button
            onClick={() => {
              if (!canResetRange) return;
              onSetBoth({
                ...draft,
                startMs: overview!.minTimeMs!,
                endMs: overview!.maxTimeMs! + 1,
              });
            }}
            disabled={!canResetRange}
          >
            Reset Range
          </Button>
        </Space>
      }
      style={{ marginBottom: 12 }}
    >
      <Form layout="vertical" size="small">
        <Row gutter={[8, 2]}>
          <Col xs={24} sm={24} md={9}>
            <Form.Item label="Time Range">
              <DatePicker.RangePicker
                value={filtersToRangeValue(draft)}
                onChange={(range) => onPatchDraft(rangeToFilterValue(range as RangeValue))}
                showTime
                disabled={importing}
                style={{ width: "100%" }}
              />
            </Form.Item>
          </Col>
          {TEXT_FILTERS.map(([label, key, placeholder]) => (
            <Col xs={24} sm={12} md={3} key={key}>
              <Form.Item label={label}>
                <Input
                  value={draft[key] ?? ""}
                  onChange={(e) => onPatchDraft({ [key]: e.target.value } as Partial<QueryFilters>)}
                  placeholder={placeholder}
                  allowClear
                  disabled={importing}
                />
              </Form.Item>
            </Col>
          ))}
        </Row>
      </Form>
    </Card>
  );
}
