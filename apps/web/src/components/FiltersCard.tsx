import { ReloadOutlined } from "@ant-design/icons";
import { Button, Card, Col, DatePicker, Form, Input, Row, Space, Switch, Typography } from "antd";
import type { Dayjs } from "dayjs";
import dayjs from "dayjs";
import type { OverviewResult, QueryFilters } from "../db/client/protocol";

const { Text } = Typography;

type RangeValue = [Dayjs | null, Dayjs | null] | null;

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
        <Row gutter={[8, 4]}>
          <Col xs={24} sm={24} md={8}>
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
          <Col xs={24} sm={8} md={4}>
            <Form.Item label="User">
              <Input
                value={draft.userName ?? ""}
                onChange={(e) => onPatchDraft({ userName: e.target.value })}
                placeholder=""
                allowClear
                disabled={importing}
              />
            </Form.Item>
          </Col>
          <Col xs={24} sm={8} md={3}>
            <Form.Item label="DB">
              <Input
                value={draft.dbName ?? ""}
                onChange={(e) => onPatchDraft({ dbName: e.target.value })}
                placeholder=""
                allowClear
                disabled={importing}
              />
            </Form.Item>
          </Col>
          <Col xs={24} sm={8} md={3}>
            <Form.Item label="Client IP">
              <Input
                value={draft.clientIp ?? ""}
                onChange={(e) => onPatchDraft({ clientIp: e.target.value })}
                placeholder=""
                allowClear
                disabled={importing}
              />
            </Form.Item>
          </Col>
          <Col xs={24} sm={24} md={6}>
            <Form.Item label="State">
              <Space style={{ width: "100%" }} align="center">
                <Input
                  value={draft.state ?? ""}
                  onChange={(e) => onPatchDraft({ state: e.target.value })}
                  placeholder="EOF"
                  allowClear
                  disabled={importing}
                  style={{ flex: 1, minWidth: 0 }}
                />
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
              </Space>
            </Form.Item>
          </Col>
        </Row>
      </Form>
    </Card>
  );
}
