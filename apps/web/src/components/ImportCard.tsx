import { InboxOutlined } from "@ant-design/icons";
import { Button, Card, Col, Row, Space, Statistic, Typography, Upload } from "antd";
import type { ImportProgress } from "../db/client/protocol";
import { formatBytes, formatNumber } from "../utils/format";

const { Text } = Typography;

export interface ImportCardProps {
  ready: boolean;
  datasetId: string | null;
  importing: boolean;
  importProgress: ImportProgress | null;
  onImport: (file: File) => void;
  onCancel: () => void;
}

export default function ImportCard(props: ImportCardProps): JSX.Element {
  const { ready, datasetId, importing, importProgress, onImport, onCancel } = props;
  return (
    <Card
      style={{ marginBottom: 12 }}
      title="Import FE Audit Log"
      size="small"
      extra={
        <Space>
          {importing && (
            <Button danger onClick={onCancel} disabled={!ready}>
              Cancel
            </Button>
          )}
        </Space>
      }
    >
      <Row gutter={12}>
        <Col xs={24} md={12}>
          <Upload.Dragger
            multiple={false}
            maxCount={1}
            showUploadList={false}
            disabled={!ready || !datasetId || importing}
            beforeUpload={(file) => {
              onImport(file as File);
              return false;
            }}
          >
            <p className="ant-upload-drag-icon">
              <InboxOutlined />
            </p>
            <p className="ant-upload-text">Click or drag file to import</p>
            <p className="ant-upload-hint">
              No data is uploaded. Everything runs locally in your browser.
            </p>
          </Upload.Dragger>
        </Col>
        <Col xs={24} md={12}>
          <Space direction="vertical" style={{ width: "100%" }}>
            <Row gutter={12}>
              <Col span={12}>
                <Statistic
                  title="Bytes"
                  value={
                    importProgress
                      ? `${formatBytes(importProgress.bytesRead)} / ${formatBytes(importProgress.bytesTotal)}`
                      : "-"
                  }
                />
              </Col>
              <Col span={12}>
                <Statistic
                  title="Parsed / Inserted / Bad"
                  value={
                    importProgress
                      ? `${formatNumber(importProgress.recordsParsed)} / ${formatNumber(
                          importProgress.recordsInserted
                        )} / ${formatNumber(importProgress.badRecords)}`
                      : "-"
                  }
                />
              </Col>
            </Row>
            {importProgress && (
              <div>
                <Text type="secondary">
                  Progress:{" "}
                  {importProgress.bytesTotal > 0
                    ? `${((importProgress.bytesRead / importProgress.bytesTotal) * 100).toFixed(1)}%`
                    : "-"}
                </Text>
              </div>
            )}
          </Space>
        </Col>
      </Row>
    </Card>
  );
}
