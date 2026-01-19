import {
  AimOutlined,
  CompressOutlined,
  MinusOutlined,
  PlusOutlined,
  ReloadOutlined,
} from "@ant-design/icons";
import { Button, Space } from "antd";

export interface ExplainDiagramControlsProps {
  onZoomIn: () => void;
  onZoomOut: () => void;
  onResetZoom: () => void;
  onFitToView: () => void;
  onFocusSelected: () => void;
  canFocusSelected: boolean;
}

export default function ExplainDiagramControls(props: ExplainDiagramControlsProps): JSX.Element {
  const { onZoomIn, onZoomOut, onResetZoom, onFitToView, onFocusSelected, canFocusSelected } =
    props;

  return (
    <div className="dd-diagram-controls">
      <Space direction="vertical" size={6}>
        <Button
          type="default"
          size="small"
          icon={<PlusOutlined />}
          aria-label="Zoom in"
          onClick={onZoomIn}
        />
        <Button
          type="default"
          size="small"
          icon={<MinusOutlined />}
          aria-label="Zoom out"
          onClick={onZoomOut}
        />
        <Button
          type="default"
          size="small"
          icon={<ReloadOutlined />}
          aria-label="Reset zoom"
          onClick={onResetZoom}
        />
        <Button
          type="default"
          size="small"
          icon={<CompressOutlined />}
          aria-label="Fit to view"
          onClick={onFitToView}
        />
        <Button
          type="default"
          size="small"
          icon={<AimOutlined />}
          aria-label="Focus selected"
          onClick={onFocusSelected}
          disabled={!canFocusSelected}
        />
      </Space>
    </div>
  );
}
