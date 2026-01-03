import { CopyOutlined } from "@ant-design/icons";
import { Button, Tooltip } from "antd";
import { useEffect, useMemo, useRef, useState } from "react";
import type { MouseEvent } from "react";
import { writeTextToClipboard } from "../utils/clipboard";

type CopyStatus = "idle" | "copied" | "failed";

export interface CopyIconButtonProps {
  text: string;
  className?: string;
  tooltip?: string;
  copiedTooltip?: string;
  failedTooltip?: string;
  ariaLabel?: string;
}

export default function CopyIconButton(props: CopyIconButtonProps): JSX.Element {
  const {
    text,
    className,
    tooltip = "Copy",
    copiedTooltip = "Copied",
    failedTooltip = "Copy failed",
    ariaLabel = tooltip,
  } = props;

  const [status, setStatus] = useState<CopyStatus>("idle");
  const timerRef = useRef<number | null>(null);

  const clearTimer = () => {
    if (timerRef.current == null) return;
    window.clearTimeout(timerRef.current);
    timerRef.current = null;
  };

  useEffect(() => () => clearTimer(), []);
  useEffect(() => {
    setStatus("idle");
    clearTimer();
  }, [text]);

  const tooltipTitle = useMemo(() => {
    if (status === "copied") return copiedTooltip;
    if (status === "failed") return failedTooltip;
    return tooltip;
  }, [copiedTooltip, failedTooltip, status, tooltip]);

  const disabled = !text || !text.trim();

  const onClick = async (e: MouseEvent<HTMLButtonElement>) => {
    e.preventDefault();
    e.stopPropagation();
    if (disabled) return;
    try {
      await writeTextToClipboard(text);
      setStatus("copied");
    } catch {
      setStatus("failed");
    } finally {
      clearTimer();
      timerRef.current = window.setTimeout(() => setStatus("idle"), 1200);
    }
  };

  return (
    <Tooltip title={tooltipTitle}>
      <Button
        type="text"
        size="small"
        icon={<CopyOutlined />}
        className={className}
        aria-label={ariaLabel}
        onClick={onClick}
        disabled={disabled}
      />
    </Tooltip>
  );
}
