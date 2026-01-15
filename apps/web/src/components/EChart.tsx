import * as echarts from "echarts";
import { useCallback, useEffect, useRef } from "react";
import { CATPPUCCIN_ECHARTS_THEME, CATPPUCCIN_ECHARTS_THEME_NAME } from "../theme/echarts";

let themeRegistered = false;

export interface EChartProps {
  option: echarts.EChartsOption;
  height?: number;
  onClick?: (params: unknown) => void;
}

export default function EChart({ option, height = 360, onClick }: EChartProps): JSX.Element {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<echarts.ECharts | null>(null);

  const onClickRef = useRef<EChartProps["onClick"]>(undefined);
  onClickRef.current = onClick;

  const stableClickHandler = useCallback((params: unknown) => {
    const cb = onClickRef.current;
    if (cb) cb(params);
  }, []);

  useEffect(() => {
    if (!containerRef.current) return;
    if (!themeRegistered) {
      echarts.registerTheme(CATPPUCCIN_ECHARTS_THEME_NAME, CATPPUCCIN_ECHARTS_THEME as any);
      themeRegistered = true;
    }
    const chart = echarts.init(containerRef.current, CATPPUCCIN_ECHARTS_THEME_NAME);
    chartRef.current = chart;

    const ro = new ResizeObserver(() => chart.resize());
    ro.observe(containerRef.current);

    chart.on("click", stableClickHandler);

    return () => {
      chart.off("click", stableClickHandler);
      ro.disconnect();
      chart.dispose();
      chartRef.current = null;
    };
  }, [stableClickHandler]);

  useEffect(() => {
    chartRef.current?.setOption(option, { notMerge: true, lazyUpdate: true });
  }, [option]);

  return <div ref={containerRef} style={{ width: "100%", height }} />;
}
