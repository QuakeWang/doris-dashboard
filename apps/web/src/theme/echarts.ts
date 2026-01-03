import { CATPPUCCIN_MOCHA } from "./catppuccin";

export const CATPPUCCIN_ECHARTS_THEME_NAME = "catppuccin-mocha";

export const CATPPUCCIN_ECHARTS_THEME = {
  color: [
    CATPPUCCIN_MOCHA.mauve,
    CATPPUCCIN_MOCHA.blue,
    CATPPUCCIN_MOCHA.sapphire,
    CATPPUCCIN_MOCHA.green,
    CATPPUCCIN_MOCHA.peach,
    CATPPUCCIN_MOCHA.red,
    CATPPUCCIN_MOCHA.pink,
    CATPPUCCIN_MOCHA.teal,
    CATPPUCCIN_MOCHA.yellow,
    CATPPUCCIN_MOCHA.lavender,
  ],
  backgroundColor: "transparent",
  textStyle: {
    color: CATPPUCCIN_MOCHA.text,
  },
  title: {
    textStyle: { color: CATPPUCCIN_MOCHA.text },
    subtextStyle: { color: CATPPUCCIN_MOCHA.subtext0 },
  },
  legend: {
    textStyle: { color: CATPPUCCIN_MOCHA.subtext0 },
  },
  tooltip: {
    backgroundColor: CATPPUCCIN_MOCHA.surface0,
    borderColor: CATPPUCCIN_MOCHA.surface2,
    textStyle: { color: CATPPUCCIN_MOCHA.text },
  },
  axisPointer: {
    lineStyle: { color: CATPPUCCIN_MOCHA.overlay0 },
    crossStyle: { color: CATPPUCCIN_MOCHA.overlay0 },
    label: { backgroundColor: CATPPUCCIN_MOCHA.surface0 },
  },
  categoryAxis: {
    axisLine: { lineStyle: { color: CATPPUCCIN_MOCHA.surface2 } },
    axisTick: { lineStyle: { color: CATPPUCCIN_MOCHA.surface2 } },
    axisLabel: { color: CATPPUCCIN_MOCHA.subtext0 },
    splitLine: { lineStyle: { color: CATPPUCCIN_MOCHA.surface1 } },
    splitArea: { areaStyle: { color: ["transparent"] } },
  },
  valueAxis: {
    axisLine: { lineStyle: { color: CATPPUCCIN_MOCHA.surface2 } },
    axisTick: { lineStyle: { color: CATPPUCCIN_MOCHA.surface2 } },
    axisLabel: { color: CATPPUCCIN_MOCHA.subtext0 },
    splitLine: { lineStyle: { color: CATPPUCCIN_MOCHA.surface1 } },
    splitArea: { areaStyle: { color: ["transparent"] } },
  },
  visualMap: {
    textStyle: { color: CATPPUCCIN_MOCHA.subtext0 },
  },
} as const;
