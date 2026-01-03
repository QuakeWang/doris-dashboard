import type { ThemeConfig } from "antd";
import { theme } from "antd";

export const CATPPUCCIN_MOCHA = {
  rosewater: "#f5e0dc",
  flamingo: "#f2cdcd",
  pink: "#f5c2e7",
  mauve: "#cba6f7",
  red: "#f38ba8",
  maroon: "#eba0ac",
  peach: "#fab387",
  yellow: "#f9e2af",
  green: "#a6e3a1",
  teal: "#94e2d5",
  sky: "#89dceb",
  sapphire: "#74c7ec",
  blue: "#89b4fa",
  lavender: "#b4befe",
  text: "#cdd6f4",
  subtext1: "#bac2de",
  subtext0: "#a6adc8",
  overlay2: "#9399b2",
  overlay1: "#7f849c",
  overlay0: "#6c7086",
  surface2: "#585b70",
  surface1: "#45475a",
  surface0: "#313244",
  base: "#1e1e2e",
  mantle: "#181825",
  crust: "#11111b",
} as const;

export const MOCHA_ANTD_THEME: ThemeConfig = {
  algorithm: theme.darkAlgorithm,
  token: {
    colorPrimary: CATPPUCCIN_MOCHA.mauve,
    colorInfo: CATPPUCCIN_MOCHA.blue,
    colorSuccess: CATPPUCCIN_MOCHA.green,
    colorWarning: CATPPUCCIN_MOCHA.yellow,
    colorError: CATPPUCCIN_MOCHA.red,

    colorBgBase: CATPPUCCIN_MOCHA.base,
    colorBgLayout: CATPPUCCIN_MOCHA.base,
    colorBgContainer: CATPPUCCIN_MOCHA.surface0,
    colorBgElevated: CATPPUCCIN_MOCHA.surface1,

    colorText: CATPPUCCIN_MOCHA.text,
    colorTextSecondary: CATPPUCCIN_MOCHA.subtext0,
    colorTextTertiary: CATPPUCCIN_MOCHA.overlay0,

    colorBorder: CATPPUCCIN_MOCHA.surface2,
    colorBorderSecondary: CATPPUCCIN_MOCHA.surface1,

    colorLink: CATPPUCCIN_MOCHA.sapphire,
    colorLinkHover: CATPPUCCIN_MOCHA.sky,
    colorLinkActive: CATPPUCCIN_MOCHA.blue,

    borderRadius: 12,
    wireframe: false,
    fontFamily:
      'ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji"',
  },
  components: {
    Layout: {
      headerBg: CATPPUCCIN_MOCHA.mantle,
    },
    Table: {
      headerBg: CATPPUCCIN_MOCHA.surface0,
      headerColor: CATPPUCCIN_MOCHA.text,
      rowHoverBg: CATPPUCCIN_MOCHA.surface1,
    },
    Card: {
      headerBg: CATPPUCCIN_MOCHA.surface0,
    },
    Drawer: {
      colorBgElevated: CATPPUCCIN_MOCHA.base,
    },
  },
};
