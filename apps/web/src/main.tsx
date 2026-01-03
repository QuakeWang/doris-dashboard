import { ConfigProvider } from "antd";
import React from "react";
import ReactDOM from "react-dom/client";
import "antd/dist/reset.css";
import App from "./App";
import "./styles.css";
import { MOCHA_ANTD_THEME } from "./theme/catppuccin";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ConfigProvider theme={MOCHA_ANTD_THEME}>
      <App />
    </ConfigProvider>
  </React.StrictMode>
);
