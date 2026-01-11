import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

const coopHeaders = {
  "Cross-Origin-Opener-Policy": "same-origin",
  "Cross-Origin-Embedder-Policy": "require-corp",
} as const;

export default defineConfig({
  plugins: [react()],
  server: {
    headers: coopHeaders,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:12306",
        changeOrigin: true,
      },
    },
    port: 12305,
  },
  preview: {
    headers: coopHeaders,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:12306",
        changeOrigin: true,
      },
    },
    port: 12305,
  },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
  },
});
